use {
    crate::{bam_dependencies::BamConnectionState, tonic_endpoint::endpoint_from_url},
    arc_swap::ArcSwap,
    chrono::{DateTime, Utc},
    futures::{StreamExt, stream},
    jito_protos::proto::bam_api::{ConfigRequest, bam_node_api_client::BamNodeApiClient},
    rand::{Rng, rng, seq::IndexedRandom},
    reqwest::Url,
    serde::{Deserialize, Deserializer},
    solana_metrics::{datapoint_info, datapoint_warn},
    std::{
        net::{IpAddr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicU8, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::{runtime, task::JoinHandle, time::timeout},
};

const RESYNC_INTERVAL_LIVE: Duration = Duration::from_secs(60);

const RESYNC_INTERVAL_STALLED: Duration = Duration::from_secs(2);

// Must outlast the worst-case BamManager connection attempt of 16 seconds.
const CONNECT_GRACE: Duration = Duration::from_secs(20);

const PROBE_CAP: usize = 32;

const PROBE_FANOUT: usize = 16;

const PROBE_SAMPLES: usize = 3;

const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

// BAM nodes disconnect validators whose mean RTT exceeds this.
const MAX_NODE_RTT: Duration = Duration::from_millis(30);

const PROBE_REQUEST_TIMEOUT: Duration = MAX_NODE_RTT.saturating_mul(2);

const PROBE_ROUND_BUDGET: Duration = Duration::from_secs(10);

// Avoid probing every poll when no node is usable.
const PROBE_COOLDOWN: Duration = Duration::from_secs(30);

const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

const POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Deserialize)]
struct ServedNodes {
    #[serde(default, deserialize_with = "lenient_rfc3339")]
    generated_at: Option<DateTime<Utc>>,
    nodes: Vec<ServedNode>,
}

// An invalid generated_at should not prevent loading the node list.
fn lenient_rfc3339<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)
        .unwrap_or_default()
        .and_then(|raw| DateTime::parse_from_rfc3339(&raw).ok())
        .map(|parsed| parsed.with_timezone(&Utc)))
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct ServedNode {
    ip: IpAddr,
    grpc_port: u16,
    region: String,
}

impl ServedNode {
    fn url(&self) -> String {
        format!("https://{}", SocketAddr::new(self.ip, self.grpc_port))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RankedNode {
    url: String,
    region: String,
    rtt_us: u64,
}

pub fn is_registry_url(url: &Url) -> bool {
    url.path() != "/"
}

struct RegistryFollower {
    http_client: reqwest::Client,
    registry_url: String,
    // Retain the last successful registry response across fetch failures.
    registry_nodes: Vec<ServedNode>,
    ranked: Vec<RankedNode>,
    cursor: usize,
    resync_at: Instant,
    probe_at: Instant,
    stalled_since: Instant,
}

impl RegistryFollower {
    fn new(registry_url: String) -> Option<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            .build()
            .inspect_err(|err| error!("Failed to build BAM discovery http client: {err}"))
            .ok()?;
        let now = Instant::now();
        Some(Self {
            http_client,
            registry_url,
            registry_nodes: Vec::new(),
            ranked: Vec::new(),
            cursor: 0,
            resync_at: now,
            probe_at: now,
            stalled_since: now,
        })
    }

    async fn step(&mut self, bam_url: &ArcSwap<Option<String>>, bam_enabled: &AtomicU8) {
        let now = Instant::now();

        if now >= self.resync_at {
            if let Some(served) = BamDiscovery::fetch(&self.http_client, &self.registry_url).await
                && served.nodes != self.registry_nodes
            {
                self.registry_nodes = served.nodes;
                self.ranked.clear();
                self.cursor = 0;
                self.probe_at = now;
            }
            self.resync_at =
                now + rng().random_range(RESYNC_INTERVAL_LIVE / 2..=RESYNC_INTERVAL_LIVE);
        }
        self.resync_at = BamDiscovery::resync_deadline(
            BamDiscovery::connection_state(bam_enabled),
            self.resync_at,
            now,
        );

        if self.ranked.is_empty() && !self.registry_nodes.is_empty() && now >= self.probe_at {
            self.probe_at = now + PROBE_COOLDOWN;
            self.ranked = BamDiscovery::probe_and_rank(&self.registry_nodes).await;
            self.cursor = 0;
        }

        // Fetching and probing can take seconds, so read the connection state afterward.
        self.stalled_since = BamDiscovery::stall_since(
            BamDiscovery::connection_state(bam_enabled),
            self.stalled_since,
        );
        let stuck = self.stalled_since.elapsed() >= CONNECT_GRACE;

        // A node can answer the probe and still refuse the scheduler stream.
        let current_url = bam_url.load_full();
        if stuck && BamDiscovery::on_pick(current_url.as_deref(), &self.ranked, self.cursor) {
            self.cursor = BamDiscovery::advance(self.cursor, self.ranked.len());
            if self.cursor == 0 {
                self.ranked.clear();
            }
        }

        if (stuck || BamDiscovery::needs_pick(current_url.as_deref(), &self.registry_nodes))
            && let Some(node) = self.ranked.get(self.cursor)
            && BamDiscovery::publish(bam_url, node)
        {
            self.stalled_since = Instant::now();
        }
    }
}

/// Discovery is the process of fetching, probing, and selecting
/// the nearest BAM node (lowest RTT) to the validator.
pub struct BamDiscovery {
    selected_url: Arc<ArcSwap<Option<String>>>,
    task: JoinHandle<()>,
}

impl BamDiscovery {
    pub fn new(
        // Discovery only runs when the configured BAM url points to a registry.
        configured_url: &Option<String>,
        bam_enabled: Arc<AtomicU8>,
        runtime: &runtime::Handle,
    ) -> Option<Self> {
        let registry_url = Self::registry_url(configured_url)?;
        info!("BAM discovery following registry {registry_url}");

        let selected_url = Arc::new(ArcSwap::from_pointee(None));
        let task = runtime.spawn({
            let selected_url = selected_url.clone();
            async move {
                let Some(mut follower) = RegistryFollower::new(registry_url) else {
                    return;
                };
                loop {
                    follower.step(&selected_url, &bam_enabled).await;
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        });

        Some(Self { selected_url, task })
    }

    pub fn selected_url(&self) -> Arc<Option<String>> {
        self.selected_url.load_full()
    }

    fn registry_url(configured: &Option<String>) -> Option<String> {
        let raw = configured.as_deref()?;
        match Url::parse(raw) {
            Ok(url) => is_registry_url(&url).then(|| raw.to_owned()),
            Err(err) => {
                error!("BAM url {raw} does not parse, discovery idle: {err}");
                None
            }
        }
    }

    fn connection_state(bam_enabled: &AtomicU8) -> BamConnectionState {
        BamConnectionState::from_u8(bam_enabled.load(Ordering::Acquire))
    }

    // We define a "live" connection as a connection who's state is past
    // 'Connecting'. This is used to determine whether a successful connection
    // has been established to a selected BAM node.
    fn is_live(state: BamConnectionState) -> bool {
        state as u8 > BamConnectionState::Connecting as u8
    }

    fn resync_deadline(state: BamConnectionState, resync_at: Instant, now: Instant) -> Instant {
        if Self::is_live(state) {
            resync_at
        } else {
            resync_at.min(now + RESYNC_INTERVAL_STALLED)
        }
    }

    // Preserve the initial failure time across BamManager connection retries.
    fn stall_since(state: BamConnectionState, stalled_since: Instant) -> Instant {
        if Self::is_live(state) {
            Instant::now()
        } else {
            stalled_since
        }
    }

    // Checks the registry's list rather than the ranking, since a node that missed a probe is
    // still listed.
    fn needs_pick(current_url: Option<&str>, nodes: &[ServedNode]) -> bool {
        !current_url.is_some_and(|url| nodes.iter().any(|node| node.url() == url))
    }

    // A rebuilt ranking must publish its best node before advancing the cursor.
    fn on_pick(current_url: Option<&str>, ranked: &[RankedNode], cursor: usize) -> bool {
        ranked
            .get(cursor)
            .is_some_and(|node| current_url == Some(node.url.as_str()))
    }

    fn advance(cursor: usize, len: usize) -> usize {
        (cursor + 1) % len.max(1)
    }

    fn publish(bam_url: &ArcSwap<Option<String>>, node: &RankedNode) -> bool {
        if bam_url.load().as_deref() == Some(node.url.as_str()) {
            return false;
        }
        bam_url.store(Arc::new(Some(node.url.clone())));
        info!(
            "BAM discovery selected {} ({}, {}us)",
            node.url, node.region, node.rtt_us
        );
        datapoint_info!(
            "bam_discovery-selected",
            ("url", node.url.clone(), String),
            ("region", node.region.clone(), String),
            ("rtt_us", node.rtt_us as i64, i64),
        );
        true
    }

    async fn fetch(http_client: &reqwest::Client, registry_url: &str) -> Option<ServedNodes> {
        let response = async {
            http_client
                .get(registry_url)
                .send()
                .await?
                .error_for_status()?
                .json::<ServedNodes>()
                .await
        }
        .await;

        let mut served = match response {
            Ok(served) => served,
            Err(err) => {
                datapoint_warn!(
                    "bam_discovery-fetch_failed",
                    ("registry_url", registry_url, String),
                    ("err", err.to_string(), String),
                );
                return None;
            }
        };

        served.nodes = Self::unique_nodes(served.nodes);
        if served.nodes.is_empty() {
            datapoint_warn!("bam_discovery-empty_node_list", ("count", 1, i64));
            return None;
        }

        let age_secs = served.generated_at.map_or(-1, |generated_at| {
            Utc::now().signed_duration_since(generated_at).num_seconds()
        });
        datapoint_info!(
            "bam_discovery-node_list",
            ("nodes", served.nodes.len() as i64, i64),
            ("age_s", age_secs, i64),
        );

        Some(served)
    }

    fn unique_nodes(mut nodes: Vec<ServedNode>) -> Vec<ServedNode> {
        nodes.sort_by_key(|node| (node.ip, node.grpc_port));
        nodes.dedup_by_key(|node| (node.ip, node.grpc_port));
        nodes
    }

    async fn probe_and_rank(nodes: &[ServedNode]) -> Vec<RankedNode> {
        let pool: Vec<ServedNode> = nodes
            .choose_multiple(&mut rng(), PROBE_CAP)
            .cloned()
            .collect();

        let probes = stream::iter(&pool)
            .map(Self::probe)
            .buffer_unordered(PROBE_FANOUT)
            .filter_map(std::future::ready)
            .collect::<Vec<_>>();

        let Ok(mut ranked) = timeout(PROBE_ROUND_BUDGET, probes).await else {
            warn!(
                "BAM probe round timed out after {PROBE_ROUND_BUDGET:?}, probed {} nodes",
                pool.len()
            );
            datapoint_warn!("bam_discovery-probe_round_timeout", ("count", 1, i64));
            return Vec::new();
        };
        ranked.sort_unstable_by_key(|node| node.rtt_us);
        let usable = ranked.partition_point(Self::is_usable);

        info!(
            "BAM probe round: {}/{} nodes answered, {usable} within {MAX_NODE_RTT:?}\n{}",
            ranked.len(),
            pool.len(),
            Self::format_probe_table(&pool, &ranked)
        );

        datapoint_info!(
            "bam_discovery-probe_round",
            ("probed", pool.len() as i64, i64),
            ("responded", ranked.len() as i64, i64),
            ("usable", usable as i64, i64),
        );
        ranked.truncate(usable);
        ranked
    }

    fn is_usable(node: &RankedNode) -> bool {
        Duration::from_micros(node.rtt_us) <= MAX_NODE_RTT
    }

    fn format_probe_table(pool: &[ServedNode], ranked: &[RankedNode]) -> String {
        let header = format!(
            "{:>4}  {:<30}  {:<24}  {:>10}",
            "rank", "url", "region", "rtt"
        );
        let responded = ranked.iter().enumerate().map(|(index, node)| {
            let rank = if Self::is_usable(node) {
                (index + 1).to_string()
            } else {
                "-".to_string()
            };
            format!(
                "{:>4}  {:<30}  {:<24}  {:>8.2}ms",
                rank,
                node.url,
                node.region,
                node.rtt_us as f64 / 1_000.0
            )
        });
        let silent = pool
            .iter()
            .filter(|node| !ranked.iter().any(|entry| entry.url == node.url()))
            .map(|node| {
                format!(
                    "{:>4}  {:<30}  {:<24}  {:>10}",
                    "-",
                    node.url(),
                    node.region,
                    "no answer"
                )
            });

        std::iter::once(header)
            .chain(responded)
            .chain(silent)
            .collect::<Vec<_>>()
            .join("\n")
    }

    // Reuse one channel so the RTT samples exclude the TLS handshake.
    async fn probe(node: &ServedNode) -> Option<RankedNode> {
        let url = node.url();
        let endpoint = endpoint_from_url(&url).ok()?.timeout(PROBE_REQUEST_TIMEOUT);
        let channel = timeout(PROBE_CONNECT_TIMEOUT, endpoint.connect())
            .await
            .ok()?
            .ok()?;
        let mut client = BamNodeApiClient::new(channel);

        let mut best_us = u64::MAX;
        for _ in 0..PROBE_SAMPLES {
            let started = Instant::now();
            if client.get_builder_config(ConfigRequest {}).await.is_ok() {
                best_us = best_us.min(started.elapsed().as_micros() as u64);
            }
        }

        (best_us != u64::MAX).then(|| RankedNode {
            url,
            region: node.region.clone(),
            rtt_us: best_us,
        })
    }
}

impl Drop for BamDiscovery {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use {super::*, test_case::test_case};

    // Keep this fixture in sync with the registry's snapshot test.
    const GOLDEN_NODE: &str = r#"{"ip":"203.0.113.1","grpc_port":50056,"region":"fra"}"#;

    #[test]
    fn test_deserialize_golden_node() {
        let node: ServedNode = serde_json::from_str(GOLDEN_NODE).unwrap();
        assert_eq!(
            node,
            ServedNode {
                ip: "203.0.113.1".parse().unwrap(),
                grpc_port: 50056,
                region: "fra".to_string(),
            },
        );
    }

    #[test]
    fn test_deserialize_ignores_unknown_fields() {
        let json = r#"{"ip":"203.0.113.1","grpc_port":50056,"region":"fra","added_later":true}"#;
        let node: ServedNode = serde_json::from_str(json).unwrap();
        assert_eq!(node.grpc_port, 50056);
    }

    #[test]
    fn test_deserialize_served_nodes() {
        let json = format!(r#"{{"generated_at":"2026-09-09T12:34:56Z","nodes":[{GOLDEN_NODE}]}}"#);
        let served: ServedNodes = serde_json::from_str(&json).unwrap();
        assert_eq!(
            served.generated_at,
            Some("2026-09-09T12:34:56Z".parse::<DateTime<Utc>>().unwrap()),
        );
        assert_eq!(served.nodes.len(), 1);
    }

    #[test_case(r#""not-a-timestamp""# ; "malformed")]
    #[test_case("null" ; "null")]
    #[test_case("1757419200" ; "unix seconds")]
    fn test_deserialize_tolerates_bad_generated_at(value: &str) {
        let json = format!(r#"{{"generated_at":{value},"nodes":[{GOLDEN_NODE}]}}"#);
        let served: ServedNodes = serde_json::from_str(&json).unwrap();
        assert_eq!(served.generated_at, None);
        assert_eq!(served.nodes.len(), 1);
    }

    #[test]
    fn test_deserialize_tolerates_absent_generated_at() {
        let json = format!(r#"{{"nodes":[{GOLDEN_NODE}]}}"#);
        let served: ServedNodes = serde_json::from_str(&json).unwrap();
        assert_eq!(served.generated_at, None);
    }

    #[test]
    fn test_deserialize_empty_node_list() {
        let json = r#"{"generated_at":"2026-09-09T12:34:56Z","nodes":[]}"#;
        let served: ServedNodes = serde_json::from_str(json).unwrap();
        assert!(served.nodes.is_empty());
    }

    #[test]
    fn test_url_ipv4() {
        let node: ServedNode = serde_json::from_str(GOLDEN_NODE).unwrap();
        assert_eq!(node.url(), "https://203.0.113.1:50056");
    }

    fn ranked_node(ip: &str) -> RankedNode {
        RankedNode {
            url: format!("https://{ip}:50056"),
            region: "fra".to_string(),
            rtt_us: 4_200,
        }
    }

    fn served_node(ip: &str) -> ServedNode {
        ServedNode {
            ip: ip.parse().unwrap(),
            grpc_port: 50056,
            region: "fra".to_string(),
        }
    }

    #[test]
    fn test_unique_nodes_ignores_order() {
        let a = served_node("203.0.113.1");
        let b = served_node("203.0.113.2");
        assert_eq!(
            BamDiscovery::unique_nodes(vec![b.clone(), a.clone()]),
            BamDiscovery::unique_nodes(vec![a, b])
        );
    }

    #[test]
    fn test_unique_nodes_drops_duplicate_endpoints() {
        let a = served_node("203.0.113.1");
        let b = served_node("203.0.113.2");
        let relabeled_a = ServedNode {
            region: "ams".to_string(),
            ..a.clone()
        };
        assert_eq!(
            BamDiscovery::unique_nodes(vec![a.clone(), b.clone(), relabeled_a, a.clone()]),
            vec![a, b]
        );
    }

    #[test]
    fn test_format_probe_table_ranks_responders_and_keeps_silent_nodes() {
        let answered = served_node("203.0.113.1");
        let silent = served_node("203.0.113.2");
        let ranked = vec![ranked_node("203.0.113.1")];

        // Pool order is shuffled, so the table must order by the ranking, not the pool.
        let table = BamDiscovery::format_probe_table(&[silent.clone(), answered.clone()], &ranked);
        let lines: Vec<&str> = table.lines().collect();

        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("rank") && lines[0].contains("rtt"));
        assert!(lines[1].contains(&answered.url()) && lines[1].contains("4.20ms"));
        assert!(lines[2].contains(&silent.url()) && lines[2].contains("no answer"));
    }

    #[test]
    fn test_format_probe_table_leaves_slow_responders_unranked() {
        let fast = ranked_node("203.0.113.1");
        let slow = RankedNode {
            rtt_us: 45_000,
            ..ranked_node("203.0.113.2")
        };

        let table = BamDiscovery::format_probe_table(&[], &[fast.clone(), slow.clone()]);
        let lines: Vec<&str> = table.lines().collect();

        assert!(lines[1].trim_start().starts_with('1') && lines[1].contains(&fast.url));
        assert!(lines[2].trim_start().starts_with('-') && lines[2].contains("45.00ms"));
    }

    #[test_case(30_000, true ; "at the limit")]
    #[test_case(30_001, false ; "above the limit")]
    fn test_is_usable(rtt_us: u64, usable: bool) {
        let node = RankedNode {
            rtt_us,
            ..ranked_node("203.0.113.1")
        };
        assert_eq!(BamDiscovery::is_usable(&node), usable);
    }

    #[test]
    fn test_needs_pick_once_the_registry_drops_the_current_pick() {
        let still_served = served_node("203.0.113.1");
        assert!(BamDiscovery::needs_pick(
            Some("https://203.0.113.9:50056"),
            &[still_served]
        ));
    }

    #[test]
    fn test_no_pick_while_the_current_one_is_served() {
        let served = served_node("203.0.113.1");
        let url = served.url();
        assert!(!BamDiscovery::needs_pick(Some(&url), &[served]));
    }

    #[test]
    fn test_needs_pick_when_nothing_is_published() {
        assert!(BamDiscovery::needs_pick(
            None,
            &[served_node("203.0.113.1")]
        ));
    }

    // BamManager alternates between these states when connection succeeds but the
    // health check fails. Resetting the timer would prevent failover.
    #[test]
    fn test_stall_survives_connect_retry_churn() {
        let began = Instant::now().checked_sub(CONNECT_GRACE).unwrap();
        let stalled_since = [
            BamConnectionState::Connecting,
            BamConnectionState::Disconnected,
        ]
        .iter()
        .cycle()
        .take(20)
        .fold(began, |since, state| {
            BamDiscovery::stall_since(*state, since)
        });

        assert_eq!(stalled_since, began);
        assert!(stalled_since.elapsed() >= CONNECT_GRACE);
    }

    #[test_case(BamConnectionState::Disconnected, false ; "disconnected")]
    #[test_case(BamConnectionState::Connecting, false ; "connecting")]
    #[test_case(BamConnectionState::DrainingBlockEngine, true ; "draining")]
    #[test_case(BamConnectionState::BlockEngineDrained, true ; "drained")]
    #[test_case(BamConnectionState::Connected, true ; "connected")]
    fn test_is_live(state: BamConnectionState, expected: bool) {
        assert_eq!(BamDiscovery::is_live(state), expected);
    }

    #[test_case(BamConnectionState::Connected, RESYNC_INTERVAL_LIVE ; "live")]
    #[test_case(BamConnectionState::Disconnected, RESYNC_INTERVAL_STALLED ; "stalled")]
    fn test_resync_deadline(state: BamConnectionState, expected: Duration) {
        let now = Instant::now();
        let resync_at = BamDiscovery::resync_deadline(state, now + RESYNC_INTERVAL_LIVE, now);
        assert_eq!(resync_at, now + expected);
    }

    #[test]
    fn test_resync_deadline_keeps_an_earlier_stalled_deadline() {
        let now = Instant::now();
        assert_eq!(
            BamDiscovery::resync_deadline(BamConnectionState::Disconnected, now, now),
            now
        );
    }

    #[test]
    fn test_stall_clears_once_the_session_is_live() {
        let began = Instant::now().checked_sub(CONNECT_GRACE).unwrap();
        let cleared = BamDiscovery::stall_since(BamConnectionState::Connected, began);
        assert!(cleared > began && cleared.elapsed() < CONNECT_GRACE);
    }

    #[test_case("https://registry.jito.wtf/nodes", true ; "path is a node list")]
    #[test_case("https://203.0.113.1:50056", false ; "bare host is one node")]
    #[test_case("https://203.0.113.1:50056/", false ; "trailing slash is one node")]
    fn test_is_registry_url(url: &str, expected: bool) {
        assert_eq!(is_registry_url(&Url::parse(url).unwrap()), expected);
    }

    #[test]
    fn test_registry_url_ignores_an_unparseable_url() {
        assert_eq!(
            BamDiscovery::registry_url(&Some("not a url".to_string())),
            None
        );
    }

    #[test_case(Some("https://203.0.113.1:50056"), false ; "direct node")]
    #[test_case(None, false ; "no url")]
    #[test_case(Some("http://127.0.0.1:1/nodes"), true ; "registry")]
    fn test_discovery_only_runs_for_a_registry(configured: Option<&str>, runs: bool) {
        let runtime = runtime::Runtime::new().unwrap();
        let discovery = BamDiscovery::new(
            &configured.map(str::to_owned),
            Arc::new(AtomicU8::new(BamConnectionState::Disconnected as u8)),
            runtime.handle(),
        );

        assert_eq!(discovery.is_some(), runs);
    }

    #[test]
    fn test_dropping_discovery_stops_its_task() {
        let runtime = runtime::Runtime::new().unwrap();
        let discovery = BamDiscovery::new(
            &Some("http://127.0.0.1:1/nodes".to_string()),
            Arc::new(AtomicU8::new(BamConnectionState::Disconnected as u8)),
            runtime.handle(),
        )
        .unwrap();
        let selected_url = discovery.selected_url.clone();

        drop(discovery);

        let deadline = Instant::now() + Duration::from_secs(1);
        while Arc::strong_count(&selected_url) > 1 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(Arc::strong_count(&selected_url), 1);
    }

    // After a re-probe the cursor is back at the best node while the published URL is still the
    // last node tried. Advancing then would skip the best node.
    #[test]
    fn test_rebuilt_ranking_is_not_the_current_pick() {
        let ranked = vec![ranked_node("203.0.113.1"), ranked_node("203.0.113.2")];

        assert!(!BamDiscovery::on_pick(
            Some("https://203.0.113.9:50056"),
            &ranked,
            0
        ));
        assert!(BamDiscovery::on_pick(
            Some("https://203.0.113.1:50056"),
            &ranked,
            0
        ));
    }

    #[test]
    fn test_empty_ranking_has_no_pick() {
        assert!(!BamDiscovery::on_pick(
            Some("https://203.0.113.1:50056"),
            &[],
            0
        ));
    }

    #[test]
    fn test_advance_steps_through_the_ranking() {
        assert_eq!(BamDiscovery::advance(0, 3), 1);
        assert_eq!(BamDiscovery::advance(1, 3), 2);
    }

    #[test]
    fn test_advance_wraps_at_the_end_of_the_ranking() {
        assert_eq!(BamDiscovery::advance(2, 3), 0);
        assert_eq!(BamDiscovery::advance(0, 1), 0);
    }

    #[test]
    fn test_advance_on_an_empty_ranking_does_not_panic() {
        assert_eq!(BamDiscovery::advance(0, 0), 0);
    }

    #[test]
    fn test_publish_stores_the_selected_url() {
        let bam_url = ArcSwap::from_pointee(None);
        BamDiscovery::publish(&bam_url, &ranked_node("203.0.113.1"));
        assert_eq!(
            bam_url.load_full().as_deref(),
            Some("https://203.0.113.1:50056"),
        );
    }

    #[test]
    fn test_publish_replaces_a_different_url() {
        let bam_url = ArcSwap::from_pointee(Some("https://203.0.113.9:50056".to_string()));
        BamDiscovery::publish(&bam_url, &ranked_node("203.0.113.1"));
        assert_eq!(
            bam_url.load_full().as_deref(),
            Some("https://203.0.113.1:50056"),
        );
    }

    #[test]
    fn test_publish_leaves_an_unchanged_url_untouched() {
        let bam_url = ArcSwap::from_pointee(Some("https://203.0.113.1:50056".to_string()));
        let before = bam_url.load_full();
        BamDiscovery::publish(&bam_url, &ranked_node("203.0.113.1"));
        assert!(Arc::ptr_eq(&before, &bam_url.load_full()));
    }

    #[test]
    fn test_connection_state_reads_the_shared_atomic() {
        let bam_enabled = AtomicU8::new(BamConnectionState::Connected as u8);
        assert_eq!(
            BamDiscovery::connection_state(&bam_enabled),
            BamConnectionState::Connected,
        );
        bam_enabled.store(BamConnectionState::Disconnected as u8, Ordering::Release);
        assert_eq!(
            BamDiscovery::connection_state(&bam_enabled),
            BamConnectionState::Disconnected,
        );
    }

    #[tokio::test]
    async fn test_probe_drops_a_node_that_does_not_answer() {
        let node = ServedNode {
            ip: "127.0.0.1".parse().unwrap(),
            grpc_port: 1,
            region: "fra".to_string(),
        };
        assert!(BamDiscovery::probe(&node).await.is_none());
    }

    #[tokio::test]
    async fn test_probe_and_rank_with_no_candidates() {
        assert!(BamDiscovery::probe_and_rank(&[]).await.is_empty());
    }

    #[test]
    fn test_url_ipv6_is_bracketed() {
        let node = ServedNode {
            ip: "2001:db8::1".parse().unwrap(),
            grpc_port: 50056,
            region: "fra".to_string(),
        };
        assert_eq!(node.url(), "https://[2001:db8::1]:50056");
    }
}
