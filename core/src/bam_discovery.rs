/// Discovers BAM nodes through the BAM Registry:
/// - Fetches the published node list
/// - Ranks candidates by measured round-trip time
/// - Publishes the winner into the shared BAM url that `BamManager` watches
use {
    crate::{bam_dependencies::BamConnectionState, tonic_endpoint::endpoint_from_url},
    arc_swap::ArcSwap,
    chrono::{DateTime, Utc},
    futures::{StreamExt, stream},
    jito_protos::proto::bam_api::{ConfigRequest, bam_node_api_client::BamNodeApiClient},
    rand::{rng, seq::SliceRandom},
    serde::{Deserialize, Deserializer},
    solana_metrics::{datapoint_info, datapoint_warn},
    std::{
        net::{IpAddr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU8, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    tokio::time::timeout,
};

/// How often the published node list is re-read while a session is live.
const RESYNC_INTERVAL_LIVE: Duration = Duration::from_secs(60);

/// How often it is re-read with no live session, including at startup.
const RESYNC_INTERVAL_STALLED: Duration = Duration::from_secs(2);

/// How long the connection may sit disconnected before the current pick is
/// treated as bad and the ranking advances.
const CONNECT_GRACE: Duration = Duration::from_secs(10);

/// Most nodes sampled in one probe round.
const PROBE_CAP: usize = 32;

/// Most probes in flight at once.
const PROBE_FANOUT: usize = 16;

/// Round trips measured per node. The minimum of the samples is kept.
const PROBE_SAMPLES: usize = 3;

/// Connect budget for one probe, covering the TLS handshake.
const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Request budget for one probe sample. A node's admission gate rejects a
/// validator above 30ms of mean RTT.
const PROBE_REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

/// Backstop for an entire probe round, and the longest shutdown waits on one.
const PROBE_ROUND_BUDGET: Duration = Duration::from_secs(10);

/// Minimum gap between probe rounds. An all-unreachable round yields an empty
/// ranking, which without this floor would re-probe the fleet on every poll.
const PROBE_COOLDOWN: Duration = Duration::from_secs(30);

/// Budget for fetching the node list.
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// How often the loop wakes to poll the exit flag and the connection state.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The node list published by the registry.
/// By appearing in the list, a node asserts its liveness and health.
#[derive(Clone, Debug, Deserialize)]
struct ServedNodes {
    /// When the registry built this list.
    #[serde(default, deserialize_with = "lenient_rfc3339")]
    generated_at: Option<DateTime<Utc>>,
    nodes: Vec<ServedNode>,
}

/// The registry sends RFC 3339. A missing or malformed value costs one metric
/// rather than failing the whole document.
fn lenient_rfc3339<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)
        .unwrap_or_default()
        .and_then(|raw| DateTime::parse_from_rfc3339(&raw).ok())
        .map(|parsed| parsed.with_timezone(&Utc)))
}

/// A node the registry lists as a candidate. Becomes a `RankedNode` once measured.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct ServedNode {
    ip: IpAddr,
    grpc_port: u16,
    region: String,
}

impl ServedNode {
    /// gRPC target for this node. `SocketAddr` brackets IPv6 addresses, and the
    /// registry builds the same string when it admits a node, so both ends agree.
    fn url(&self) -> String {
        format!("https://{}", SocketAddr::new(self.ip, self.grpc_port))
    }
}

/// A `ServedNode` that answered a probe, with its measured round-trip time.
/// Rankings hold only these, ordered by `rtt_us` ascending.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RankedNode {
    url: String,
    region: String,
    rtt_us: u64,
}

/// The node list a `--bam-url` names, or `None` when it names one node. A node
/// is a bare host and port; the node list carries a path.
pub fn registry_url(bam_url: &str) -> Option<&str> {
    reqwest::Url::parse(bam_url)
        .is_ok_and(|url| url.path() != "/")
        .then_some(bam_url)
}

/// Keeps the shared BAM url pointed at a live node from the registry's list.
/// `BamManager` reconnects whenever that url changes.
pub struct BamDiscovery {
    /// Background worker that fetches, probes and publishes.
    thread_hdl: JoinHandle<()>,
}

impl BamDiscovery {
    pub fn new(
        exit: Arc<AtomicBool>,
        bam_config: Arc<ArcSwap<Option<String>>>,
        bam_url: Arc<ArcSwap<Option<String>>>,
        bam_enabled: Arc<AtomicU8>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("solBamDisc".to_string())
            .spawn(move || {
                Self::run(exit, bam_config, bam_url, bam_enabled);
            })
            .unwrap();

        Self { thread_hdl }
    }

    fn run(
        exit: Arc<AtomicBool>,
        bam_config: Arc<ArcSwap<Option<String>>>,
        bam_url: Arc<ArcSwap<Option<String>>>,
        bam_enabled: Arc<AtomicU8>,
    ) {
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(err) => {
                error!("Failed to start BAM discovery runtime, discovery disabled: {err}");
                return;
            }
        };
        let http_client = match reqwest::Client::builder().timeout(FETCH_TIMEOUT).build() {
            Ok(client) => client,
            Err(err) => {
                error!("Failed to build BAM discovery http client, discovery disabled: {err}");
                return;
            }
        };

        // The full published list, kept across fetch failures.
        let mut nodes: Vec<ServedNode> = Vec::new();
        let mut ranked: Vec<RankedNode> = Vec::new();
        let mut cursor = 0usize;

        let mut time_until_resync = Duration::ZERO;
        let mut time_until_probe = Duration::ZERO;
        let mut stalled_for = Duration::ZERO;
        let mut config = bam_config.load_full();
        Self::announce(&config);

        while !exit.load(Ordering::Relaxed) {
            let current = bam_config.load_full();
            if current != config {
                config = current;
                Self::announce(&config);
                // Nothing measured against the old value survives it.
                nodes.clear();
                ranked.clear();
                cursor = 0;
                time_until_resync = Duration::ZERO;
                time_until_probe = Duration::ZERO;
                stalled_for = Duration::ZERO;
            }

            // Empty stays disconnected, and a single node is already the answer.
            // Only a registry needs the fetch, probe and rank machinery below.
            let Some(registry_url) = config.as_deref().and_then(registry_url) else {
                Self::set_url(&bam_url, config.as_deref());
                thread::sleep(POLL_INTERVAL);
                continue;
            };

            let state = Self::connection_state(&bam_enabled);

            // Time without a live session, not time in one state.
            let stuck = stalled_for >= CONNECT_GRACE;

            if time_until_resync == Duration::ZERO {
                if let Some(served) = runtime.block_on(Self::fetch(&http_client, registry_url))
                    && served.nodes != nodes
                {
                    nodes = served.nodes;
                    ranked.clear();
                    cursor = 0;
                    // Re-probe against the new list instead of serving out the
                    // cooldown.
                    time_until_probe = Duration::ZERO;
                }
                time_until_resync = if Self::is_live(state) {
                    RESYNC_INTERVAL_LIVE
                } else {
                    RESYNC_INTERVAL_STALLED
                };
            }

            if ranked.is_empty() && !nodes.is_empty() && time_until_probe == Duration::ZERO {
                time_until_probe = PROBE_COOLDOWN;
                ranked = runtime.block_on(Self::probe_and_rank(&nodes));
                cursor = 0;
            }

            // A node can answer the probe and still refuse the scheduler stream.
            let current_url = bam_url.load_full();
            if stuck && Self::on_pick(current_url.as_deref(), &ranked, cursor) {
                cursor = Self::advance(cursor, ranked.len());
                if cursor == 0 {
                    // Ranking is stale, so clear and re-probe.
                    ranked.clear();
                }
            }

            if (stuck || Self::needs_pick(current_url.as_deref(), &nodes))
                && let Some(node) = ranked.get(cursor)
                && Self::publish(&bam_url, node)
            {
                // Let the new pick have a grace window of its own.
                stalled_for = Duration::ZERO;
            }

            thread::sleep(POLL_INTERVAL);
            time_until_resync = time_until_resync.saturating_sub(POLL_INTERVAL);
            time_until_probe = time_until_probe.saturating_sub(POLL_INTERVAL);
            stalled_for = Self::stall_after_poll(state, stalled_for);
        }
    }

    fn connection_state(bam_enabled: &AtomicU8) -> BamConnectionState {
        BamConnectionState::from_u8(bam_enabled.load(Ordering::Acquire))
    }

    /// Whether the BAM session is usable. Everything past `Connecting` is
    /// authenticated, including the unbounded `DrainingBlockEngine` wait.
    /// `Connecting` is not: BamManager cycles it against `Disconnected` about once
    /// a second against a node that connects then fails its health check.
    fn is_live(state: BamConnectionState) -> bool {
        state as u8 > BamConnectionState::Connecting as u8
    }

    /// Advance the stall clock by one poll, or clear it once the session is live.
    fn stall_after_poll(state: BamConnectionState, stalled_for: Duration) -> Duration {
        if Self::is_live(state) {
            Duration::ZERO
        } else {
            stalled_for.saturating_add(POLL_INTERVAL)
        }
    }

    /// True when the shared url does not name a node the registry currently
    /// serves: nothing published yet, or a pick that has left the list. Reads the
    /// served list, not the ranking, so a node that missed one probe round is not
    /// mistaken for a drain.
    fn needs_pick(current_url: Option<&str>, nodes: &[ServedNode]) -> bool {
        !current_url.is_some_and(|url| nodes.iter().any(|node| node.url() == url))
    }

    /// Whether the shared url is the candidate the cursor points at. A ranking
    /// built since the last publish is not, so its best node is published before
    /// the walk is allowed to move past it.
    fn on_pick(current_url: Option<&str>, ranked: &[RankedNode], cursor: usize) -> bool {
        ranked
            .get(cursor)
            .is_some_and(|node| current_url == Some(node.url.as_str()))
    }

    /// Step to the next candidate, wrapping at the end of the ranking.
    fn advance(cursor: usize, len: usize) -> usize {
        (cursor + 1) % len.max(1)
    }

    /// Point BamManager at `url`, or at nothing. Reports whether that moved it.
    fn set_url(bam_url: &ArcSwap<Option<String>>, url: Option<&str>) -> bool {
        if bam_url.load_full().as_deref() == url {
            return false;
        }
        bam_url.store(Arc::new(url.map(str::to_owned)));
        true
    }

    fn announce(bam_config: &Option<String>) {
        match (bam_config.as_deref().and_then(registry_url), bam_config) {
            (Some(url), _) => info!("BAM discovery following registry {url}"),
            (None, Some(url)) => info!("BAM discovery idle, url names one node: {url}"),
            (None, None) => info!("BAM discovery idle, no url set"),
        }
    }

    /// Reports whether this moved the shared url.
    fn publish(bam_url: &ArcSwap<Option<String>>, node: &RankedNode) -> bool {
        if !Self::set_url(bam_url, Some(&node.url)) {
            return false;
        }
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

    /// Read the published list. `None` leaves the caller holding whatever it
    /// already has.
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

        let served = match response {
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

    /// Probe a sample of the published nodes and order them by round-trip time.
    /// Nodes that do not answer are dropped.
    async fn probe_and_rank(nodes: &[ServedNode]) -> Vec<RankedNode> {
        let mut pool = nodes.to_vec();
        pool.shuffle(&mut rng());
        pool.truncate(PROBE_CAP);

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

        info!(
            "BAM probe round: {}/{} nodes answered\n{}",
            ranked.len(),
            pool.len(),
            Self::probe_table(&pool, &ranked)
        );

        datapoint_info!(
            "bam_discovery-probe_round",
            ("probed", pool.len() as i64, i64),
            ("responded", ranked.len() as i64, i64),
        );
        ranked
    }

    /// One row per probed node, responders first in ranking order. Nodes that did
    /// not answer are absent from the ranking, so they are listed after it.
    fn probe_table(pool: &[ServedNode], ranked: &[RankedNode]) -> String {
        let header = format!(
            "{:>4}  {:<30}  {:<24}  {:>10}",
            "rank", "url", "region", "rtt"
        );
        let responded = ranked.iter().enumerate().map(|(index, node)| {
            format!(
                "{:>4}  {:<30}  {:<24}  {:>8.2}ms",
                index + 1,
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

    /// Time `GetBuilderConfig` against one node. All samples share one channel,
    /// so the measurement excludes the TLS handshake.
    async fn probe(node: &ServedNode) -> Option<RankedNode> {
        let url = node.url();
        let channel = endpoint_from_url(&url)
            .ok()?
            .connect_timeout(PROBE_CONNECT_TIMEOUT)
            .timeout(PROBE_REQUEST_TIMEOUT)
            .connect()
            .await
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

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, test_case::test_case};

    /// Byte-for-byte the object the registry pins in its own snapshot test, so a
    /// rename on either side fails here instead of emptying the fleet.
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

    /// The timestamp only feeds a metric, so a broken one must still yield a
    /// usable node list.
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
    fn test_probe_table_ranks_responders_and_keeps_silent_nodes() {
        let answered = served_node("203.0.113.1");
        let silent = served_node("203.0.113.2");
        let ranked = vec![ranked_node("203.0.113.1")];

        // Pool order is shuffled, so the table must order by the ranking, not the pool.
        let table = BamDiscovery::probe_table(&[silent.clone(), answered.clone()], &ranked);
        let lines: Vec<&str> = table.lines().collect();

        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("rank") && lines[0].contains("rtt"));
        assert!(lines[1].contains(&answered.url()) && lines[1].contains("4.20ms"));
        assert!(lines[2].contains(&silent.url()) && lines[2].contains("no answer"));
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

    // Bootstrap: nothing published yet also wants a pick.
    #[test]
    fn test_needs_pick_when_nothing_is_published() {
        assert!(BamDiscovery::needs_pick(
            None,
            &[served_node("203.0.113.1")]
        ));
    }

    // Regression: BamManager cycles `Connecting` -> `Disconnected` about once a
    // second while a node accepts the connection then fails its health check.
    // The stall clock has to survive that churn.
    #[test]
    fn test_stall_accumulates_across_connect_retry_churn() {
        let stalled_for = [
            BamConnectionState::Connecting,
            BamConnectionState::Disconnected,
        ]
        .iter()
        .cycle()
        .take(2 * (CONNECT_GRACE.as_millis() / POLL_INTERVAL.as_millis()) as usize)
        .fold(Duration::ZERO, |stalled, state| {
            BamDiscovery::stall_after_poll(*state, stalled)
        });

        assert!(stalled_for >= CONNECT_GRACE);
    }

    #[test_case(BamConnectionState::Disconnected, false ; "disconnected")]
    #[test_case(BamConnectionState::Connecting, false ; "connecting")]
    #[test_case(BamConnectionState::DrainingBlockEngine, true ; "draining")]
    #[test_case(BamConnectionState::BlockEngineDrained, true ; "drained")]
    #[test_case(BamConnectionState::Connected, true ; "connected")]
    fn test_is_live(state: BamConnectionState, expected: bool) {
        assert_eq!(BamDiscovery::is_live(state), expected);
    }

    #[test]
    fn test_stall_clears_once_the_session_is_live() {
        assert_eq!(
            BamDiscovery::stall_after_poll(BamConnectionState::Connected, CONNECT_GRACE),
            Duration::ZERO
        );
    }

    // Regression: after a re-probe the cursor is back at the best node while the
    // shared url is still the one the last walk ended on. Advancing then would
    // step straight past the best node, and it would never be published again.
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
