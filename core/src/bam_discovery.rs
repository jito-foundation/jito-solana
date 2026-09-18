/// Discovers BAM nodes through the BAM Registry:
/// - Fetches the published node list
/// - Ranks candidates by measured round-trip time
/// - Publishes the winner into the shared BAM url that `BamManager` watches
use {
    crate::{bam_dependencies::BamConnectionState, tonic_endpoint::endpoint_from_url},
    arc_swap::ArcSwap,
    chrono::{DateTime, Utc},
    futures::future::join_all,
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
    tokio::{sync::Semaphore, time::timeout},
};

/// How often the published node list is re-read while a session is live. Only
/// has to be fresh enough to follow a drain inside a maintenance window; ~800
/// validators re-reading a small JSON document at this cadence is ~13 requests
/// per second.
const RESYNC_INTERVAL_LIVE: Duration = Duration::from_secs(60);

/// How often it is re-read with no live session. The list is the only route
/// back to a working node, so it is read hard until one is found. Subsumes the
/// boot-time case, which has neither a list nor a session.
const RESYNC_INTERVAL_STALLED: Duration = Duration::from_secs(2);

/// How long the connection may sit disconnected before the current pick is
/// treated as bad and the ranking advances.
const CONNECT_GRACE: Duration = Duration::from_secs(10);

/// A probe round is one pass over a sample of the published.
const PROBE_CAP: usize = 32;

/// Most probes in flight at once.
const PROBE_FANOUT: usize = 16;

/// Round trips measured per node. The minimum is kept, so a single scheduling
/// hiccup does not push an otherwise close node down the ranking.
const PROBE_SAMPLES: usize = 3;

/// Connect budget for one probe. More generous than the request budget because
/// a TLS handshake to a distant node costs several round trips.
const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Request budget for one probe sample. The node's own admission gate rejects a
/// validator above 30ms of mean RTT, so a sample slower than this is describing
/// a node that would refuse the connection anyway.
const PROBE_REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

/// Backstop for an entire probe round, independent of the fan-out arithmetic.
/// Only trips when the fleet is unreachable, and bounds how long shutdown can
/// wait on a round in progress.
const PROBE_ROUND_BUDGET: Duration = Duration::from_secs(10);

/// Minimum gap between probe rounds. Load-bearing: when every candidate is
/// unreachable a round yields an empty ranking, so without a floor the loop
/// would re-probe the whole fleet on every poll.
const PROBE_COOLDOWN: Duration = Duration::from_secs(30);

/// Budget for fetching the node list.
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// How often the loop wakes to poll the exit flag and the connection state.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The node list published by the registry.
/// By appearing in the list, a node asserts its liveness and health.
#[derive(Clone, Debug, Deserialize)]
pub struct ServedNodes {
    /// When the registry built this list.
    #[serde(default, deserialize_with = "lenient_rfc3339")]
    pub generated_at: Option<DateTime<Utc>>,
    pub nodes: Vec<ServedNode>,
}

/// The registry sends RFC 3339 on the wire. A missing or malformed value costs
/// one metric, so it must not fail the document and take the node list with it.
fn lenient_rfc3339<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)
        .unwrap_or_default()
        .and_then(|raw| DateTime::parse_from_rfc3339(&raw).ok())
        .map(|parsed| parsed.with_timezone(&Utc)))
}

/// A node the registry lists as a candidate.
/// When its measured, it will be a `RankedNode`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ServedNode {
    pub ip: IpAddr,
    pub grpc_port: u16,
    pub region: String,
}

impl ServedNode {
    /// gRPC target for this node. `SocketAddr` brackets IPv6 addresses, and the
    /// registry builds the same string when it dials a node back to admit it,
    /// so both ends agree byte for byte.
    pub fn url(&self) -> String {
        format!("https://{}", SocketAddr::new(self.ip, self.grpc_port))
    }
}

/// A `ServedNode` that answered a probe, carrying the round-trip time that was
/// measured. Rankings hold only these, ordered by `rtt_us` ascending, so a node
/// reaches a ranking only by proving it is reachable.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RankedNode {
    url: String,
    region: String,
    rtt_us: u64,
}

/// Keeps the shared BAM url pointed at a live node from the registry's list.
/// `BamManager` already reconnects whenever that url changes, so discovery only
/// has to decide what belongs there. The pick advances only while the connection
/// is disconnected.
pub struct BamDiscovery {
    /// Background worker that fetches, probes and publishes.
    thread_hdl: JoinHandle<()>,
}

impl BamDiscovery {
    pub fn new(
        exit: Arc<AtomicBool>,
        bam_url: Arc<ArcSwap<Option<String>>>,
        bam_enabled: Arc<AtomicU8>,
        registry_url: String,
    ) -> Self {
        info!("Starting BamDiscovery against {registry_url}");
        let thread_hdl = Builder::new()
            .name("solBamDisc".to_string())
            .spawn(move || {
                Self::run(exit, bam_url, bam_enabled, registry_url);
            })
            .unwrap();

        Self { thread_hdl }
    }

    fn run(
        exit: Arc<AtomicBool>,
        bam_url: Arc<ArcSwap<Option<String>>>,
        bam_enabled: Arc<AtomicU8>,
        registry_url: String,
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

        while !exit.load(Ordering::Relaxed) {
            let state = Self::connection_state(&bam_enabled);

            // Time without a live session, not time in one state; see `is_live`.
            let stuck = stalled_for >= CONNECT_GRACE;

            if time_until_resync == Duration::ZERO {
                if let Some(served) = runtime.block_on(Self::fetch(&http_client, &registry_url))
                    && served.nodes != nodes
                {
                    nodes = served.nodes;
                    ranked.clear();
                    cursor = 0;
                    // The ranking now answers a stale question, so re-probe on
                    // the next pass rather than serving out the cooldown.
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

            // A node can answer the probe and still refuse the scheduler stream,
            // so walking the ranking keeps it from being a sink.
            if stuck && !ranked.is_empty() {
                cursor = Self::advance(cursor, ranked.len());
                if cursor == 0 {
                    // Ranking is stale, so clear and re-probe.
                    ranked.clear();
                }
                // Let the new pick have a grace window of its own.
                stalled_for = Duration::ZERO;
            }

            let current_url = bam_url.load_full();
            if (stuck || Self::is_drained(current_url.as_deref(), &nodes) || current_url.is_none())
                && let Some(node) = ranked.get(cursor)
            {
                Self::publish(&bam_url, node);
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

    /// A session past `Connecting` is live and authenticated: BamManager moves to
    /// `DrainingBlockEngine` and waits for BundleStage to finish the bundles it
    /// already took from the Block Engine before BAM starts scheduling. That wait
    /// is unbounded from here, so treating it as a bad pick would pull the url out
    /// from under a session that works.
    ///
    /// `Connecting` is not progress on its own: a node that answers the probe and
    /// then fails its health check leaves BamManager cycling `Connecting` ->
    /// `Disconnected` about once a second, so anything keyed on a state *change*
    /// resets forever and the pick never advances.
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

    /// True once the registry stops serving the url we published. A pick that
    /// leaves the list is draining for maintenance, so stepping off it early
    /// beats waiting for the operator to take it down under a live session.
    /// Keyed off the served list rather than the ranking, so a node that merely
    /// missed one probe round is not mistaken for a drain.
    fn is_drained(current_url: Option<&str>, nodes: &[ServedNode]) -> bool {
        current_url.is_some_and(|url| !nodes.iter().any(|node| node.url() == url))
    }

    /// Step to the next candidate, wrapping at the end of the ranking.
    fn advance(cursor: usize, len: usize) -> usize {
        let next = cursor.saturating_add(1);
        if next >= len { 0 } else { next }
    }

    fn publish(bam_url: &ArcSwap<Option<String>>, node: &RankedNode) {
        if bam_url.load_full().as_deref() == Some(node.url.as_str()) {
            return;
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
        bam_url.store(Arc::new(Some(node.url.clone())));
    }

    /// Read the published list. `None` leaves the caller holding whatever it
    /// already has. Better to have a stale list than no list at all.
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

        let permits = Arc::new(Semaphore::new(PROBE_FANOUT));
        let probes = pool.iter().map(|node| {
            let permits = permits.clone();
            async move {
                let _permit = permits.acquire().await.ok()?;
                Self::probe(node).await
            }
        });

        let mut ranked: Vec<RankedNode> = match timeout(PROBE_ROUND_BUDGET, join_all(probes)).await
        {
            Ok(results) => results.into_iter().flatten().collect(),
            Err(_) => {
                warn!(
                    "BAM probe round timed out after {PROBE_ROUND_BUDGET:?}, probed {} nodes",
                    pool.len()
                );
                datapoint_warn!("bam_discovery-probe_round_timeout", ("count", 1, i64));
                return Vec::new();
            }
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
    /// not answer are kept rather than dropped: a node the registry publishes that
    /// never responds is the case worth seeing, and it is absent from the ranking
    /// by construction.
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

    /// Time `GetBuilderConfig` against one node.
    /// All samples share a single channel, so the ranking reflects round trips
    /// rather than TLS handshakes.
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

    /// Byte-for-byte the object the registry pins in its own snapshot test. If
    /// either side renames a field, this fails instead of the fleet emptying.
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

    /// The timestamp only feeds a metric, so a registry that sends a broken or
    /// missing one must still hand us a usable node list.
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
    fn test_drained_once_the_registry_drops_the_current_pick() {
        let still_served = served_node("203.0.113.1");
        assert!(BamDiscovery::is_drained(
            Some("https://203.0.113.9:50056"),
            &[still_served]
        ));
    }

    #[test]
    fn test_not_drained_while_the_current_pick_is_served() {
        let served = served_node("203.0.113.1");
        let url = served.url();
        assert!(!BamDiscovery::is_drained(Some(&url), &[served]));
    }

    // Nothing published yet is not a drain; the bootstrap publish path covers it.
    #[test]
    fn test_no_current_pick_is_not_drained() {
        assert!(!BamDiscovery::is_drained(
            None,
            &[served_node("203.0.113.1")]
        ));
    }

    fn polls_to_grace() -> usize {
        (CONNECT_GRACE.as_millis() / POLL_INTERVAL.as_millis()) as usize
    }

    // Regression: a node that answers the probe but refuses the session leaves
    // BamManager cycling `Connecting` -> `Disconnected` about once a second. The
    // stall clock has to survive that churn, or the cursor never advances and the
    // validator stays pinned to a node it can never connect to.
    #[test]
    fn test_stall_accumulates_across_connect_retry_churn() {
        let stalled_for = [
            BamConnectionState::Connecting,
            BamConnectionState::Disconnected,
        ]
        .iter()
        .cycle()
        .take(2 * polls_to_grace())
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
