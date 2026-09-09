/// Discovers BAM nodes through the BAM Registry:
/// - Fetches the published node list
/// - Ranks candidates by measured round-trip time
/// - Publishes the winner into the shared BAM url that `BamManager` watches
use {
    serde::Deserialize,
    std::{
        net::{IpAddr, SocketAddr},
        time::Duration,
    },
};

/// How often the published node list is re-read in the steady state.
const RESYNC_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// Resync interval used while no node list is held at all. A validator that
/// boots into a registry outage has nothing to fall back on, so it retries far
/// more eagerly than the steady-state cadence.
const RESYNC_INTERVAL_EMPTY: Duration = Duration::from_secs(60);

/// How long the connection may sit disconnected before the current pick is
/// treated as bad and the ranking advances.
const CONNECT_GRACE: Duration = Duration::from_secs(10);

/// A probe round is one pass over a sample of the published list: every node in
/// the sample is dialled, timed `PROBE_SAMPLES` times, and the survivors are
/// sorted into a fresh ranking. A round runs on startup, whenever the published
/// list changes, and whenever the ranking is exhausted.
///
/// Most nodes probed in one round. A random sample is taken when the published
/// list is longer, which keeps probe load spread across the fleet. The sample is
/// what makes the shuffle meaningful: without a cap every validator would probe
/// the same prefix of the list.
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
///
/// Nothing health-derived appears in the object: a node's presence *is* the
/// health assertion. The registry publishes no schema version, so unknown
/// fields must be ignored rather than rejected - a field added on the registry
/// side cannot be allowed to take the fleet offline.
#[derive(Clone, Debug, Deserialize)]
pub struct ServedNodes {
    /// RFC 3339, e.g. `2026-09-09T12:34:56Z`. Not a unix timestamp.
    pub generated_at: String,
    pub nodes: Vec<ServedNode>,
}

/// A single entry in the published list. `region` is descriptive only and is
/// never verified by the registry.
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(served.generated_at, "2026-09-09T12:34:56Z");
        assert_eq!(served.nodes.len(), 1);
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
