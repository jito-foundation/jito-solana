/// Discovers BAM nodes through the BAM Registry:
/// - Fetches the published node list
/// - Ranks candidates by measured round-trip time
/// - Publishes the winner into the shared BAM url that `BamManager` watches
use {
    serde::Deserialize,
    std::net::{IpAddr, SocketAddr},
};

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
