use {
    arc_swap::ArcSwap,
    solana_cluster_type::ClusterType,
    solana_metrics::datapoint_info,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

/// DoubleZero validator multicast client configuration:
/// https://docs.malbeclabs.com/Validator%20Multicast%20Connection/#1-client-configuration
pub const MULTICAST_SHRED_ADDR_MAINNET: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(233, 84, 178, 1)), 7733);

pub const MULTICAST_SHRED_ADDR_TESTNET: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(233, 84, 178, 10)), 7733);

/// How often to run the multicast check.
const CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// How often to poll the exit flag.
const EXIT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Host route mask (/32) in `/proc/net/route` hex format.
#[cfg(test)]
const MULTICAST_ROUTE_MASK_HEX: &str = "FFFFFFFF";
const MULTICAST_ROUTE_MASK: u32 = u32::MAX;

/// Watches the routing table and keeps the cluster multicast shred address in
/// sync with kernel route availability.
pub struct MulticastShredCheckService {
    /// Background worker that polls for multicast route changes.
    thread_hdl: JoinHandle<()>,
}

impl MulticastShredCheckService {
    pub fn new(
        exit: Arc<AtomicBool>,
        multicast_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
        cluster_type: ClusterType,
    ) -> Self {
        let multicast_addr = match cluster_type {
            ClusterType::Testnet => MULTICAST_SHRED_ADDR_TESTNET,
            _ => MULTICAST_SHRED_ADDR_MAINNET,
        };
        info!("Starting MulticastShredCheckService for {cluster_type:?} ({multicast_addr})");
        let thread_hdl = Builder::new()
            .name("solMcastShrdChk".to_string())
            .spawn(move || {
                Self::run(exit, multicast_receiver_address, multicast_addr);
            })
            .unwrap();

        Self { thread_hdl }
    }

    fn run(
        exit: Arc<AtomicBool>,
        multicast_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
        multicast_addr: SocketAddr,
    ) {
        let mut time_until_check = CHECK_INTERVAL;

        // Run first check immediately on startup.
        Self::apply_multicast_receiver_state(
            &multicast_receiver_address,
            Self::has_multicast_route(multicast_addr),
            multicast_addr,
        );

        while !exit.load(Ordering::Relaxed) {
            thread::sleep(EXIT_POLL_INTERVAL);
            time_until_check = time_until_check.saturating_sub(EXIT_POLL_INTERVAL);
            if time_until_check == Duration::ZERO {
                Self::apply_multicast_receiver_state(
                    &multicast_receiver_address,
                    Self::has_multicast_route(multicast_addr),
                    multicast_addr,
                );
                time_until_check = CHECK_INTERVAL;
            }
        }
    }

    fn apply_multicast_receiver_state(
        multicast_receiver_address: &ArcSwap<Option<SocketAddr>>,
        route_found: bool,
        multicast_addr: SocketAddr,
    ) {
        let next = route_found.then_some(multicast_addr);
        let current = multicast_receiver_address.load();
        if current.as_ref() == &next {
            return;
        }

        multicast_receiver_address.store(Arc::new(next));
        let status = if route_found { "enabled" } else { "disabled" };
        info!("Set multicast receiver state for {multicast_addr}: {status}");
        datapoint_info!(
            "multicast_shred_check",
            ("status", status, String),
            ("address", multicast_addr.to_string(), String),
        );
    }

    /// Encode an IPv4 address as the `/proc/net/route` destination value
    /// (e.g. 233.84.178.1 -> 0x01B254E9).
    fn ipv4_to_route_value(addr: SocketAddr) -> Option<u32> {
        match addr.ip() {
            IpAddr::V4(v4) => Some(u32::from_le_bytes(v4.octets())),
            IpAddr::V6(_) => None,
        }
    }

    fn parse_route_field(field: &str) -> Option<u32> {
        if field.len() != 8 {
            return None;
        }
        u32::from_str_radix(field, 16).ok()
    }

    /// Check if a host route (/32) for `multicast_addr` exists in the kernel routing table.
    #[cfg(target_os = "linux")]
    fn has_multicast_route(multicast_addr: SocketAddr) -> bool {
        let route_data = match std::fs::read_to_string("/proc/net/route") {
            Ok(data) => data,
            Err(err) => {
                warn!("Failed to read /proc/net/route: {err}");
                return false;
            }
        };
        Self::route_table_contains_addr(&route_data, multicast_addr)
    }

    #[cfg(not(target_os = "linux"))]
    fn has_multicast_route(_multicast_addr: SocketAddr) -> bool {
        false
    }

    /// Parse the contents of `/proc/net/route` and return true if a host route (/32)
    /// for `multicast_addr` is present.
    ///
    /// Format: Iface Destination Gateway Flags RefCnt Use Metric Mask ...
    fn route_table_contains_addr(data: &str, multicast_addr: SocketAddr) -> bool {
        // Instead of polling, the ideal approach to this is to use Linux API's Netlink RTNETLINK socket.
        // It subscribes to the RTMGRP_IPV4_ROUTE multicast group and the kernel pushes
        // RTM_NEWROUTE/RTM_DELROUTE messages to us in real time whenever a route is added or removed.
        // This is exactly how ip monitor route works. This is what Firedancer is using
        // https://github.com/firedancer-io/firedancer/blob/main/src/waltz/mib/fd_netdev_netlink.c#L56
        //     socket(AF_NETLINK, SOCK_RAW, NETLINK_ROUTE)
        //     bind with nl_groups = RTMGRP_IPV4_ROUTE
        //     recv() — blocks until kernel sends RTM_NEWROUTE or RTM_DELROUTE
        let Some(destination) = Self::ipv4_to_route_value(multicast_addr) else {
            return false;
        };
        for line in data.lines().skip(1) {
            let mut fields = line.split_whitespace();
            let Some(dest_field) = fields.nth(1) else {
                continue;
            };
            let Some(mask_field) = fields.nth(5) else {
                continue;
            };
            // Match only an exact host route (/32) for the multicast address.
            if Self::parse_route_field(dest_field) == Some(destination)
                && Self::parse_route_field(mask_field) == Some(MULTICAST_ROUTE_MASK)
            {
                return true;
            }
        }
        false
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multicast_addrs() {
        for (multicast_addr, expected) in [
            (MULTICAST_SHRED_ADDR_MAINNET, "233.84.178.1:7733"),
            (MULTICAST_SHRED_ADDR_TESTNET, "233.84.178.10:7733"),
        ] {
            assert_eq!(multicast_addr, expected.parse::<SocketAddr>().unwrap());
        }
    }

    #[test]
    fn test_ipv4_to_route_value() {
        for (multicast_addr, expected) in [
            (MULTICAST_SHRED_ADDR_MAINNET, 0x01B254E9),
            (MULTICAST_SHRED_ADDR_TESTNET, 0x0AB254E9),
        ] {
            assert_eq!(
                MulticastShredCheckService::ipv4_to_route_value(multicast_addr),
                Some(expected),
            );
        }
    }

    fn make_route_table(dest: &str, mask: &str) -> String {
        format!(
            "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\\
             neth0\t{dest}\t00000000\t0001\t0\t0\t0\t{mask}\t0\t0\t0\n"
        )
    }

    // Route-table parsing

    #[test]
    fn test_route_table_matching_entry_returns_true() {
        let data = make_route_table("01B254E9", MULTICAST_ROUTE_MASK_HEX);
        assert!(MulticastShredCheckService::route_table_contains_addr(
            &data,
            MULTICAST_SHRED_ADDR_MAINNET,
        ));
    }

    #[test]
    fn test_route_table_case_insensitive_match() {
        let data = make_route_table("01b254e9", "ffffffff");
        assert!(MulticastShredCheckService::route_table_contains_addr(
            &data,
            MULTICAST_SHRED_ADDR_MAINNET,
        ));
    }

    #[test]
    fn test_route_table_wrong_destination_returns_false() {
        let data = make_route_table("00000000", MULTICAST_ROUTE_MASK_HEX);
        assert!(!MulticastShredCheckService::route_table_contains_addr(
            &data,
            MULTICAST_SHRED_ADDR_MAINNET,
        ));
    }

    #[test]
    fn test_route_table_wrong_mask_returns_false() {
        let data = make_route_table("01B254E9", "FFFFFF00");
        assert!(!MulticastShredCheckService::route_table_contains_addr(
            &data,
            MULTICAST_SHRED_ADDR_MAINNET,
        ));
    }

    #[test]
    fn test_route_table_short_line_skipped() {
        let data = "Iface\tDestination\tGateway\neth0\t01B254E9\n";
        assert!(!MulticastShredCheckService::route_table_contains_addr(
            data,
            MULTICAST_SHRED_ADDR_MAINNET,
        ));
    }

    #[test]
    fn test_route_table_empty_returns_false() {
        assert!(!MulticastShredCheckService::route_table_contains_addr(
            "",
            MULTICAST_SHRED_ADDR_MAINNET,
        ));
    }

    #[test]
    fn test_route_table_header_only_returns_false() {
        let data =
            "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n";
        assert!(!MulticastShredCheckService::route_table_contains_addr(
            data,
            MULTICAST_SHRED_ADDR_MAINNET,
        ));
    }

    #[test]
    fn test_route_table_testnet_dest_does_not_match_mainnet() {
        let data = make_route_table("0AB254E9", MULTICAST_ROUTE_MASK_HEX);
        assert!(!MulticastShredCheckService::route_table_contains_addr(
            &data,
            MULTICAST_SHRED_ADDR_MAINNET,
        ));
    }

    #[test]
    fn test_route_table_contains_testnet_addr() {
        let data = make_route_table("0AB254E9", MULTICAST_ROUTE_MASK_HEX);
        assert!(MulticastShredCheckService::route_table_contains_addr(
            &data,
            MULTICAST_SHRED_ADDR_TESTNET,
        ));
    }

    #[test]
    fn test_route_table_contains_both_routes() {
        let data = concat!(
            "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n",
            "eth0\t01B254E9\t00000000\t0001\t0\t0\t0\tFFFFFFFF\t0\t0\t0\n",
            "eth1\t0AB254E9\t00000000\t0001\t0\t0\t0\tFFFFFFFF\t0\t0\t0\n",
        );
        assert!(MulticastShredCheckService::route_table_contains_addr(
            data,
            MULTICAST_SHRED_ADDR_MAINNET,
        ));
        assert!(MulticastShredCheckService::route_table_contains_addr(
            data,
            MULTICAST_SHRED_ADDR_TESTNET,
        ));
    }

    // Published receiver state

    #[test]
    fn test_apply_multicast_receiver_state_enables_address() {
        let multicast_receiver_address = Arc::new(ArcSwap::from_pointee(None));
        MulticastShredCheckService::apply_multicast_receiver_state(
            &multicast_receiver_address,
            true,
            MULTICAST_SHRED_ADDR_MAINNET,
        );
        assert_eq!(
            multicast_receiver_address.load().as_ref(),
            &Some(MULTICAST_SHRED_ADDR_MAINNET),
        );
    }

    #[test]
    fn test_apply_multicast_receiver_state_disables_address() {
        let multicast_receiver_address =
            Arc::new(ArcSwap::from_pointee(Some(MULTICAST_SHRED_ADDR_MAINNET)));
        MulticastShredCheckService::apply_multicast_receiver_state(
            &multicast_receiver_address,
            false,
            MULTICAST_SHRED_ADDR_MAINNET,
        );
        assert_eq!(multicast_receiver_address.load().as_ref(), &None);
    }
}
