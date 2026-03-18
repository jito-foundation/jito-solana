use {
    solana_cluster_type::ClusterType,
    solana_metrics::datapoint_info,
    solana_turbine::ShredReceiverAddresses,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

use arc_swap::ArcSwap;

pub const MULTICAST_SHRED_ADDR_MAINNET: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(233, 84, 178, 1)), 7733);

pub const MULTICAST_SHRED_ADDR_TESTNET: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(233, 84, 178, 10)), 7733);

/// Maximum number of shred receiver addresses (mirrors shred_receiver_addresses.rs).
const MAX_SHRED_RECEIVER_ADDRESSES: usize = 32;

/// How often to run the multicast check (seconds).
const CHECK_INTERVAL_SECS: u64 = 60;

/// How often to poll the exit flag (seconds).
const EXIT_POLL_SECS: u64 = 1;

/// Host route mask (/32) in `/proc/net/route` hex format.
const MULTICAST_ROUTE_MASK_HEX: &str = "FFFFFFFF";

#[derive(Default)]
struct CheckState {
    route_detected: bool,
}

pub struct MulticastShredCheckService {
    thread_hdl: JoinHandle<()>,
}

impl MulticastShredCheckService {
    pub fn new(
        exit: Arc<AtomicBool>,
        shred_receiver_addresses: Arc<ArcSwap<ShredReceiverAddresses>>,
        cluster_type: ClusterType,
    ) -> Self {
        let multicast_addr = match cluster_type {
            ClusterType::Testnet => MULTICAST_SHRED_ADDR_TESTNET,
            _ => MULTICAST_SHRED_ADDR_MAINNET,
        };
        info!(
            "Starting MulticastShredCheckService for {:?} ({})",
            cluster_type, multicast_addr,
        );
        let thread_hdl = Builder::new()
            .name("solMcastShrdChk".to_string())
            .spawn(move || {
                Self::run(exit, shred_receiver_addresses, multicast_addr);
            })
            .unwrap();

        Self { thread_hdl }
    }

    fn run(
        exit: Arc<AtomicBool>,
        shred_receiver_addresses: Arc<ArcSwap<ShredReceiverAddresses>>,
        multicast_addr: SocketAddr,
    ) {
        let mut state = CheckState::default();

        // Run first check immediately on startup.
        Self::check_and_enable(&shred_receiver_addresses, multicast_addr, &mut state);
        let mut last_check = Instant::now();

        while !exit.load(Ordering::Relaxed) {
            if last_check.elapsed() >= Duration::from_secs(CHECK_INTERVAL_SECS) {
                Self::check_and_enable(&shred_receiver_addresses, multicast_addr, &mut state);
                last_check = Instant::now();
            }
            thread::sleep(Duration::from_secs(EXIT_POLL_SECS));
        }
    }

    fn check_and_enable(
        shred_receiver_addresses: &ArcSwap<ShredReceiverAddresses>,
        multicast_addr: SocketAddr,
        state: &mut CheckState,
    ) {
        let iface = Self::check_multicast_route(multicast_addr);
        let route_found = iface.is_some();
        if route_found != state.route_detected {
            state.route_detected = route_found;
        }

        if !route_found {
            // Route gone — remove address from the receiver list if present.
            let current = shred_receiver_addresses.load();
            if current.contains(&multicast_addr) {
                let new_addrs: ShredReceiverAddresses = current
                    .iter()
                    .filter(|&&addr| addr != multicast_addr)
                    .copied()
                    .collect();
                shred_receiver_addresses.store(Arc::new(new_addrs));
                info!(
                    "Removed multicast address {} from shred receivers",
                    multicast_addr,
                );
                datapoint_info!(
                    "multicast_shred_check",
                    ("status", "disabled", String),
                    ("address", multicast_addr.to_string(), String),
                );
            }
            return;
        }

        // Route detected — ensure address is in the broadcast receiver list.
        // Always re-load from ArcSwap (admin RPC can replace the full list at any time).
        let current = shred_receiver_addresses.load();
        if current.contains(&multicast_addr) {
            trace!(
                "Multicast address {} already present in shred receivers",
                multicast_addr,
            );
            return;
        }

        if current.len() >= MAX_SHRED_RECEIVER_ADDRESSES {
            warn!(
                "Cannot add multicast address {}: shred receiver list is full ({}/{})",
                multicast_addr,
                current.len(),
                MAX_SHRED_RECEIVER_ADDRESSES,
            );
            return;
        }

        let mut new_addrs = (**current).clone();
        new_addrs.push(multicast_addr);
        shred_receiver_addresses.store(Arc::new(new_addrs));

        info!(
            "Added multicast address {} to shred receivers",
            multicast_addr
        );
        datapoint_info!(
            "multicast_shred_check",
            ("status", "enabled", String),
            ("address", multicast_addr.to_string(), String),
        );
    }

    /// Encode an IPv4 address as little-endian hex, matching the format used
    /// by `/proc/net/route` (e.g. 233.84.178.1 -> "01B254E9").
    fn ipv4_to_route_hex(addr: SocketAddr) -> Option<String> {
        
        match addr.ip() {
            IpAddr::V4(v4) => {
                let o = v4.octets();
                Some(format!("{:02X}{:02X}{:02X}{:02X}", o[3], o[2], o[1], o[0]))
            }
            IpAddr::V6(_) => None,
        }
    }

    /// Check if a host route (/32) for `multicast_addr` exists in the kernel routing table.
    /// Returns the interface name if found, or None otherwise.
    #[cfg(target_os = "linux")]
    fn check_multicast_route(multicast_addr: SocketAddr) -> Option<String> {
        let route_data = match std::fs::read_to_string("/proc/net/route") {
            Ok(data) => data,
            Err(err) => {
                warn!("Failed to read /proc/net/route: {}", err);
                return None;
            }
        };
        Self::find_multicast_iface(&route_data, multicast_addr)
    }

    #[cfg(not(target_os = "linux"))]
    fn check_multicast_route(_multicast_addr: SocketAddr) -> Option<String> {
        None
    }

    /// Parse the contents of `/proc/net/route` and return the interface name if a host route (/32)
    /// for `multicast_addr` is present.
    ///
    /// Format: Iface Destination Gateway Flags RefCnt Use Metric Mask ...
    fn find_multicast_iface(data: &str, multicast_addr: SocketAddr) -> Option<String> {
        let dest_hex = Self::ipv4_to_route_hex(multicast_addr)?;
        for line in data.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 8 {
                continue;
            }
            let destination = fields[1];
            let mask = fields[7];
            if destination.eq_ignore_ascii_case(&dest_hex)
                && mask.eq_ignore_ascii_case(MULTICAST_ROUTE_MASK_HEX)
            {
                return Some(fields[0].to_string());
            }
        }
        None
    }

    #[cfg(test)]
    fn route_table_contains_multicast(data: &str) -> bool {
        Self::find_multicast_iface(data, MULTICAST_SHRED_ADDR_MAINNET).is_some()
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Constants ────────────────────────────────────────────────────────────

    #[test]
    fn test_multicast_addr_mainnet() {
        assert_eq!(
            MULTICAST_SHRED_ADDR_MAINNET,
            "233.84.178.1:7733".parse().unwrap(),
        );
    }

    #[test]
    fn test_multicast_addr_testnet() {
        assert_eq!(
            MULTICAST_SHRED_ADDR_TESTNET,
            "233.84.178.10:7733".parse().unwrap(),
        );
    }

    // ── ipv4_to_route_hex ─────────────────────────────────────────────────────

    /// 233.84.178.1 in little-endian bytes: [1, 178, 84, 233] -> "01B254E9"
    #[test]
    fn test_ipv4_to_route_hex_mainnet() {
        assert_eq!(
            MulticastShredCheckService::ipv4_to_route_hex(MULTICAST_SHRED_ADDR_MAINNET),
            Some("01B254E9".to_string()),
        );
    }

    /// 233.84.178.10 in little-endian bytes: [10, 178, 84, 233] -> "0AB254E9"
    #[test]
    fn test_ipv4_to_route_hex_testnet() {
        assert_eq!(
            MulticastShredCheckService::ipv4_to_route_hex(MULTICAST_SHRED_ADDR_TESTNET),
            Some("0AB254E9".to_string()),
        );
    }

    // ── find_multicast_iface ──────────────────────────────────────────────────

    fn make_route_table(dest: &str, mask: &str) -> String {
        format!(
            "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n\
             eth0\t{dest}\t00000000\t0001\t0\t0\t0\t{mask}\t0\t0\t0\n"
        )
    }

    #[test]
    fn test_route_table_matching_entry_returns_true() {
        let data = make_route_table("01B254E9", MULTICAST_ROUTE_MASK_HEX);
        assert!(MulticastShredCheckService::route_table_contains_multicast(
            &data
        ));
    }

    #[test]
    fn test_route_table_case_insensitive_match() {
        let data = make_route_table("01b254e9", "ffffffff");
        assert!(MulticastShredCheckService::route_table_contains_multicast(
            &data
        ));
    }

    #[test]
    fn test_route_table_wrong_destination_returns_false() {
        let data = make_route_table("00000000", MULTICAST_ROUTE_MASK_HEX);
        assert!(!MulticastShredCheckService::route_table_contains_multicast(
            &data
        ));
    }

    #[test]
    fn test_route_table_wrong_mask_returns_false() {
        let data = make_route_table("01B254E9", "FFFFFF00");
        assert!(!MulticastShredCheckService::route_table_contains_multicast(
            &data
        ));
    }

    #[test]
    fn test_route_table_short_line_skipped() {
        let data = "Iface\tDestination\tGateway\n\
                    eth0\t01B254E9\n"; // fewer than 8 fields
        assert!(!MulticastShredCheckService::route_table_contains_multicast(
            data
        ));
    }

    #[test]
    fn test_route_table_empty_returns_false() {
        assert!(!MulticastShredCheckService::route_table_contains_multicast(
            ""
        ));
    }

    #[test]
    fn test_route_table_header_only_returns_false() {
        let data =
            "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n";
        assert!(!MulticastShredCheckService::route_table_contains_multicast(
            data
        ));
    }

    /// Testnet route (0AB254E9) must NOT match when checking for mainnet address.
    #[test]
    fn test_route_table_testnet_dest_does_not_match_mainnet() {
        let data = make_route_table("0AB254E9", MULTICAST_ROUTE_MASK_HEX);
        assert!(!MulticastShredCheckService::route_table_contains_multicast(
            &data
        ));
    }

    /// find_multicast_iface must match the testnet address specifically.
    #[test]
    fn test_find_multicast_iface_testnet() {
        let data = make_route_table("0AB254E9", MULTICAST_ROUTE_MASK_HEX);
        assert_eq!(
            MulticastShredCheckService::find_multicast_iface(&data, MULTICAST_SHRED_ADDR_TESTNET),
            Some("eth0".to_string()),
        );
    }

    /// A table with both routes must resolve each address to its own entry.
    #[test]
    fn test_find_multicast_iface_both_routes() {
        let data = format!(
            "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n\
             eth0\t01B254E9\t00000000\t0001\t0\t0\t0\tFFFFFFFF\t0\t0\t0\n\
             eth1\t0AB254E9\t00000000\t0001\t0\t0\t0\tFFFFFFFF\t0\t0\t0\n"
        );
        assert_eq!(
            MulticastShredCheckService::find_multicast_iface(&data, MULTICAST_SHRED_ADDR_MAINNET),
            Some("eth0".to_string()),
        );
        assert_eq!(
            MulticastShredCheckService::find_multicast_iface(&data, MULTICAST_SHRED_ADDR_TESTNET),
            Some("eth1".to_string()),
        );
    }

    // ── check_and_enable (insertion logic) ───────────────────────────────────

    fn run_check(addrs: &Arc<ArcSwap<ShredReceiverAddresses>>, state: &mut CheckState) {
        MulticastShredCheckService::check_and_enable(addrs, MULTICAST_SHRED_ADDR_MAINNET, state);
    }

    fn run_check_testnet(addrs: &Arc<ArcSwap<ShredReceiverAddresses>>, state: &mut CheckState) {
        MulticastShredCheckService::check_and_enable(addrs, MULTICAST_SHRED_ADDR_TESTNET, state);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_no_route_does_not_add_address() {
        let addrs = Arc::new(ArcSwap::from_pointee(ShredReceiverAddresses::new()));
        let mut state = CheckState::default();
        run_check(&addrs, &mut state);
        assert!(addrs.load().is_empty());
        assert!(!state.route_detected);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_no_route_does_not_add_testnet_address() {
        let addrs = Arc::new(ArcSwap::from_pointee(ShredReceiverAddresses::new()));
        let mut state = CheckState::default();
        run_check_testnet(&addrs, &mut state);
        assert!(addrs.load().is_empty());
        assert!(!state.route_detected);
    }

    #[test]
    fn test_max_addresses_prevents_insertion() {
        let mut full_addrs = ShredReceiverAddresses::new();
        for i in 0..MAX_SHRED_RECEIVER_ADDRESSES {
            full_addrs.push(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                10_000 + i as u16,
            ));
        }
        let addrs = Arc::new(ArcSwap::from_pointee(full_addrs));
        assert_eq!(addrs.load().len(), MAX_SHRED_RECEIVER_ADDRESSES);
        let current = addrs.load();
        assert!(current.len() >= MAX_SHRED_RECEIVER_ADDRESSES);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_address_already_present_is_noop() {
        let mut initial = ShredReceiverAddresses::new();
        initial.push(MULTICAST_SHRED_ADDR_MAINNET);
        let addrs = Arc::new(ArcSwap::from_pointee(initial));
        let mut state = CheckState::default();
        run_check(&addrs, &mut state);
        assert_eq!(addrs.load().len(), 1);
        assert!(addrs.load().contains(&MULTICAST_SHRED_ADDR_MAINNET));
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_testnet_address_already_present_is_noop() {
        let mut initial = ShredReceiverAddresses::new();
        initial.push(MULTICAST_SHRED_ADDR_TESTNET);
        let addrs = Arc::new(ArcSwap::from_pointee(initial));
        let mut state = CheckState::default();
        run_check_testnet(&addrs, &mut state);
        assert_eq!(addrs.load().len(), 1);
        assert!(addrs.load().contains(&MULTICAST_SHRED_ADDR_TESTNET));
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_route_gone_removes_address() {
        let mut initial = ShredReceiverAddresses::new();
        initial.push(MULTICAST_SHRED_ADDR_MAINNET);
        let addrs = Arc::new(ArcSwap::from_pointee(initial));
        let mut state = CheckState { route_detected: true };
        run_check(&addrs, &mut state);
        assert!(addrs.load().is_empty());
        assert!(!state.route_detected);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_route_gone_removes_testnet_address() {
        let mut initial = ShredReceiverAddresses::new();
        initial.push(MULTICAST_SHRED_ADDR_TESTNET);
        let addrs = Arc::new(ArcSwap::from_pointee(initial));
        let mut state = CheckState { route_detected: true };
        run_check_testnet(&addrs, &mut state);
        assert!(addrs.load().is_empty());
        assert!(!state.route_detected);
    }

    /// Removing the mainnet address must not touch the testnet address.
    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_route_gone_only_removes_own_address() {
        let mut initial = ShredReceiverAddresses::new();
        initial.push(MULTICAST_SHRED_ADDR_MAINNET);
        initial.push(MULTICAST_SHRED_ADDR_TESTNET);
        let addrs = Arc::new(ArcSwap::from_pointee(initial));
        let mut state = CheckState { route_detected: true };
        run_check(&addrs, &mut state); // only mainnet check loses its route
        assert_eq!(addrs.load().len(), 1);
        assert!(addrs.load().contains(&MULTICAST_SHRED_ADDR_TESTNET));
        assert!(!addrs.load().contains(&MULTICAST_SHRED_ADDR_MAINNET));
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_no_route_no_address_is_noop() {
        let other_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8000);
        let mut initial = ShredReceiverAddresses::new();
        initial.push(other_addr);
        let addrs = Arc::new(ArcSwap::from_pointee(initial));
        let mut state = CheckState::default();
        run_check(&addrs, &mut state);
        assert_eq!(addrs.load().len(), 1);
        assert!(addrs.load().contains(&other_addr));
    }
}
