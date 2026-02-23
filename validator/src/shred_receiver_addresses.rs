use {
    solana_turbine::ShredReceiverAddresses,
    std::net::{SocketAddr, ToSocketAddrs},
};

pub const MAX_SHRED_RECEIVER_ADDRESSES: usize = 32;

fn push_unique_addr(
    addrs: &mut ShredReceiverAddresses,
    socket_addr: SocketAddr,
) -> Result<(), String> {
    if addrs.contains(&socket_addr) {
        return Ok(());
    }
    if addrs.len() >= MAX_SHRED_RECEIVER_ADDRESSES {
        return Err(format!(
            "too many shred receiver addresses: maximum is {MAX_SHRED_RECEIVER_ADDRESSES}"
        ));
    }
    addrs.push(socket_addr);
    Ok(())
}

pub fn parse_shred_receiver_addresses<I: IntoIterator<Item = S>, S: AsRef<str>>(
    values: I,
) -> Result<ShredReceiverAddresses, String> {
    let mut addrs = ShredReceiverAddresses::new();
    let mut saw_non_empty_value = false;
    for value in values {
        let value = value.as_ref().trim();
        if value.is_empty() {
            continue;
        }
        saw_non_empty_value = true;
        for addr in value.split(',').map(str::trim) {
            if addr.is_empty() {
                continue;
            }
            // Avoid DNS resolution for direct socket addresses.
            if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
                push_unique_addr(&mut addrs, socket_addr)?;
                continue;
            }
            let resolved = addr
                .to_socket_addrs()
                .map_err(|err| format!("Unable to resolve host {addr}: {err}"))?;
            let mut resolved_any_ipv4 = false;
            for socket_addr in resolved {
                if socket_addr.is_ipv4() {
                    resolved_any_ipv4 = true; // avoid ipv6 due to XDP path will panic
                    push_unique_addr(&mut addrs, socket_addr)?;
                }
            }
            if !resolved_any_ipv4 {
                return Err(format!(
                    "Unable to resolve host to any IPv4 address: {addr}"
                ));
            }
        }
    }
    // Treat an empty-only input set as an explicit disable.
    if !saw_non_empty_value {
        return Ok(ShredReceiverAddresses::new());
    }
    Ok(addrs)
}

#[cfg(test)]
mod tests {
    use {super::*, std::net::SocketAddr};

    #[test]
    fn test_parse_empty() {
        let addrs = parse_shred_receiver_addresses([""]).unwrap();
        assert!(addrs.is_empty());
    }

    #[test]
    fn test_parse_multiple_empty_values_disable() {
        let addrs = parse_shred_receiver_addresses(["", "   "]).unwrap();
        assert!(addrs.is_empty());
    }

    #[test]
    fn test_parse_multiple_and_dedup() {
        let addrs =
            parse_shred_receiver_addresses(["127.0.0.1:9001,127.0.0.1:9002", "127.0.0.1:9001"])
                .unwrap();
        let expected = [
            "127.0.0.1:9001".parse().unwrap(),
            "127.0.0.1:9002".parse().unwrap(),
        ];
        assert_eq!(addrs.as_slice(), &expected);
    }

    #[test]
    fn test_parse_hostname_with_dns_resolution_ipv4_only() {
        let addrs = parse_shred_receiver_addresses(["localhost:9003"]).unwrap();
        assert!(!addrs.is_empty());
        assert!(addrs.iter().all(SocketAddr::is_ipv4));
        assert!(addrs.iter().all(|addr| addr.port() == 9003));
    }

    #[test]
    fn test_parse_explicit_ipv6_socket_addr() {
        let addrs = parse_shred_receiver_addresses(["[::1]:9003"]).unwrap();
        assert_eq!(addrs.len(), 1);
        assert!(addrs[0].is_ipv6());
        assert_eq!(addrs[0].port(), 9003);
    }

    #[test]
    fn test_parse_invalid() {
        assert!(parse_shred_receiver_addresses(["not-a-host"]).is_err());
    }

    #[test]
    fn test_parse_max_addresses() {
        let values = (0..=MAX_SHRED_RECEIVER_ADDRESSES)
            .map(|index| format!("127.0.0.1:{}", 10_000 + index))
            .collect::<Vec<_>>();
        let err = parse_shred_receiver_addresses(values.iter().map(String::as_str)).unwrap_err();
        assert!(err.contains(&MAX_SHRED_RECEIVER_ADDRESSES.to_string()));
    }
}
