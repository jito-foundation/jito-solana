#![cfg(feature = "agave-unstable-api")]
#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]
#![allow(clippy::arithmetic_side_effects)]
#![recursion_limit = "2048"]
//! The `solana` library implements the Solana high-performance blockchain architecture.
//! It includes a full Rust implementation of the architecture (see
//! [Validator](validator/struct.Validator.html))).  It also includes
//! command-line tools to spin up validators and a Rust library
//!

pub mod admin_rpc_post_init;
pub mod bam_connection;
pub mod bam_dependencies;
pub mod bam_manager;
pub mod banking_simulation;
pub mod banking_stage;
pub mod banking_trace;
pub(crate) mod block_creation_loop;
pub mod bundle;
mod bundle_sigverify_stage;
pub mod bundle_stage;
pub mod cluster_info_vote_listener;
pub mod cluster_slots_service;
pub mod commitment_service;
pub mod completed_data_sets_service;
pub mod consensus;
pub mod cost_update_service;
pub mod drop_bank_service;
pub mod epoch_specs;
pub mod fetch_stage;
pub mod forwarding_stage;
pub mod gen_keys;
pub mod multicast_shred_check_service;
pub mod next_leader;
pub mod optimistic_confirmation_verifier;
pub mod packet_bundle;
pub mod proxy;
pub mod repair;
pub mod replay_stage;
pub mod resource_limits;
mod result;
pub mod sample_performance_service;
#[cfg(unix)]
mod scheduler_bindings_server;
mod shred_fetch_stage;
pub mod sigverify;
pub mod sigverify_stage;
pub mod snapshot_packager_service;
pub mod staked_nodes_updater_service;
pub mod stats_reporter_service;
pub mod system_monitor_service;
pub mod tip_manager;
mod tonic_endpoint;
pub mod tpu;
mod tpu_entry_notifier;
mod transaction_priority;
pub mod tvu;
pub mod unfrozen_gossip_verified_vote_hashes;
pub mod validator;
pub mod vote_simulator;
pub mod voting_service;
pub mod warm_quic_cache_service;
pub mod window_service;

#[macro_use]
extern crate log;

#[macro_use]
extern crate solana_metrics;

#[cfg_attr(feature = "frozen-abi", macro_use)]
#[cfg(feature = "frozen-abi")]
extern crate solana_frozen_abi_macro;

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

use {
    solana_packet::{Meta, PacketFlags},
    solana_perf::packet::BytesPacket,
    std::net::{IpAddr, Ipv4Addr},
};

const UNKNOWN_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

// NOTE: last profiled at around 180ns
pub fn proto_packet_to_packet(p: jito_protos::proto::packet::Packet) -> BytesPacket {
    let mut data = p.data;
    data.truncate(solana_message::v1::MAX_TRANSACTION_SIZE);
    let mut packet = BytesPacket::new(data, Meta::default());

    if let Some(meta) = p.meta {
        packet.meta_mut().size = meta.size as usize;
        packet.meta_mut().addr = meta.addr.parse().unwrap_or(UNKNOWN_IP);
        packet.meta_mut().port = meta.port as u16;
        if let Some(flags) = meta.flags {
            if flags.simple_vote_tx {
                packet.meta_mut().flags.insert(PacketFlags::SIMPLE_VOTE_TX);
            }
            if flags.forwarded {
                packet.meta_mut().flags.insert(PacketFlags::FORWARDED);
            }
            if flags.repair {
                packet.meta_mut().flags.insert(PacketFlags::REPAIR);
            }
            if flags.from_staked_node {
                packet
                    .meta_mut()
                    .flags
                    .insert(PacketFlags::FROM_STAKED_NODE);
            }
        }
    }
    packet
}

#[cfg(test)]
mod proto_packet_to_packet_tests {
    use {
        super::*,
        jito_protos::proto::packet::{Meta as ProtoMeta, Packet as ProtoPacket},
    };

    fn proto_with_len(len: usize) -> ProtoPacket {
        ProtoPacket {
            data: vec![7u8; len],
            meta: Some(ProtoMeta {
                size: len as u64,
                addr: "0.0.0.0".to_string(),
                port: 0,
                flags: None,
                sender_stake: 0,
            }),
        }
    }

    #[test]
    fn txv1_sized_packet_is_not_truncated() {
        // 2000 bytes: bigger than the old PACKET_DATA_SIZE (1232), within txv1 max (4096)
        let packet = proto_packet_to_packet(proto_with_len(2000));
        assert_eq!(packet.data(..).unwrap().len(), 2000);
    }

    #[test]
    fn packet_over_txv1_max_is_capped_at_max_transaction_size() {
        let packet = proto_packet_to_packet(proto_with_len(10_000));
        assert_eq!(
            packet.data(..).unwrap().len(),
            solana_message::v1::MAX_TRANSACTION_SIZE
        );
    }

    #[test]
    fn legacy_sized_packet_is_unchanged() {
        let packet = proto_packet_to_packet(proto_with_len(1000));
        assert_eq!(packet.data(..).unwrap().len(), 1000);
    }
}
