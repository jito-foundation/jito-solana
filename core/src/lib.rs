#![cfg_attr(RUSTC_WITH_SPECIALIZATION, feature(min_specialization))]
#![allow(clippy::arithmetic_side_effects)]
#![recursion_limit = "2048"]
//! The `solana` library implements the Solana high-performance blockchain architecture.
//! It includes a full Rust implementation of the architecture (see
//! [Validator](validator/struct.Validator.html)) as well as hooks to GPU implementations of its most
//! paralellizable components (i.e. [SigVerify](sigverify/index.html)).  It also includes
//! command-line tools to spin up validators and a Rust library
//!

pub mod accounts_hash_verifier;
pub mod admin_rpc_post_init;
pub mod banking_stage;
pub mod banking_trace;
pub mod bundle_stage;
pub mod cache_block_meta_service;
pub mod cluster_info_vote_listener;
pub mod cluster_slots_service;
pub mod commitment_service;
pub mod completed_data_sets_service;
pub mod consensus;
pub mod consensus_cache_updater;
pub mod cost_update_service;
pub mod drop_bank_service;
pub mod fetch_stage;
pub mod gen_keys;
pub mod immutable_deserialized_bundle;
pub mod ledger_cleanup_service;
pub mod ledger_metric_report_service;
pub mod next_leader;
pub mod optimistic_confirmation_verifier;
pub mod packet_bundle;
pub mod poh_timing_report_service;
pub mod poh_timing_reporter;
pub mod proxy;
pub mod repair;
pub mod replay_stage;
mod result;
pub mod rewards_recorder_service;
pub mod sample_performance_service;
mod shred_fetch_stage;
pub mod sigverify;
pub mod sigverify_stage;
pub mod snapshot_packager_service;
pub mod staked_nodes_updater_service;
pub mod stats_reporter_service;
pub mod system_monitor_service;
pub mod tip_manager;
pub mod tpu;
mod tpu_entry_notifier;
pub mod tracer_packet_stats;
pub mod tvu;
pub mod unfrozen_gossip_verified_vote_hashes;
pub mod validator;
pub mod verified_vote_packets;
pub mod vote_simulator;
pub mod voting_service;
pub mod warm_quic_cache_service;
pub mod window_service;

#[macro_use]
extern crate eager;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate solana_metrics;

#[macro_use]
extern crate solana_frozen_abi_macro;

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

use {
    solana_sdk::packet::{Meta, Packet, PacketFlags, PACKET_DATA_SIZE},
    std::{
        cmp::min,
        net::{IpAddr, Ipv4Addr},
    },
};

const UNKNOWN_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

// NOTE: last profiled at around 180ns
pub fn proto_packet_to_packet(p: jito_protos::proto::packet::Packet) -> Packet {
    let mut data = [0; PACKET_DATA_SIZE];
    let copy_len = min(data.len(), p.data.len());
    data[..copy_len].copy_from_slice(&p.data[..copy_len]);
    let mut packet = Packet::new(data, Meta::default());
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
            if flags.tracer_packet {
                packet.meta_mut().flags.insert(PacketFlags::TRACER_PACKET);
            }
            if flags.repair {
                packet.meta_mut().flags.insert(PacketFlags::REPAIR);
            }
        }
    }
    packet
}
