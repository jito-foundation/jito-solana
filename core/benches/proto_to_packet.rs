#![feature(test)]

extern crate test;

use {
    jito_protos::proto::packet::{
        Meta as PbMeta, Packet as PbPacket, PacketBatch, PacketFlags as PbFlags,
    },
    solana_core::proto_packet_to_packet,
    solana_sdk::packet::{Packet, PACKET_DATA_SIZE},
    std::iter::repeat,
    test::{black_box, Bencher},
};

fn get_proto_packet(i: u8) -> PbPacket {
    PbPacket {
        data: repeat(i).take(PACKET_DATA_SIZE).collect(),
        meta: Some(PbMeta {
            size: PACKET_DATA_SIZE as u64,
            addr: "255.255.255.255:65535".to_string(),
            port: 65535,
            flags: Some(PbFlags {
                discard: false,
                forwarded: false,
                repair: false,
                simple_vote_tx: false,
                tracer_packet: false,
            }),
            sender_stake: 0,
        }),
    }
}

#[bench]
fn bench_proto_to_packet(bencher: &mut Bencher) {
    bencher.iter(|| {
        black_box(proto_packet_to_packet(get_proto_packet(1)));
    });
}

#[bench]
fn bench_batch_list_to_packets(bencher: &mut Bencher) {
    let packet_batch = PacketBatch {
        packets: (0..128).map(get_proto_packet).collect(),
    };

    bencher.iter(|| {
        black_box(
            packet_batch
                .packets
                .iter()
                .map(|p| proto_packet_to_packet(p.clone()))
                .collect::<Vec<Packet>>(),
        );
    });
}
