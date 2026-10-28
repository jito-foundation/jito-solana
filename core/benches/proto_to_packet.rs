use {
    criterion::{Criterion, criterion_group, criterion_main},
    jito_protos::proto::packet::{
        Meta as PbMeta, Packet as PbPacket, PacketBatch, PacketFlags as PbFlags,
    },
    solana_core::proto_packet_to_packet,
    solana_packet::PACKET_DATA_SIZE,
    solana_perf::packet::BytesPacket,
    std::{hint::black_box, iter::repeat_n},
};

fn get_proto_packet(i: u8) -> PbPacket {
    PbPacket {
        data: repeat_n(i, PACKET_DATA_SIZE).collect::<Vec<_>>().into(),
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
                from_staked_node: false,
            }),
            sender_stake: 0,
        }),
    }
}

fn bench_proto_to_packet(c: &mut Criterion) {
    c.bench_function("proto_to_packet", |b| {
        b.iter(|| black_box(proto_packet_to_packet(get_proto_packet(1))));
    });
}

fn bench_batch_list_to_packets(c: &mut Criterion) {
    let packet_batch = PacketBatch {
        packets: (0..128).map(get_proto_packet).collect(),
    };

    c.bench_function("batch_list_to_packets", |b| {
        b.iter(|| {
            black_box(
                packet_batch
                    .packets
                    .iter()
                    .map(|p| proto_packet_to_packet(p.clone()))
                    .collect::<Vec<BytesPacket>>(),
            )
        });
    });
}

criterion_group!(benches, bench_proto_to_packet, bench_batch_list_to_packets);
criterion_main!(benches);
