use {
    solana_sdk::packet::{Packet, PacketFlags, PACKET_DATA_SIZE},
    std::{
        cmp::min,
        net::{IpAddr, Ipv4Addr},
    },
};

pub mod proto {
    pub mod bundle {
        tonic::include_proto!("bundle");
    }

    pub mod packet {
        tonic::include_proto!("packet");
    }

    pub mod searcher {
        tonic::include_proto!("searcher");
    }

    pub mod shared {
        tonic::include_proto!("shared");
    }

    pub mod validator_interface {
        tonic::include_proto!("validator_interface");
    }
}

mod backoff;
pub mod blocking_proxy_client;
pub mod bundle;
pub mod bundle_scheduler;
pub mod mev_stage;

const UNKNOWN_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

// NOTE: last profiled at around 180ns
pub fn proto_packet_to_packet(p: proto::packet::Packet) -> Packet {
    let mut data = [0; PACKET_DATA_SIZE];
    let copy_len = min(data.len(), p.data.len());
    data[..copy_len].copy_from_slice(&p.data[..copy_len]);
    let mut packet = Packet::new(data, Default::default());
    if let Some(meta) = p.meta {
        packet.meta.size = meta.size as usize;
        packet.meta.addr = meta.addr.parse().unwrap_or(UNKNOWN_IP);
        packet.meta.port = meta.port as u16;
        if let Some(flags) = meta.flags {
            if flags.simple_vote_tx {
                packet.meta.flags.insert(PacketFlags::SIMPLE_VOTE_TX);
            }
            if flags.forwarded {
                packet.meta.flags.insert(PacketFlags::FORWARDED);
            }
            if flags.tracer_tx {
                packet.meta.flags.insert(PacketFlags::TRACER_TX);
            }
            if flags.repair {
                packet.meta.flags.insert(PacketFlags::REPAIR);
            }
        }
    }
    packet
}
