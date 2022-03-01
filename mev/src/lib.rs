pub mod packet {
    tonic::include_proto!("packet");
}

pub mod shared {
    tonic::include_proto!("shared");
}

pub mod validator_interface {
    tonic::include_proto!("validator_interface");
}

mod backoff;
pub mod recv_verify_stage;
pub mod tpu_proxy_advertiser;
