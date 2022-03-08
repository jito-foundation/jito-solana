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

mod backoff;
pub mod recv_verify_stage;
pub mod tpu_proxy_advertiser;
