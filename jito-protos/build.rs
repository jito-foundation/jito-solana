use tonic_build::configure;

fn main() {
    configure()
        .build_client(true)
        .build_server(false)
        .compile(
            &[
                "protos/auth.proto",
                "protos/block_engine.proto",
                "protos/bundle.proto",
                "protos/packet.proto",
                "protos/relayer.proto",
                "protos/shared.proto",
            ],
            &["protos"],
        )
        .unwrap();
}
