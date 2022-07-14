use tonic_build::configure;

fn main() {
    configure()
        .compile(
            &[
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
