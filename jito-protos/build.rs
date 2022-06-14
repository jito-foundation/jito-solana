use tonic_build::configure;

fn main() {
    configure()
        .compile(
            &[
                "protos/block.proto",
                "protos/bundle.proto",
                "protos/packet.proto",
                "protos/searcher.proto",
                "protos/shared.proto",
                "protos/validator_interface_service.proto",
            ],
            &["protos"],
        )
        .unwrap();
}
