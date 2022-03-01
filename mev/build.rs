use tonic_build::configure;

fn main() {
    configure()
        .compile(
            &[
                "protos/packet.proto",
                "protos/shared.proto",
                "protos/validator_interface_service.proto",
            ],
            &["protos"],
        )
        .unwrap();
}
