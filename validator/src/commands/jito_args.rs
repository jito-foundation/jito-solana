use clap::Arg;

pub fn block_engine_url<'a>() -> Arg<'a, 'a> {
    Arg::with_name("block_engine_url")
        .long("block-engine-url")
        .help(
            "URL entrypoint to the Block Engine. Connected Block Engine will be autoconfigured \
             unless `--disable-block-engine-autoconfig` is used. Set to empty string to disable \
             block engine connection.",
        )
        .takes_value(true)
}

pub fn disable_block_engine_autoconfig<'a>() -> Arg<'a, 'a> {
    Arg::with_name("disable_block_engine_autoconfig")
        .long("disable-block-engine-autoconfig")
        .takes_value(false)
        .help(
            "Disables Block Engine auto-configuration. This stops the validator client from using \
             the most performant Block Engine region. Values provided to `--block-engine-url` \
             will be used as-is.",
        )
}

pub fn trust_block_engine_packets<'a>() -> Arg<'a, 'a> {
    Arg::with_name("trust_block_engine_packets")
        .long("trust-block-engine-packets")
        .takes_value(false)
        .help(
            "Skip signature verification on block engine packets. Not recommended unless the \
             block engine is trusted.",
        )
}

pub fn relayer_url<'a>() -> Arg<'a, 'a> {
    Arg::with_name("relayer_url")
        .long("relayer-url")
        .help("Relayer url. Set to empty string to disable relayer connection.")
        .takes_value(true)
}

pub fn relayer_expected_heartbeat_interval_ms<'a>(default_value: &'a str) -> Arg<'a, 'a> {
    Arg::with_name("relayer_expected_heartbeat_interval_ms")
        .long("relayer-expected-heartbeat-interval-ms")
        .takes_value(true)
        .help("Interval at which the Relayer is expected to send heartbeat messages.")
        .default_value(default_value)
}

pub fn relayer_max_failed_heartbeats<'a>(default_value: &'a str) -> Arg<'a, 'a> {
    Arg::with_name("relayer_max_failed_heartbeats")
        .long("relayer-max-failed-heartbeats")
        .takes_value(true)
        .help(
            "Maximum number of heartbeats the Relayer can miss before falling back to the normal \
             TPU pipeline.",
        )
        .default_value(default_value)
}

pub fn shred_receiver_address<'a>() -> Arg<'a, 'a> {
    Arg::with_name("shred_receiver_address")
        .long("shred-receiver-address")
        .value_name("SHRED_RECEIVER_ADDRESS")
        .takes_value(true)
        .multiple(true)
        .number_of_values(1)
        .help(
            "Validator will mirror this validator's own broadcast shreds to these addresses in \
             addition to normal turbine operation. This covers the direct leader path and \
             replay-triggered rebroadcasts of this validator's slots. Pass this option multiple \
             times or use comma-separated ip:port or host:port entries. Hostnames resolve to IPv4 \
             addresses only. Up to 32 unique addresses are allowed. Set to empty string to \
             configure an empty explicit receiver list.",
        )
}

pub fn shred_retransmit_receiver_address<'a>() -> Arg<'a, 'a> {
    Arg::with_name("shred_retransmit_receiver_address")
        .long("shred-retransmit-receiver-address")
        .value_name("SHRED_RETRANSMIT_RECEIVER_ADDRESS")
        .takes_value(true)
        .multiple(true)
        .number_of_values(1)
        .help(
            "Validator will mirror TVU retransmit-stage shreds to these addresses in addition to \
             normal turbine operation. This applies only to shreds that enter retransmit; it does \
             not mirror this validator's own leader broadcast path. Pass this option multiple \
             times or use comma-separated ip:port or host:port entries. Hostnames resolve to IPv4 \
             addresses only. Up to 32 unique addresses are allowed. Set to empty string to \
             disable.",
        )
}
