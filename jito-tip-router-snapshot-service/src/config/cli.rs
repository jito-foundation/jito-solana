use {
    super::TipRouterSnapshotConfig,
    clap::{Arg, ArgMatches},
    solana_clap_utils::input_validators::is_pubkey,
    solana_pubkey::Pubkey,
    std::{path::PathBuf, str::FromStr},
};

const ENABLE_ARG: &str = "enable_tip_router_snapshot_service";
const OUTPUT_DIR_ARG: &str = "tip_router_snapshot_output_dir";
const TIP_DISTRIBUTION_PROGRAM_ID_ARG: &str = "tip_router_snapshot_tip_distribution_program_id";
const PRIORITY_FEE_DISTRIBUTION_PROGRAM_ID_ARG: &str =
    "tip_router_snapshot_priority_fee_distribution_program_id";
const TIP_PAYMENT_PROGRAM_ID_ARG: &str = "tip_router_snapshot_tip_payment_program_id";

pub fn args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![
        Arg::with_name(ENABLE_ARG)
            .long("enable-tip-router-snapshot-service")
            .takes_value(false)
            .requires("no_voting")
            .requires(OUTPUT_DIR_ARG)
            .help("Enable the Jito tip-router snapshot service; requires --no-voting"),
        Arg::with_name(OUTPUT_DIR_ARG)
            .long("tip-router-snapshot-output-dir")
            .value_name("PATH")
            .takes_value(true)
            .requires(ENABLE_ARG)
            .help("Directory for tip-router snapshot artifacts"),
        Arg::with_name(TIP_DISTRIBUTION_PROGRAM_ID_ARG)
            .long("tip-router-snapshot-tip-distribution-program-id")
            .value_name("PUBKEY")
            .takes_value(true)
            .validator(is_pubkey)
            .requires(ENABLE_ARG)
            .help("Tip-distribution program id"),
        Arg::with_name(PRIORITY_FEE_DISTRIBUTION_PROGRAM_ID_ARG)
            .long("tip-router-snapshot-priority-fee-distribution-program-id")
            .value_name("PUBKEY")
            .takes_value(true)
            .validator(is_pubkey)
            .requires(ENABLE_ARG)
            .help("Priority-fee distribution program id"),
        Arg::with_name(TIP_PAYMENT_PROGRAM_ID_ARG)
            .long("tip-router-snapshot-tip-payment-program-id")
            .value_name("PUBKEY")
            .takes_value(true)
            .validator(is_pubkey)
            .requires(ENABLE_ARG)
            .help("Tip-payment program id"),
    ]
}

pub fn config_from_matches(
    matches: &ArgMatches,
) -> Result<Option<TipRouterSnapshotConfig>, clap::Error> {
    if !matches.is_present(ENABLE_ARG) {
        return Ok(None);
    }

    let output_dir = matches.value_of(OUTPUT_DIR_ARG).ok_or_else(|| {
        clap::Error::with_description(
            "The --tip-router-snapshot-output-dir <PATH> argument is required when \
             --enable-tip-router-snapshot-service is supplied",
            clap::ErrorKind::ArgumentNotFound,
        )
    })?;

    Ok(Some(TipRouterSnapshotConfig {
        output_dir: PathBuf::from(output_dir),
        tip_distribution_program_id: parse_required_pubkey(
            matches,
            TIP_DISTRIBUTION_PROGRAM_ID_ARG,
        )?,
        priority_fee_distribution_program_id: parse_required_pubkey(
            matches,
            PRIORITY_FEE_DISTRIBUTION_PROGRAM_ID_ARG,
        )?,
        tip_payment_program_id: parse_required_pubkey(matches, TIP_PAYMENT_PROGRAM_ID_ARG)?,
    }))
}

fn parse_required_pubkey(matches: &ArgMatches, arg_name: &str) -> Result<Pubkey, clap::Error> {
    let value = matches.value_of(arg_name).ok_or_else(|| {
        clap::Error::with_description(
            &format!(
                "The --{} <PUBKEY> argument is required when --enable-tip-router-snapshot-service \
                 is supplied",
                arg_name.replace('_', "-"),
            ),
            clap::ErrorKind::ArgumentNotFound,
        )
    })?;

    Pubkey::from_str(value).map_err(|err| {
        clap::Error::with_description(
            &format!("failed to parse {arg_name} as pubkey: {err}"),
            clap::ErrorKind::InvalidValue,
        )
    })
}
