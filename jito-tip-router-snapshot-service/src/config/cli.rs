use {
    super::TipRouterSnapshotConfig,
    clap::{Arg, ArgMatches},
    solana_clap_utils::input_validators::{is_parsable, is_pubkey},
    solana_pubkey::Pubkey,
    std::{path::PathBuf, str::FromStr},
};

const ENABLE_ARG: &str = "enable_tip_router_snapshot_service";
const OUTPUT_DIR_ARG: &str = "tip_router_snapshot_output_dir";
const NCN_ARG: &str = "tip_router_snapshot_ncn";
const TIP_ROUTER_PROGRAM_ID_ARG: &str = "tip_router_snapshot_tip_router_program_id";
const TIP_DISTRIBUTION_PROGRAM_ID_ARG: &str = "tip_router_snapshot_tip_distribution_program_id";
const PRIORITY_FEE_DISTRIBUTION_PROGRAM_ID_ARG: &str =
    "tip_router_snapshot_priority_fee_distribution_program_id";
const TIP_PAYMENT_PROGRAM_ID_ARG: &str = "tip_router_snapshot_tip_payment_program_id";
const MAX_CANDIDATES_ARG: &str = "tip_router_snapshot_max_candidates";

pub fn args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![
        Arg::with_name(ENABLE_ARG)
            .long("enable-tip-router-snapshot-service")
            .takes_value(false)
            .requires(OUTPUT_DIR_ARG)
            .help("Enable the Jito tip-router snapshot service"),
        Arg::with_name(OUTPUT_DIR_ARG)
            .long("tip-router-snapshot-output-dir")
            .value_name("PATH")
            .takes_value(true)
            .requires(ENABLE_ARG)
            .help("Directory for tip-router snapshot artifacts"),
        Arg::with_name(NCN_ARG)
            .long("tip-router-snapshot-ncn")
            .value_name("PUBKEY")
            .takes_value(true)
            .validator(is_pubkey)
            .requires(ENABLE_ARG)
            .help("NCN account for tip-router snapshot filtering"),
        Arg::with_name(TIP_ROUTER_PROGRAM_ID_ARG)
            .long("tip-router-snapshot-tip-router-program-id")
            .value_name("PUBKEY")
            .takes_value(true)
            .validator(is_pubkey)
            .requires(ENABLE_ARG)
            .help("Tip-router program id"),
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
        Arg::with_name(MAX_CANDIDATES_ARG)
            .long("tip-router-snapshot-max-candidates")
            .value_name("N")
            .takes_value(true)
            .validator(is_parsable::<usize>)
            .requires(ENABLE_ARG)
            .help("Maximum number of tip-router snapshot candidates to collect"),
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
        ncn: parse_optional_pubkey(matches, NCN_ARG)?,
        tip_router_program_id: parse_optional_pubkey(matches, TIP_ROUTER_PROGRAM_ID_ARG)?,
        tip_distribution_program_id: parse_optional_pubkey(
            matches,
            TIP_DISTRIBUTION_PROGRAM_ID_ARG,
        )?,
        priority_fee_distribution_program_id: parse_optional_pubkey(
            matches,
            PRIORITY_FEE_DISTRIBUTION_PROGRAM_ID_ARG,
        )?,
        tip_payment_program_id: parse_optional_pubkey(matches, TIP_PAYMENT_PROGRAM_ID_ARG)?,
        max_candidates: parse_optional_usize(matches, MAX_CANDIDATES_ARG)?,
    }))
}

fn parse_optional_pubkey(
    matches: &ArgMatches,
    arg_name: &str,
) -> Result<Option<Pubkey>, clap::Error> {
    matches
        .value_of(arg_name)
        .map(Pubkey::from_str)
        .transpose()
        .map_err(|err| {
            clap::Error::with_description(
                &format!("failed to parse {arg_name} as pubkey: {err}"),
                clap::ErrorKind::InvalidValue,
            )
        })
}

fn parse_optional_usize(
    matches: &ArgMatches,
    arg_name: &str,
) -> Result<Option<usize>, clap::Error> {
    matches
        .value_of(arg_name)
        .map(str::parse)
        .transpose()
        .map_err(|err| {
            clap::Error::with_description(
                &format!("failed to parse {arg_name} as usize: {err}"),
                clap::ErrorKind::InvalidValue,
            )
        })
}
