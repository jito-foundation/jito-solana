#[cfg(feature = "tip-router")]
mod implementation {
    use {
        clap::{Arg, ArgMatches},
        jito_tip_router_snapshot_service::{
            config::{TipRouterSnapshotConfig, cli},
            notification_filter::TipRouterEpochBoundaryFilter,
            service::TipRouterSnapshotService,
        },
        solana_rpc::optimistically_confirmed_bank_tracker::{
            BankNotificationReceiver, BankNotificationSender,
        },
        std::sync::{Arc, atomic::AtomicBool},
    };

    pub struct ServiceSetup {
        config: TipRouterSnapshotConfig,
        receiver: BankNotificationReceiver,
    }

    pub type Service = TipRouterSnapshotService;

    pub fn args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
        cli::args()
    }

    pub fn setup(
        matches: &ArgMatches<'_>,
        senders: &mut Vec<BankNotificationSender>,
    ) -> Result<Option<ServiceSetup>, Box<dyn std::error::Error>> {
        let Some(config) = cli::config_from_matches(matches)? else {
            return Ok(None);
        };

        let (sender, receiver) = BankNotificationSender::channel_with_filter(
            "tip-router-snapshot-service",
            TipRouterEpochBoundaryFilter,
        );
        senders.push(sender);

        Ok(Some(ServiceSetup { config, receiver }))
    }

    pub fn start(
        service_setup: Option<ServiceSetup>,
        exit: Arc<AtomicBool>,
    ) -> Result<Option<Service>, Box<dyn std::error::Error>> {
        let Some(ServiceSetup { config, receiver }) = service_setup else {
            return Ok(None);
        };

        Ok(Some(TipRouterSnapshotService::init(
            config, receiver, exit,
        )?))
    }

    pub fn join(service: Option<Service>) {
        if let Some(service) = service {
            match service.join().expect("tip_router_snapshot_service") {
                Ok(()) => {}
                Err(err) => log::error!("tip_router_snapshot_service exited with error: {err}"),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use {
            super::*,
            crate::{cli::DefaultArgs, commands::run::args::add_args},
            clap::App,
            solana_pubkey::Pubkey,
        };

        #[test]
        fn tip_router_snapshot_service_requires_no_voting() {
            let default_args = DefaultArgs::default();
            let matches =
                add_args(App::new("run_command"), &default_args).get_matches_from_safe(vec![
                    "run_command",
                    "--enable-tip-router-snapshot-service",
                    "--tip-router-snapshot-output-dir",
                    "tip-router-artifacts",
                ]);
            assert!(matches.is_err());

            let matches =
                add_args(App::new("run_command"), &default_args).get_matches_from_safe(vec![
                    "run_command",
                    "--no-voting",
                    "--enable-tip-router-snapshot-service",
                    "--tip-router-snapshot-output-dir",
                    "tip-router-artifacts",
                ]);
            assert!(matches.is_ok());
        }

        #[test]
        fn setup_adds_filtered_notification_sender() {
            let default_args = DefaultArgs::default();
            let program_id = Pubkey::new_unique().to_string();
            let matches = add_args(App::new("run_command"), &default_args)
                .get_matches_from_safe(vec![
                    "run_command",
                    "--no-voting",
                    "--enable-tip-router-snapshot-service",
                    "--tip-router-snapshot-output-dir",
                    "tip-router-artifacts",
                    "--tip-router-snapshot-tip-distribution-program-id",
                    &program_id,
                    "--tip-router-snapshot-priority-fee-distribution-program-id",
                    &program_id,
                    "--tip-router-snapshot-tip-payment-program-id",
                    &program_id,
                ])
                .unwrap();
            let mut senders = Vec::new();

            let service_setup = setup(&matches, &mut senders).unwrap();

            assert!(service_setup.is_some());
            assert_eq!(senders.len(), 1);
        }
    }
}

#[cfg(not(feature = "tip-router"))]
mod implementation {
    use {
        clap::{Arg, ArgMatches},
        solana_rpc::optimistically_confirmed_bank_tracker::BankNotificationSender,
        std::sync::{Arc, atomic::AtomicBool},
    };

    pub struct ServiceSetup;
    pub struct Service;

    pub fn args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
        Vec::new()
    }

    pub fn setup(
        _matches: &ArgMatches<'_>,
        _senders: &mut Vec<BankNotificationSender>,
    ) -> Result<Option<ServiceSetup>, Box<dyn std::error::Error>> {
        Ok(None)
    }

    pub fn start(
        _service_setup: Option<ServiceSetup>,
        _exit: Arc<AtomicBool>,
    ) -> Result<Option<Service>, Box<dyn std::error::Error>> {
        Ok(None)
    }

    pub fn join(_service: Option<Service>) {}

    #[cfg(test)]
    mod tests {
        use {super::*, clap::App, std::sync::atomic::AtomicBool};

        #[test]
        fn disabled_tip_router_is_a_no_op() {
            let matches = App::new("run_command").get_matches_from(vec!["run_command"]);
            let mut senders = Vec::new();

            let service_setup = setup(&matches, &mut senders).unwrap();
            let service = start(service_setup, Arc::new(AtomicBool::new(false))).unwrap();
            join(service);

            assert!(args().is_empty());
            assert!(senders.is_empty());
        }
    }
}

pub use implementation::*;
