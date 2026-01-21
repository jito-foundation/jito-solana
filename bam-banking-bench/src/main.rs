#![allow(clippy::arithmetic_side_effects)]
mod mock_bam_server;

use {
    crate::mock_bam_server::MockBamServer,
    assert_matches::assert_matches,
    clap::{crate_description, crate_name, Arg, Command},
    crossbeam_channel::{unbounded, Receiver},
    log::*,
    solana_core::{
        bam_dependencies::{BamConnectionState, BamDependencies},
        banking_stage::{
            transaction_scheduler::scheduler_controller::SchedulerConfig,
            update_bank_forks_and_poh_recorder_for_new_tpu_bank, BankingStage,
        },
        banking_trace::{BankingTracer, Channels},
        bundle_stage::bundle_account_locker::BundleAccountLocker,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        validator::BlockProductionMethod,
    },
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_keypair::Keypair,
    solana_ledger::{
        blockstore::Blockstore,
        genesis_utils::{create_genesis_config, GenesisConfigInfo},
        get_tmp_ledger_path_auto_delete,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_net_utils::SocketAddrSpace,
    solana_poh::{
        poh_controller::PohController,
        poh_recorder::{create_test_recorder, PohRecorder, WorkingBankEntry},
    },
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank, bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
    },
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    solana_time_utils::timestamp,
    std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicBool, AtomicU8, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{sleep, spawn},
        time::{Duration, Instant},
    },
    tokio::sync::mpsc,
};

#[allow(clippy::cognitive_complexity)]
fn main() {
    agave_logger::setup();
    let matches = Command::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::new("num_keypairs")
                .long("num-keypairs")
                .takes_value(true)
                .help("Number of keypairs")
                .default_value("1000"),
        )
        .arg(
            Arg::new("test_duration")
                .long("test-duration")
                .takes_value(true)
                .help("Test duration in seconds")
                .default_value("60"),
        )
        .get_matches();

    let test_duration = matches.value_of_t::<u64>("test_duration").unwrap();

    let mint_total = 10_000 * 1_000_000_000; // 10k SOL
    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(mint_total);

    let (replay_vote_sender, _replay_vote_receiver) = unbounded();
    let bank0 = Bank::new_for_benches(&genesis_config);
    let bank_forks = BankForks::new_rw_arc(bank0);
    let bank = bank_forks.read().unwrap().working_bank_with_scheduler();

    // set cost tracker limits to MAX so it will not filter out TXs
    bank.write_cost_tracker()
        .unwrap()
        .set_limits(u64::MAX, u64::MAX, u64::MAX);

    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore = Arc::new(
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger"),
    );
    let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
    let (exit, poh_recorder, poh_controller, transaction_recorder, poh_service, signal_receiver) =
        create_test_recorder(
            bank.clone(),
            blockstore.clone(),
            None,
            Some(leader_schedule_cache),
        );
    let (banking_tracer, tracer_thread) = BankingTracer::new(None).unwrap();
    let prioritization_fee_cache = Arc::new(PrioritizationFeeCache::new(0u64));

    // create a mock bam server
    let (batch_sender, batch_receiver) = unbounded();
    let (outbound_sender, outbound_receiver) = unbounded();
    let keypair = Keypair::new();
    let bam_dependencies = BamDependencies {
        bam_enabled: Arc::new(AtomicU8::new(BamConnectionState::Connected as u8)),
        batch_sender: batch_sender.clone(),
        batch_receiver,
        outbound_sender,
        outbound_receiver: outbound_receiver.clone(), // unused
        cluster_info: Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&keypair.pubkey(), timestamp()),
            Arc::new(keypair),
            SocketAddrSpace::new(true),
        )),
        block_builder_fee_info: Arc::new(Mutex::new(BlockBuilderFeeInfo::default())),
        bank_forks: bank_forks.clone(),
        bam_node_pubkey: Arc::new(Mutex::new(Pubkey::new_unique())),
    };

    let keypairs = (0..matches.value_of_t::<usize>("num_keypairs").unwrap())
        .map(|_| Keypair::new())
        .collect::<Vec<_>>();

    keypairs.iter().for_each(|k: &Keypair| {
        bank.process_transaction(&system_transaction::transfer(
            &mint_keypair,
            &k.pubkey(),
            1_000_000_000, // 1 SOL
            bank.last_blockhash(),
        ))
        .unwrap();
    });

    let shared_leader_state = poh_recorder.read().unwrap().shared_leader_state();
    let mock_bam_server = MockBamServer::run(
        batch_sender,
        outbound_receiver,
        shared_leader_state,
        exit.clone(),
        keypairs,
    );

    let Channels {
        non_vote_sender,
        non_vote_receiver,
        tpu_vote_sender,
        tpu_vote_receiver,
        gossip_vote_sender,
        gossip_vote_receiver,
    } = banking_tracer.create_channels(false);

    let banking_stage = BankingStage::new_num_threads(
        // this doesn't matter for the BAM test
        BlockProductionMethod::CentralScheduler,
        poh_recorder.clone(),
        transaction_recorder,
        non_vote_receiver,
        tpu_vote_receiver,
        gossip_vote_receiver,
        // this doesn't matter for the BAM test
        mpsc::channel(1).1,
        BankingStage::default_num_workers(),
        SchedulerConfig::default(),
        None,
        replay_vote_sender,
        None,
        bank_forks.clone(),
        prioritization_fee_cache,
        HashSet::default(),
        BundleAccountLocker::default(),
        None,
        Some(bam_dependencies),
    );

    let bank_setting_thread = {
        let bank_forks = bank_forks.clone();
        let poh_recorder = poh_recorder.clone();
        let exit = exit.clone();
        spawn(move || {
            bank_setting_loop(
                bank_forks,
                poh_recorder,
                poh_controller,
                signal_receiver,
                exit.clone(),
            )
        })
    };

    sleep(Duration::from_secs(test_duration));

    exit.store(true, Ordering::Relaxed);
    drop(non_vote_sender);
    drop(tpu_vote_sender);
    drop(gossip_vote_sender);
    banking_stage.join().unwrap();
    debug!("waited for banking_stage");
    poh_service.join().unwrap();
    sleep(Duration::from_secs(1));
    debug!("waited for poh_service");
    if let Some(tracer_thread) = tracer_thread {
        tracer_thread.join().unwrap().unwrap();
    }
    mock_bam_server.join().unwrap();
    bank_setting_thread.join().unwrap();
}

fn bank_setting_loop(
    bank_forks: Arc<RwLock<BankForks>>,
    poh_recorder: Arc<RwLock<PohRecorder>>,
    mut poh_controller: PohController,
    signal_receiver: Receiver<WorkingBankEntry>,
    exit: Arc<AtomicBool>,
) {
    let mut bank = bank_forks.read().unwrap().working_bank_with_scheduler();
    let mut last_bank_time = Instant::now();
    let mut bank_transaction_count = bank.transaction_count();
    let mut total_txs = 0;

    while !exit.load(Ordering::Relaxed) {
        if let Ok((_bank, (entry, _tick_height))) =
            signal_receiver.recv_timeout(Duration::from_millis(10))
        {
            total_txs += entry.transactions.len();
        }

        if poh_recorder.read().unwrap().bank().is_none() {
            let new_bank_transaction_count = bank.transaction_count();
            eprintln!(
                "[bank: {} done, tx count: {}, elapsed: {}ms total_txs: {}]",
                bank.slot(),
                new_bank_transaction_count - bank_transaction_count,
                last_bank_time.elapsed().as_millis(),
                total_txs
            );

            poh_recorder
                .write()
                .unwrap()
                .reset(bank.clone(), Some((bank.slot(), bank.slot() + 1)));

            if let Some((result, _timings)) = bank.wait_for_completed_scheduler() {
                assert_matches!(result, Ok(_));
            }

            let new_slot = bank.slot() + 1;
            let new_bank =
                Bank::new_from_parent(bank.clone(), &solana_pubkey::new_rand(), new_slot);

            assert_matches!(poh_recorder.read().unwrap().bank(), None);
            update_bank_forks_and_poh_recorder_for_new_tpu_bank(
                &bank_forks,
                &mut poh_controller,
                new_bank,
            );
            bank = bank_forks.read().unwrap().working_bank_with_scheduler();

            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(1)
                && poh_recorder.read().unwrap().bank().is_none()
            {}
            assert_matches!(poh_recorder.read().unwrap().bank(), Some(_));

            last_bank_time = Instant::now();
            bank_transaction_count = new_bank_transaction_count;
        }
    }
}
