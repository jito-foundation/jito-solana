#![allow(clippy::arithmetic_side_effects)]
mod mock_bam_server;

use {
    crate::mock_bam_server::MockBamServer,
    arc_swap::ArcSwap,
    clap::{App, Arg, crate_description, crate_name, value_t},
    crossbeam_channel::unbounded,
    log::*,
    solana_accounts_db::accounts_db::AccountsDbConfig,
    solana_core::{
        bam_dependencies::{BamConnectionState, BamDependencies},
        banking_channels::Channels,
        banking_stage::{
            BankingStage, transaction_scheduler::scheduler_controller::SchedulerConfig,
        },
        bundle_stage::bundle_account_locker::BundleAccountLocker,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        validator::BlockProductionMethod,
    },
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_keypair::Keypair,
    solana_leader_schedule::SlotLeader,
    solana_ledger::{
        blockstore::Blockstore,
        genesis_utils::{GenesisConfigInfo, create_genesis_config},
        get_tmp_ledger_path_auto_delete,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_net_utils::SocketAddrSpace,
    solana_poh::{
        poh_recorder::PohRecorder,
        record_channels::{RecordReceiver, record_channels},
        transaction_recorder::TransactionRecorder,
    },
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank, bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
        runtime_config::RuntimeConfig,
    },
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    solana_time_utils::timestamp,
    std::{
        assert_matches,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, AtomicU8, Ordering},
        },
        thread::{sleep, spawn},
        time::{Duration, Instant},
    },
    tokio::sync::mpsc,
};

/// Slot length used by the bench; nothing ticks PoH to end a slot.
const SLOT_DURATION: Duration = Duration::from_millis(400);

#[allow(clippy::cognitive_complexity)]
fn main() {
    agave_logger::setup();
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("num_keypairs")
                .long("num-keypairs")
                .takes_value(true)
                .help("Number of keypairs")
                .default_value("1000"),
        )
        .arg(
            Arg::with_name("test_duration")
                .long("test-duration")
                .takes_value(true)
                .help("Test duration in seconds")
                .default_value("60"),
        )
        .get_matches();

    let test_duration = value_t!(matches, "test_duration", u64).unwrap();

    let mint_total = 10_000 * 1_000_000_000; // 10k SOL
    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(mint_total);

    let exit_for_bank = Arc::new(AtomicBool::new(false));
    let bank0 = Bank::new_from_genesis(
        &genesis_config,
        Arc::new(RuntimeConfig::default()),
        vec![],
        None,
        AccountsDbConfig::default(),
        None,
        None,
        exit_for_bank,
        None,
        None,
    );
    let bank_forks = BankForks::new_rw_arc(bank0);
    let bank = bank_forks.read().unwrap().working_bank_with_scheduler();

    // set cost tracker limits to MAX so it will not filter out TXs
    bank.write_cost_tracker().unwrap().set_limits_max();

    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore = Arc::new(
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger"),
    );
    let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
    let exit = Arc::new(AtomicBool::new(false));
    let (mut poh_recorder, _entry_receiver) = PohRecorder::new(
        bank.tick_height(),
        bank.last_blockhash(),
        bank.clone(),
        Some((4, 4)),
        bank.ticks_per_slot(),
        blockstore.clone(),
        &leader_schedule_cache,
        &genesis_config.poh_config,
        exit.clone(),
    );
    poh_recorder.set_bank(bank.clone_with_scheduler());
    let poh_recorder = Arc::new(RwLock::new(poh_recorder));
    let (record_sender, mut record_receiver) = record_channels(false);
    record_receiver.restart(bank.bank_id());
    let transaction_recorder = TransactionRecorder::new(record_sender);
    let prioritization_fee_cache = Arc::new(PrioritizationFeeCache::new(0u64));

    // create a mock bam server
    let (batch_sender, batch_receiver) = unbounded();
    let (outbound_sender, outbound_receiver) = tokio::sync::mpsc::channel(100_000);
    let keypair = Keypair::new();
    let bam_dependencies = BamDependencies {
        bam_enabled: Arc::new(AtomicU8::new(BamConnectionState::Connected as u8)),
        batch_sender: batch_sender.clone(),
        batch_receiver,
        outbound_sender,
        cluster_info: Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&keypair.pubkey(), timestamp()),
            Arc::new(keypair),
            SocketAddrSpace::new(true),
        )),
        block_builder_fee_info: Arc::new(ArcSwap::from_pointee(BlockBuilderFeeInfo::default())),
        bank_forks: bank_forks.clone(),
        bam_node_pubkey: Arc::new(ArcSwap::from_pointee(Pubkey::new_unique())),
        bam_tpu_info: Arc::new(ArcSwap::new(Arc::new(None))),
        bam_shred_receiver_addresses: Arc::default(),
    };

    let keypairs = (0..value_t!(matches, "num_keypairs", usize).unwrap())
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
        ..
    } = Channels::new();

    let banking_stage = BankingStage::new_num_threads(
        // this doesn't matter for the BAM test
        BlockProductionMethod::CentralScheduler,
        poh_recorder.clone(),
        transaction_recorder,
        non_vote_receiver,
        // this doesn't matter for the BAM test
        mpsc::channel(1).1,
        BankingStage::default_num_workers(),
        SchedulerConfig::default(),
        None,
        None,
        None,
        bank_forks.clone(),
        agave_votor::slot_clock::SharedAlpenglowSlotClock::default(),
        Some(prioritization_fee_cache),
        Arc::default(),
        Arc::new(agave_banking_stage_ingress_types::SchedulerPriorityFloor::new()),
        BundleAccountLocker::default(),
        None,
        Some(bam_dependencies),
    );

    let bank_setting_thread = {
        let bank_forks = bank_forks.clone();
        let poh_recorder = poh_recorder.clone();
        let exit = exit.clone();
        spawn(move || bank_setting_loop(bank_forks, poh_recorder, record_receiver, exit.clone()))
    };

    sleep(Duration::from_secs(test_duration));

    exit.store(true, Ordering::Relaxed);
    drop(non_vote_sender);
    banking_stage.join().unwrap();
    debug!("waited for banking_stage");
    mock_bam_server.join().unwrap();
    bank_setting_thread.join().unwrap();
}

/// Ends each slot on a timer and installs the next bank, standing in for `BlockCreationLoop`.
fn bank_setting_loop(
    bank_forks: Arc<RwLock<BankForks>>,
    poh_recorder: Arc<RwLock<PohRecorder>>,
    mut record_receiver: RecordReceiver,
    exit: Arc<AtomicBool>,
) {
    let mut bank = bank_forks.read().unwrap().working_bank_with_scheduler();
    let mut last_bank_time = Instant::now();
    let mut bank_transaction_count = bank.transaction_count();
    let mut total_txs = 0;

    while !exit.load(Ordering::Relaxed) {
        if let Ok(record) = record_receiver
            .inner()
            .recv_timeout(Duration::from_millis(10))
        {
            record_receiver.on_received_record();
            total_txs += record.transactions.len();
        }

        if last_bank_time.elapsed() >= SLOT_DURATION {
            // Stop admitting records, then count the ones workers had already reserved.
            record_receiver.shutdown();
            total_txs += record_receiver
                .drain_after_shutdown()
                .map(|record| record.transactions.len())
                .sum::<usize>();

            let new_bank_transaction_count = bank.transaction_count();
            eprintln!(
                "[bank: {} done, tx count: {}, elapsed: {}ms total_txs: {}]",
                bank.slot(),
                new_bank_transaction_count - bank_transaction_count,
                last_bank_time.elapsed().as_millis(),
                total_txs
            );

            {
                let mut poh_recorder = poh_recorder.write().unwrap();
                poh_recorder.clear_bank(true);
                poh_recorder.reset(bank.clone(), Some((bank.slot(), bank.slot() + 1)));
            }

            if let Some((result, _timings)) = bank.wait_for_completed_scheduler() {
                assert_matches!(result, Ok(_));
            }

            let new_slot = bank.slot() + 1;
            let new_bank = Bank::new_from_parent(bank.clone(), SlotLeader::new_unique(), new_slot);
            let tpu_bank = bank_forks.write().unwrap().insert(new_bank);
            let bank_id = tpu_bank.bank_id();
            poh_recorder.write().unwrap().set_bank(tpu_bank);
            record_receiver.restart(bank_id);
            bank = bank_forks.read().unwrap().working_bank_with_scheduler();

            last_bank_time = Instant::now();
            bank_transaction_count = new_bank_transaction_count;
        }
    }
}
