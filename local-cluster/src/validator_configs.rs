use {
    solana_core::validator::ValidatorConfig,
    solana_sdk::exit::Exit,
    std::sync::{Arc, RwLock},
};

pub fn safe_clone_config(config: &ValidatorConfig) -> ValidatorConfig {
    ValidatorConfig {
        halt_at_slot: config.halt_at_slot,
        expected_genesis_hash: config.expected_genesis_hash,
        expected_bank_hash: config.expected_bank_hash,
        expected_shred_version: config.expected_shred_version,
        voting_disabled: config.voting_disabled,
        account_paths: config.account_paths.clone(),
        account_snapshot_paths: config.account_snapshot_paths.clone(),
        rpc_config: config.rpc_config.clone(),
        on_start_geyser_plugin_config_files: config.on_start_geyser_plugin_config_files.clone(),
        geyser_plugin_always_enabled: config.geyser_plugin_always_enabled,
        rpc_addrs: config.rpc_addrs,
        pubsub_config: config.pubsub_config.clone(),
        snapshot_config: config.snapshot_config.clone(),
        max_ledger_shreds: config.max_ledger_shreds,
        blockstore_options: config.blockstore_options.clone(),
        broadcast_stage_type: config.broadcast_stage_type.clone(),
        turbine_disabled: config.turbine_disabled.clone(),
        fixed_leader_schedule: config.fixed_leader_schedule.clone(),
        wait_for_supermajority: config.wait_for_supermajority,
        new_hard_forks: config.new_hard_forks.clone(),
        known_validators: config.known_validators.clone(),
        repair_validators: config.repair_validators.clone(),
        repair_whitelist: config.repair_whitelist.clone(),
        gossip_validators: config.gossip_validators.clone(),
        max_genesis_archive_unpacked_size: config.max_genesis_archive_unpacked_size,
        run_verification: config.run_verification,
        require_tower: config.require_tower,
        tower_storage: config.tower_storage.clone(),
        debug_keys: config.debug_keys.clone(),
        contact_debug_interval: config.contact_debug_interval,
        contact_save_interval: config.contact_save_interval,
        send_transaction_service_config: config.send_transaction_service_config.clone(),
        no_poh_speed_test: config.no_poh_speed_test,
        no_os_memory_stats_reporting: config.no_os_memory_stats_reporting,
        no_os_network_stats_reporting: config.no_os_network_stats_reporting,
        no_os_cpu_stats_reporting: config.no_os_cpu_stats_reporting,
        no_os_disk_stats_reporting: config.no_os_disk_stats_reporting,
        poh_pinned_cpu_core: config.poh_pinned_cpu_core,
        warp_slot: config.warp_slot,
        accounts_db_test_hash_calculation: config.accounts_db_test_hash_calculation,
        accounts_db_skip_shrink: config.accounts_db_skip_shrink,
        accounts_db_force_initial_clean: config.accounts_db_force_initial_clean,
        tpu_coalesce: config.tpu_coalesce,
        staked_nodes_overrides: config.staked_nodes_overrides.clone(),
        validator_exit: Arc::new(RwLock::new(Exit::default())),
        poh_hashes_per_batch: config.poh_hashes_per_batch,
        process_ledger_before_services: config.process_ledger_before_services,
        no_wait_for_vote_to_start_leader: config.no_wait_for_vote_to_start_leader,
        accounts_db_config: config.accounts_db_config.clone(),
        wait_to_vote_slot: config.wait_to_vote_slot,
        runtime_config: config.runtime_config.clone(),
        banking_trace_dir_byte_limit: config.banking_trace_dir_byte_limit,
        block_verification_method: config.block_verification_method.clone(),
        block_production_method: config.block_production_method.clone(),
        transaction_struct: config.transaction_struct.clone(),
        enable_block_production_forwarding: config.enable_block_production_forwarding,
        generator_config: config.generator_config.clone(),
        use_snapshot_archives_at_startup: config.use_snapshot_archives_at_startup,
        wen_restart_proto_path: config.wen_restart_proto_path.clone(),
        wen_restart_coordinator: config.wen_restart_coordinator,
        unified_scheduler_handler_threads: config.unified_scheduler_handler_threads,
        ip_echo_server_threads: config.ip_echo_server_threads,
        rayon_global_threads: config.rayon_global_threads,
        replay_forks_threads: config.replay_forks_threads,
        replay_transactions_threads: config.replay_transactions_threads,
        tvu_shred_sigverify_threads: config.tvu_shred_sigverify_threads,
        delay_leader_block_for_pending_fork: config.delay_leader_block_for_pending_fork,
        use_tpu_client_next: config.use_tpu_client_next,
        relayer_config: config.relayer_config.clone(),
        block_engine_config: config.block_engine_config.clone(),
        shred_receiver_address: config.shred_receiver_address.clone(),
        shred_retransmit_receiver_address: config.shred_retransmit_receiver_address.clone(),
        tip_manager_config: config.tip_manager_config.clone(),
        preallocated_bundle_cost: config.preallocated_bundle_cost,
    }
}

pub fn make_identical_validator_configs(
    config: &ValidatorConfig,
    num: usize,
) -> Vec<ValidatorConfig> {
    let mut configs = vec![];
    for _ in 0..num {
        configs.push(safe_clone_config(config));
    }
    configs
}
