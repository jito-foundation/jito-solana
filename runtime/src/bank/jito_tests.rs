#![cfg(test)]

use {
    super::{test_utils::goto_end_of_slot, *},
    assert_matches::assert_matches,
    solana_account::state_traits::StateMut,
    solana_instruction::{Instruction, error::InstructionError},
    solana_loader_v3_interface::{
        get_program_data_address,
        instruction::{self as loader_v3_instruction},
        state::UpgradeableLoaderState,
    },
    solana_message::Message,
    solana_native_token::LAMPORTS_PER_SOL,
    solana_program_runtime::program_cache_entry::ProgramCacheEntryType,
    solana_sdk_ids::{bpf_loader, bpf_loader_upgradeable},
    solana_signer::Signer,
    std::sync::{Arc, RwLock},
};

fn create_genesis_config(lamports: u64) -> (GenesisConfig, Keypair) {
    solana_genesis_config::create_genesis_config(lamports)
}

fn create_genesis_config_no_tx_fee(lamports: u64) -> (GenesisConfig, Keypair) {
    let (mut genesis_config, mint_keypair) = create_genesis_config(lamports);
    genesis_config.fee_rate_governor = FeeRateGovernor::new(0, 0);
    (genesis_config, mint_keypair)
}

fn new_from_parent_with_fork_next_slot(parent: Arc<Bank>, fork: &RwLock<BankForks>) -> Arc<Bank> {
    let slot = parent.slot() + 1;
    Bank::new_from_parent_with_bank_forks(fork, parent, SlotLeader::default(), slot)
}

fn simulate_program_cache_test_tx(
    bank: &Bank,
    instructions: Vec<Instruction>,
    signers: &[&Keypair],
) -> RuntimeTransaction<SanitizedTransaction> {
    let message = Message::new(&instructions, Some(&signers[0].pubkey()));
    RuntimeTransaction::from_transaction_for_tests(Transaction::new(
        signers,
        message,
        bank.last_blockhash(),
    ))
}

fn simulate_loader_v3_deploy_test_txs(
    bank: &Bank,
    payer_keypair: &Keypair,
    program_keypair: &Keypair,
    buffer_keypair: &Keypair,
    upgrade_authority_keypair: &Keypair,
    program_data: &[u8],
) -> Vec<RuntimeTransaction<SanitizedTransaction>> {
    const WRITE_CHUNK_SIZE: usize = 700;

    let program_len = program_data.len();
    let program_lamports =
        bank.get_minimum_balance_for_rent_exemption(UpgradeableLoaderState::size_of_program());
    let buffer_lamports = bank.get_minimum_balance_for_rent_exemption(
        UpgradeableLoaderState::size_of_buffer(program_len),
    );

    let mut transactions = vec![simulate_program_cache_test_tx(
        bank,
        loader_v3_instruction::create_buffer(
            &payer_keypair.pubkey(),
            &buffer_keypair.pubkey(),
            &upgrade_authority_keypair.pubkey(),
            buffer_lamports,
            program_len,
        )
        .unwrap(),
        &[payer_keypair, buffer_keypair],
    )];

    for (offset, chunk) in program_data.chunks(WRITE_CHUNK_SIZE).enumerate() {
        transactions.push(simulate_program_cache_test_tx(
            bank,
            vec![loader_v3_instruction::write(
                &buffer_keypair.pubkey(),
                &upgrade_authority_keypair.pubkey(),
                (offset * WRITE_CHUNK_SIZE) as u32,
                chunk.to_vec(),
            )],
            &[payer_keypair, upgrade_authority_keypair],
        ));
    }

    transactions.push(simulate_program_cache_test_tx(
        bank,
        loader_v3_instruction::deploy_with_max_program_len(
            &payer_keypair.pubkey(),
            &program_keypair.pubkey(),
            &buffer_keypair.pubkey(),
            &upgrade_authority_keypair.pubkey(),
            program_lamports,
            program_len,
        )
        .unwrap(),
        &[payer_keypair, program_keypair, upgrade_authority_keypair],
    ));

    transactions
}

fn store_loader_v3_program(
    bank: &Bank,
    program_key: &Pubkey,
    deployment_slot: Slot,
    upgrade_authority: Option<Pubkey>,
    program_data: &[u8],
) {
    let programdata_key = get_program_data_address(program_key);
    let mut program_account = AccountSharedData::new(
        bank.get_minimum_balance_for_rent_exemption(UpgradeableLoaderState::size_of_program()),
        UpgradeableLoaderState::size_of_program(),
        &bpf_loader_upgradeable::id(),
    );
    program_account
        .set_state(&UpgradeableLoaderState::Program {
            programdata_address: programdata_key,
        })
        .unwrap();
    program_account.set_executable(true);

    let programdata_len = UpgradeableLoaderState::size_of_programdata(program_data.len());
    let mut programdata_account = AccountSharedData::new(
        bank.get_minimum_balance_for_rent_exemption(programdata_len),
        programdata_len,
        &bpf_loader_upgradeable::id(),
    );
    programdata_account
        .set_state(&UpgradeableLoaderState::ProgramData {
            slot: deployment_slot,
            upgrade_authority_address: upgrade_authority,
        })
        .unwrap();
    let programdata = programdata_account.data_as_mut_slice();
    programdata[UpgradeableLoaderState::size_of_programdata_metadata()..]
        .copy_from_slice(program_data);

    bank.store_account(program_key, &program_account);
    bank.store_account(&programdata_key, &programdata_account);
}

#[test]
fn test_simulate_transactions_unchecked_with_pre_accounts_does_not_populate_global_program_cache() {
    let (genesis_config, mint_keypair) = create_genesis_config(100_000);
    let (root_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

    let program_key = Pubkey::new_unique();
    let program_data = include_bytes!("../../../programs/bpf_loader/test_elfs/out/noop_aligned.so");
    let program_account = AccountSharedData::from(Account {
        lamports: Rent::default().minimum_balance(program_data.len()).min(1),
        data: program_data.to_vec(),
        owner: bpf_loader::id(),
        executable: true,
        rent_epoch: 0,
    });
    root_bank.store_account(&program_key, &program_account);

    goto_end_of_slot(root_bank.clone());
    let bank = new_from_parent_with_fork_next_slot(root_bank, bank_forks.as_ref());

    {
        let program_cache = bank
            .transaction_processor
            .global_program_cache
            .read()
            .unwrap();
        assert!(
            program_cache
                .get_slot_versions_for_tests(&program_key)
                .is_empty(),
            "newly stored program must not exist in global cache before simulation",
        );
    }

    let instruction = Instruction::new_with_bytes(program_key, &[], Vec::new());
    let message = Message::new(&[instruction], Some(&mint_keypair.pubkey()));
    let binding = mint_keypair.insecure_clone();
    let transaction = Transaction::new(&[&binding], message, bank.last_blockhash());
    let sanitized = RuntimeTransaction::from_transaction_for_tests(transaction);

    let simulation_results = bank.simulate_transactions_unchecked_with_pre_accounts(
        &[sanitized],
        &vec![vec![]],
        &vec![vec![]],
        None,
    );
    assert_eq!(simulation_results.len(), 1);
    assert_eq!(
        simulation_results[0].1.result,
        Ok(()),
        "simulation should execute successfully with a valid program",
    );

    {
        let program_cache = bank
            .transaction_processor
            .global_program_cache
            .read()
            .unwrap();
        assert!(
            program_cache
                .get_slot_versions_for_tests(&program_key)
                .is_empty(),
            "simulate_transactions_unchecked_with_pre_accounts must not populate global program \
             cache",
        );
    }
}

#[test]
fn test_simulate_bundle_deploy_then_invoke_does_not_poison_global_program_cache() {
    let (genesis_config, mint_keypair) = create_genesis_config_no_tx_fee(10 * LAMPORTS_PER_SOL);
    let (root_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
    goto_end_of_slot(root_bank.clone());
    let bank = new_from_parent_with_fork_next_slot(root_bank, bank_forks.as_ref());

    let program_data = include_bytes!("../../../programs/bpf_loader/test_elfs/out/noop_aligned.so");
    let program_keypair = Keypair::new();
    let buffer_keypair = Keypair::new();

    let mut transactions = simulate_loader_v3_deploy_test_txs(
        &bank,
        &mint_keypair,
        &program_keypair,
        &buffer_keypair,
        &mint_keypair,
        program_data,
    );
    transactions.push(simulate_program_cache_test_tx(
        &bank,
        vec![Instruction::new_with_bytes(
            program_keypair.pubkey(),
            &[],
            Vec::new(),
        )],
        &[&mint_keypair],
    ));

    let requested_accounts = vec![vec![]; transactions.len()];
    let simulation_results = bank.simulate_transactions_unchecked_with_pre_accounts(
        &transactions,
        &requested_accounts,
        &requested_accounts,
        None,
    );
    assert_eq!(simulation_results.len(), transactions.len());
    assert!(
        simulation_results
            .iter()
            .take(simulation_results.len() - 1)
            .all(|(_pre_accounts, result, _post_accounts)| result.result.is_ok())
    );
    assert_eq!(
        simulation_results.last().unwrap().1.result,
        Err(TransactionError::InstructionError(
            0,
            InstructionError::UnsupportedProgramId
        )),
    );
    {
        let program_cache = bank
            .transaction_processor
            .global_program_cache
            .read()
            .unwrap();
        assert!(
            program_cache
                .get_slot_versions_for_tests(&program_keypair.pubkey())
                .is_empty(),
            "simulation must not populate the global program cache",
        );
    }

    store_loader_v3_program(
        &bank,
        &program_keypair.pubkey(),
        bank.slot(),
        Some(mint_keypair.pubkey()),
        &[0; 123],
    );

    goto_end_of_slot(bank.clone());
    let bank = new_from_parent_with_fork_next_slot(bank, bank_forks.as_ref());
    let invoke_transaction = Transaction::new(
        &[&mint_keypair],
        Message::new(
            &[Instruction::new_with_bytes(
                program_keypair.pubkey(),
                &[],
                Vec::new(),
            )],
            Some(&mint_keypair.pubkey()),
        ),
        bank.last_blockhash(),
    );
    assert_eq!(
        bank.process_transaction(&invoke_transaction),
        Err(TransactionError::InstructionError(
            0,
            InstructionError::UnsupportedProgramId
        )),
    );
}

#[test]
fn test_simulate_bundle_close_tombstone_applies_to_later_simulated_invoke() {
    let (genesis_config, mint_keypair) = create_genesis_config_no_tx_fee(10 * LAMPORTS_PER_SOL);
    let (root_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
    goto_end_of_slot(root_bank.clone());
    let bank = new_from_parent_with_fork_next_slot(root_bank, bank_forks.as_ref());

    let program_data = include_bytes!("../../../programs/bpf_loader/test_elfs/out/noop_aligned.so");
    let program_key = Pubkey::new_unique();
    let upgrade_authority = Keypair::new();
    let programdata_key = get_program_data_address(&program_key);
    store_loader_v3_program(
        &bank,
        &program_key,
        bank.slot() - 1,
        Some(upgrade_authority.pubkey()),
        program_data,
    );

    let invoke_message = Message::new(
        &[Instruction::new_with_bytes(program_key, &[], Vec::new())],
        Some(&mint_keypair.pubkey()),
    );
    let invoke_transaction = Transaction::new(
        &[&mint_keypair],
        invoke_message.clone(),
        bank.last_blockhash(),
    );
    assert_eq!(bank.process_transaction(&invoke_transaction), Ok(()));
    {
        let program_cache = bank
            .transaction_processor
            .global_program_cache
            .read()
            .unwrap();
        let slot_versions = program_cache.get_slot_versions_for_tests(&program_key);
        assert_eq!(slot_versions.len(), 1);
        assert_matches!(slot_versions[0].program, ProgramCacheEntryType::Loaded(_));
    }

    goto_end_of_slot(bank.clone());
    let bank = new_from_parent_with_fork_next_slot(bank, bank_forks.as_ref());

    let transactions = vec![
        simulate_program_cache_test_tx(
            &bank,
            vec![loader_v3_instruction::close_any(
                &programdata_key,
                &mint_keypair.pubkey(),
                Some(&upgrade_authority.pubkey()),
                Some(&program_key),
            )],
            &[&mint_keypair, &upgrade_authority],
        ),
        simulate_program_cache_test_tx(
            &bank,
            vec![Instruction::new_with_bytes(program_key, &[], Vec::new())],
            &[&mint_keypair],
        ),
    ];
    let requested_accounts = vec![vec![]; transactions.len()];
    let simulation_results = bank.simulate_transactions_unchecked_with_pre_accounts(
        &transactions,
        &requested_accounts,
        &requested_accounts,
        None,
    );

    assert_eq!(simulation_results.len(), transactions.len());
    assert_eq!(simulation_results[0].1.result, Ok(()));
    assert_eq!(
        simulation_results[1].1.result,
        Err(TransactionError::InstructionError(
            0,
            InstructionError::UnsupportedProgramId
        )),
    );
    {
        let program_cache = bank
            .transaction_processor
            .global_program_cache
            .read()
            .unwrap();
        let slot_versions = program_cache.get_slot_versions_for_tests(&program_key);
        assert_eq!(slot_versions.len(), 1);
        assert_matches!(slot_versions[0].program, ProgramCacheEntryType::Loaded(_));
    }
}
