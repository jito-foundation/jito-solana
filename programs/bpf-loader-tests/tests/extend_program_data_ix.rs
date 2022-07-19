use {
    assert_matches::assert_matches,
    common::{add_upgradeable_loader_account, assert_ix_error, setup_test_context},
    solana_program_test::*,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        account_utils::StateMut,
        bpf_loader_upgradeable::{extend_program_data, id, UpgradeableLoaderState},
        instruction::InstructionError,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        system_instruction::{self, SystemError, MAX_PERMITTED_DATA_LENGTH},
        system_program,
        transaction::Transaction,
    },
};

mod common;

#[tokio::test]
async fn test_extend_program_data() {
    let mut context = setup_test_context().await;

    let program_data_address = Pubkey::new_unique();
    let program_data_len = 100;
    add_upgradeable_loader_account(
        &mut context,
        &program_data_address,
        &UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(Pubkey::new_unique()),
        },
        program_data_len,
    )
    .await;

    let client = &mut context.banks_client;
    let payer = &context.payer;
    let recent_blockhash = context.last_blockhash;
    const ADDITIONAL_BYTES: u32 = 42;
    let transaction = Transaction::new_signed_with_payer(
        &[extend_program_data(
            &program_data_address,
            Some(&payer.pubkey()),
            ADDITIONAL_BYTES,
        )],
        Some(&payer.pubkey()),
        &[payer],
        recent_blockhash,
    );

    assert_matches!(client.process_transaction(transaction).await, Ok(()));
    let updated_program_data_account = client
        .get_account(program_data_address)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        updated_program_data_account.data().len(),
        program_data_len + ADDITIONAL_BYTES as usize
    );
}

#[tokio::test]
async fn test_extend_program_data_not_upgradeable() {
    let mut context = setup_test_context().await;

    let program_data_address = Pubkey::new_unique();
    add_upgradeable_loader_account(
        &mut context,
        &program_data_address,
        &UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: None,
        },
        100,
    )
    .await;

    let payer_address = context.payer.pubkey();
    assert_ix_error(
        &mut context,
        extend_program_data(&program_data_address, Some(&payer_address), 42),
        None,
        InstructionError::Immutable,
        "should fail because the program data account isn't upgradeable",
    )
    .await;
}

#[tokio::test]
async fn test_extend_program_data_by_zero_bytes() {
    let mut context = setup_test_context().await;

    let program_data_address = Pubkey::new_unique();
    add_upgradeable_loader_account(
        &mut context,
        &program_data_address,
        &UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(Pubkey::new_unique()),
        },
        100,
    )
    .await;

    let payer_address = context.payer.pubkey();
    assert_ix_error(
        &mut context,
        extend_program_data(&program_data_address, Some(&payer_address), 0),
        None,
        InstructionError::InvalidInstructionData,
        "should fail because the program data account must be extended by more than 0 bytes",
    )
    .await;
}

#[tokio::test]
async fn test_extend_program_data_past_max_size() {
    let mut context = setup_test_context().await;

    let program_data_address = Pubkey::new_unique();
    add_upgradeable_loader_account(
        &mut context,
        &program_data_address,
        &UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(Pubkey::new_unique()),
        },
        MAX_PERMITTED_DATA_LENGTH as usize,
    )
    .await;

    let payer_address = context.payer.pubkey();
    assert_ix_error(
        &mut context,
        extend_program_data(&program_data_address, Some(&payer_address), 1),
        None,
        InstructionError::InvalidRealloc,
        "should fail because the program data account cannot be extended past the max data size",
    )
    .await;
}

#[tokio::test]
async fn test_extend_program_data_with_invalid_payer() {
    let mut context = setup_test_context().await;
    let rent = context.banks_client.get_rent().await.unwrap();

    let program_data_address = Pubkey::new_unique();
    add_upgradeable_loader_account(
        &mut context,
        &program_data_address,
        &UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(Pubkey::new_unique()),
        },
        100,
    )
    .await;

    let payer_with_sufficient_funds = Keypair::new();
    context.set_account(
        &payer_with_sufficient_funds.pubkey(),
        &AccountSharedData::new(10_000_000_000, 0, &system_program::id()),
    );

    let payer_with_insufficient_funds = Keypair::new();
    context.set_account(
        &payer_with_insufficient_funds.pubkey(),
        &AccountSharedData::new(rent.minimum_balance(0), 0, &system_program::id()),
    );

    let payer_with_invalid_owner = Keypair::new();
    context.set_account(
        &payer_with_invalid_owner.pubkey(),
        &AccountSharedData::new(rent.minimum_balance(0), 0, &id()),
    );

    assert_ix_error(
        &mut context,
        extend_program_data(
            &program_data_address,
            Some(&payer_with_insufficient_funds.pubkey()),
            1024,
        ),
        Some(&payer_with_insufficient_funds),
        InstructionError::from(SystemError::ResultWithNegativeLamports),
        "should fail because the payer has insufficient funds to cover program data account rent",
    )
    .await;

    assert_ix_error(
        &mut context,
        extend_program_data(
            &program_data_address,
            Some(&payer_with_invalid_owner.pubkey()),
            1,
        ),
        Some(&payer_with_invalid_owner),
        InstructionError::ExternalAccountLamportSpend,
        "should fail because the payer is not a system account",
    )
    .await;

    let mut ix = extend_program_data(
        &program_data_address,
        Some(&payer_with_sufficient_funds.pubkey()),
        1,
    );

    // Demote payer account meta to non-signer so that transaction signing succeeds
    {
        let payer_meta = ix
            .accounts
            .iter_mut()
            .find(|meta| meta.pubkey == payer_with_sufficient_funds.pubkey())
            .expect("expected to find payer account meta");
        payer_meta.is_signer = false;
    }

    assert_ix_error(
        &mut context,
        ix,
        None,
        InstructionError::PrivilegeEscalation,
        "should fail because the payer did not sign",
    )
    .await;
}

#[tokio::test]
async fn test_extend_program_data_without_payer() {
    let mut context = setup_test_context().await;
    let rent = context.banks_client.get_rent().await.unwrap();

    let program_data_address = Pubkey::new_unique();
    let program_data_len = 100;
    add_upgradeable_loader_account(
        &mut context,
        &program_data_address,
        &UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(Pubkey::new_unique()),
        },
        program_data_len,
    )
    .await;

    assert_ix_error(
        &mut context,
        extend_program_data(&program_data_address, None, 1024),
        None,
        InstructionError::NotEnoughAccountKeys,
        "should fail because program data has insufficient funds to cover rent",
    )
    .await;

    let client = &mut context.banks_client;
    let payer = &context.payer;
    let recent_blockhash = context.last_blockhash;

    const ADDITIONAL_BYTES: u32 = 42;
    let min_balance_increase_for_extend = rent
        .minimum_balance(ADDITIONAL_BYTES as usize)
        .saturating_sub(rent.minimum_balance(0));

    let transaction = Transaction::new_signed_with_payer(
        &[
            system_instruction::transfer(
                &payer.pubkey(),
                &program_data_address,
                min_balance_increase_for_extend,
            ),
            extend_program_data(&program_data_address, None, ADDITIONAL_BYTES),
        ],
        Some(&payer.pubkey()),
        &[payer],
        recent_blockhash,
    );

    assert_matches!(client.process_transaction(transaction).await, Ok(()));
    let updated_program_data_account = client
        .get_account(program_data_address)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        updated_program_data_account.data().len(),
        program_data_len + ADDITIONAL_BYTES as usize
    );
}

#[tokio::test]
async fn test_extend_program_data_with_invalid_system_program() {
    let mut context = setup_test_context().await;

    let program_data_address = Pubkey::new_unique();
    let program_data_len = 100;
    add_upgradeable_loader_account(
        &mut context,
        &program_data_address,
        &UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(Pubkey::new_unique()),
        },
        program_data_len,
    )
    .await;

    let payer_address = context.payer.pubkey();
    let mut ix = extend_program_data(&program_data_address, Some(&payer_address), 1);

    // Change system program to an invalid key
    {
        let system_program_meta = ix
            .accounts
            .iter_mut()
            .find(|meta| meta.pubkey == crate::system_program::ID)
            .expect("expected to find system program account meta");
        system_program_meta.pubkey = Pubkey::new_unique();
    }

    assert_ix_error(
        &mut context,
        ix,
        None,
        InstructionError::MissingAccount,
        "should fail because the system program is missing",
    )
    .await;
}

#[tokio::test]
async fn test_extend_program_data_with_invalid_program_data() {
    let mut context = setup_test_context().await;
    let rent = context.banks_client.get_rent().await.unwrap();
    let payer_address = context.payer.pubkey();

    let program_data_address = Pubkey::new_unique();
    add_upgradeable_loader_account(
        &mut context,
        &program_data_address,
        &UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(Pubkey::new_unique()),
        },
        100,
    )
    .await;

    let program_data_address_with_invalid_state = Pubkey::new_unique();
    {
        let mut account = AccountSharedData::new(rent.minimum_balance(100), 100, &id());
        account
            .set_state(&UpgradeableLoaderState::Buffer {
                authority_address: Some(payer_address),
            })
            .expect("serialization failed");
        context.set_account(&program_data_address_with_invalid_state, &account);
    }

    let program_data_address_with_invalid_owner = Pubkey::new_unique();
    {
        let invalid_owner = Pubkey::new_unique();
        let mut account = AccountSharedData::new(rent.minimum_balance(100), 100, &invalid_owner);
        account
            .set_state(&UpgradeableLoaderState::ProgramData {
                slot: 0,
                upgrade_authority_address: Some(payer_address),
            })
            .expect("serialization failed");
        context.set_account(&program_data_address_with_invalid_owner, &account);
    }

    assert_ix_error(
        &mut context,
        extend_program_data(
            &program_data_address_with_invalid_state,
            Some(&payer_address),
            1024,
        ),
        None,
        InstructionError::InvalidAccountData,
        "should fail because the program data account state isn't valid",
    )
    .await;

    assert_ix_error(
        &mut context,
        extend_program_data(
            &program_data_address_with_invalid_owner,
            Some(&payer_address),
            1024,
        ),
        None,
        InstructionError::InvalidAccountOwner,
        "should fail because the program data account owner isn't valid",
    )
    .await;

    let mut ix = extend_program_data(&program_data_address, Some(&payer_address), 1);

    // Demote ProgramData account meta to read-only
    {
        let program_data_meta = ix
            .accounts
            .iter_mut()
            .find(|meta| meta.pubkey == program_data_address)
            .expect("expected to find program data account meta");
        program_data_meta.is_writable = false;
    }

    assert_ix_error(
        &mut context,
        ix,
        None,
        InstructionError::InvalidArgument,
        "should fail because the program data account is not writable",
    )
    .await;
}
