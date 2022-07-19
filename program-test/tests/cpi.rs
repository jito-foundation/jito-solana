use {
    solana_program_test::{processor, ProgramTest},
    solana_sdk::{
        account_info::{next_account_info, AccountInfo},
        entrypoint::{ProgramResult, MAX_PERMITTED_DATA_INCREASE},
        instruction::{AccountMeta, Instruction},
        msg,
        program::invoke,
        pubkey::Pubkey,
        rent::Rent,
        signature::Signer,
        signer::keypair::Keypair,
        system_instruction, system_program,
        sysvar::Sysvar,
        transaction::Transaction,
    },
};

// Process instruction to invoke into another program
// We pass this specific number of accounts in order to test for a reported error.
fn invoker_process_instruction(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    _input: &[u8],
) -> ProgramResult {
    // if we can call `msg!` successfully, then InvokeContext exists as required
    msg!("Processing invoker instruction before CPI");
    let account_info_iter = &mut accounts.iter();
    let invoked_program_info = next_account_info(account_info_iter)?;
    invoke(
        &Instruction::new_with_bincode(
            *invoked_program_info.key,
            &[0],
            vec![AccountMeta::new_readonly(*invoked_program_info.key, false)],
        ),
        &[invoked_program_info.clone()],
    )?;
    msg!("Processing invoker instruction after CPI");
    Ok(())
}

// Process instruction to invoke into another program with duplicates
fn invoker_dupes_process_instruction(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    _input: &[u8],
) -> ProgramResult {
    // if we can call `msg!` successfully, then InvokeContext exists as required
    msg!("Processing invoker instruction before CPI");
    let account_info_iter = &mut accounts.iter();
    let invoked_program_info = next_account_info(account_info_iter)?;
    invoke(
        &Instruction::new_with_bincode(
            *invoked_program_info.key,
            &[0],
            vec![
                AccountMeta::new_readonly(*invoked_program_info.key, false),
                AccountMeta::new_readonly(*invoked_program_info.key, false),
                AccountMeta::new_readonly(*invoked_program_info.key, false),
                AccountMeta::new_readonly(*invoked_program_info.key, false),
            ],
        ),
        &[
            invoked_program_info.clone(),
            invoked_program_info.clone(),
            invoked_program_info.clone(),
            invoked_program_info.clone(),
            invoked_program_info.clone(),
        ],
    )?;
    msg!("Processing invoker instruction after CPI");
    Ok(())
}

// Process instruction to be invoked by another program
#[allow(clippy::unnecessary_wraps)]
fn invoked_process_instruction(
    _program_id: &Pubkey,
    _accounts: &[AccountInfo],
    _input: &[u8],
) -> ProgramResult {
    // if we can call `msg!` successfully, then InvokeContext exists as required
    msg!("Processing invoked instruction");
    Ok(())
}

// Process instruction to invoke into system program to create an account
fn invoke_create_account(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    _input: &[u8],
) -> ProgramResult {
    msg!("Processing instruction before system program CPI instruction");
    let account_info_iter = &mut accounts.iter();
    let payer_info = next_account_info(account_info_iter)?;
    let create_account_info = next_account_info(account_info_iter)?;
    let system_program_info = next_account_info(account_info_iter)?;
    let rent = Rent::get()?;
    let minimum_balance = rent.minimum_balance(MAX_PERMITTED_DATA_INCREASE);
    invoke(
        &system_instruction::create_account(
            payer_info.key,
            create_account_info.key,
            minimum_balance,
            MAX_PERMITTED_DATA_INCREASE as u64,
            program_id,
        ),
        &[
            payer_info.clone(),
            create_account_info.clone(),
            system_program_info.clone(),
        ],
    )?;
    msg!("Processing instruction after system program CPI");
    Ok(())
}

#[tokio::test]
async fn cpi() {
    let invoker_program_id = Pubkey::new_unique();
    let mut program_test = ProgramTest::new(
        "invoker",
        invoker_program_id,
        processor!(invoker_process_instruction),
    );
    let invoked_program_id = Pubkey::new_unique();
    program_test.add_program(
        "invoked",
        invoked_program_id,
        processor!(invoked_process_instruction),
    );

    let mut context = program_test.start_with_context().await;
    let instructions = vec![Instruction::new_with_bincode(
        invoker_program_id,
        &[0],
        vec![AccountMeta::new_readonly(invoked_program_id, false)],
    )];

    let transaction = Transaction::new_signed_with_payer(
        &instructions,
        Some(&context.payer.pubkey()),
        &[&context.payer],
        context.last_blockhash,
    );

    context
        .banks_client
        .process_transaction(transaction)
        .await
        .unwrap();
}

#[tokio::test]
async fn cpi_dupes() {
    let invoker_program_id = Pubkey::new_unique();
    let mut program_test = ProgramTest::new(
        "invoker",
        invoker_program_id,
        processor!(invoker_dupes_process_instruction),
    );
    let invoked_program_id = Pubkey::new_unique();
    program_test.add_program(
        "invoked",
        invoked_program_id,
        processor!(invoked_process_instruction),
    );

    let mut context = program_test.start_with_context().await;
    let instructions = vec![Instruction::new_with_bincode(
        invoker_program_id,
        &[0],
        vec![
            AccountMeta::new_readonly(invoked_program_id, false),
            AccountMeta::new_readonly(invoked_program_id, false),
            AccountMeta::new_readonly(invoked_program_id, false),
            AccountMeta::new_readonly(invoked_program_id, false),
        ],
    )];

    let transaction = Transaction::new_signed_with_payer(
        &instructions,
        Some(&context.payer.pubkey()),
        &[&context.payer],
        context.last_blockhash,
    );

    context
        .banks_client
        .process_transaction(transaction)
        .await
        .unwrap();
}

#[tokio::test]
async fn cpi_create_account() {
    let create_account_program_id = Pubkey::new_unique();
    let program_test = ProgramTest::new(
        "create_account",
        create_account_program_id,
        processor!(invoke_create_account),
    );

    let create_account_keypair = Keypair::new();
    let mut context = program_test.start_with_context().await;
    let instructions = vec![Instruction::new_with_bincode(
        create_account_program_id,
        &[0],
        vec![
            AccountMeta::new(context.payer.pubkey(), true),
            AccountMeta::new(create_account_keypair.pubkey(), true),
            AccountMeta::new_readonly(system_program::id(), false),
        ],
    )];

    let transaction = Transaction::new_signed_with_payer(
        &instructions,
        Some(&context.payer.pubkey()),
        &[&context.payer, &create_account_keypair],
        context.last_blockhash,
    );

    context
        .banks_client
        .process_transaction(transaction)
        .await
        .unwrap();
}
