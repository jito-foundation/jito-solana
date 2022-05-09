//! Vote program processor

use {
    crate::{id, vote_instruction::VoteInstruction, vote_state},
    log::*,
    solana_program_runtime::{
        invoke_context::InvokeContext, sysvar_cache::get_sysvar_with_account_check,
    },
    solana_sdk::{feature_set, instruction::InstructionError, program_utils::limited_deserialize},
};

pub fn process_instruction(
    first_instruction_account: usize,
    invoke_context: &mut InvokeContext,
) -> Result<(), InstructionError> {
    let transaction_context = &invoke_context.transaction_context;
    let instruction_context = transaction_context.get_current_instruction_context()?;
    let data = instruction_context.get_instruction_data();

    trace!("process_instruction: {:?}", data);

    let mut me =
        instruction_context.try_borrow_account(transaction_context, first_instruction_account)?;
    if *me.get_owner() != id() {
        return Err(InstructionError::InvalidAccountOwner);
    }

    let signers = instruction_context.get_signers(transaction_context);
    match limited_deserialize(data)? {
        VoteInstruction::InitializeAccount(vote_init) => {
            let rent = get_sysvar_with_account_check::rent(invoke_context, instruction_context, 1)?;
            if !rent.is_exempt(me.get_lamports(), me.get_data().len()) {
                return Err(InstructionError::InsufficientFunds);
            }
            let clock =
                get_sysvar_with_account_check::clock(invoke_context, instruction_context, 2)?;
            vote_state::initialize_account(&mut me, &vote_init, &signers, &clock)
        }
        VoteInstruction::Authorize(voter_pubkey, vote_authorize) => {
            let clock =
                get_sysvar_with_account_check::clock(invoke_context, instruction_context, 1)?;
            vote_state::authorize(
                &mut me,
                &voter_pubkey,
                vote_authorize,
                &signers,
                &clock,
                &invoke_context.feature_set,
            )
        }
        VoteInstruction::UpdateValidatorIdentity => {
            instruction_context.check_number_of_instruction_accounts(2)?;
            let node_pubkey = transaction_context.get_key_of_account_at_index(
                instruction_context.get_index_in_transaction(first_instruction_account + 1)?,
            )?;
            vote_state::update_validator_identity(&mut me, node_pubkey, &signers)
        }
        VoteInstruction::UpdateCommission(commission) => {
            vote_state::update_commission(&mut me, commission, &signers)
        }
        VoteInstruction::Vote(vote) | VoteInstruction::VoteSwitch(vote, _) => {
            let slot_hashes =
                get_sysvar_with_account_check::slot_hashes(invoke_context, instruction_context, 1)?;
            let clock =
                get_sysvar_with_account_check::clock(invoke_context, instruction_context, 2)?;
            vote_state::process_vote(
                &mut me,
                &slot_hashes,
                &clock,
                &vote,
                &signers,
                &invoke_context.feature_set,
            )
        }
        VoteInstruction::UpdateVoteState(vote_state_update)
        | VoteInstruction::UpdateVoteStateSwitch(vote_state_update, _) => {
            if invoke_context
                .feature_set
                .is_active(&feature_set::allow_votes_to_directly_update_vote_state::id())
            {
                let sysvar_cache = invoke_context.get_sysvar_cache();
                let slot_hashes = sysvar_cache.get_slot_hashes()?;
                let clock = sysvar_cache.get_clock()?;
                vote_state::process_vote_state_update(
                    &mut me,
                    slot_hashes.slot_hashes(),
                    &clock,
                    vote_state_update,
                    &signers,
                )
            } else {
                Err(InstructionError::InvalidInstructionData)
            }
        }
        VoteInstruction::Withdraw(lamports) => {
            instruction_context.check_number_of_instruction_accounts(2)?;
            let rent_sysvar = if invoke_context
                .feature_set
                .is_active(&feature_set::reject_non_rent_exempt_vote_withdraws::id())
            {
                Some(invoke_context.get_sysvar_cache().get_rent()?)
            } else {
                None
            };

            let clock_if_feature_active = if invoke_context
                .feature_set
                .is_active(&feature_set::reject_vote_account_close_unless_zero_credit_epoch::id())
            {
                Some(invoke_context.get_sysvar_cache().get_clock()?)
            } else {
                None
            };

            drop(me);
            vote_state::withdraw(
                transaction_context,
                instruction_context,
                first_instruction_account,
                lamports,
                first_instruction_account + 1,
                &signers,
                rent_sysvar.as_deref(),
                clock_if_feature_active.as_deref(),
            )
        }
        VoteInstruction::AuthorizeChecked(vote_authorize) => {
            if invoke_context
                .feature_set
                .is_active(&feature_set::vote_stake_checked_instructions::id())
            {
                instruction_context.check_number_of_instruction_accounts(4)?;
                let voter_pubkey = transaction_context.get_key_of_account_at_index(
                    instruction_context.get_index_in_transaction(first_instruction_account + 3)?,
                )?;
                if !instruction_context.is_signer(first_instruction_account + 3)? {
                    return Err(InstructionError::MissingRequiredSignature);
                }
                let clock =
                    get_sysvar_with_account_check::clock(invoke_context, instruction_context, 1)?;
                vote_state::authorize(
                    &mut me,
                    voter_pubkey,
                    vote_authorize,
                    &signers,
                    &clock,
                    &invoke_context.feature_set,
                )
            } else {
                Err(InstructionError::InvalidInstructionData)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            vote_error::VoteError,
            vote_instruction::{
                authorize, authorize_checked, create_account, update_commission,
                update_validator_identity, update_vote_state, update_vote_state_switch, vote,
                vote_switch, withdraw, VoteInstruction,
            },
            vote_state::{
                Lockout, Vote, VoteAuthorize, VoteInit, VoteState, VoteStateUpdate,
                VoteStateVersions,
            },
        },
        bincode::serialize,
        solana_program_runtime::invoke_context::mock_process_instruction,
        solana_sdk::{
            account::{self, Account, AccountSharedData, ReadableAccount},
            account_utils::StateMut,
            feature_set::FeatureSet,
            hash::Hash,
            instruction::{AccountMeta, Instruction},
            pubkey::Pubkey,
            sysvar::{self, clock::Clock, rent::Rent, slot_hashes::SlotHashes},
        },
        std::{collections::HashSet, str::FromStr},
    };

    fn create_default_account() -> AccountSharedData {
        AccountSharedData::new(0, 0, &Pubkey::new_unique())
    }

    fn process_instruction(
        instruction_data: &[u8],
        transaction_accounts: Vec<(Pubkey, AccountSharedData)>,
        instruction_accounts: Vec<AccountMeta>,
        expected_result: Result<(), InstructionError>,
    ) -> Vec<AccountSharedData> {
        mock_process_instruction(
            &id(),
            Vec::new(),
            instruction_data,
            transaction_accounts,
            instruction_accounts,
            None,
            None,
            expected_result,
            super::process_instruction,
        )
    }

    fn process_instruction_disabled_features(
        instruction_data: &[u8],
        transaction_accounts: Vec<(Pubkey, AccountSharedData)>,
        instruction_accounts: Vec<AccountMeta>,
        expected_result: Result<(), InstructionError>,
    ) -> Vec<AccountSharedData> {
        mock_process_instruction(
            &id(),
            Vec::new(),
            instruction_data,
            transaction_accounts,
            instruction_accounts,
            None,
            Some(std::sync::Arc::new(FeatureSet::default())),
            expected_result,
            super::process_instruction,
        )
    }

    fn process_instruction_as_one_arg(
        instruction: &Instruction,
        expected_result: Result<(), InstructionError>,
    ) -> Vec<AccountSharedData> {
        let mut pubkeys: HashSet<Pubkey> = instruction
            .accounts
            .iter()
            .map(|meta| meta.pubkey)
            .collect();
        pubkeys.insert(sysvar::clock::id());
        pubkeys.insert(sysvar::rent::id());
        pubkeys.insert(sysvar::slot_hashes::id());
        let transaction_accounts: Vec<_> = pubkeys
            .iter()
            .map(|pubkey| {
                (
                    *pubkey,
                    if sysvar::clock::check_id(pubkey) {
                        account::create_account_shared_data_for_test(&Clock::default())
                    } else if sysvar::slot_hashes::check_id(pubkey) {
                        account::create_account_shared_data_for_test(&SlotHashes::default())
                    } else if sysvar::rent::check_id(pubkey) {
                        account::create_account_shared_data_for_test(&Rent::free())
                    } else if *pubkey == invalid_vote_state_pubkey() {
                        AccountSharedData::from(Account {
                            owner: invalid_vote_state_pubkey(),
                            ..Account::default()
                        })
                    } else {
                        AccountSharedData::from(Account {
                            owner: id(),
                            ..Account::default()
                        })
                    },
                )
            })
            .collect();
        process_instruction(
            &instruction.data,
            transaction_accounts,
            instruction.accounts.clone(),
            expected_result,
        )
    }

    fn invalid_vote_state_pubkey() -> Pubkey {
        Pubkey::from_str("BadVote111111111111111111111111111111111111").unwrap()
    }

    fn create_default_rent_account() -> AccountSharedData {
        account::create_account_shared_data_for_test(&Rent::free())
    }

    fn create_default_clock_account() -> AccountSharedData {
        account::create_account_shared_data_for_test(&Clock::default())
    }

    fn create_test_account() -> (Pubkey, AccountSharedData) {
        let rent = Rent::default();
        let balance = VoteState::get_rent_exempt_reserve(&rent);
        let vote_pubkey = solana_sdk::pubkey::new_rand();
        (
            vote_pubkey,
            vote_state::create_account(&vote_pubkey, &solana_sdk::pubkey::new_rand(), 0, balance),
        )
    }

    fn create_test_account_with_authorized() -> (Pubkey, Pubkey, Pubkey, AccountSharedData) {
        let vote_pubkey = solana_sdk::pubkey::new_rand();
        let authorized_voter = solana_sdk::pubkey::new_rand();
        let authorized_withdrawer = solana_sdk::pubkey::new_rand();

        (
            vote_pubkey,
            authorized_voter,
            authorized_withdrawer,
            vote_state::create_account_with_authorized(
                &solana_sdk::pubkey::new_rand(),
                &authorized_voter,
                &authorized_withdrawer,
                0,
                100,
            ),
        )
    }

    fn create_test_account_with_epoch_credits(
        credits_to_append: &[u64],
    ) -> (Pubkey, AccountSharedData) {
        let (vote_pubkey, vote_account) = create_test_account();
        let vote_account_space = vote_account.data().len();

        let mut vote_state = VoteState::from(&vote_account).unwrap();
        vote_state.authorized_withdrawer = vote_pubkey;
        vote_state.epoch_credits = Vec::new();

        let mut current_epoch_credits = 0;
        let mut previous_epoch_credits = 0;
        for (epoch, credits) in credits_to_append.iter().enumerate() {
            current_epoch_credits += credits;
            vote_state.epoch_credits.push((
                u64::try_from(epoch).unwrap(),
                current_epoch_credits,
                previous_epoch_credits,
            ));
            previous_epoch_credits = current_epoch_credits;
        }

        let lamports = vote_account.lamports();
        let mut vote_account_with_epoch_credits =
            AccountSharedData::new(lamports, vote_account_space, &id());
        let versioned = VoteStateVersions::new_current(vote_state);
        VoteState::to(&versioned, &mut vote_account_with_epoch_credits);

        (vote_pubkey, vote_account_with_epoch_credits)
    }

    #[test]
    fn test_vote_process_instruction_decode_bail() {
        process_instruction(
            &[],
            Vec::new(),
            Vec::new(),
            Err(InstructionError::NotEnoughAccountKeys),
        );
    }

    #[test]
    fn test_initialize_vote_account() {
        let vote_pubkey = solana_sdk::pubkey::new_rand();
        let vote_account = AccountSharedData::new(100, VoteState::size_of(), &id());
        let node_pubkey = solana_sdk::pubkey::new_rand();
        let node_account = AccountSharedData::default();
        let instruction_data = serialize(&VoteInstruction::InitializeAccount(VoteInit {
            node_pubkey,
            authorized_voter: vote_pubkey,
            authorized_withdrawer: vote_pubkey,
            commission: 0,
        }))
        .unwrap();
        let mut instruction_accounts = vec![
            AccountMeta {
                pubkey: vote_pubkey,
                is_signer: false,
                is_writable: false,
            },
            AccountMeta {
                pubkey: sysvar::rent::id(),
                is_signer: false,
                is_writable: false,
            },
            AccountMeta {
                pubkey: sysvar::clock::id(),
                is_signer: false,
                is_writable: false,
            },
            AccountMeta {
                pubkey: node_pubkey,
                is_signer: true,
                is_writable: false,
            },
        ];

        // init should pass
        let accounts = process_instruction(
            &instruction_data,
            vec![
                (vote_pubkey, vote_account.clone()),
                (sysvar::rent::id(), create_default_rent_account()),
                (sysvar::clock::id(), create_default_clock_account()),
                (node_pubkey, node_account.clone()),
            ],
            instruction_accounts.clone(),
            Ok(()),
        );

        // reinit should fail
        process_instruction(
            &instruction_data,
            vec![
                (vote_pubkey, accounts[0].clone()),
                (sysvar::rent::id(), create_default_rent_account()),
                (sysvar::clock::id(), create_default_clock_account()),
                (node_pubkey, accounts[3].clone()),
            ],
            instruction_accounts.clone(),
            Err(InstructionError::AccountAlreadyInitialized),
        );

        // init should fail, account is too big
        process_instruction(
            &instruction_data,
            vec![
                (
                    vote_pubkey,
                    AccountSharedData::new(100, 2 * VoteState::size_of(), &id()),
                ),
                (sysvar::rent::id(), create_default_rent_account()),
                (sysvar::clock::id(), create_default_clock_account()),
                (node_pubkey, node_account.clone()),
            ],
            instruction_accounts.clone(),
            Err(InstructionError::InvalidAccountData),
        );

        // init should fail, node_pubkey didn't sign the transaction
        instruction_accounts[3].is_signer = false;
        process_instruction(
            &instruction_data,
            vec![
                (vote_pubkey, vote_account),
                (sysvar::rent::id(), create_default_rent_account()),
                (sysvar::clock::id(), create_default_clock_account()),
                (node_pubkey, node_account),
            ],
            instruction_accounts,
            Err(InstructionError::MissingRequiredSignature),
        );
    }

    #[test]
    fn test_vote_update_validator_identity() {
        let (vote_pubkey, _authorized_voter, authorized_withdrawer, vote_account) =
            create_test_account_with_authorized();
        let node_pubkey = solana_sdk::pubkey::new_rand();
        let instruction_data = serialize(&VoteInstruction::UpdateValidatorIdentity).unwrap();
        let transaction_accounts = vec![
            (vote_pubkey, vote_account),
            (node_pubkey, AccountSharedData::default()),
            (authorized_withdrawer, AccountSharedData::default()),
        ];
        let mut instruction_accounts = vec![
            AccountMeta {
                pubkey: vote_pubkey,
                is_signer: false,
                is_writable: false,
            },
            AccountMeta {
                pubkey: node_pubkey,
                is_signer: true,
                is_writable: false,
            },
            AccountMeta {
                pubkey: authorized_withdrawer,
                is_signer: true,
                is_writable: false,
            },
        ];

        // should fail, node_pubkey didn't sign the transaction
        instruction_accounts[1].is_signer = false;
        let accounts = process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::MissingRequiredSignature),
        );
        instruction_accounts[1].is_signer = true;
        let vote_state: VoteState = StateMut::<VoteStateVersions>::state(&accounts[0])
            .unwrap()
            .convert_to_current();
        assert_ne!(vote_state.node_pubkey, node_pubkey);

        // should fail, authorized_withdrawer didn't sign the transaction
        instruction_accounts[2].is_signer = false;
        let accounts = process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::MissingRequiredSignature),
        );
        instruction_accounts[2].is_signer = true;
        let vote_state: VoteState = StateMut::<VoteStateVersions>::state(&accounts[0])
            .unwrap()
            .convert_to_current();
        assert_ne!(vote_state.node_pubkey, node_pubkey);

        // should pass
        let accounts = process_instruction(
            &instruction_data,
            transaction_accounts,
            instruction_accounts,
            Ok(()),
        );
        let vote_state: VoteState = StateMut::<VoteStateVersions>::state(&accounts[0])
            .unwrap()
            .convert_to_current();
        assert_eq!(vote_state.node_pubkey, node_pubkey);
    }

    #[test]
    fn test_vote_update_commission() {
        let (vote_pubkey, _authorized_voter, authorized_withdrawer, vote_account) =
            create_test_account_with_authorized();
        let instruction_data = serialize(&VoteInstruction::UpdateCommission(42)).unwrap();
        let transaction_accounts = vec![
            (vote_pubkey, vote_account),
            (authorized_withdrawer, AccountSharedData::default()),
        ];
        let mut instruction_accounts = vec![
            AccountMeta {
                pubkey: vote_pubkey,
                is_signer: false,
                is_writable: false,
            },
            AccountMeta {
                pubkey: authorized_withdrawer,
                is_signer: true,
                is_writable: false,
            },
        ];

        // should pass
        let accounts = process_instruction(
            &serialize(&VoteInstruction::UpdateCommission(u8::MAX)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );
        let vote_state: VoteState = StateMut::<VoteStateVersions>::state(&accounts[0])
            .unwrap()
            .convert_to_current();
        assert_eq!(vote_state.commission, u8::MAX);

        // should pass
        let accounts = process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );
        let vote_state: VoteState = StateMut::<VoteStateVersions>::state(&accounts[0])
            .unwrap()
            .convert_to_current();
        assert_eq!(vote_state.commission, 42);

        // should fail, authorized_withdrawer didn't sign the transaction
        instruction_accounts[1].is_signer = false;
        let accounts = process_instruction(
            &instruction_data,
            transaction_accounts,
            instruction_accounts,
            Err(InstructionError::MissingRequiredSignature),
        );
        let vote_state: VoteState = StateMut::<VoteStateVersions>::state(&accounts[0])
            .unwrap()
            .convert_to_current();
        assert_eq!(vote_state.commission, 0);
    }

    #[test]
    fn test_vote_signature() {
        let (vote_pubkey, vote_account) = create_test_account();
        let vote = Vote::new(vec![1], Hash::default());
        let slot_hashes = SlotHashes::new(&[(*vote.slots.last().unwrap(), vote.hash)]);
        let slot_hashes_account = account::create_account_shared_data_for_test(&slot_hashes);
        let instruction_data = serialize(&VoteInstruction::Vote(vote.clone())).unwrap();
        let mut transaction_accounts = vec![
            (vote_pubkey, vote_account),
            (sysvar::slot_hashes::id(), slot_hashes_account.clone()),
            (sysvar::clock::id(), create_default_clock_account()),
        ];
        let mut instruction_accounts = vec![
            AccountMeta {
                pubkey: vote_pubkey,
                is_signer: true,
                is_writable: false,
            },
            AccountMeta {
                pubkey: sysvar::slot_hashes::id(),
                is_signer: false,
                is_writable: false,
            },
            AccountMeta {
                pubkey: sysvar::clock::id(),
                is_signer: false,
                is_writable: false,
            },
        ];

        // should fail, unsigned
        instruction_accounts[0].is_signer = false;
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::MissingRequiredSignature),
        );
        instruction_accounts[0].is_signer = true;

        // should pass
        let accounts = process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );
        let vote_state: VoteState = StateMut::<VoteStateVersions>::state(&accounts[0])
            .unwrap()
            .convert_to_current();
        assert_eq!(
            vote_state.votes,
            vec![Lockout::new(*vote.slots.last().unwrap())]
        );
        assert_eq!(vote_state.credits(), 0);

        // should fail, wrong hash
        transaction_accounts[1] = (
            sysvar::slot_hashes::id(),
            account::create_account_shared_data_for_test(&SlotHashes::new(&[(
                *vote.slots.last().unwrap(),
                solana_sdk::hash::hash(&[0u8]),
            )])),
        );
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(VoteError::SlotHashMismatch.into()),
        );

        // should fail, wrong slot
        transaction_accounts[1] = (
            sysvar::slot_hashes::id(),
            account::create_account_shared_data_for_test(&SlotHashes::new(&[(0, vote.hash)])),
        );
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(VoteError::SlotsMismatch.into()),
        );

        // should fail, empty slot_hashes
        transaction_accounts[1] = (
            sysvar::slot_hashes::id(),
            account::create_account_shared_data_for_test(&SlotHashes::new(&[])),
        );
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(VoteError::VoteTooOld.into()),
        );
        transaction_accounts[1] = (sysvar::slot_hashes::id(), slot_hashes_account);

        // should fail, uninitialized
        let vote_account = AccountSharedData::new(100, VoteState::size_of(), &id());
        transaction_accounts[0] = (vote_pubkey, vote_account);
        process_instruction(
            &instruction_data,
            transaction_accounts,
            instruction_accounts,
            Err(InstructionError::UninitializedAccount),
        );
    }

    #[test]
    fn test_authorize_voter() {
        let (vote_pubkey, vote_account) = create_test_account();
        let authorized_voter_pubkey = solana_sdk::pubkey::new_rand();
        let clock = Clock {
            epoch: 1,
            leader_schedule_epoch: 2,
            ..Clock::default()
        };
        let clock_account = account::create_account_shared_data_for_test(&clock);
        let instruction_data = serialize(&VoteInstruction::Authorize(
            authorized_voter_pubkey,
            VoteAuthorize::Voter,
        ))
        .unwrap();
        let mut transaction_accounts = vec![
            (vote_pubkey, vote_account),
            (sysvar::clock::id(), clock_account),
            (authorized_voter_pubkey, AccountSharedData::default()),
        ];
        let mut instruction_accounts = vec![
            AccountMeta {
                pubkey: vote_pubkey,
                is_signer: true,
                is_writable: false,
            },
            AccountMeta {
                pubkey: sysvar::clock::id(),
                is_signer: false,
                is_writable: false,
            },
        ];

        // should fail, unsigned
        instruction_accounts[0].is_signer = false;
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::MissingRequiredSignature),
        );
        instruction_accounts[0].is_signer = true;

        // should pass
        let accounts = process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );

        // should fail, already set an authorized voter earlier for leader_schedule_epoch == 2
        transaction_accounts[0] = (vote_pubkey, accounts[0].clone());
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(VoteError::TooSoonToReauthorize.into()),
        );

        // should pass, verify authorized_voter_pubkey can authorize authorized_voter_pubkey ;)
        instruction_accounts[0].is_signer = false;
        instruction_accounts.push(AccountMeta {
            pubkey: authorized_voter_pubkey,
            is_signer: true,
            is_writable: false,
        });
        let clock = Clock {
            // The authorized voter was set when leader_schedule_epoch == 2, so will
            // take effect when epoch == 3
            epoch: 3,
            leader_schedule_epoch: 4,
            ..Clock::default()
        };
        let clock_account = account::create_account_shared_data_for_test(&clock);
        transaction_accounts[1] = (sysvar::clock::id(), clock_account);
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );
        instruction_accounts[0].is_signer = true;
        instruction_accounts.pop();

        // should fail, not signed by authorized voter
        let vote = Vote::new(vec![1], Hash::default());
        let slot_hashes = SlotHashes::new(&[(*vote.slots.last().unwrap(), vote.hash)]);
        let slot_hashes_account = account::create_account_shared_data_for_test(&slot_hashes);
        let instruction_data = serialize(&VoteInstruction::Vote(vote)).unwrap();
        transaction_accounts.push((sysvar::slot_hashes::id(), slot_hashes_account));
        instruction_accounts.insert(
            1,
            AccountMeta {
                pubkey: sysvar::slot_hashes::id(),
                is_signer: false,
                is_writable: false,
            },
        );
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::MissingRequiredSignature),
        );

        // should pass, signed by authorized voter
        instruction_accounts.push(AccountMeta {
            pubkey: authorized_voter_pubkey,
            is_signer: true,
            is_writable: false,
        });
        process_instruction(
            &instruction_data,
            transaction_accounts,
            instruction_accounts,
            Ok(()),
        );
    }

    #[test]
    fn test_authorize_withdrawer() {
        let (vote_pubkey, vote_account) = create_test_account();
        let authorized_withdrawer_pubkey = solana_sdk::pubkey::new_rand();
        let instruction_data = serialize(&VoteInstruction::Authorize(
            authorized_withdrawer_pubkey,
            VoteAuthorize::Withdrawer,
        ))
        .unwrap();
        let mut transaction_accounts = vec![
            (vote_pubkey, vote_account),
            (sysvar::clock::id(), create_default_clock_account()),
            (authorized_withdrawer_pubkey, AccountSharedData::default()),
        ];
        let mut instruction_accounts = vec![
            AccountMeta {
                pubkey: vote_pubkey,
                is_signer: true,
                is_writable: false,
            },
            AccountMeta {
                pubkey: sysvar::clock::id(),
                is_signer: false,
                is_writable: false,
            },
        ];

        // should fail, unsigned
        instruction_accounts[0].is_signer = false;
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::MissingRequiredSignature),
        );
        instruction_accounts[0].is_signer = true;

        // should pass
        let accounts = process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );

        // should pass, verify authorized_withdrawer can authorize authorized_withdrawer ;)
        instruction_accounts[0].is_signer = false;
        instruction_accounts.push(AccountMeta {
            pubkey: authorized_withdrawer_pubkey,
            is_signer: true,
            is_writable: false,
        });
        transaction_accounts[0] = (vote_pubkey, accounts[0].clone());
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );

        // should pass, verify authorized_withdrawer can authorize a new authorized_voter
        let authorized_voter_pubkey = solana_sdk::pubkey::new_rand();
        transaction_accounts.push((authorized_voter_pubkey, AccountSharedData::default()));
        let instruction_data = serialize(&VoteInstruction::Authorize(
            authorized_voter_pubkey,
            VoteAuthorize::Voter,
        ))
        .unwrap();
        process_instruction(
            &instruction_data,
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );

        // should fail, if vote_withdraw_authority_may_change_authorized_voter is disabled
        process_instruction_disabled_features(
            &instruction_data,
            transaction_accounts,
            instruction_accounts,
            Err(InstructionError::MissingRequiredSignature),
        );
    }

    #[test]
    fn test_vote_withdraw() {
        let (vote_pubkey, vote_account) = create_test_account();
        let lamports = vote_account.lamports();
        let authorized_withdrawer_pubkey = solana_sdk::pubkey::new_rand();
        let mut transaction_accounts = vec![
            (vote_pubkey, vote_account.clone()),
            (sysvar::clock::id(), create_default_clock_account()),
            (sysvar::rent::id(), create_default_rent_account()),
            (authorized_withdrawer_pubkey, AccountSharedData::default()),
        ];
        let mut instruction_accounts = vec![
            AccountMeta {
                pubkey: vote_pubkey,
                is_signer: true,
                is_writable: false,
            },
            AccountMeta {
                pubkey: sysvar::clock::id(),
                is_signer: false,
                is_writable: false,
            },
        ];

        // should pass, withdraw using authorized_withdrawer to authorized_withdrawer's account
        let accounts = process_instruction(
            &serialize(&VoteInstruction::Authorize(
                authorized_withdrawer_pubkey,
                VoteAuthorize::Withdrawer,
            ))
            .unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );
        instruction_accounts[0].is_signer = false;
        instruction_accounts[1] = AccountMeta {
            pubkey: authorized_withdrawer_pubkey,
            is_signer: true,
            is_writable: false,
        };
        transaction_accounts[0] = (vote_pubkey, accounts[0].clone());
        let accounts = process_instruction(
            &serialize(&VoteInstruction::Withdraw(lamports)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );
        assert_eq!(accounts[0].lamports(), 0);
        assert_eq!(accounts[3].lamports(), lamports);
        let post_state: VoteStateVersions = accounts[0].state().unwrap();
        // State has been deinitialized since balance is zero
        assert!(post_state.is_uninitialized());

        // should fail, unsigned
        transaction_accounts[0] = (vote_pubkey, vote_account);
        process_instruction(
            &serialize(&VoteInstruction::Withdraw(lamports)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::MissingRequiredSignature),
        );
        instruction_accounts[0].is_signer = true;

        // should pass
        process_instruction(
            &serialize(&VoteInstruction::Withdraw(lamports)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );

        // should fail, insufficient funds
        process_instruction(
            &serialize(&VoteInstruction::Withdraw(lamports + 1)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::InsufficientFunds),
        );

        // should pass, partial withdraw
        let withdraw_lamports = 42;
        let accounts = process_instruction(
            &serialize(&VoteInstruction::Withdraw(withdraw_lamports)).unwrap(),
            transaction_accounts,
            instruction_accounts,
            Ok(()),
        );
        assert_eq!(accounts[0].lamports(), lamports - withdraw_lamports);
        assert_eq!(accounts[3].lamports(), withdraw_lamports);
    }

    #[test]
    fn test_vote_state_withdraw() {
        let authorized_withdrawer_pubkey = solana_sdk::pubkey::new_rand();
        let (vote_pubkey_1, vote_account_with_epoch_credits_1) =
            create_test_account_with_epoch_credits(&[2, 1]);
        let (vote_pubkey_2, vote_account_with_epoch_credits_2) =
            create_test_account_with_epoch_credits(&[2, 1, 3]);
        let clock = Clock {
            epoch: 3,
            ..Clock::default()
        };
        let clock_account = account::create_account_shared_data_for_test(&clock);
        let rent_sysvar = Rent::default();
        let minimum_balance = rent_sysvar
            .minimum_balance(vote_account_with_epoch_credits_1.data().len())
            .max(1);
        let lamports = vote_account_with_epoch_credits_1.lamports();
        let transaction_accounts = vec![
            (vote_pubkey_1, vote_account_with_epoch_credits_1),
            (vote_pubkey_2, vote_account_with_epoch_credits_2),
            (sysvar::clock::id(), clock_account),
            (
                sysvar::rent::id(),
                account::create_account_shared_data_for_test(&rent_sysvar),
            ),
            (authorized_withdrawer_pubkey, AccountSharedData::default()),
        ];
        let mut instruction_accounts = vec![
            AccountMeta {
                pubkey: vote_pubkey_1,
                is_signer: true,
                is_writable: false,
            },
            AccountMeta {
                pubkey: sysvar::clock::id(),
                is_signer: false,
                is_writable: false,
            },
        ];

        // non rent exempt withdraw, with 0 credit epoch
        instruction_accounts[0].pubkey = vote_pubkey_1;
        process_instruction(
            &serialize(&VoteInstruction::Withdraw(lamports - minimum_balance + 1)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::InsufficientFunds),
        );

        // non rent exempt withdraw, without 0 credit epoch
        instruction_accounts[0].pubkey = vote_pubkey_2;
        process_instruction(
            &serialize(&VoteInstruction::Withdraw(lamports - minimum_balance + 1)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::InsufficientFunds),
        );

        // full withdraw, with 0 credit epoch
        instruction_accounts[0].pubkey = vote_pubkey_1;
        process_instruction(
            &serialize(&VoteInstruction::Withdraw(lamports)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );

        // full withdraw, without 0 credit epoch
        instruction_accounts[0].pubkey = vote_pubkey_2;
        process_instruction(
            &serialize(&VoteInstruction::Withdraw(lamports)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Err(InstructionError::ActiveVoteAccountClose),
        );

        // Both features disabled:
        // reject_non_rent_exempt_vote_withdraws
        // reject_vote_account_close_unless_zero_credit_epoch

        // non rent exempt withdraw, with 0 credit epoch
        instruction_accounts[0].pubkey = vote_pubkey_1;
        process_instruction_disabled_features(
            &serialize(&VoteInstruction::Withdraw(lamports - minimum_balance + 1)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );

        // non rent exempt withdraw, without 0 credit epoch
        instruction_accounts[0].pubkey = vote_pubkey_2;
        process_instruction_disabled_features(
            &serialize(&VoteInstruction::Withdraw(lamports - minimum_balance + 1)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );

        // full withdraw, with 0 credit epoch
        instruction_accounts[0].pubkey = vote_pubkey_1;
        process_instruction_disabled_features(
            &serialize(&VoteInstruction::Withdraw(lamports)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );

        // full withdraw, without 0 credit epoch
        instruction_accounts[0].pubkey = vote_pubkey_2;
        process_instruction_disabled_features(
            &serialize(&VoteInstruction::Withdraw(lamports)).unwrap(),
            transaction_accounts,
            instruction_accounts,
            Ok(()),
        );
    }

    #[test]
    fn test_spoofed_vote() {
        process_instruction_as_one_arg(
            &vote(
                &invalid_vote_state_pubkey(),
                &Pubkey::new_unique(),
                Vote::default(),
            ),
            Err(InstructionError::InvalidAccountOwner),
        );
        process_instruction_as_one_arg(
            &update_vote_state(
                &invalid_vote_state_pubkey(),
                &Pubkey::default(),
                VoteStateUpdate::default(),
            ),
            Err(InstructionError::InvalidAccountOwner),
        );
    }

    #[test]
    fn test_vote_process_instruction() {
        solana_logger::setup();
        let instructions = create_account(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &VoteInit::default(),
            101,
        );
        process_instruction_as_one_arg(&instructions[1], Err(InstructionError::InvalidAccountData));
        process_instruction_as_one_arg(
            &vote(
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                Vote::default(),
            ),
            Err(InstructionError::InvalidAccountData),
        );
        process_instruction_as_one_arg(
            &vote_switch(
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                Vote::default(),
                Hash::default(),
            ),
            Err(InstructionError::InvalidAccountData),
        );
        process_instruction_as_one_arg(
            &authorize(
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                VoteAuthorize::Voter,
            ),
            Err(InstructionError::InvalidAccountData),
        );
        process_instruction_as_one_arg(
            &update_vote_state(
                &Pubkey::default(),
                &Pubkey::default(),
                VoteStateUpdate::default(),
            ),
            Err(InstructionError::InvalidAccountData),
        );

        process_instruction_as_one_arg(
            &update_vote_state_switch(
                &Pubkey::default(),
                &Pubkey::default(),
                VoteStateUpdate::default(),
                Hash::default(),
            ),
            Err(InstructionError::InvalidAccountData),
        );

        process_instruction_as_one_arg(
            &update_validator_identity(
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
            ),
            Err(InstructionError::InvalidAccountData),
        );
        process_instruction_as_one_arg(
            &update_commission(&Pubkey::new_unique(), &Pubkey::new_unique(), 0),
            Err(InstructionError::InvalidAccountData),
        );

        process_instruction_as_one_arg(
            &withdraw(
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                0,
                &Pubkey::new_unique(),
            ),
            Err(InstructionError::InvalidAccountData),
        );
    }

    #[test]
    fn test_vote_authorize_checked() {
        let vote_pubkey = Pubkey::new_unique();
        let authorized_pubkey = Pubkey::new_unique();
        let new_authorized_pubkey = Pubkey::new_unique();

        // Test with vanilla authorize accounts
        let mut instruction = authorize_checked(
            &vote_pubkey,
            &authorized_pubkey,
            &new_authorized_pubkey,
            VoteAuthorize::Voter,
        );
        instruction.accounts = instruction.accounts[0..2].to_vec();
        process_instruction_as_one_arg(&instruction, Err(InstructionError::NotEnoughAccountKeys));

        let mut instruction = authorize_checked(
            &vote_pubkey,
            &authorized_pubkey,
            &new_authorized_pubkey,
            VoteAuthorize::Withdrawer,
        );
        instruction.accounts = instruction.accounts[0..2].to_vec();
        process_instruction_as_one_arg(&instruction, Err(InstructionError::NotEnoughAccountKeys));

        // Test with non-signing new_authorized_pubkey
        let mut instruction = authorize_checked(
            &vote_pubkey,
            &authorized_pubkey,
            &new_authorized_pubkey,
            VoteAuthorize::Voter,
        );
        instruction.accounts[3] = AccountMeta::new_readonly(new_authorized_pubkey, false);
        process_instruction_as_one_arg(
            &instruction,
            Err(InstructionError::MissingRequiredSignature),
        );

        let mut instruction = authorize_checked(
            &vote_pubkey,
            &authorized_pubkey,
            &new_authorized_pubkey,
            VoteAuthorize::Withdrawer,
        );
        instruction.accounts[3] = AccountMeta::new_readonly(new_authorized_pubkey, false);
        process_instruction_as_one_arg(
            &instruction,
            Err(InstructionError::MissingRequiredSignature),
        );

        // Test with new_authorized_pubkey signer
        let vote_account = AccountSharedData::new(100, VoteState::size_of(), &id());
        let clock_address = sysvar::clock::id();
        let clock_account = account::create_account_shared_data_for_test(&Clock::default());
        let default_authorized_pubkey = Pubkey::default();
        let authorized_account = create_default_account();
        let new_authorized_account = create_default_account();
        let transaction_accounts = vec![
            (vote_pubkey, vote_account),
            (clock_address, clock_account),
            (default_authorized_pubkey, authorized_account),
            (new_authorized_pubkey, new_authorized_account),
        ];
        let instruction_accounts = vec![
            AccountMeta {
                pubkey: vote_pubkey,
                is_signer: false,
                is_writable: false,
            },
            AccountMeta {
                pubkey: clock_address,
                is_signer: false,
                is_writable: false,
            },
            AccountMeta {
                pubkey: default_authorized_pubkey,
                is_signer: true,
                is_writable: false,
            },
            AccountMeta {
                pubkey: new_authorized_pubkey,
                is_signer: true,
                is_writable: false,
            },
        ];
        process_instruction(
            &serialize(&VoteInstruction::AuthorizeChecked(VoteAuthorize::Voter)).unwrap(),
            transaction_accounts.clone(),
            instruction_accounts.clone(),
            Ok(()),
        );
        process_instruction(
            &serialize(&VoteInstruction::AuthorizeChecked(
                VoteAuthorize::Withdrawer,
            ))
            .unwrap(),
            transaction_accounts,
            instruction_accounts,
            Ok(()),
        );
    }
}
