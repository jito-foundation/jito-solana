use {
    crate::{
        account_rent_state::{check_rent_state, RentState},
        bank::Bank,
    },
    solana_sdk::{
        account::ReadableAccount, feature_set, message::SanitizedMessage, native_loader,
        transaction::Result, transaction_context::TransactionContext,
    },
};

pub(crate) struct TransactionAccountStateInfo {
    rent_state: Option<RentState>, // None: readonly account
}

impl Bank {
    pub(crate) fn get_transaction_account_state_info(
        &self,
        transaction_context: &TransactionContext,
        message: &SanitizedMessage,
    ) -> Vec<TransactionAccountStateInfo> {
        (0..message.account_keys().len())
            .map(|i| {
                let rent_state = if message.is_writable(i) {
                    let state = if let Ok(account) = transaction_context.get_account_at_index(i) {
                        let account = account.borrow();

                        // Native programs appear to be RentPaying because they carry low lamport
                        // balances; however they will never be loaded as writable
                        debug_assert!(!native_loader::check_id(account.owner()));

                        Some(RentState::from_account(
                            &account,
                            &self.rent_collector().rent,
                        ))
                    } else {
                        None
                    };
                    debug_assert!(
                        state.is_some(),
                        "message and transaction context out of sync, fatal"
                    );
                    state
                } else {
                    None
                };
                TransactionAccountStateInfo { rent_state }
            })
            .collect()
    }

    pub(crate) fn verify_transaction_account_state_changes(
        &self,
        pre_state_infos: &[TransactionAccountStateInfo],
        post_state_infos: &[TransactionAccountStateInfo],
        transaction_context: &TransactionContext,
    ) -> Result<()> {
        let include_account_index_in_err = self
            .feature_set
            .is_active(&feature_set::include_account_index_in_rent_error::id());
        let prevent_crediting_accounts_that_end_rent_paying = self
            .feature_set
            .is_active(&feature_set::prevent_crediting_accounts_that_end_rent_paying::id());
        for (i, (pre_state_info, post_state_info)) in
            pre_state_infos.iter().zip(post_state_infos).enumerate()
        {
            check_rent_state(
                pre_state_info.rent_state.as_ref(),
                post_state_info.rent_state.as_ref(),
                transaction_context,
                i,
                include_account_index_in_err,
                prevent_crediting_accounts_that_end_rent_paying,
            )?;
        }
        Ok(())
    }
}
