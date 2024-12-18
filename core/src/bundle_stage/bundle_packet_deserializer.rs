use {
    crate::banking_stage::transaction_scheduler::{
        receive_and_buffer::{
            calculate_max_age, calculate_priority_and_cost, translate_to_runtime_view,
            PacketHandlingError,
        },
        transaction_state::TransactionState,
        transaction_state_container::{SharedBytes, TransactionViewState},
    },
    ahash::HashSet,
    solana_accounts_db::account_locks::validate_account_locks,
    solana_fee_structure::FeeBudgetLimits,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::transaction_meta::StaticMeta,
    solana_svm_transaction::svm_message::SVMMessage,
};

pub struct BundlePacketDeserializer;

impl BundlePacketDeserializer {
    /// Tries to deserialize a packet into a transaction state, returning an error if the packet is invalid.
    ///
    /// This should match the code in the following method:
    /// [`crate::banking_stage::transaction_scheduler::receive_and_buffer::TransactionViewReceiveAndBuffer::try_handle_packet`]
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes of the packet to deserialize.
    /// * `root_bank` - The root bank to use for the deserialization.
    /// * `working_bank` - The working bank to use for the deserialization.
    /// * `enable_static_instruction_limit` - Whether to enable the static instruction limit.
    /// * `transaction_account_lock_limit` - The transaction account lock limit to use for the deserialization.
    /// * `blacklisted_accounts` - The blacklisted accounts to use for the deserialization.
    pub fn try_handle_packet(
        bytes: SharedBytes,
        root_bank: &Bank,
        working_bank: &Bank,
        enable_static_instruction_limit: bool,
        transaction_account_lock_limit: usize,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> Result<TransactionViewState, PacketHandlingError> {
        let (view, deactivation_slot) = translate_to_runtime_view(
            bytes,
            working_bank,
            enable_static_instruction_limit,
            transaction_account_lock_limit,
        )?;
        if validate_account_locks(
            view.account_keys(),
            root_bank.get_transaction_account_lock_limit(),
        )
        .is_err()
        {
            return Err(PacketHandlingError::LockValidation);
        }

        if view
            .account_keys()
            .iter()
            .any(|account| blacklisted_accounts.contains(account))
        {
            return Err(PacketHandlingError::BlacklistedAccount);
        }

        let Ok(compute_budget_limits) = view
            .compute_budget_instruction_details()
            .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set)
        else {
            return Err(PacketHandlingError::ComputeBudget);
        };

        let max_age = calculate_max_age(root_bank.epoch(), deactivation_slot, root_bank.slot());
        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);
        let (priority, cost) = calculate_priority_and_cost(&view, &fee_budget_limits, working_bank);

        Ok(TransactionState::new(view, max_age, priority, cost))
    }
}
