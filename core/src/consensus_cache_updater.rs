use {
    solana_runtime::bank::Bank,
    solana_sdk::{clock::Epoch, pubkey::Pubkey},
    std::collections::HashSet,
};

#[derive(Default)]
pub(crate) struct ConsensusCacheUpdater {
    last_epoch_updated: Epoch,
    consensus_accounts_cache: HashSet<Pubkey>,
}

impl ConsensusCacheUpdater {
    pub(crate) fn consensus_accounts_cache(&self) -> &HashSet<Pubkey> {
        &self.consensus_accounts_cache
    }

    /// Builds a HashSet of all consensus related accounts for the Bank's epoch
    fn get_consensus_accounts(bank: &Bank) -> HashSet<Pubkey> {
        let mut consensus_accounts: HashSet<Pubkey> = HashSet::new();
        if let Some(epoch_stakes) = bank.epoch_stakes(bank.epoch()) {
            // votes use the following accounts:
            // - vote_account pubkey: writeable
            // - authorized_voter_pubkey: read-only
            // - node_keypair pubkey: payer (writeable)
            let node_id_vote_accounts = epoch_stakes.node_id_to_vote_accounts();

            let vote_accounts = node_id_vote_accounts
                .values()
                .flat_map(|v| v.vote_accounts.clone());

            // vote_account
            consensus_accounts.extend(vote_accounts);
            // authorized_voter_pubkey
            consensus_accounts.extend(epoch_stakes.epoch_authorized_voters().keys());
            // node_keypair
            consensus_accounts.extend(epoch_stakes.node_id_to_vote_accounts().keys());
        }
        consensus_accounts
    }

    /// Updates consensus-related accounts on epoch boundaries
    pub(crate) fn maybe_update(&mut self, bank: &Bank) -> bool {
        if bank.epoch() > self.last_epoch_updated {
            self.consensus_accounts_cache = Self::get_consensus_accounts(bank);
            self.last_epoch_updated = bank.epoch();
            true
        } else {
            false
        }
    }
}
