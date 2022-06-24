use {
    crate::{
        bundle::PacketBundle,
        unprocessed_packet_batches::{deserialize_packets, ImmutableDeserializedPacket},
    },
    solana_perf::packet::PacketBatch,
    solana_runtime::bank::Bank,
    solana_sdk::{
        bpf_loader_upgradeable,
        bundle::sanitized::SanitizedBundle,
        feature_set::FeatureSet,
        pubkey::Pubkey,
        transaction::{AddressLoader, SanitizedTransaction, TransactionAccountLocks},
    },
    std::{
        collections::{HashMap, HashSet, VecDeque},
        sync::Arc,
    },
    thiserror::Error,
    uuid::Uuid,
};

#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum BundleSchedulerError {
    #[error("Bundle locking error uuid: {0}")]
    GetLocksError(Uuid),
    #[error("Bundle contains invalid packets uuid: {0}")]
    InvalidPackets(Uuid),
}

pub type Result<T> = std::result::Result<T, BundleSchedulerError>;

pub struct LockedBundle {
    packet_bundle: PacketBundle,
    sanitized_bundle: SanitizedBundle,
    read_locks: HashMap<Pubkey, u64>,
    write_locks: HashMap<Pubkey, u64>,
}

impl LockedBundle {
    pub fn new(
        packet_bundle: PacketBundle,
        sanitized_bundle: SanitizedBundle,
        read_locks: HashMap<Pubkey, u64>,
        write_locks: HashMap<Pubkey, u64>,
    ) -> LockedBundle {
        LockedBundle {
            packet_bundle,
            sanitized_bundle,
            read_locks,
            write_locks,
        }
    }

    pub fn packet_bundle(&self) -> &PacketBundle {
        &self.packet_bundle
    }

    pub fn sanitized_bundle(&self) -> &SanitizedBundle {
        &self.sanitized_bundle
    }

    pub fn read_locks(&self) -> &HashMap<Pubkey, u64> {
        &self.read_locks
    }

    pub fn write_locks(&self) -> &HashMap<Pubkey, u64> {
        &self.write_locks
    }
}

pub struct BundleAccountLocker {
    num_bundle_batches_prelock: u64,
    unlocked_bundles: VecDeque<PacketBundle>,
    locked_bundles: VecDeque<LockedBundle>,
    read_locks: HashMap<Pubkey, u64>,
    write_locks: HashMap<Pubkey, u64>,
}

/// One can think of this like a bundle-level AccountLocks.
///
/// Ensures that BankingStage doesn't execute a transaction that mentions an account in a currently
/// executing bundle. Bundles can span multiple transactions and we want to ensure that BankingStage
/// can't execute a transaction that contains overlap with ANY accounts in a bundle such that BankingStage
/// can load, execute, and commit before executing a bundle is finished.
///
/// It also helps pre-lock bundles ahead of time. This attempts to prevent BundleStage from being
/// starved by a transaction being executed in BankingStage at the same time.
///
/// NOTE: this is currently used as a bundle locker to protect BankingStage and BundleStage race conditions.
/// When bundle stage is multi-threaded, we'll need to make this a scheduler that supports scheduling
/// bundles across multiple threads and self-references read and write locks when determining what to schedule.
impl BundleAccountLocker {
    pub fn new(num_bundle_batches_prelock: u64) -> BundleAccountLocker {
        BundleAccountLocker {
            num_bundle_batches_prelock,
            unlocked_bundles: VecDeque::with_capacity(100),
            locked_bundles: VecDeque::with_capacity((num_bundle_batches_prelock + 1) as usize),
            read_locks: HashMap::with_capacity(100),
            write_locks: HashMap::with_capacity(100),
        }
    }

    /// Push bundles onto unlocked bundles deque
    pub fn push(&mut self, bundles: Vec<PacketBundle>) {
        for bundle in bundles {
            self.unlocked_bundles.push_back(bundle);
        }
    }

    /// Pushes an already locked bundle to the front of locked bundles in case PoH max height reached
    pub fn push_front(&mut self, locked_bundle: LockedBundle) {
        self.locked_bundles.push_front(locked_bundle)
    }

    /// returns total number of bundles pending
    pub fn num_bundles(&self) -> usize {
        self.unlocked_bundles.len() + self.locked_bundles.len()
    }

    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn read_locks(&self) -> HashSet<Pubkey> {
        self.read_locks.keys().cloned().collect()
    }

    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn write_locks(&self) -> HashSet<Pubkey> {
        self.write_locks.keys().cloned().collect()
    }

    /// Pop bundles off unlocked bundles deque.
    /// Ensures the locked_bundles deque is refilled.
    pub fn pop(
        &mut self,
        bank: &Arc<Bank>,
        tip_program_id: &Pubkey,
        consensus_accounts_cache: &HashSet<Pubkey>,
    ) -> Option<LockedBundle> {
        // pre-lock bundles up to num_bundle_batches_prelock
        // +1 because it will immediately pop one off
        while !self.unlocked_bundles.is_empty()
            && self.locked_bundles.len() < self.num_bundle_batches_prelock as usize + 1
        {
            let bundle = self.unlocked_bundles.pop_front().unwrap();
            match Self::get_locked_bundle(&bundle, bank, tip_program_id, consensus_accounts_cache) {
                Ok(locked_bundle) => {
                    self.lock_bundle(&locked_bundle);
                    self.locked_bundles.push_back(locked_bundle);
                }
                Err(e) => {
                    error!("error locking bundle: {:?}", e);
                }
            }
        }

        while !self.locked_bundles.is_empty() {
            let old_locked_bundle = self.locked_bundles.pop_front()?;

            let new_locked_bundle = match Self::get_locked_bundle(
                old_locked_bundle.packet_bundle(),
                bank,
                tip_program_id,
                consensus_accounts_cache,
            ) {
                Ok(new_locked_bundle) => new_locked_bundle,
                Err(e) => {
                    error!("dropping bundle error: {:?}", e);
                    self.unlock_bundle(old_locked_bundle);
                    continue;
                }
            };

            // rollback state and apply new state because transaction serialized differently
            // this can happen because a change in the lookup table for TX v2
            if new_locked_bundle.read_locks() != old_locked_bundle.read_locks()
                || new_locked_bundle.write_locks() != old_locked_bundle.write_locks()
            {
                self.unlock_bundle(old_locked_bundle);
                self.lock_bundle(&new_locked_bundle);
            }

            return Some(new_locked_bundle);
        }
        None
    }

    fn get_locked_bundle(
        packet_bundle: &PacketBundle,
        bank: &Arc<Bank>,
        tip_program_id: &Pubkey,
        consensus_accounts_cache: &HashSet<Pubkey>,
    ) -> Result<LockedBundle> {
        let sanitized_bundle = Self::get_sanitized_bundle(
            packet_bundle,
            bank,
            tip_program_id,
            consensus_accounts_cache,
        )?;
        let (read_locks, write_locks) =
            Self::get_read_write_locks(&sanitized_bundle, &bank.feature_set)?;

        Ok(LockedBundle::new(
            packet_bundle.clone(),
            sanitized_bundle,
            read_locks,
            write_locks,
        ))
    }

    fn lock_bundle(&mut self, locked_bundle: &LockedBundle) {
        for (acc, count) in locked_bundle.read_locks() {
            *self.read_locks.entry(*acc).or_insert(0) += count;
        }
        for (acc, count) in locked_bundle.write_locks() {
            *self.write_locks.entry(*acc).or_insert(0) += count;
        }

        debug!("lock read locks: {:?}", self.read_locks);
        debug!("lock write locks: {:?}", self.write_locks);
    }

    /// Returns the read and write locks for this bundle
    /// Each lock type contains a HashMap which maps Pubkey to number of locks held
    fn get_read_write_locks(
        bundle: &SanitizedBundle,
        feature_set: &FeatureSet,
    ) -> Result<(HashMap<Pubkey, u64>, HashMap<Pubkey, u64>)> {
        let transaction_locks: Vec<TransactionAccountLocks> = bundle
            .transactions
            .iter()
            .filter_map(|tx| tx.get_account_locks(feature_set).ok())
            .collect();

        if transaction_locks.len() != bundle.transactions.len() {
            return Err(BundleSchedulerError::GetLocksError(bundle.uuid));
        }

        let bundle_read_locks = transaction_locks
            .iter()
            .flat_map(|tx| tx.readonly.iter().map(|a| **a));
        let bundle_write_locks = transaction_locks
            .iter()
            .flat_map(|tx| tx.writable.iter().map(|a| **a));

        let bundle_read_locks =
            bundle_read_locks
                .into_iter()
                .fold(HashMap::new(), |mut map, acc| {
                    *map.entry(acc).or_insert(0) += 1;
                    map
                });

        let bundle_write_locks =
            bundle_write_locks
                .into_iter()
                .fold(HashMap::new(), |mut map, acc| {
                    *map.entry(acc).or_insert(0) += 1;
                    map
                });

        Ok((bundle_read_locks, bundle_write_locks))
    }

    /// unlocks any pre-locked accounts in this bundle
    /// the caller is responsible for ensuring the LockedBundle passed in here was returned from
    /// BundleScheduler::pop
    pub fn unlock_bundle(&mut self, locked_bundle: LockedBundle) {
        for (acc, count) in locked_bundle.read_locks() {
            let entry = self.read_locks.entry(*acc).or_insert(0);
            *entry = entry.saturating_sub(*count);
            if self.read_locks.get(acc) == Some(&0) {
                self.read_locks.remove(acc);
            }
        }
        for (acc, count) in locked_bundle.write_locks() {
            let entry = self.write_locks.entry(*acc).or_insert(0);
            *entry = entry.saturating_sub(*count);
            if self.write_locks.get(acc) == Some(&0) {
                self.write_locks.remove(acc);
            }
        }

        debug!("unlock read locks: {:?}", self.read_locks);
        debug!("unlock write locks: {:?}", self.write_locks);
    }

    fn get_sanitized_bundle(
        bundle: &PacketBundle,
        bank: &Arc<Bank>,
        tip_program_id: &Pubkey,
        consensus_accounts_cache: &HashSet<Pubkey>,
    ) -> Result<SanitizedBundle> {
        let packet_indexes = Self::generate_packet_indexes(&bundle.batch);
        let deserialized_packets = deserialize_packets(&bundle.batch, &packet_indexes);

        let transactions: Vec<SanitizedTransaction> = deserialized_packets
            .filter_map(|p| {
                let immutable_packet = p.immutable_section().clone();
                Self::transaction_from_deserialized_packet(
                    &immutable_packet,
                    &bank.feature_set,
                    bank.vote_only_bank(),
                    bank.as_ref(),
                    tip_program_id,
                    consensus_accounts_cache,
                )
            })
            .collect();

        if transactions.is_empty() || bundle.batch.packets.len() != transactions.len() {
            return Err(BundleSchedulerError::InvalidPackets(bundle.uuid));
        }

        Ok(SanitizedBundle {
            transactions,
            uuid: bundle.uuid,
        })
    }

    fn generate_packet_indexes(batch: &PacketBatch) -> Vec<usize> {
        batch
            .iter()
            .enumerate()
            .filter(|(_, pkt)| !pkt.meta.discard())
            .map(|(index, _)| index)
            .collect()
    }

    // This function deserializes packets into transactions, computes the blake3 hash of transaction
    // messages, and verifies secp256k1 instructions. A list of sanitized transactions are returned
    // with their packet indexes.
    #[allow(clippy::needless_collect)]
    fn transaction_from_deserialized_packet(
        deserialized_packet: &ImmutableDeserializedPacket,
        feature_set: &Arc<FeatureSet>,
        votes_only: bool,
        address_loader: impl AddressLoader,
        tip_program_id: &Pubkey,
        consensus_accounts_cache: &HashSet<Pubkey>,
    ) -> Option<SanitizedTransaction> {
        if votes_only && !deserialized_packet.is_simple_vote() {
            return None;
        }

        let tx = SanitizedTransaction::try_new(
            deserialized_packet.transaction().clone(),
            *deserialized_packet.message_hash(),
            deserialized_packet.is_simple_vote(),
            address_loader,
        )
        .ok()?;
        tx.verify_precompiles(feature_set).ok()?;

        // Prevent transactions from mentioning the tip program to avoid getting the tip_receiver
        // changed mid-slot and the rest of the tips stolen.
        // NOTE: if this is a weak assumption helpful for testing deployment,
        // before production it shall only be the tip program
        let tx_accounts = tx.message().account_keys();
        if tx_accounts.iter().any(|a| a == tip_program_id)
            && !tx_accounts
                .iter()
                .any(|a| a == &bpf_loader_upgradeable::id())
        {
            warn!("someone attempted to change the tip program!! tx: {:?}", tx);
            return None;
        }

        // NOTE: may want to revisit this as it may reduce use cases of locking accounts
        // used for legitimate cases in bundles.
        if tx_accounts
            .iter()
            .any(|a| consensus_accounts_cache.contains(a))
        {
            warn!(
                "someone attempted to lock a consensus related account!! tx: {:?}",
                tx
            );
            return None;
        }
        Some(tx)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_single_push_pop() {}

    #[test]
    fn test_simple_push_pop() {}

    #[test]
    fn test_txv2_single_push_pop() {}

    #[test]
    fn test_txv2_addr_changes() {}

    #[test]
    fn test_locking() {}
}
