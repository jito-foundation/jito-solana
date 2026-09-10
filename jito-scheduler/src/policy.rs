use {
    agave_scheduling_utils::thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadSet},
    solana_pubkey::Pubkey,
    std::collections::BTreeMap,
};

/// The union of a job's account accesses. A write supersedes every read of the same key.
#[derive(Clone, Debug, Default)]
pub(crate) struct Access {
    pub writes: Vec<Pubkey>,
    pub reads: Vec<Pubkey>,
}

impl Access {
    pub fn from_keys(keys: impl IntoIterator<Item = (Pubkey, bool)>) -> Self {
        let mut union = BTreeMap::new();
        for (key, write) in keys {
            *union.entry(key).or_insert(false) |= write;
        }
        let (writes, reads) = union
            .into_iter()
            .partition::<Vec<_>, _>(|(_, write)| *write);
        Self {
            writes: writes.into_iter().map(|(key, _)| key).collect(),
            reads: reads.into_iter().map(|(key, _)| key).collect(),
        }
    }

    pub fn conflicts(&self, other: &Self) -> bool {
        self.writes
            .iter()
            .any(|key| other.writes.contains(key) || other.reads.contains(key))
            || self.reads.iter().any(|key| other.writes.contains(key))
    }

    pub fn merge(&mut self, other: &Self) {
        *self = Self::from_keys(
            self.writes
                .iter()
                .map(|key| (*key, true))
                .chain(self.reads.iter().map(|key| (*key, false)))
                .chain(other.writes.iter().map(|key| (*key, true)))
                .chain(other.reads.iter().map(|key| (*key, false))),
        );
    }
}

pub(crate) struct Dispatch {
    locks: ThreadAwareAccountLocks,
    pub outstanding: Vec<usize>,
    max_outstanding: usize,
}

impl Dispatch {
    pub fn new(workers: usize, max_outstanding: usize) -> Self {
        Self {
            locks: ThreadAwareAccountLocks::new(workers),
            outstanding: vec![0; workers],
            max_outstanding,
        }
    }

    pub fn reserve(&mut self, access: &Access) -> Option<usize> {
        let mut allowed = ThreadSet::none();
        for (worker, count) in self.outstanding.iter().enumerate() {
            if *count < self.max_outstanding {
                allowed.insert(worker);
            }
        }
        let counts = &self.outstanding;
        let worker = self
            .locks
            .try_lock_accounts(
                access.writes.iter(),
                access.reads.iter(),
                allowed,
                |threads| {
                    threads
                        .contained_threads_iter()
                        .min_by_key(|worker| counts[*worker])
                        .unwrap()
                },
            )
            .ok()?;
        self.outstanding[worker] = self.outstanding[worker].checked_add(1).unwrap();
        Some(worker)
    }

    pub fn release(&mut self, access: &Access, worker: usize) {
        self.locks
            .unlock_accounts(access.writes.iter(), access.reads.iter(), worker);
        self.outstanding[worker] = self.outstanding[worker].checked_sub(1).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }
    fn write(byte: u8) -> Access {
        Access::from_keys([(key(byte), true)])
    }

    #[test]
    fn bundle_union_prefers_write_and_deduplicates() {
        let access = Access::from_keys([
            (key(1), false),
            (key(1), true),
            (key(1), false),
            (key(2), false),
        ]);
        assert_eq!(access.writes, vec![key(1)]);
        assert_eq!(access.reads, vec![key(2)]);
    }

    #[test]
    fn parallel_disjoint_work_and_fifo_for_conflicts() {
        let mut dispatch = Dispatch::new(2, 3);
        assert_eq!(dispatch.reserve(&write(1)), Some(0));
        assert_eq!(dispatch.reserve(&write(2)), Some(1));
        assert_eq!(dispatch.reserve(&write(1)), Some(0));
        let bundle = Access::from_keys([(key(1), true), (key(2), true)]);
        assert_eq!(dispatch.reserve(&bundle), None);
        dispatch.release(&write(2), 1);
        assert_eq!(dispatch.reserve(&bundle), Some(0));
        dispatch.release(&bundle, 0);
        dispatch.release(&write(1), 0);
        dispatch.release(&write(1), 0);
        assert_eq!(dispatch.outstanding, vec![0, 0]);
    }

    #[test]
    fn blocked_bundle_barrier_prevents_overtaking() {
        let first = Access::from_keys([(key(1), true), (key(2), false)]);
        let mut barrier = Access::default();
        barrier.merge(&first);
        assert!(barrier.conflicts(&write(1)));
        assert!(barrier.conflicts(&write(2)));
        assert!(!barrier.conflicts(&write(3)));
        assert!(!barrier.conflicts(&Access::from_keys([(key(2), false)])));
    }

    #[test]
    fn queue_capacity_keeps_conflicting_work_on_its_worker() {
        let mut dispatch = Dispatch::new(2, 1);
        assert_eq!(dispatch.reserve(&write(1)), Some(0));
        assert_eq!(dispatch.reserve(&write(1)), None);
        assert_eq!(dispatch.reserve(&write(2)), Some(1));
        dispatch.release(&write(1), 0);
        assert_eq!(dispatch.reserve(&write(1)), Some(0));
    }
}
