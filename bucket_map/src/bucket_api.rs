// Conditional utility imports for development contexts only.
#[cfg(feature = "dev-context-only-utils")]
use crate::bucket_item::BucketItem;

use {
    // Core bucket structures and error types from the local crate.
    crate::{
        bucket::Bucket, bucket_map::BucketMapError, bucket_stats::BucketMapStats,
        restart::RestartableBucket, MaxSearch, RefCount,
    },
    // Solana's public key type.
    solana_pubkey::Pubkey,
    // Standard library imports for concurrency and file paths.
    std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock, RwLockWriteGuard,
        },
    },
};

/// A type alias for the bucket, protected by a Read/Write lock.
/// The Option<T> allows the bucket to be lazily initialized or temporarily dropped.
type LockedBucket<T> = RwLock<Option<Bucket<T>>>;

/// The public API interface for managing a single Bucket in the BucketMap.
pub struct BucketApi<T: Clone + Copy + PartialEq + 'static> {
    /// List of disk paths where the bucket can store its data.
    drives: Arc<Vec<PathBuf>>,
    /// Maximum search distance for hash probing.
    max_search: MaxSearch,
    /// Statistics tracking for the bucket map.
    pub stats: Arc<BucketMapStats>,

    /// The bucket itself, protected by a lock for concurrent access.
    bucket: LockedBucket<T>,
    /// Atomically tracked count of items currently in the bucket.
    count: Arc<AtomicU64>,

    /// Keeps track of the index file this bucket is currently using,
    /// crucial for restart and persistence.
    restartable_bucket: RestartableBucket,
}

impl<T: Clone + Copy + PartialEq + std::fmt::Debug> BucketApi<T> {
    /// Creates a new, uninitialized BucketApi instance.
    pub(crate) fn new(
        drives: Arc<Vec<PathBuf>>,
        max_search: MaxSearch,
        stats: Arc<BucketMapStats>,
        restartable_bucket: RestartableBucket,
    ) -> Self {
        Self {
            drives,
            max_search,
            stats,
            // Use RwLock::default() which initializes the Option to None.
            bucket: RwLock::default(),
            // Use Arc::default() which initializes the AtomicU64 to 0.
            count: Arc::default(),
            restartable_bucket,
        }
    }

    /// Get all items for the bucket (used mainly for dev context).
    #[cfg(feature = "dev-context-only-utils")]
    pub fn items(&self) -> Vec<BucketItem<T>> {
        // Use expect() for better error diagnostics on poisoned locks.
        self.bucket
            .read()
            .expect("BucketApi::items: Failed to acquire read lock")
            .as_ref()
            .map(|bucket| bucket.items())
            .unwrap_or_default()
    }

    /// Get the Pubkeys stored in this bucket.
    pub fn keys(&self) -> Vec<Pubkey> {
        self.bucket
            .read()
            .expect("BucketApi::keys: Failed to acquire read lock")
            .as_ref()
            .map_or_else(Vec::default, |bucket| bucket.keys())
    }

    /// Read the value and reference count associated with a Pubkey.
    /// The value is converted into the type C using the From<&[T]> trait.
    pub fn read_value<C: for<'a> From<&'a [T]>>(&self, key: &Pubkey) -> Option<(C, RefCount)> {
        self.bucket
            .read()
            .expect("BucketApi::read_value: Failed to acquire read lock")
            .as_ref()
            .and_then(|bucket| {
                bucket
                    .read_value(key)
                    // Map the returned slice & RefCount into the target type C.
                    .map(|(value, ref_count)| (C::from(value), ref_count))
            })
    }

    /// Returns the current number of entries in the bucket, read from the atomic counter.
    pub fn bucket_len(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Deletes a key and its associated value from the bucket.
    pub fn delete_key(&self, key: &Pubkey) {
        let mut bucket = self.get_write_bucket();
        // The bucket is guaranteed to be Some in get_write_bucket, but we check defensively.
        if let Some(bucket) = bucket.as_mut() {
            bucket.delete_key(key)
        }
    }

    /// Allocates and initializes the inner Bucket object if it is currently None.
    fn allocate_bucket(&self, bucket: &mut RwLockWriteGuard<Option<Bucket<T>>>) {
        if bucket.is_none() {
            **bucket = Some(Bucket::new(
                Arc::clone(&self.drives),
                self.max_search,
                Arc::clone(&self.stats),
                Arc::clone(&self.count),
                self.restartable_bucket.clone(),
            ));
        }
    }

    /// Acquires a write lock on the bucket, ensuring it is allocated before returning.
    /// Also handles any deferred grow operations.
    fn get_write_bucket(&self) -> RwLockWriteGuard<'_, Option<Bucket<T>>> {
        let mut bucket = self.bucket.write().expect("BucketApi::get_write_bucket: Failed to acquire write lock");
        
        if let Some(bucket) = bucket.as_mut() {
            // Apply any pending grow operations if the bucket is already allocated.
            bucket.handle_delayed_grows();
        } else {
            // If not allocated, perform lazy initialization.
            self.allocate_bucket(&mut bucket);
        }
        bucket
    }

    /// Inserts a new key-value pair into the bucket.
    pub fn insert(&self, pubkey: &Pubkey, value: (&[T], RefCount)) {
        let mut bucket = self.get_write_bucket();
        // The bucket is guaranteed to be initialized by get_write_bucket().
        bucket.as_mut().unwrap().insert(pubkey, value)
    }

    /// Queues a grow operation to resize the underlying bucket.
    /// This happens under a read lock and is applied later on the next write lock acquisition.
    pub fn grow(&self, err: BucketMapError) {
        if let Some(bucket) = self.bucket
            .read()
            .expect("BucketApi::grow: Failed to acquire read lock")
            .as_ref() 
        {
            bucket.grow(err)
        }
    }

    /// Hints the resizing algorithm that the index needs to hold approximately `count` entries soon,
    /// preventing repeated incremental resizes.
    pub fn set_anticipated_count(&self, count: u64) {
        let mut bucket = self.get_write_bucket();
        // The bucket is guaranteed to be initialized.
        bucket.as_mut().unwrap().set_anticipated_count(count);
    }

    /// Performs a batch insert of items. Assumes a single slot list element and ref_count == 1.
    /// Returns a list of (index, value) for keys that already existed (duplicates).
    pub fn batch_insert_non_duplicates(&self, items: &[(Pubkey, T)]) -> Vec<(usize, T)> {
        let mut bucket = self.get_write_bucket();
        // The bucket is guaranteed to be initialized.
        bucket.as_mut().unwrap().batch_insert_non_duplicates(items)
    }

    /// Updates the value associated with a key using a mutable closure.
    pub fn update<F>(&self, key: &Pubkey, updatefn: F)
    where
        F: FnMut(Option<(&[T], RefCount)>) -> Option<(Vec<T>, RefCount)>,
    {
        let mut bucket = self.get_write_bucket();
        // The bucket is guaranteed to be initialized.
        bucket.as_mut().unwrap().update(key, updatefn)
    }

    /// Attempts a non-locking write operation on the underlying storage.
    /// Returns an error if the write fails (e.g., due to space constraints).
    pub fn try_write(
        &self,
        pubkey: &Pubkey,
        value: (&[T], RefCount),
    ) -> Result<(), BucketMapError> {
        let mut bucket = self.get_write_bucket();
        bucket
            .as_mut()
            .unwrap()
            .try_write(pubkey, value.0.iter(), value.0.len(), value.1)
    }
}
