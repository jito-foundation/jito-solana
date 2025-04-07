use anyhow::Result;
use bincode;
use csv::Writer;
use rocksdb::{Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

//------------------------------------------------------------------------------
// FeeRecords Design Overview
//------------------------------------------------------------------------------
// This module implements a persistent storage system for tracking priority fee records
// using RocksDB as the underlying key-value store. The design uses a sophisticated
// multi-key indexing approach:
//
// 1. Record Storage:
//    - Each fee record is stored under a "0:{epoch}:{slot}" key
//    - The value contains the full serialized FeeRecordEntry with all metadata
//    - Records persist indefinitely for historical tracking and reporting
//
// 2. State Index Keys:
//    - "1:{epoch}:{slot}" - Marks records in Unprocessed state (created but not yet processed)
//    - "3:{epoch}:{slot}" - Marks records in Skipped state (failed blocks or unavailable)
//    - "4:{epoch}:{slot}" - Marks records in AntedUp state (paid via ante-up mechanism)
//    - "2:{epoch}:{slot}" - Marks records in ProcessedAndPending state (processed but fees not sent)
//    - "5:{epoch}:{slot}" - Marks records in Complete state (fees successfully sent)
//    - "8:{epoch}:{slot}" - Retrieves records regardless of state (wildcard)
//
// 3. Category Index Keys:
//    - "6:{epoch}:{slot}" - Marks records in PriorityFee category
//    - "7:{epoch}:{slot}" - Marks records in Ante category
//    - When a record transitions states, old indexes are deleted and new ones created
//
// 4. Epoch-Based Query Support:
//    - Keys include epoch information as a primary component of the key structure
//    - The format used is "{prefix}{epoch}{slot}" where epoch and slot are encoded as big-endian bytes
//    - This enables efficient prefix-based queries at both the epoch and slot level
//    - Prefix extraction is configured on the first byte to support key type filtering
//
// This approach allows:
//    - Efficient O(1) retrieval of specific records by slot and epoch
//    - Fast enumeration of records in specific states without full DB scans
//    - Category-based filtering for specialized reporting
//    - Atomic state transitions with batch operations for consistency
//    - Epoch-based querying for aggregated analysis
//
// Key Functions:
//    - add_priority_fee_record(): Creates a new fee record in Unprocessed state
//    - add_ante_record(): Adds a record in Ante category for balance recovery
//    - process_record(): Updates a record with fee data and marks as ProcessedAndPending
//    - skip_record(): Marks a record as Skipped (block unavailable)
//    - complete_record(): Marks a record as Complete (fees sent)
//    - get_record(): Retrieves a specific record by slot and epoch
//    - get_records_by_state(): Gets records of a specific state
//    - get_records_by_category(): Gets records of a specific category
//    - get_total_pending_lamports(): Calculates total pending fees ready to send
//    - export_to_csv(): Exports records filtered by state and epoch
//    - compact(): Optimizes database storage and performance
//
//------------------------------------------------------------------------------

/// Configuration options for RocksDB performance tuning
#[derive(Debug, Clone)]
pub struct FeeRecordsConfig {
    /// Write buffer size in bytes (default: 64MB)
    pub write_buffer_size: Option<usize>,
    /// Maximum number of open files (default: 1000)
    pub max_open_files: Option<i32>,
    /// Compaction style (default: Level)
    pub compaction_style: Option<rocksdb::DBCompactionStyle>,
}

impl Default for FeeRecordsConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: Some(64 * 1024 * 1024), // 64MB
            max_open_files: Some(1000),
            compaction_style: Some(rocksdb::DBCompactionStyle::Level),
        }
    }
}

pub struct FeeRecords {
    db: DB,
}

impl FeeRecords {
    /// Creates a new FeeRecords instance with the given db directory
    pub fn new<P: AsRef<Path>>(fee_records_db_directory: P) -> Result<Self> {
        Self::new_with_config(fee_records_db_directory, FeeRecordsConfig::default())
    }

    /// Creates a new FeeRecords instance with custom configuration
    pub fn new_with_config<P: AsRef<Path>>(
        fee_records_db_directory: P,
        config: FeeRecordsConfig,
    ) -> Result<Self> {
        let path = fee_records_db_directory.as_ref();

        // Create directory if it doesn't exist
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let mut opts = Options::default();
        opts.create_if_missing(true);

        // Apply configuration options if provided
        if let Some(buffer_size) = config.write_buffer_size {
            opts.set_write_buffer_size(buffer_size);
        }

        if let Some(max_files) = config.max_open_files {
            opts.set_max_open_files(max_files);
        }

        if let Some(compaction) = config.compaction_style {
            opts.set_compaction_style(compaction);
        }

        // NOTE: This line is really important, without it - RocksDB will match several
        // records to the same prefix for some reason.
        //
        // For fututre features that will filter by epoch - this will have to be modified
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(1));

        let db = DB::open(&opts, path)?;
        Ok(Self { db })
    }

    /// Helper function to get current timestamp
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn prepare_add_record(&self, record: &FeeRecordEntry, batch: &mut WriteBatch) -> Result<()> {
        let encoded = bincode::serialize(record)?;
        let record_key = FeeRecordKey::record(record.slot, record.epoch);
        let state_key = FeeRecordKey::state(record.state, record.slot, record.epoch);
        let category_key = FeeRecordKey::category(record.category, record.slot, record.epoch);
        let any_key = FeeRecordKey::any(record.slot, record.epoch);

        // Use a WriteBatch for atomicity
        batch.put(&record_key, &encoded);
        batch.put(&state_key, &[]); // Empty value since the key itself is the index
        batch.put(&category_key, &[]); // Empty value since the key itself is the index
        batch.put(&any_key, &[]); // Add to "any" index for querying all records regardless of state

        Ok(())
    }

    fn prepare_transition_state(
        &self,
        record: &FeeRecordEntry,
        old_state: FeeRecordState,
        new_state: FeeRecordState,
        batch: &mut WriteBatch,
    ) -> Result<()> {
        let mut record = record.clone();
        record.state = new_state;

        let encoded = bincode::serialize(&record)?;
        let record_key = FeeRecordKey::record(record.slot, record.epoch);
        let old_state_key = FeeRecordKey::state(old_state, record.slot, record.epoch);
        let new_state_key = FeeRecordKey::state(new_state, record.slot, record.epoch);

        batch.put(&record_key, &encoded);
        batch.delete(&old_state_key); // Delete old state
        batch.put(&new_state_key, &[]); // Add in new state

        Ok(())
    }

    /// Adds a new fee record in Unprocessed state
    pub fn add_priority_fee_record(&self, slot: u64, epoch: u64) -> Result<()> {
        // Check if record already exists
        if self.does_record_exsist(slot, epoch) {
            return Err(anyhow::anyhow!("Record for slot {} already exists", slot));
        }

        let record = FeeRecordEntry {
            timestamp: Self::current_timestamp(),
            slot,
            epoch,
            priority_fee_lamports: 0, // No fees yet since it's unprocessed
            state: FeeRecordState::Unprocessed,
            category: FeeRecordCategory::PriorityFee,
            slot_landed: 0,
            signature: String::new(),
        };

        let mut batch = WriteBatch::default();
        self.prepare_add_record(&record, &mut batch)?;
        self.db.write(batch)?;
        Ok(())
    }

    /// Adds a new fee record in Ante state (special state for remaining balance payout)
    pub fn add_ante_record(
        &self,
        slot: u64,
        epoch: u64,
        priority_fee_lamports: u64,
        signature: &str,
        slot_landed: u64,
        cancel_all_outstanding_records: bool,
    ) -> Result<()> {
        // Check if record already exists
        if self.does_record_exsist(slot, epoch) {
            return Err(anyhow::anyhow!("Record for slot {} already exists", slot));
        }

        let record = FeeRecordEntry {
            timestamp: Self::current_timestamp(),
            slot,
            epoch,
            priority_fee_lamports,
            state: FeeRecordState::Complete,
            category: FeeRecordCategory::Ante,
            slot_landed,
            signature: signature.to_string(),
        };

        let mut batch = WriteBatch::default();
        self.prepare_add_record(&record, &mut batch)?;

        if cancel_all_outstanding_records {
            let unprocessed_records = self.get_records_by_state(FeeRecordState::Unprocessed)?;
            let pending_records = self.get_records_by_state(FeeRecordState::ProcessedAndPending)?;

            for record in unprocessed_records {
                self.prepare_transition_state(
                    &record,
                    FeeRecordState::Unprocessed,
                    FeeRecordState::AntedUp,
                    &mut batch,
                )?;
            }
            for record in pending_records {
                self.prepare_transition_state(
                    &record,
                    FeeRecordState::ProcessedAndPending,
                    FeeRecordState::AntedUp,
                    &mut batch,
                )?;
            }
        }

        self.db.write(batch)?;

        Ok(())
    }

    /// Adds info record
    pub fn add_info_record(&self, slot: u64, epoch: u64, priority_fee_lamports: u64) -> Result<()> {
        // Check if record already exists
        if self.does_record_exsist(slot, epoch) {
            return Err(anyhow::anyhow!("Record for slot {} already exists", slot));
        }

        let record = FeeRecordEntry {
            timestamp: Self::current_timestamp(),
            slot,
            epoch,
            priority_fee_lamports,
            state: FeeRecordState::Complete,
            category: FeeRecordCategory::Info,
            slot_landed: 0,
            signature: String::new(),
        };

        let mut batch = WriteBatch::default();
        self.prepare_add_record(&record, &mut batch)?;

        self.db.write(batch)?;

        Ok(())
    }

    /// Marks a record as skipped (block failed or not available)
    pub fn skip_record(&self, slot: u64, epoch: u64) -> Result<()> {
        let record_key = FeeRecordKey::record(slot, epoch);

        // Get the existing record
        match self.db.get(&record_key)? {
            Some(data) => {
                let mut record: FeeRecordEntry = bincode::deserialize(&data)?;

                if record.state != FeeRecordState::Unprocessed {
                    return Err(anyhow::anyhow!(
                        "Record for slot {} is not in Unprocessed state",
                        slot
                    ));
                }

                // Update the record
                record.state = FeeRecordState::Skipped;

                let mut batch = WriteBatch::default();
                self.prepare_transition_state(
                    &record,
                    FeeRecordState::Unprocessed,
                    FeeRecordState::Skipped,
                    &mut batch,
                )?;
                self.db.write(batch)?;
                Ok(())
            }
            None => Err(anyhow::anyhow!("No record found for slot {}", slot)),
        }
    }

    /// Marks a record as processed and adds the priority fee amount
    pub fn process_record(&self, slot: u64, epoch: u64, priority_fee_lamports: u64) -> Result<()> {
        let record_key = FeeRecordKey::record(slot, epoch);

        // Get the existing record
        match self.db.get(&record_key)? {
            Some(data) => {
                let mut record: FeeRecordEntry = bincode::deserialize(&data)?;

                if record.state != FeeRecordState::Unprocessed {
                    return Err(anyhow::anyhow!(
                        "Record for slot {} is not in Unprocessed state",
                        slot
                    ));
                }

                // Update the record
                record.state = FeeRecordState::ProcessedAndPending;
                record.priority_fee_lamports = priority_fee_lamports;

                let mut batch = WriteBatch::default();
                self.prepare_transition_state(
                    &record,
                    FeeRecordState::Unprocessed,
                    FeeRecordState::ProcessedAndPending,
                    &mut batch,
                )?;
                self.db.write(batch)?;
                Ok(())
            }
            None => Err(anyhow::anyhow!("No record found for slot {}", slot)),
        }
    }

    /// Marks a record as complete with transaction details
    pub fn complete_record(
        &self,
        slot: u64,
        epoch: u64,
        signature: &str,
        slot_landed: u64,
    ) -> Result<()> {
        let record_key = FeeRecordKey::record(slot, epoch);

        // Get the existing record
        match self.db.get(&record_key)? {
            Some(data) => {
                let mut record: FeeRecordEntry = bincode::deserialize(&data)?;

                if record.state != FeeRecordState::ProcessedAndPending {
                    return Err(anyhow::anyhow!(
                        "Record for slot {} is not in Processed state",
                        slot
                    ));
                }

                // Update the record
                record.state = FeeRecordState::Complete;
                record.signature = signature.to_string();
                record.slot_landed = slot_landed;

                let mut batch = WriteBatch::default();
                self.prepare_transition_state(
                    &record,
                    FeeRecordState::ProcessedAndPending,
                    FeeRecordState::Complete,
                    &mut batch,
                )?;
                self.db.write(batch)?;
                Ok(())
            }
            None => Err(anyhow::anyhow!("No record found for slot {}", slot)),
        }
    }

    pub fn does_record_exsist(&self, slot: u64, epoch: u64) -> bool {
        let key = FeeRecordKey::record(slot, epoch);
        self.db.key_may_exist(key)
    }

    /// Gets a specific fee record for the given slot
    pub fn get_record(&self, slot: u64, epoch: u64) -> Result<Option<FeeRecordEntry>> {
        let key = FeeRecordKey::record(slot, epoch);

        match self.db.get(&key)? {
            Some(data) => {
                let record: FeeRecordEntry = bincode::deserialize(&data)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    fn get_records(&self, prefix: Vec<u8>) -> Result<Vec<FeeRecordEntry>> {
        let mut records = Vec::new();

        let iter = self.db.prefix_iterator(prefix.clone());
        for item in iter {
            let (key_bytes, _) = item?;
            let key = FeeRecordKey::from(&key_bytes[..]);
            let record_key = FeeRecordKey::record(key.slot, key.epoch);

            if let Some(record_data) = self.db.get_pinned(&record_key)? {
                let record: FeeRecordEntry = bincode::deserialize(&record_data)?;
                records.push(record);
            }
        }

        Ok(records)
    }

    pub fn get_records_by_state(&self, state: FeeRecordState) -> Result<Vec<FeeRecordEntry>> {
        let key = FeeRecordKey::state(state, 0, 0);
        self.get_records(key.prefix)
    }

    pub fn get_records_by_category(
        &self,
        category: FeeRecordCategory,
    ) -> Result<Vec<FeeRecordEntry>> {
        let key = FeeRecordKey::category(category, 0, 0);
        self.get_records(key.prefix)
    }

    /// Calculates the total pending lamports across all pending (processed) records
    pub fn get_total_pending_lamports(&self) -> Result<u64> {
        let pending_records = self.get_records_by_state(FeeRecordState::ProcessedAndPending)?;
        let total = pending_records
            .iter()
            .map(|r| r.priority_fee_lamports)
            .sum();
        Ok(total)
    }

    /// Exports fee records to a CSV file, optionally filtering by slot range
    /// If start_slot is None, starts from slot 0
    /// If end_slot is None, continues to u64::MAX
    pub fn export_to_csv<P: AsRef<Path>>(&self, csv_path: P, state: FeeRecordState) -> Result<()> {
        // Get records in the specified range
        let records = self.get_records_by_state(state)?;

        let mut writer = Writer::from_path(csv_path)?;

        // Write header
        writer.write_record(&[
            "timestamp",
            "slot",
            "state",
            "category",
            "priority_fee_lamports",
            "slot_landed",
            "signature",
            "link",
        ])?;

        // Write records to CSV
        for record in records {
            // Calculate link only for completed records that have a signature
            let link = if !record.signature.is_empty() {
                format!("https://explorer.solana.com/tx/{}", record.signature)
            } else {
                String::new()
            };

            // Write the record fields plus the computed link
            writer.write_record(&[
                record.timestamp.to_string(),
                record.slot.to_string(),
                format!("{:?}", record.state),
                format!("{:?}", record.category),
                record.priority_fee_lamports.to_string(),
                record.slot_landed.to_string(),
                record.signature.clone(),
                link,
            ])?;
        }

        writer.flush()?;
        Ok(())
    }

    /// Compact the database to reclaim space and optimize performance
    /// Should be run periodically, especially after many updates
    pub fn compact(&self) -> Result<()> {
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        Ok(())
    }
}

/// State of a fee record in its lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum FeeRecordCategory {
    /// Only slot has been filled, waiting to be processed
    PriorityFee,

    /// Block failed to be created or is no longer in ledger
    Ante,

    /// Other fees for record keeping
    Info,

    /// Any state
    Any,
}

/// State of a fee record in its lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum FeeRecordState {
    /// Only slot has been filled, waiting to be processed
    Unprocessed,

    /// Block failed to be created or is no longer in ledger
    Skipped,

    /// Have been paid by ante-up
    AntedUp,

    /// Priority fee has been recorded, waiting to be sent
    ProcessedAndPending,

    /// Fees have been sent
    Complete,

    /// Any state
    Any,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeeRecordEntry {
    pub timestamp: u64,              // Unix timestamp when the record was created
    pub slot: u64,                   // Leader slot
    pub epoch: u64,                  // Epoch of the leader slot
    pub priority_fee_lamports: u64,  // Amount of priority fees acquired
    pub state: FeeRecordState,       // Current state of the record
    pub category: FeeRecordCategory, // Fee Record Catagory
    pub slot_landed: u64,            // Slot the % fee transfer to the router PDA landed
    pub signature: String,           // Signature of the transaction
}

/// Key management for fee records database
pub struct FeeRecordKey {
    pub prefix: Vec<u8>,
    pub epoch_prefix: Vec<u8>,
    pub epoch: u64,
    pub slot: u64,
    pub key: Vec<u8>,
}

impl FeeRecordKey {
    // Constants for prefixes
    pub const RECORD_PREFIX: &[u8; 1] = b"0"; // Full record with data

    // State
    pub const STATE_UNPROCESSED_PREFIX: &[u8; 1] = b"1"; // Records waiting to be processed
    pub const STATE_PROCESSED_AND_PENDING_PREFIX: &[u8; 1] = b"2"; // Records with fees ready to send
    pub const STATE_SKIPPED_PREFIX: &[u8; 1] = b"3"; // Records for blocks that were skipped
    pub const STATE_ANTED_UP_PREFIX: &[u8; 1] = b"4"; // Records with completed fee transfers
    pub const STATE_COMPLETE_PREFIX: &[u8; 1] = b"5"; // Records with completed fee transfers

    // Category
    pub const CATEGORY_PRIORITY_FEE_PREFIX: &[u8; 1] = b"a"; // Records categorized by type
    pub const CATEGORY_ANTE_PREFIX: &[u8; 1] = b"b"; // Records categorized by type
    pub const CATEGORY_INFO_PREFIX: &[u8; 1] = b"c"; // Records categorized by type

    // Wildcard
    pub const ANY_PREFIX: &[u8; 1] = b"A"; // Used to query records regardless of state

    fn new(prefix: &[u8], slot: u64, epoch: u64) -> Self {
        let mut key = Vec::new();

        key.extend_from_slice(prefix);
        key.extend_from_slice(&epoch.to_be_bytes());
        let epoch_prefix = key.clone();

        key.extend_from_slice(&slot.to_be_bytes());

        Self {
            prefix: prefix.to_vec(),
            epoch_prefix: epoch_prefix.to_vec(),
            epoch,
            slot,
            key,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let prefix = &bytes[..1];
        let epoch = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        let slot = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
        Self::new(prefix, slot, epoch)
    }

    pub fn record(slot: u64, epoch: u64) -> Self {
        Self::new(Self::RECORD_PREFIX, slot, epoch)
    }

    pub fn any(slot: u64, epoch: u64) -> Self {
        Self::new(Self::ANY_PREFIX, slot, epoch)
    }

    pub fn state(state: FeeRecordState, slot: u64, epoch: u64) -> Self {
        match state {
            FeeRecordState::Unprocessed => Self::new(Self::STATE_UNPROCESSED_PREFIX, slot, epoch),
            FeeRecordState::ProcessedAndPending => {
                Self::new(Self::STATE_PROCESSED_AND_PENDING_PREFIX, slot, epoch)
            }
            FeeRecordState::Skipped => Self::new(Self::STATE_SKIPPED_PREFIX, slot, epoch),
            FeeRecordState::AntedUp => Self::new(Self::STATE_ANTED_UP_PREFIX, slot, epoch),
            FeeRecordState::Complete => Self::new(Self::STATE_COMPLETE_PREFIX, slot, epoch),
            FeeRecordState::Any => Self::new(Self::ANY_PREFIX, slot, epoch),
        }
    }

    pub fn category(category: FeeRecordCategory, slot: u64, epoch: u64) -> Self {
        match category {
            FeeRecordCategory::PriorityFee => {
                Self::new(Self::CATEGORY_PRIORITY_FEE_PREFIX, slot, epoch)
            }
            FeeRecordCategory::Ante => Self::new(Self::CATEGORY_ANTE_PREFIX, slot, epoch),
            FeeRecordCategory::Info => Self::new(Self::CATEGORY_INFO_PREFIX, slot, epoch),
            FeeRecordCategory::Any => Self::new(Self::ANY_PREFIX, slot, epoch),
        }
    }
}

// Implement conversion from &[u8] to FeeRecordKey for convenience
impl From<&[u8]> for FeeRecordKey {
    fn from(bytes: &[u8]) -> Self {
        Self::from_bytes(bytes.to_vec())
    }
}

// Implement conversion from Vec<u8> to FeeRecordKey
impl From<Vec<u8>> for FeeRecordKey {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_bytes(bytes)
    }
}

// Implement AsRef<[u8]> so FeeRecordKey can be used directly with RocksDB APIs
impl AsRef<[u8]> for FeeRecordKey {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

impl fmt::Display for FeeRecordKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = String::from_utf8_lossy(&self.prefix);
        write!(f, "{}{:08}-{:08}", prefix, self.epoch, self.slot)
    }
}

// Optionally, implement Debug to use the same formatting
impl fmt::Debug for FeeRecordKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
