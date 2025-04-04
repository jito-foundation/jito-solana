use anyhow::Result;
use bincode;
use csv::Writer;
use rocksdb::{IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

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

        let db = DB::open(&opts, path)?;
        Ok(Self { db })
    }

    /// Gets a specific fee record for the given slot
    pub fn get_record(&self, slot: u64) -> Result<Option<FeeRecordEntry>> {
        let key = FeeRecordKey::new(FeeRecordKeyPrefix::Record, slot);
        let key_bytes = key.to_bytes();

        match self.db.get(key_bytes)? {
            Some(data) => {
                let record: FeeRecordEntry = bincode::deserialize(&data)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Gets all pending (unsent) fee records
    pub fn get_pending_records(&self) -> Result<Vec<FeeRecordEntry>> {
        let mut pending_records = Vec::new();
        let prefix = FeeRecordKeyPrefix::Pending;

        let iter = self.db.prefix_iterator(prefix.as_bytes());
        for item in iter {
            let (key, _) = item?;
            let key_str = String::from_utf8_lossy(&key);

            if let Some(key_obj) = FeeRecordKey::parse(&key_str) {
                let slot = key_obj.slot();
                let record_key = FeeRecordKey::new(FeeRecordKeyPrefix::Record, slot);

                // Use get_pinned to avoid unnecessary allocation
                if let Some(record_data) = self.db.get_pinned(record_key.to_string().as_bytes())? {
                    let record: FeeRecordEntry = bincode::deserialize(&record_data)?;

                    // Only add records that are not yet sent
                    if !record.sent {
                        pending_records.push(record);
                    }
                }
            }
        }

        Ok(pending_records)
    }

    /// Calculates the total pending lamports across all unsent records
    pub fn get_total_pending_lamports(&self) -> Result<u64> {
        let pending_records = self.get_pending_records()?;
        let total = pending_records
            .iter()
            .map(|r| r.priority_fee_lamports)
            .sum();
        Ok(total)
    }

    /// Adds a new fee record
    pub fn add_record(&self, slot: u64, priority_fee_lamports: u64) -> Result<()> {
        let key = FeeRecordKey::new(FeeRecordKeyPrefix::Record, slot);

        // Check if record already exists
        if self.db.get(key.to_bytes())?.is_some() {
            return Err(anyhow::anyhow!("Record for slot {} already exists", slot));
        }

        let record = FeeRecordEntry {
            slot,
            priority_fee_lamports,
            sent: false,
            slot_landed: 0, // Initialize to 0 (no landing slot yet)
            signature: String::new(),
        };

        let encoded = bincode::serialize(&record)?;

        // Create a batch operation to update both the main record and the pending index
        let pending_key = FeeRecordKey::new(FeeRecordKeyPrefix::Pending, slot);

        // Use a WriteBatch for atomicity
        let mut batch = WriteBatch::default();
        batch.put(key.to_string().as_bytes(), &encoded);
        batch.put(pending_key.to_string().as_bytes(), &[]); // Empty value since the key itself is the index

        self.db.write(batch)?;

        Ok(())
    }

    /// Marks a fee record as sent and updates its signature and landed slot
    pub fn finish_record(&self, slot: u64, signature: &str, slot_landed: u64) -> Result<()> {
        let key = FeeRecordKey::new(FeeRecordKeyPrefix::Record, slot);

        // Get the existing record
        match self.db.get(key.to_string().as_bytes())? {
            Some(data) => {
                let mut record: FeeRecordEntry = bincode::deserialize(&data)?;

                // Update the record
                record.sent = true;
                record.signature = signature.to_string();
                record.slot_landed = slot_landed;

                // Write the updated record and remove from pending index
                let encoded = bincode::serialize(&record)?;
                let pending_key = FeeRecordKey::new(FeeRecordKeyPrefix::Pending, slot);

                let mut batch = WriteBatch::default();
                batch.put(key.to_string().as_bytes(), encoded);
                batch.delete(pending_key.to_string().as_bytes()); // Remove from pending index

                self.db.write(batch)?;

                Ok(())
            }
            None => Err(anyhow::anyhow!("No entry found for slot {}", slot)),
        }
    }

    /// Exports fee records to a CSV file, optionally filtering by slot range
    /// If start_slot is None, starts from slot 0
    /// If end_slot is None, continues to u64::MAX
    pub fn export_to_csv<P: AsRef<Path>>(
        &self,
        csv_path: P,
        start_slot: Option<u64>,
        end_slot: Option<u64>,
    ) -> Result<()> {
        let start_slot = start_slot.unwrap_or(0);
        let end_slot = end_slot.unwrap_or(u64::MAX);

        let mut writer = Writer::from_path(csv_path)?;

        // Write header
        writer.write_record(&[
            "slot",
            "priority_fee_lamports",
            "sent",
            "signature",
            "slot_landed",
            "link",
        ])?;

        // Determine start key
        let start_key = FeeRecordKey::new(FeeRecordKeyPrefix::Record, start_slot).to_string();

        // Iterate over records within range
        let iter = self.db.iterator(IteratorMode::From(
            start_key.as_bytes(),
            rocksdb::Direction::Forward,
        ));
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key);

            // Check if still within prefix
            if !key_str.starts_with(&prefix) {
                break;
            }

            // Parse the key to extract the slot
            if let Some(key_obj) = FeeRecordKey::parse(&key_str) {
                let slot = key_obj.slot();

                // Check if we've reached end_slot
                if slot > end_slot {
                    break;
                }

                // Process record
                let record: FeeRecordEntry = bincode::deserialize(&value)?;

                // Calculate link only for records that have a signature
                let link = if !record.signature.is_empty() {
                    format!("https://explorer.solana.com/tx/{}", record.signature)
                } else {
                    String::new()
                };

                // Write the record fields plus the computed link
                writer.write_record(&[
                    record.slot.to_string(),
                    record.priority_fee_lamports.to_string(),
                    record.sent.to_string(),
                    record.signature.clone(),
                    record.slot_landed.to_string(),
                    link,
                ])?;
            }
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeeRecordEntry {
    pub slot: u64,
    pub priority_fee_lamports: u64,
    pub sent: bool,
    pub slot_landed: u64,
    pub signature: String,
}

impl std::fmt::Display for FeeRecordEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Slot: {}, Fee: {} lamports, Sent: {}",
            self.slot, self.priority_fee_lamports, self.sent
        )?;

        if !self.signature.is_empty() {
            write!(
                f,
                ", Signature: {}, Link: https://explorer.solana.com/tx/{}",
                self.signature, self.signature
            )?;
        }

        if self.slot_landed > 0 {
            write!(f, ", Landed at slot: {}", self.slot_landed)?;
        }

        Ok(())
    }
}

/// Key prefix types for database organization
#[derive(Debug, Clone, Copy)]
pub enum FeeRecordKeyPrefix {
    /// Main record storage - "completed"
    Record,
    /// Pending records index - "pending"
    Pending,
}

impl FeeRecordKeyPrefix {
    /// Get the string representation of the prefix
    fn as_str(&self) -> &'static str {
        match self {
            FeeRecordKeyPrefix::Record => "record:",
            FeeRecordKeyPrefix::Pending => "pending:",
        }
    }

    /// Get the byte representation of the prefix
    fn as_bytes(&self) -> &'static [u8] {
        self.as_str().as_bytes()
    }

    /// Attempt to convert a string to a FeeRecordKeyPrefix
    fn from_str(s: &str) -> Option<Self> {
        let cleaned = s.replace(":", "");
        match cleaned.as_str() {
            "record" => Some(FeeRecordKeyPrefix::Record),
            "pending" => Some(FeeRecordKeyPrefix::Pending),
            _ => None,
        }
    }
}

/// Struct to encapsulate key generation and parsing
#[derive(Debug, Clone)]
pub struct FeeRecordKey {
    prefix: FeeRecordKeyPrefix,
    slot: u64,
}

impl FeeRecordKey {
    /// Create a new key with the specified prefix and slot
    pub fn new(prefix: FeeRecordKeyPrefix, slot: u64) -> Self {
        Self { prefix, slot }
    }

    /// Create a key for a completed record
    pub fn record(slot: u64) -> Self {
        Self::new(FeeRecordKeyPrefix::Record, slot)
    }

    /// Create a key for a pending record
    pub fn pending(slot: u64) -> Self {
        Self::new(FeeRecordKeyPrefix::Pending, slot)
    }

    /// Convert the key to a string for database storage
    pub fn to_string(&self) -> String {
        format!("{}{}", self.prefix.as_str(), self.slot)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let string = self.to_string();
        string.as_bytes().to_vec()
    }

    /// Get just the prefix part for range scans
    pub fn prefix_string(&self) -> String {
        self.prefix.as_str().to_string()
    }

    pub fn prefix_bytes(&self) -> Vec<u8> {
        let string = self.prefix_string();
        string.as_bytes().to_vec()
    }

    /// Parse a key string into a FeeRecordKey, returning None if invalid
    pub fn parse(key_str: &str) -> Option<Self> {
        let parts: Vec<&str> = key_str.split(':').collect();
        if parts.len() != 2 {
            return None;
        }

        let prefix = FeeRecordKeyPrefix::from_str(parts[0])?;

        if let Ok(slot) = parts[1].parse::<u64>() {
            Some(Self { prefix, slot })
        } else {
            None
        }
    }

    /// Get the slot from the key
    pub fn slot(&self) -> u64 {
        self.slot
    }

    /// Get the prefix from the key
    pub fn prefix(&self) -> FeeRecordKeyPrefix {
        self.prefix
    }
}
