use anyhow::Result;
use csv::{Reader, Writer};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

#[derive(Debug, Deserialize, Serialize)]
pub struct FeeRecord {
    pub slot: u64,
    pub priority_fee_lamports: u64,
    pub sent: bool,
    pub signature: String,
}

/// Reads fee records from a CSV file
/// If the file doesn't exist, returns an empty vector
pub fn read_fee_records<P: AsRef<Path>>(path: P) -> Result<Vec<FeeRecord>> {
    // Check if file exists, if not return empty vector
    if !path.as_ref().exists() {
        return Ok(Vec::new());
    }

    let mut records = Vec::new();
    let mut reader = Reader::from_path(path)?;

    for result in reader.deserialize() {
        let record: FeeRecord = result?;
        records.push(record);
    }

    Ok(records)
}

/// Writes fee records to a CSV file
/// Creates the file if it doesn't exist or appends to it if it does
pub fn write_fee_records<P: AsRef<Path>>(path: P, records: &[FeeRecord]) -> Result<()> {
    let file_exists = path.as_ref().exists();

    let mut writer = Writer::from_writer(
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true) // Overwrite the file with the complete dataset
            .open(path)?,
    );

    // If file doesn't exist, write header
    if !file_exists {
        writer.write_record(&["slot", "priority_fee_lamports", "sent", "signature"])?;
    }

    // Write each record
    for record in records {
        writer.serialize(record)?;
    }

    writer.flush()?;
    Ok(())
}

/// Appends a single fee record to the CSV file
/// Creates the file with a header if it doesn't exist
pub fn append_fee_record<P: AsRef<Path>>(path: P, record: &FeeRecord) -> Result<()> {
    let file_exists = path.as_ref().exists();

    let mut writer = Writer::from_writer(
        OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)?,
    );

    // If file doesn't exist, write header
    if !file_exists {
        writer.write_record(&["slot", "priority_fee_lamports", "sent", "signature"])?;
    }

    // Write the record
    writer.serialize(record)?;
    writer.flush()?;

    Ok(())
}

/// Updates the 'sent' status and signature for a specific slot
pub fn update_fee_record<P: AsRef<Path>>(
    path: P,
    slot: u64,
    sent: bool,
    signature: &str,
) -> Result<()> {
    let mut records = read_fee_records(&path)?;

    // Find and update the record for the given slot
    let mut updated = false;
    for record in &mut records {
        if record.slot == slot {
            record.sent = sent;
            record.signature = signature.to_string();
            updated = true;
            break;
        }
    }

    // Only write back if we actually updated something
    if updated {
        write_fee_records(path, &records)?;
    }

    Ok(())
}
