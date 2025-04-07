use anyhow::Result;
use priority_fee_sharing::fee_records::{
    FeeRecordCategory, FeeRecordKey, FeeRecordState, FeeRecords, FeeRecordsConfig,
};
use std::fs;
use std::path::Path;

// Helper function to create a clean test directory
fn setup_test_dir(dir_name: &str) -> Result<String> {
    let test_dir = format!("/tmp/{}", dir_name);

    // Clean up any existing directory from previous test runs
    if Path::new(&test_dir).exists() {
        fs::remove_dir_all(&test_dir)?;
    }

    Ok(test_dir)
}

#[test]
fn test_initialization() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_init_test")?;

    // Test with default config
    let _fee_records = FeeRecords::new(&test_dir)?;
    assert!(Path::new(&test_dir).exists());

    // Test with custom config
    let config = FeeRecordsConfig {
        write_buffer_size: Some(32 * 1024 * 1024), // 32MB
        max_open_files: Some(500),
        compaction_style: Some(rocksdb::DBCompactionStyle::Universal),
    };

    let custom_dir = format!("{}_custom", test_dir);
    let _fee_records_custom = FeeRecords::new_with_config(&custom_dir, config)?;
    assert!(Path::new(&custom_dir).exists());

    // Clean up
    fs::remove_dir_all(&test_dir)?;
    fs::remove_dir_all(&custom_dir)?;

    Ok(())
}

#[test]
fn test_add_get_record() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_add_get_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add a record
    let test_slot = 12345;
    let test_epoch = 100;
    fee_records.add_priority_fee_record(test_slot, test_epoch)?;

    // Get the record
    let record = fee_records.get_record(test_slot, test_epoch)?;
    assert!(record.is_some());

    let record = record.unwrap();
    assert_eq!(record.slot, test_slot);
    assert_eq!(record.epoch, test_epoch);
    assert_eq!(record.priority_fee_lamports, 0);
    assert_eq!(record.state, FeeRecordState::Unprocessed);
    assert_eq!(record.category, FeeRecordCategory::PriorityFee);
    assert_eq!(record.signature, "");
    assert_eq!(record.slot_landed, 0);

    // Try getting a non-existent record
    let non_existent = fee_records.get_record(99999, test_epoch)?;
    assert!(non_existent.is_none());

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[test]
fn test_add_duplicate_record() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_duplicate_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add a record
    let test_slot = 12345;
    let test_epoch = 100;
    fee_records.add_priority_fee_record(test_slot, test_epoch)?;

    // Try to add a duplicate record (should fail)
    let result = fee_records.add_priority_fee_record(test_slot, test_epoch);
    assert!(result.is_err());

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[test]
fn test_process_record() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_process_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add several records
    let test_epoch = 100;
    fee_records.add_priority_fee_record(1001, test_epoch)?;
    fee_records.add_priority_fee_record(1002, test_epoch)?;
    fee_records.add_priority_fee_record(1003, test_epoch)?;

    // Get unprocessed records
    let unprocessed = fee_records.get_records_by_state(FeeRecordState::Unprocessed)?;
    assert_eq!(unprocessed.len(), 3);

    // Process records
    fee_records.process_record(1001, test_epoch, 100000)?;
    fee_records.process_record(1002, test_epoch, 200000)?;
    fee_records.process_record(1003, test_epoch, 300000)?;

    // Get pending records (should be all 3 now)
    let pending = fee_records.get_records_by_state(FeeRecordState::ProcessedAndPending)?;
    assert_eq!(pending.len(), 3);

    // Verify the total lamports
    let total = fee_records.get_total_pending_lamports()?;
    assert_eq!(total, 600000); // 100000 + 200000 + 300000

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[test]
fn test_skip_record() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_skip_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add several records
    let test_epoch = 100;
    fee_records.add_priority_fee_record(1001, test_epoch)?;
    fee_records.add_priority_fee_record(1002, test_epoch)?;
    fee_records.add_priority_fee_record(1003, test_epoch)?;

    // Skip one record
    fee_records.skip_record(1002, test_epoch)?;

    // Check states
    let unprocessed = fee_records.get_records_by_state(FeeRecordState::Unprocessed)?;
    assert_eq!(unprocessed.len(), 2);
    assert!(unprocessed.iter().any(|r| r.slot == 1001));
    assert!(unprocessed.iter().any(|r| r.slot == 1003));

    let skipped = fee_records.get_records_by_state(FeeRecordState::Skipped)?;
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].slot, 1002);
    assert_eq!(skipped[0].state, FeeRecordState::Skipped);

    let all_records = fee_records.get_records_by_state(FeeRecordState::Any)?;
    assert_eq!(all_records.len(), 3);
    assert!(all_records.iter().any(|r| r.slot == 1001));
    assert!(all_records.iter().any(|r| r.slot == 1002));
    assert!(all_records.iter().any(|r| r.slot == 1003));

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[test]
fn test_complete_record() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_complete_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add two records
    let test_epoch = 100;
    fee_records.add_priority_fee_record(1001, test_epoch)?;
    fee_records.add_priority_fee_record(1002, test_epoch)?;

    // Process the records
    fee_records.process_record(1001, test_epoch, 100000)?;
    fee_records.process_record(1002, test_epoch, 200000)?;

    // Initially both are pending
    let pending = fee_records.get_records_by_state(FeeRecordState::ProcessedAndPending)?;
    assert_eq!(pending.len(), 2);

    // Complete one record
    let test_signature = "test_signature_12345";
    let landed_slot = 1050;
    fee_records.complete_record(1001, test_epoch, test_signature, landed_slot)?;

    // Check pending records again
    let pending = fee_records.get_records_by_state(FeeRecordState::ProcessedAndPending)?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].slot, 1002);

    // Get the completed record
    let completed = fee_records.get_record(1001, test_epoch)?.unwrap();
    assert_eq!(completed.state, FeeRecordState::Complete);
    assert_eq!(completed.signature, test_signature);
    assert_eq!(completed.slot_landed, landed_slot);

    // Try to complete a non-existent record
    let result = fee_records.complete_record(9999, test_epoch, "non_existent", 2000);
    assert!(result.is_err());

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[test]
fn test_add_ante_record() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_ante_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add an ante record
    let test_slot = 12345;
    let test_epoch = 100;
    let test_fee = 500000;
    let test_signature = "ante_signature_12345";
    let test_landed_slot = 12350;

    fee_records.add_ante_record(
        test_slot,
        test_epoch,
        test_fee,
        test_signature,
        test_landed_slot,
        true,
    )?;

    // Get the record
    let record = fee_records.get_record(test_slot, test_epoch)?.unwrap();
    assert_eq!(record.state, FeeRecordState::Complete);
    assert_eq!(record.category, FeeRecordCategory::Ante);
    assert_eq!(record.priority_fee_lamports, test_fee);
    assert_eq!(record.signature, test_signature);
    assert_eq!(record.slot_landed, test_landed_slot);

    // Check that it appears in ante records
    let ante_records = fee_records.get_records_by_category(FeeRecordCategory::Ante)?;
    assert_eq!(ante_records.len(), 1);
    assert_eq!(ante_records[0].slot, test_slot);

    // Try adding duplicate
    let result = fee_records.add_priority_fee_record(test_slot, test_epoch);
    assert!(result.is_err());

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[test]
fn test_ante_up_transition() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_ante_up_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add several records in different states
    let test_epoch = 100;
    fee_records.add_priority_fee_record(1001, test_epoch)?;
    fee_records.add_priority_fee_record(1002, test_epoch)?;
    fee_records.add_priority_fee_record(1003, test_epoch)?;

    // Process some records
    fee_records.process_record(1002, test_epoch, 200000)?;
    fee_records.process_record(1003, test_epoch, 300000)?;

    // Complete one record
    fee_records.complete_record(1003, test_epoch, "sig_1003", 1103)?;

    // Verify initial states
    assert_eq!(
        fee_records
            .get_records_by_state(FeeRecordState::Unprocessed)?
            .len(),
        1
    );
    assert_eq!(
        fee_records
            .get_records_by_state(FeeRecordState::ProcessedAndPending)?
            .len(),
        1
    );
    assert_eq!(
        fee_records
            .get_records_by_state(FeeRecordState::Complete)?
            .len(),
        1
    );

    // Add ante record which should transition all unprocessed and pending records to AntedUp
    fee_records.add_ante_record(2000, test_epoch, 500000, "ante_sig", 2100, true)?;

    // Check that records have transitioned
    assert_eq!(
        fee_records
            .get_records_by_state(FeeRecordState::Unprocessed)?
            .len(),
        0
    );
    assert_eq!(
        fee_records
            .get_records_by_state(FeeRecordState::ProcessedAndPending)?
            .len(),
        0
    );
    assert_eq!(
        fee_records
            .get_records_by_state(FeeRecordState::AntedUp)?
            .len(),
        2
    );
    assert_eq!(
        fee_records
            .get_records_by_state(FeeRecordState::Complete)?
            .len(),
        2
    ); // Original complete + ante

    // Verify the ante record itself
    let ante_records = fee_records.get_records_by_category(FeeRecordCategory::Ante)?;
    assert_eq!(ante_records.len(), 1);
    assert_eq!(ante_records[0].slot, 2000);

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[test]
fn test_export_to_csv() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_export_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add several records
    let test_epoch = 100;
    for i in 0..5 {
        let slot = 1000 + i;
        fee_records.add_priority_fee_record(slot, test_epoch)?;
    }

    // Process and complete some records
    fee_records.process_record(1000, test_epoch, 100000)?;
    fee_records.process_record(1001, test_epoch, 200000)?;
    fee_records.process_record(1002, test_epoch, 300000)?;
    fee_records.complete_record(1000, test_epoch, "sig_1000", 1100)?;
    fee_records.complete_record(1001, test_epoch, "sig_1001", 1101)?;

    // Add an ante record
    fee_records.add_ante_record(2000, test_epoch, 600000, "sig_ante", 2100, true)?;

    // Skip a record (but we need to add it first)
    fee_records.add_priority_fee_record(3000, test_epoch)?;
    fee_records.skip_record(3000, test_epoch)?;

    // Export records by state
    let unprocessed_csv = format!("{}/unprocessed.csv", test_dir);
    fee_records.export_to_csv(&unprocessed_csv, FeeRecordState::Unprocessed)?;

    let completed_csv = format!("{}/completed.csv", test_dir);
    fee_records.export_to_csv(&completed_csv, FeeRecordState::Complete)?;

    let skipped_csv = format!("{}/skipped.csv", test_dir);
    fee_records.export_to_csv(&skipped_csv, FeeRecordState::Skipped)?;

    // Verify CSV files exist
    assert!(Path::new(&unprocessed_csv).exists());
    assert!(Path::new(&completed_csv).exists());
    assert!(Path::new(&skipped_csv).exists());

    // Basic content verification for completed records
    let csv_content = fs::read_to_string(&completed_csv)?;
    assert!(csv_content.contains("timestamp"));
    assert!(csv_content.contains("slot"));
    assert!(csv_content.contains("epoch"));
    assert!(csv_content.contains("state"));
    assert!(csv_content.contains("category"));
    assert!(csv_content.contains("priority_fee_lamports"));
    assert!(csv_content.contains("sig_1000"));
    assert!(csv_content.contains("sig_1001"));
    assert!(csv_content.contains("sig_ante"));

    // Export with epoch specified
    let epoch_csv = format!("{}/epoch_records.csv", test_dir);
    fee_records.export_to_csv(&epoch_csv, FeeRecordState::Any)?;
    assert!(Path::new(&epoch_csv).exists());

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[test]
fn test_fee_record_key_serialization() -> Result<()> {
    // Create keys for various states and categories
    let slot = 12345;
    let epoch = 100;
    let record_key = FeeRecordKey::record(slot, epoch);
    let unprocessed_key = FeeRecordKey::state(FeeRecordState::Unprocessed, slot, epoch);
    let pending_key = FeeRecordKey::state(FeeRecordState::ProcessedAndPending, slot, epoch);
    let complete_key = FeeRecordKey::state(FeeRecordState::Complete, slot, epoch);
    let priority_fee_key = FeeRecordKey::category(FeeRecordCategory::PriorityFee, slot, epoch);
    let ante_key = FeeRecordKey::category(FeeRecordCategory::Ante, slot, epoch);
    let any_key = FeeRecordKey::any(slot, epoch);

    // Verify key prefixes
    assert_eq!(record_key.prefix, FeeRecordKey::RECORD_PREFIX);
    assert_eq!(
        unprocessed_key.prefix,
        FeeRecordKey::STATE_UNPROCESSED_PREFIX
    );
    assert_eq!(
        pending_key.prefix,
        FeeRecordKey::STATE_PROCESSED_AND_PENDING_PREFIX
    );
    assert_eq!(complete_key.prefix, FeeRecordKey::STATE_COMPLETE_PREFIX);
    assert_eq!(
        priority_fee_key.prefix,
        FeeRecordKey::CATEGORY_PRIORITY_FEE_PREFIX
    );
    assert_eq!(ante_key.prefix, FeeRecordKey::CATEGORY_ANTE_PREFIX);
    assert_eq!(any_key.prefix, FeeRecordKey::ANY_PREFIX);

    // Verify slots and epochs
    assert_eq!(record_key.slot, slot);
    assert_eq!(record_key.epoch, epoch);
    assert_eq!(unprocessed_key.slot, slot);
    assert_eq!(unprocessed_key.epoch, epoch);
    assert_eq!(pending_key.slot, slot);
    assert_eq!(pending_key.epoch, epoch);
    assert_eq!(complete_key.slot, slot);
    assert_eq!(complete_key.epoch, epoch);
    assert_eq!(priority_fee_key.slot, slot);
    assert_eq!(priority_fee_key.epoch, epoch);
    assert_eq!(ante_key.slot, slot);
    assert_eq!(ante_key.epoch, epoch);
    assert_eq!(any_key.slot, slot);
    assert_eq!(any_key.epoch, epoch);

    // Test conversion from bytes
    let raw_bytes = record_key.as_ref();
    let reconstructed = FeeRecordKey::from(raw_bytes);
    assert_eq!(reconstructed.slot, slot);
    assert_eq!(reconstructed.epoch, epoch);

    Ok(())
}

#[test]
fn test_performance() -> Result<()> {
    use std::time::Instant;

    // Test parameters (reduced for faster test runs)
    let total_records = 1000;
    let test_epoch = 100;

    let test_dir = setup_test_dir("fee_records_perf_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add records
    let start_add = Instant::now();
    for i in 0..total_records {
        let slot = 1000 + i as u64;
        fee_records.add_priority_fee_record(slot, test_epoch)?;
    }
    let add_duration = start_add.elapsed();
    println!(
        "Time to add {} records: {:?} ({:?} per record)",
        total_records,
        add_duration,
        add_duration / total_records as u32
    );

    // Process records
    let start_process = Instant::now();
    for i in 0..total_records {
        let slot = 1000 + i as u64;
        let fee = 100000 + (i % 10) * 10000;
        fee_records.process_record(slot, test_epoch, fee)?;
    }
    let process_duration = start_process.elapsed();
    println!(
        "Time to process {} records: {:?} ({:?} per record)",
        total_records,
        process_duration,
        process_duration / total_records as u32
    );

    // Query all processed records
    let start_query = Instant::now();
    let pending_records = fee_records.get_records_by_state(FeeRecordState::ProcessedAndPending)?;
    let query_duration = start_query.elapsed();

    assert_eq!(pending_records.len(), total_records as usize);
    println!(
        "Time to query {} pending records: {:?} ({:?} per record)",
        total_records,
        query_duration,
        query_duration / total_records as u32
    );

    // Complete records
    let start_complete = Instant::now();
    for i in 0..total_records {
        let slot = 1000 + i as u64;
        fee_records.complete_record(slot, test_epoch, &format!("sig_{}", slot), slot + 100)?;
    }
    let complete_duration = start_complete.elapsed();
    println!(
        "Time to complete {} records: {:?} ({:?} per record)",
        total_records,
        complete_duration,
        complete_duration / total_records as u32
    );

    // Compact the database
    let start_compact = Instant::now();
    fee_records.compact()?;
    let compact_duration = start_compact.elapsed();
    println!("Time to compact database: {:?}", compact_duration);

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[test]
fn test_batch_operations() -> Result<()> {
    let test_dir = setup_test_dir("fee_records_batch_test")?;
    let fee_records = FeeRecords::new(&test_dir)?;

    // Add many records at once
    let test_epoch = 100;
    for i in 1..=100 {
        fee_records.add_priority_fee_record(i, test_epoch)?;
    }

    // Test transition to ante-up state by adding an ante record
    fee_records.add_ante_record(1000, test_epoch, 1000000, "ante_sig", 1100, true)?;

    // Verify all records transitioned to AntedUp
    let anted_up = fee_records.get_records_by_state(FeeRecordState::AntedUp)?;
    assert_eq!(anted_up.len(), 100);

    // Clean up
    fs::remove_dir_all(&test_dir)?;

    Ok(())
}
