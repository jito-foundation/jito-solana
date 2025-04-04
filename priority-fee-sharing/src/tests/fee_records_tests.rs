#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;
    use uuid::Uuid;

    // Helper function to create a test database in a temporary directory
    fn setup_test_db() -> (FeeRecords, TempDir) {
        // Create a unique temporary directory for each test
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let path = temp_dir.path().to_path_buf();

        // Create FeeRecords with default configuration
        let fee_records = FeeRecords::new(&path).expect("Failed to create FeeRecords");

        (fee_records, temp_dir)
    }

    #[test]
    fn test_add_and_get_record() {
        let (fee_records, _temp_dir) = setup_test_db();

        // Add a record
        let slot = 12345;
        let fee = 50000;
        fee_records
            .add_record(slot, fee)
            .expect("Failed to add record");

        // Retrieve the record
        let record = fee_records.get_record(slot).expect("Failed to get record");

        // Verify record contents
        assert!(record.is_some(), "Record should exist");
        let record = record.unwrap();
        assert_eq!(record.slot, slot);
        assert_eq!(record.priority_fee_lamports, fee);
        assert_eq!(record.sent, false);
        assert_eq!(record.slot_landed, 0);
        assert_eq!(record.signature, "");
    }

    #[test]
    fn test_add_duplicate_record() {
        let (fee_records, _temp_dir) = setup_test_db();

        // Add a record
        let slot = 12345;
        let fee = 50000;
        fee_records
            .add_record(slot, fee)
            .expect("Failed to add record");

        // Try to add a duplicate record
        let result = fee_records.add_record(slot, fee);
        assert!(result.is_err(), "Adding duplicate record should fail");
    }

    #[test]
    fn test_get_nonexistent_record() {
        let (fee_records, _temp_dir) = setup_test_db();

        // Try to get a record that doesn't exist
        let record = fee_records
            .get_record(99999)
            .expect("get_record should not error");
        assert!(record.is_none(), "Nonexistent record should return None");
    }

    #[test]
    fn test_finish_record() {
        let (fee_records, _temp_dir) = setup_test_db();

        // Add a record
        let slot = 12345;
        let fee = 50000;
        fee_records
            .add_record(slot, fee)
            .expect("Failed to add record");

        // Finish the record
        let signature = "test_signature_123";
        let slot_landed = 12350;
        fee_records
            .finish_record(slot, signature, slot_landed)
            .expect("Failed to finish record");

        // Verify record was updated
        let record = fee_records
            .get_record(slot)
            .expect("Failed to get record")
            .unwrap();
        assert_eq!(record.sent, true);
        assert_eq!(record.signature, signature);
        assert_eq!(record.slot_landed, slot_landed);
    }

    #[test]
    fn test_finish_nonexistent_record() {
        let (fee_records, _temp_dir) = setup_test_db();

        // Try to finish a record that doesn't exist
        let result = fee_records.finish_record(99999, "test_signature", 100000);
        assert!(result.is_err(), "Finishing nonexistent record should fail");
    }

    #[test]
    fn test_get_pending_records() {
        let (fee_records, _temp_dir) = setup_test_db();

        // Add some records
        fee_records
            .add_record(12345, 50000)
            .expect("Failed to add record");
        fee_records
            .add_record(12346, 60000)
            .expect("Failed to add record");
        fee_records
            .add_record(12347, 70000)
            .expect("Failed to add record");

        // Mark one as finished
        fee_records
            .finish_record(12346, "test_signature", 12350)
            .expect("Failed to finish record");

        // Get pending records
        let pending = fee_records
            .get_pending_records()
            .expect("Failed to get pending records");

        // Verify we have two pending records
        assert_eq!(pending.len(), 2);

        // Verify the finished record is not in the pending list
        assert!(pending.iter().all(|r| r.slot != 12346));

        // Verify the pending records are in the list
        let has_12345 = pending.iter().any(|r| r.slot == 12345);
        let has_12347 = pending.iter().any(|r| r.slot == 12347);
        assert!(has_12345, "Pending list should contain slot 12345");
        assert!(has_12347, "Pending list should contain slot 12347");
    }

    #[test]
    fn test_get_all_records() {
        let (fee_records, _temp_dir) = setup_test_db();

        // Add some records
        fee_records
            .add_record(12345, 50000)
            .expect("Failed to add record");
        fee_records
            .add_record(12346, 60000)
            .expect("Failed to add record");

        // Mark one as finished
        fee_records
            .finish_record(12346, "test_signature", 12350)
            .expect("Failed to finish record");

        // Get all records
        let all_records = fee_records
            .get_all_records()
            .expect("Failed to get all records");

        // Verify we have two records
        assert_eq!(all_records.len(), 2);

        // Verify both records are in the list
        let has_12345 = all_records.iter().any(|r| r.slot == 12345);
        let has_12346 = all_records.iter().any(|r| r.slot == 12346);
        assert!(has_12345, "All records should contain slot 12345");
        assert!(has_12346, "All records should contain slot 12346");

        // Verify the finished record is marked as sent
        let record_12346 = all_records.iter().find(|r| r.slot == 12346).unwrap();
        assert_eq!(record_12346.sent, true);
    }

    #[test]
    fn test_get_total_pending_lamports() {
        let (fee_records, _temp_dir) = setup_test_db();

        // Add some records
        fee_records
            .add_record(12345, 50000)
            .expect("Failed to add record");
        fee_records
            .add_record(12346, 60000)
            .expect("Failed to add record");
        fee_records
            .add_record(12347, 70000)
            .expect("Failed to add record");

        // Mark one as finished
        fee_records
            .finish_record(12346, "test_signature", 12350)
            .expect("Failed to finish record");

        // Get total pending lamports
        let total = fee_records
            .get_total_pending_lamports()
            .expect("Failed to get total pending lamports");

        // Verify total is sum of pending records (50000 + 70000 = 120000)
        assert_eq!(total, 120000);
    }

    #[test]
    fn test_export_to_csv() {
        let (fee_records, temp_dir) = setup_test_db();

        // Add some records
        fee_records
            .add_record(12345, 50000)
            .expect("Failed to add record");
        fee_records
            .add_record(12346, 60000)
            .expect("Failed to add record");

        // Mark one as finished
        fee_records
            .finish_record(12346, "test_signature", 12350)
            .expect("Failed to finish record");

        // Export to CSV
        let csv_path = temp_dir.path().join("export.csv");
        fee_records
            .export_to_csv(&csv_path, None, None)
            .expect("Failed to export to CSV");

        // Verify CSV file exists
        assert!(csv_path.exists(), "CSV file should exist");

        // Read CSV content
        let csv_content = fs::read_to_string(&csv_path).expect("Failed to read CSV file");

        // Verify header
        assert!(csv_content.contains("slot,priority_fee_lamports,sent,signature,slot_landed,link"));

        // Verify records
        assert!(csv_content.contains("12345,50000,false,,0,"));
        assert!(csv_content.contains(
            "12346,60000,true,test_signature,12350,https://explorer.solana.com/tx/test_signature"
        ));
    }

    #[test]
    fn test_custom_config() {
        // Create a custom configuration
        let config = FeeRecordsConfig {
            write_buffer_size: Some(32 * 1024 * 1024), // 32MB
            max_open_files: Some(500),
            compaction_style: Some(rocksdb::DBCompactionStyle::Universal),
        };

        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let path = temp_dir.path().to_path_buf();

        // Create FeeRecords with custom configuration
        let fee_records =
            FeeRecords::new_with_config(&path, config).expect("Failed to create FeeRecords");

        // Basic functionality test
        fee_records
            .add_record(12345, 50000)
            .expect("Failed to add record");
        let record = fee_records
            .get_record(12345)
            .expect("Failed to get record")
            .unwrap();
        assert_eq!(record.slot, 12345);
    }

    #[test]
    fn test_compact() {
        let (fee_records, _temp_dir) = setup_test_db();

        // Add some records
        for i in 0..100 {
            fee_records
                .add_record(i, 50000)
                .expect("Failed to add record");
        }

        // Mark some as finished
        for i in 0..50 {
            fee_records
                .finish_record(i, &format!("sig_{}", i), i + 5)
                .expect("Failed to finish record");
        }

        // Compact the database
        fee_records.compact().expect("Failed to compact database");

        // Verify records still exist
        let all_records = fee_records
            .get_all_records()
            .expect("Failed to get all records");
        assert_eq!(all_records.len(), 100);
    }

    #[test]
    fn test_record_display() {
        // Create a record
        let record = FeeRecordEntry {
            slot: 12345,
            priority_fee_lamports: 50000,
            sent: true,
            slot_landed: 12350,
            signature: "test_signature".to_string(),
        };

        // Convert to string
        let display = format!("{}", record);

        // Verify string contains expected parts
        assert!(display.contains("Slot: 12345"));
        assert!(display.contains("Fee: 50000 lamports"));
        assert!(display.contains("Sent: true"));
        assert!(display.contains("Signature: test_signature"));
        assert!(display.contains("Link: https://explorer.solana.com/tx/test_signature"));
        assert!(display.contains("Landed at slot: 12350"));
    }
}
