use lorealdb::db_engine::DBEngine;
use pyo3::prelude::*;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_100k_inserts_and_reads() {
    // 1. Boot up the Python Interpreter for this test process
    pyo3::prepare_freethreaded_python();

    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("fiori_test.redb");

    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    let mut data_store = Vec::with_capacity(100_000);
    for i in 0..100_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"status\": \"open\", \"id\": \"{}\"}}", id).into_bytes();
        data_store.push((id, payload));
    }

    let start_write = Instant::now();

    engine.insert_many(data_store).expect("Batch insert failed");

    println!(
        "100k batch-transaction inserts completed in: {:?}",
        start_write.elapsed()
    );

    let test_id = "ticket_88888";
    let expected_payload = format!("{{\"status\": \"open\", \"id\": \"{}\"}}", test_id);

    Python::with_gil(|py| {
        let retrieved = engine
            .get(py, test_id)
            .expect("Read transaction failed")
            .expect("Key not found");

        assert_eq!(
            retrieved.as_bytes(),
            expected_payload.as_bytes(),
            "Byte equality assertion failed!"
        );
    });
}

#[test]
fn test_json_validation_rejection() {
    // 1. Boot up the Python Interpreter for this test process
    pyo3::prepare_freethreaded_python();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("fiori_validation_test.redb");

    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    let id = "ticket_malformed";
    let bad_payload = String::from("{\"status\": \"open, \"id\": 123").into_bytes();

    let result = engine.insert(id, &bad_payload);

    assert!(
        result.is_err(),
        "Engine should have rejected malformed JSON!"
    );

    // We can unwrap_err() safely now because the interpreter is running
    println!("Successfully rejected bad JSON: {:?}", result.unwrap_err());
}

#[test]
fn test_prefix_scanning_100k() {
    // 1. Boot up the Python Interpreter
    pyo3::prepare_freethreaded_python();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("prefix_scan_test.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    // 2. Prepare 100k mixed records (80k tickets, 20k invoices)
    let mut data_store = Vec::with_capacity(100_000);
    for i in 0..80_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"type\": \"ticket\", \"id\": \"{}\"}}", id).into_bytes();
        data_store.push((id, payload));
    }
    for i in 0..20_000 {
        let id = format!("invoice_{}", i);
        let payload = format!("{{\"type\": \"invoice\", \"id\": \"{}\"}}", id).into_bytes();
        data_store.push((id, payload));
    }

    // 3. Insert the batch
    engine.insert_many(data_store).expect("Batch insert failed");

    // 4. Test Prefix Scanning performance
    Python::with_gil(|py| {
        let start_scan = Instant::now();

        let results = engine
            .scan_prefix(py, "invoice_")
            .expect("Prefix scan failed");

        println!(
            "Scanned and retrieved 20k 'invoice_' records out of 100k total in: {:?}",
            start_scan.elapsed()
        );

        // 5. Assertions
        assert_eq!(
            results.len(),
            20_000,
            "Engine should have found exactly 20,000 invoices!"
        );
    });
}

#[test]
fn test_get_many_performance() {
    // 1. Boot up the Python Interpreter
    pyo3::prepare_freethreaded_python();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("get_many_test.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    // 2. Prepare 100k records
    let mut data_store = Vec::with_capacity(100_000);
    for i in 0..100_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"status\": \"closed\", \"id\": \"{}\"}}", id).into_bytes();
        data_store.push((id, payload));
    }
    engine.insert_many(data_store).expect("Batch insert failed");

    // 3. Create a target list of 10,000 distinct IDs scattered across the DB
    let mut target_ids = Vec::with_capacity(10_000);
    for i in (0..100_000).step_by(10) {
        target_ids.push(format!("ticket_{}", i));
    }

    // 4. Test get_many performance
    Python::with_gil(|py| {
        let start_get = Instant::now();

        let results = engine
            .get_many(py, target_ids)
            .expect("get_many transaction failed");

        println!(
            "Fetched 10,000 distinct records using get_many in: {:?}",
            start_get.elapsed()
        );

        // 5. Assertions
        assert_eq!(
            results.len(),
            10_000,
            "Should have returned exactly 10,000 results in the hashmap"
        );

        // Spot check that a specific record was actually found (is Some)
        assert!(
            results.get("ticket_50000").unwrap().is_some(),
            "ticket_50000 should exist in the results"
        );
    });
}

#[test]
fn test_indexed_metadata_filtering_100k() {
    // 1. Boot up the Python Interpreter
    pyo3::prepare_freethreaded_python();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("metadata_filter_test.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    // 2. Prepare 100k records: 95k closed, 5k open
    let mut data_store = Vec::with_capacity(100_000);

    for i in 0..95_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"status\": \"closed\", \"id\": \"{}\"}}", id).into_bytes();
        data_store.push((id, payload));
    }

    for i in 95_000..100_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"status\": \"open\", \"id\": \"{}\"}}", id).into_bytes();
        data_store.push((id, payload));
    }

    // 3. Insert the batch (This will measure the "Write Tax" of building the index)
    let start_insert = Instant::now();
    engine.insert_many(data_store).expect("Batch insert failed");
    println!(
        "Inserted 100k records (including building the Multimap Index) in: {:?}",
        start_insert.elapsed()
    );

    // 4. Test the O(1) Metadata Filtering performance
    Python::with_gil(|py| {
        let start_filter = Instant::now();

        // Fetch ONLY the open tickets
        let results = engine
            .filter_by_metadata(py, "status", "open")
            .expect("Metadata filtering failed");

        println!(
            "O(1) Indexed Filter found 5,000 'open' records out of 100k in: {:?}",
            start_filter.elapsed()
        );

        // 5. Assertions
        assert_eq!(
            results.len(),
            5_000,
            "Engine should have found exactly 5,000 open tickets!"
        );

        // Verify that the data we pulled is actually correct
        let sample_record = results.get("ticket_99999").expect("Record should exist");
        let sample_bytes = sample_record.as_bytes();
        let sample_str = std::str::from_utf8(sample_bytes).unwrap();

        assert!(
            sample_str.contains("\"status\": \"open\""),
            "Retrieved record did not contain the correct metadata"
        );
    });
}

#[test]
fn test_upsert_index_cleanup_and_enrichment() {
    pyo3::prepare_freethreaded_python();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("upsert_test.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    let id = "ticket_101";

    // Step 1: Insert initial record
    let initial_payload = b"{\"status\": \"open\", \"priority\": \"high\"}";
    engine
        .insert(id, initial_payload)
        .expect("Initial insert failed");

    // Step 2: Perform an Upsert modifying status and enriching data with an AI summary
    let updated_payload =
        b"{\"status\": \"closed\", \"priority\": \"high\", \"summary\": \"Resolved by AI agent.\"}";
    engine
        .upsert(id, updated_payload)
        .expect("Upsert operation failed");

    Python::with_gil(|py| {
        // Assert the new payload is written successfully
        let retrieved = engine.get(py, id).unwrap().unwrap();
        let retrieved_str = std::str::from_utf8(retrieved.as_bytes()).unwrap();
        assert!(retrieved_str.contains("closed"));
        assert!(retrieved_str.contains("Resolved by AI agent."));

        // Assert stale index key ("status:open") was successfully purged
        let old_index_results = engine.filter_by_metadata(py, "status", "open").unwrap();
        assert!(
            !old_index_results.contains_key(id),
            "Stale index key entry found! Upsert failed to purge the old index record."
        );

        // Assert new index keys match perfectly
        let new_index_results = engine.filter_by_metadata(py, "status", "closed").unwrap();
        assert!(new_index_results.contains_key(id));

        let enriched_index_results = engine
            .filter_by_metadata(py, "summary", "Resolved by AI agent.")
            .unwrap();
        assert!(enriched_index_results.contains_key(id));
    });
}
