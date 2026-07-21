use lorealdb::db_engine::DBEngine;
use pyo3::prelude::*;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_100k_inserts_and_reads() {
    // 1. Boot up the Python Interpreter using the 0.29 API
    Python::initialize();

    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("fiori_test.redb");

    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    let mut data_store = Vec::with_capacity(100_000);
    for i in 0..100_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"status\": \"open\", \"id\": \"{}\"}}", id);
        data_store.push((id, payload));
    }

    let start_write = Instant::now();

    // Python::with_gil is now Python::attach
    Python::attach(|py| {
        engine
            .insert_many_json(py, data_store)
            .expect("Batch insert failed");
    });

    println!(
        "100k batch-transaction queued to worker in: {:?}",
        start_write.elapsed()
    );

    // Allow background worker thread to process the 10k batch queue and commit to disk
    std::thread::sleep(std::time::Duration::from_millis(1500));

    let test_id = "ticket_88888";
    let expected_payload = format!("{{\"status\": \"open\", \"id\": \"{}\"}}", test_id);

    Python::attach(|py| {
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
    Python::initialize();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("fiori_validation_test.redb");

    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    let id = "ticket_malformed";
    let bad_payload = "{\"status\": \"open, \"id\": 123";

    let result = engine.insert_json(id, bad_payload);

    assert!(
        result.is_err(),
        "Engine should have rejected malformed JSON!"
    );

    println!("Successfully rejected bad JSON: {:?}", result.unwrap_err());
}

#[test]
fn test_prefix_scanning_100k() {
    Python::initialize();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("prefix_scan_test.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    let mut data_store = Vec::with_capacity(100_000);
    for i in 0..80_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"type\": \"ticket\", \"id\": \"{}\"}}", id);
        data_store.push((id, payload));
    }
    for i in 0..20_000 {
        let id = format!("invoice_{}", i);
        let payload = format!("{{\"type\": \"invoice\", \"id\": \"{}\"}}", id);
        data_store.push((id, payload));
    }

    Python::attach(|py| {
        engine
            .insert_many_json(py, data_store)
            .expect("Batch insert failed");
    });

    std::thread::sleep(std::time::Duration::from_millis(1500));

    Python::attach(|py| {
        let start_scan = Instant::now();

        let results = engine
            .scan_prefix(py, "invoice_")
            .expect("Prefix scan failed");

        println!(
            "Scanned and retrieved 20k 'invoice_' records out of 100k total in: {:?}",
            start_scan.elapsed()
        );

        assert_eq!(
            results.len(),
            20_000,
            "Engine should have found exactly 20,000 invoices!"
        );
    });
}

#[test]
fn test_get_many_performance() {
    Python::initialize();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("get_many_test.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    let mut data_store = Vec::with_capacity(100_000);
    for i in 0..100_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"status\": \"closed\", \"id\": \"{}\"}}", id);
        data_store.push((id, payload));
    }

    Python::attach(|py| {
        engine
            .insert_many_json(py, data_store)
            .expect("Batch insert failed");
    });

    std::thread::sleep(std::time::Duration::from_millis(1500));

    let mut target_ids = Vec::with_capacity(10_000);
    for i in (0..100_000).step_by(10) {
        target_ids.push(format!("ticket_{}", i));
    }

    Python::attach(|py| {
        let start_get = Instant::now();

        let results = engine
            .get_many(py, target_ids)
            .expect("get_many transaction failed");

        println!(
            "Fetched 10,000 distinct records using get_many in: {:?}",
            start_get.elapsed()
        );

        assert_eq!(
            results.len(),
            10_000,
            "Should have returned exactly 10,000 results in the hashmap"
        );

        assert!(
            results.get("ticket_50000").unwrap().is_some(),
            "ticket_50000 should exist in the results"
        );
    });
}

#[test]
fn test_indexed_metadata_filtering_100k() {
    Python::initialize();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("metadata_filter_test.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    let mut data_store = Vec::with_capacity(100_000);

    for i in 0..95_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"status\": \"closed\", \"id\": \"{}\"}}", id);
        data_store.push((id, payload));
    }

    for i in 95_000..100_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"status\": \"open\", \"id\": \"{}\"}}", id);
        data_store.push((id, payload));
    }

    let start_insert = Instant::now();
    Python::attach(|py| {
        engine
            .insert_many_json(py, data_store)
            .expect("Batch insert failed");
    });

    println!(
        "Sent 100k records to indexer queue in: {:?}",
        start_insert.elapsed()
    );

    std::thread::sleep(std::time::Duration::from_millis(4000));

    Python::attach(|py| {
        let start_filter = Instant::now();

        let results = engine
            .filter_by_metadata(py, "status", "open")
            .expect("Metadata filtering failed");

        println!(
            "O(1) Indexed Filter found 5,000 'open' records out of 100k in: {:?}",
            start_filter.elapsed()
        );

        assert_eq!(
            results.len(),
            5_000,
            "Engine should have found exactly 5,000 open tickets!"
        );

        // Vector of tuples uses iter().find() instead of hashmap .get()
        let sample_record = results
            .iter()
            .find(|(k, _)| k == "ticket_99999")
            .expect("Record should exist");
        let sample_bytes = sample_record.1.as_bytes();
        let sample_str = std::str::from_utf8(sample_bytes).unwrap();

        assert!(
            sample_str.contains("\"status\": \"open\""),
            "Retrieved record did not contain the correct metadata"
        );
    });
}

#[test]
fn test_upsert_index_cleanup_and_enrichment() {
    Python::initialize();

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("upsert_test.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to initialize engine");

    let id = "ticket_101";

    let initial_payload = "{\"status\": \"open\", \"priority\": \"high\"}";
    engine
        .insert_json(id, initial_payload)
        .expect("Initial insert failed");

    std::thread::sleep(std::time::Duration::from_millis(1000));

    let updated_payload =
        b"{\"status\": \"closed\", \"priority\": \"high\", \"summary\": \"Resolved by AI agent.\"}";
    engine
        .upsert(id, updated_payload)
        .expect("Upsert operation failed");

    std::thread::sleep(std::time::Duration::from_millis(200));

    Python::attach(|py| {
        let retrieved = engine.get(py, id).unwrap().unwrap();
        let retrieved_str = std::str::from_utf8(retrieved.as_bytes()).unwrap();
        assert!(retrieved_str.contains("closed"));
        assert!(retrieved_str.contains("Resolved by AI agent."));

        let old_index_results = engine.filter_by_metadata(py, "status", "open").unwrap();
        assert!(
            !old_index_results.iter().any(|(k, _)| k == id),
            "Stale index key entry found! Upsert failed to purge the old index record."
        );

        let new_index_results = engine.filter_by_metadata(py, "status", "closed").unwrap();
        assert!(new_index_results.iter().any(|(k, _)| k == id));

        let enriched_index_results = engine
            .filter_by_metadata(py, "summary", "Resolved by AI agent.")
            .unwrap();
        assert!(enriched_index_results.iter().any(|(k, _)| k == id));
    });
}
