use lorealdb::db_engine::DBEngine;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::time::Duration;
use tempfile::tempdir;

/// Helper to create an engine in a temporary directory
fn setup_engine() -> (tempfile::TempDir, DBEngine) {
    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("test_db.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).unwrap();
    (dir, engine)
}

/// Because writes happen on a background thread with a 10ms batch window,
/// we must wait briefly for the transaction to commit before asserting reads.
fn wait_for_background_worker() {
    std::thread::sleep(Duration::from_millis(50));
}

/// Verifies that a standard Python dictionary can be successfully inserted into
/// the database and retrieved with its data intact.
#[test]
fn test_insert_and_get() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    Python::attach(|py| {
        // Create a Python dictionary (0.22+ syntax, no _bound suffix)
        let dict = PyDict::new(py);
        dict.set_item("name", "Alice").unwrap();
        dict.set_item("role", "Engineer").unwrap();

        // Insert the data
        engine.insert(py, "doc_1", dict).expect("Failed to insert");
    });

    // Wait for the background thread to commit
    wait_for_background_worker();

    Python::attach(|py| {
        // Retrieve the data
        let result = engine
            .get(py, "doc_1")
            .unwrap()
            .expect("Document should exist");

        // Assert data integrity
        let name: String = result.get_item("name").unwrap().unwrap().extract().unwrap();
        assert_eq!(name, "Alice");
    });

    engine.close_engine().unwrap();
}

/// Tests the insertion of raw JSON strings and confirms that multiple records
/// (including missing ones) can be fetched accurately in a single batch request.
#[test]
fn test_insert_json_and_get_many() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    let json_payload_1 = r#"{"sensor": "temp", "value": 22.5}"#;
    let json_payload_2 = r#"{"sensor": "humidity", "value": 60}"#;

    engine.insert_json("sensor_1", json_payload_1).unwrap();
    engine.insert_json("sensor_2", json_payload_2).unwrap();

    wait_for_background_worker();

    Python::attach(|py| {
        let ids = vec![
            "sensor_1".to_string(),
            "sensor_2".to_string(),
            "sensor_missing".to_string(),
        ];
        let results = engine.get_many(py, ids).unwrap();

        assert_eq!(results.len(), 3);

        // Validate sensor_1
        assert!(results[0].1.is_some());
        let dict_1 = results[0].1.as_ref().unwrap();
        let sensor_1_name: String = dict_1
            .get_item("sensor")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(sensor_1_name, "temp");

        // Validate missing record
        assert!(results[2].1.is_none());
    });

    engine.close_engine().unwrap();
}

/// Ensures that nested fields (like strings and integers) within JSON payloads are properly flattened,
/// indexed in the background, and can be used to filter records.
#[test]
fn test_metadata_filtering() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    let payload_1 = r#"{"user": {"status": "active", "id": 100}}"#;
    let payload_2 = r#"{"user": {"status": "inactive", "id": 101}}"#;
    let payload_3 = r#"{"user": {"status": "active", "id": 102}}"#;

    engine.insert_json("u1", payload_1).unwrap();
    engine.insert_json("u2", payload_2).unwrap();
    engine.insert_json("u3", payload_3).unwrap();

    wait_for_background_worker();

    Python::attach(|py| {
        // Test searching nested JSON via your flattener mapping.
        let matches = engine
            .filter_by_metadata(py, "user.status", "active")
            .unwrap();

        assert_eq!(matches.len(), 2);
        let mut matched_ids: Vec<String> = matches.into_iter().map(|(id, _dict)| id).collect();
        matched_ids.sort();

        assert_eq!(matched_ids, vec!["u1", "u3"]);

        // NEW: Test integer index flattening
        let matches_int = engine.filter_by_metadata(py, "user.id", "100").unwrap();
        assert_eq!(matches_int.len(), 1, "Failed to index integer '100'");
        assert_eq!(matches_int[0].0, "u1");
    });

    engine.close_engine().unwrap();
}

/// Validates the update (upsert) process by confirming old metadata indexes are cleaned up
/// and replaced with new ones, and tests the complete removal of a record via delete.
#[test]
fn test_upsert_and_delete() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    Python::attach(|py| {
        let dict = PyDict::new(py);
        dict.set_item("status", "pending").unwrap();
        engine.insert(py, "task_1", dict).unwrap();
    });

    wait_for_background_worker();

    Python::attach(|py| {
        // Upsert with new data
        let new_dict = PyDict::new(py);
        new_dict.set_item("status", "completed").unwrap();
        engine.upsert("task_1", new_dict).unwrap();
    });

    wait_for_background_worker();

    Python::attach(|py| {
        // Verify upsert modified the metadata table
        let matches = engine
            .filter_by_metadata(py, "status", "completed")
            .unwrap();
        assert_eq!(matches.len(), 1);
        let pending_matches = engine.filter_by_metadata(py, "status", "pending").unwrap();
        assert_eq!(pending_matches.len(), 0); // Old index must be gone
    });

    // Delete
    engine.delete("task_1").unwrap();
    wait_for_background_worker();

    Python::attach(|py| {
        let fetched = engine.get(py, "task_1").unwrap();
        assert!(fetched.is_none());
    });

    engine.close_engine().unwrap();
}

/// Checks that records can be correctly scanned and fetched when their
/// document IDs match a specific string prefix.
#[test]
fn test_scan_prefix() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    engine.insert_json("user:100", r#"{"name": "A"}"#).unwrap();
    engine.insert_json("user:101", r#"{"name": "B"}"#).unwrap();
    engine.insert_json("org:500", r#"{"name": "C"}"#).unwrap();

    wait_for_background_worker();

    Python::attach(|py| {
        let matches = engine.scan_prefix(py, "user:").unwrap();

        assert_eq!(matches.len(), 2);
        let ids: Vec<String> = matches.into_iter().map(|(id, _)| id).collect();
        assert!(ids.contains(&"user:100".to_string()));
        assert!(ids.contains(&"user:101".to_string()));
        assert!(!ids.contains(&"org:500".to_string()));
    });

    engine.close_engine().unwrap();
}

/// Ensures the engine safely catches and rejects structurally invalid JSON strings in
/// both single and batch inserts without crashing the background worker.
#[test]
fn test_malformed_json_rejection() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    // A structurally broken JSON string (missing closing quote and brace)
    let bad_json = r#"{"name": "Alice, "status": "active" "#;

    // 1. Single insert should fail gracefully and return Err(PyRuntimeError)
    let result = engine.insert_json("bad_1", bad_json);
    assert!(
        result.is_err(),
        "Engine should reject malformed JSON in insert_json"
    );

    Python::attach(|py| {
        // 2. Batch insert should fail if *any* record is invalid, protecting the DB
        let batch = vec![
            ("good_1".to_string(), r#"{"status": "ok"}"#.to_string()),
            ("bad_2".to_string(), r#"{"status": "broken""#.to_string()),
        ];

        let batch_result = engine.insert_many_json(py, batch);
        assert!(
            batch_result.is_err(),
            "Engine should reject batch with any malformed JSON"
        );
    });

    // We don't need to wait for the background worker here because
    // the validation happens synchronously before the queue.

    engine.close_engine().unwrap();
}

/// Verifies that complex JSON data types—such as booleans, floats, and nested arrays—are
/// accurately flattened and mapped in the metadata index for querying.
#[test]
fn test_complex_type_indexing() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    // A complex payload with booleans, floats, and nested arrays
    let payload = r#"{
        "is_active": true,
        "score": 95.5,
        "metadata": {
            "tags": ["rust", "database"]
        }
    }"#;

    engine.insert_json("complex_1", payload).unwrap();

    wait_for_background_worker();

    Python::attach(|py| {
        // 1. Test Boolean index flattening
        let matches_bool = engine.filter_by_metadata(py, "is_active", "true").unwrap();
        assert_eq!(matches_bool.len(), 1, "Failed to index boolean 'true'");
        assert_eq!(matches_bool[0].0, "complex_1");

        // 2. Test Float index flattening
        let matches_float = engine.filter_by_metadata(py, "score", "95.5").unwrap();
        assert_eq!(matches_float.len(), 1, "Failed to index float '95.5'");

        // 3. Test Array index flattening
        // "metadata.tags.0" -> "rust"
        // "metadata.tags.1" -> "database"
        let matches_array_0 = engine
            .filter_by_metadata(py, "metadata.tags.0", "rust")
            .unwrap();
        assert_eq!(
            matches_array_0.len(),
            1,
            "Failed to index 0th array element"
        );

        let matches_array_1 = engine
            .filter_by_metadata(py, "metadata.tags.1", "database")
            .unwrap();
        assert_eq!(
            matches_array_1.len(),
            1,
            "Failed to index 1st array element"
        );

        // 4. Ensure non-existent indices return empty, not an error
        let matches_missing = engine
            .filter_by_metadata(py, "metadata.tags.2", "rust")
            .unwrap();
        assert_eq!(
            matches_missing.len(),
            0,
            "Querying out-of-bounds array index should return empty"
        );
    });

    engine.close_engine().unwrap();
}

/// Confirms that if a batch insert encounters invalid data (a poison pill),
/// the engine halts the batch safely to prevent bad data from entering the database,
/// while preserving prior valid inserts.
#[test]
fn test_batch_operation_isolation() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    Python::attach(|py| {
        // A batch with a poison pill in the middle
        let batch = vec![
            ("doc_1".to_string(), r#"{"status": "valid"}"#.to_string()),
            ("doc_2".to_string(), r#"{"status": "broken""#.to_string()), // Malformed
            ("doc_3".to_string(), r#"{"status": "valid"}"#.to_string()),
        ];

        let batch_result = engine.insert_many_json(py, batch);
        assert!(
            batch_result.is_err(),
            "Batch should return an error when hitting malformed JSON"
        );
    });

    wait_for_background_worker();

    Python::attach(|py| {
        // doc_1 was processed before the error, so it should exist
        let doc_1 = engine.get(py, "doc_1").unwrap();
        assert!(doc_1.is_some(), "Preceding valid data should be committed");

        // doc_2 was the poison pill, so it should NOT exist
        let doc_2 = engine.get(py, "doc_2").unwrap();
        assert!(
            doc_2.is_none(),
            "Poison data should never enter the database"
        );

        // doc_3 came after the error, so execution halted before it reached the queue
        let doc_3 = engine.get(py, "doc_3").unwrap();
        assert!(
            doc_3.is_none(),
            "Execution should halt after throwing the error"
        );
    });

    engine.close_engine().unwrap();
}

/// Tests the engine's shutdown sequence, ensuring that calling `close_engine`
/// safely drains the queue and cleanly blocks any subsequent write attempts.
#[test]
fn test_lifecycle_and_channel_closure() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    // 1. Explicitly close the engine to trigger the shutdown sequence
    engine.close_engine().unwrap();

    // 2. Attempt a write operation after the background thread has been joined
    let late_payload = r#"{"status": "too_late"}"#;
    let bad_write = engine.insert_json("late_doc", late_payload);

    assert!(
        bad_write.is_err(),
        "Engine should block writes after close_engine() is called"
    );

    // 3. Verify it triggered the WorkerHealthGuard crash protection
    // or the crossbeam channel disconnected error
    let err_msg = bad_write.unwrap_err().to_string();
    assert!(
        err_msg.contains("Background write worker has crashed")
            || err_msg.contains("Write queue full or closed"),
        "Expected worker shutdown error, got: {}",
        err_msg
    );
}

/// Simulates a high-load environment with multiple threads to
/// ensure the bounded write channel processes thousands of concurrent
/// inserts safely without dropping records or blocking indefinitely.
#[test]
fn test_high_concurrency_thread_safety() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    let num_threads = 10;
    let inserts_per_thread = 1000;

    // Use std::thread::scope to safely borrow the DBEngine across multiple threads
    // mimicking FastAPI's concurrent worker pool.
    std::thread::scope(|s| {
        for t in 0..num_threads {
            let engine_ref = &engine;
            s.spawn(move || {
                for i in 0..inserts_per_thread {
                    let id = format!("ticket_async_{}_{}", t, i);
                    let payload = r#"{"priority": "high", "type": "concurrent_test"}"#;

                    // The bounded(10000) channel must handle this load without blocking indefinitely
                    engine_ref.insert_json(&id, payload).unwrap();
                }
            });
        }
    });

    // THE FIX: Instead of sleep(), we cleanly shut down the engine.
    // The ShutDown command goes to the back of the FIFO queue.
    // handle.join() inside close_engine guarantees all 10,000 writes are committed to disk.
    engine.close_engine().unwrap();

    // Since our scan_prefix only relies on the internal Arc<Database> and not
    // the write queue, it is perfectly safe to read from the DB after the engine is closed.
    Python::attach(|py| {
        let matches = engine.scan_prefix(py, "ticket_async_").unwrap();
        assert_eq!(
            matches.len(),
            num_threads * inserts_per_thread,
            "Database dropped records under high concurrency"
        );
    });
}

/// Tests graceful handling of operations on non-existent records,
/// ensuring that deleting a missing ID does not fail,
/// and upserting a missing ID safely acts as a fresh insert.
#[test]
fn test_ghost_operations() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    // 1. Delete a non-existent ID
    // The underlying code uses documents_table.remove(), which returns Ok(None) if missing.
    let delete_result = engine.delete("missing_ticket_id");
    assert!(
        delete_result.is_ok(),
        "Deleting a missing ID should gracefully succeed and do nothing"
    );

    wait_for_background_worker();

    Python::attach(|py| {
        let fetched = engine.get(py, "missing_ticket_id").unwrap();
        assert!(fetched.is_none());

        // 2. Upsert a non-existent ID
        // The upsert logic attempts to remove old metadata, which will cleanly bypass,
        // and then insert the new record.
        let dict = PyDict::new(py);
        dict.set_item("priority", "urgent").unwrap();

        let upsert_result = engine.upsert("missing_ticket_id", dict);
        assert!(
            upsert_result.is_ok(),
            "Upserting a missing ID should not fail"
        );
    });

    wait_for_background_worker();

    Python::attach(|py| {
        // Verify upsert acted as a fresh insert for the missing ID
        let fetched = engine
            .get(py, "missing_ticket_id")
            .unwrap()
            .expect("Upsert should create missing records");

        let priority: String = fetched
            .get_item("priority")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(priority, "urgent");

        // Verify metadata was successfully indexed despite missing the "delete old metadata" phase
        let matches = engine.filter_by_metadata(py, "priority", "urgent").unwrap();
        assert_eq!(matches.len(), 1);
    });

    engine.close_engine().unwrap();
}

/// Verifies that a collection of native Python dictionaries (`PyDict`) can be
/// successfully batch-inserted via the foreground queue and queried afterward.
#[test]
fn test_insert_many_pydict() {
    Python::initialize();
    let (_dir, mut engine) = setup_engine();

    Python::attach(|py| {
        let dict1 = PyDict::new(py);
        dict1.set_item("type", "batch_dict").unwrap();

        let dict2 = PyDict::new(py);
        dict2.set_item("type", "batch_dict").unwrap();

        // Pass the Bound dictionaries directly, just like your single insert test
        let records = vec![
            ("batch_1".to_string(), dict1),
            ("batch_2".to_string(), dict2),
        ];

        engine.insert_many(py, records).unwrap();
    });

    wait_for_background_worker();

    Python::attach(|py| {
        let matches = engine.scan_prefix(py, "batch_").unwrap();
        assert_eq!(matches.len(), 2, "insert_many failed to insert PyDicts");
    });

    engine.close_engine().unwrap();
}

/// Ensures that attempting to create a database at an invalid or restricted file
/// path returns a graceful PyRuntimeError rather than panicking the application.
#[test]
fn test_engine_initialization_failure() {
    // A path that is a directory or lacks permissions
    let bad_path = "/this/path/does/not/exist/db.redb";
    let result = DBEngine::new(bad_path);

    assert!(
        result.is_err(),
        "Engine should return PyRuntimeError on invalid paths"
    );
}
