use lorealdb::db_engine::DBEngine;
use pyo3::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
fn benchmark_batch_delete_and_index_cleanup() {
    Python::initialize();
    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("delete_benchmark.redb");

    let engine = Arc::new(DBEngine::new(db_path.to_str().unwrap()).expect("Failed to init engine"));
    let total_records = 10_000;

    // 1. Prepare and Insert 10,000 records
    println!(
        "Inserting {} records for delete benchmark...",
        total_records
    );
    let mut insert_payloads = Vec::with_capacity(total_records);
    for i in 0..total_records {
        let id = format!("user_{}", i);
        // JSON payload with predictable metadata for index verification
        let payload = format!(
            "{{\"type\": \"account\", \"status\": \"pending\", \"user_id\": {}}}",
            i
        );
        insert_payloads.push((id, payload));
    }

    Python::attach(|py| {
        engine
            .insert_many_json(py, insert_payloads.clone())
            .unwrap();
    });

    // 2. Wait for inserts to finish (Polling)
    let final_insert_id = format!("user_{}", total_records - 1);
    loop {
        let is_committed = Python::attach(|py| engine.get(py, &final_insert_id).unwrap().is_some());
        if is_committed {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // 3. Verify indexes are populated BEFORE deletion (Sanity Check)
    Python::attach(|py| {
        let indexed_records = engine.filter_by_metadata(py, "status", "pending").unwrap();
        assert_eq!(
            indexed_records.len(),
            total_records,
            "Pre-delete index check failed: expected {} records, found {}",
            total_records,
            indexed_records.len()
        );
    });

    // 4. Execute 10,000 Deletes
    println!("Starting batch deletion of {} records...", total_records);
    let delete_start = Instant::now();

    for i in 0..total_records {
        let id = format!("user_{}", i);
        engine
            .delete(&id)
            .expect("Failed to enqueue delete operation");
    }
    let queue_time = delete_start.elapsed();

    // 5. Enqueue a sync token to know when deletes are finished
    // Since the channel is FIFO, this insert will only process AFTER all 10,000 deletes.
    let sync_id = "sync_token_after_delete";
    engine
        .insert_json(sync_id, "{\"status\": \"sync\"}")
        .unwrap();

    let commit_start = Instant::now();
    loop {
        let is_sync_committed = Python::attach(|py| engine.get(py, sync_id).unwrap().is_some());
        if is_sync_committed {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    let total_delete_time = delete_start.elapsed();

    // 6. Post-Delete Verification
    Python::attach(|py| {
        // Verify document removal
        let sample_doc = engine.get(py, "user_0").unwrap();
        assert!(
            sample_doc.is_none(),
            "Document user_0 still exists in DOCUMENTS_TABLE after delete!"
        );

        // Verify metadata index cleanup
        let leftover_indexes = engine.filter_by_metadata(py, "status", "pending").unwrap();
        assert_eq!(
            leftover_indexes.len(),
            0,
            "Metadata indexes were not properly cleaned up! Found {} orphaned entries.",
            leftover_indexes.len()
        );
    });

    // 7. Output Metrics
    println!("-----------------------------------------------");
    println!("Total Deletes:        {}", total_records);
    println!("Main Thread Queue Q:  {:?}", queue_time);
    println!("Background I/O Wait:  {:?}", commit_start.elapsed());
    println!("Total System Time:    {:?}", total_delete_time);
    println!("-----------------------------------------------");
    println!(
        "Delete Throughput:    {:.0} ops/sec",
        total_records as f64 / total_delete_time.as_secs_f64()
    );
    println!("Assertion Check:      PASSED (Documents and Indexes completely purged)");
}
