use lorealdb::db_engine::DBEngine;
use pyo3::prelude::*;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
fn benchmark_background_io_throughput() {
    Python::initialize();
    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("io_benchmark.redb");
    let engine = DBEngine::new(db_path.to_str().unwrap()).expect("Failed to init engine");

    let total_records = 100_000;
    let mut data_store = Vec::with_capacity(total_records);
    let mut total_bytes = 0;

    // Generate realistic JSON payloads to trigger the simd_json indexing logic
    for i in 0..total_records {
        let id = format!("ticket_{}", i);
        let payload = format!(
            "{{\"status\": \"open\", \"priority\": \"high\", \"id\": \"{}\", \"description\": \"This is a simulated payload to test the I/O throughput and metadata indexing of the embedded database engine.\"}}",
            id
        );
        total_bytes += payload.len();
        data_store.push((id, payload));
    }

    println!("\n--- Starting Background I/O Write Benchmark ---");
    println!("Total Records:      {}", total_records);
    println!(
        "Total Payload Size: {:.2} MB",
        total_bytes as f64 / 1_048_576.0
    );

    // system_start measures from the moment we start pushing to the queue
    // until the data is fully written to the disk.
    let system_start = Instant::now();

    // 1. Measure Queue Time (Main Thread blocking time)
    let queue_start = Instant::now();
    Python::attach(|py| {
        engine
            .insert_many_json(py, data_store)
            .expect("Batch insert failed");
    });
    let queue_time = queue_start.elapsed();

    // 2. Measure Background Commit Time (Disk I/O and Indexing)
    let commit_start = Instant::now();
    let last_id = format!("ticket_{}", total_records - 1);

    // Poll the DB until the very last record appears
    loop {
        let is_committed = Python::attach(|py| engine.get(py, &last_id).unwrap().is_some());

        if is_committed {
            break;
        }
        // Yield the CPU briefly to prevent starving the background worker thread
        std::thread::sleep(Duration::from_millis(2));
    }

    let commit_wait_time = commit_start.elapsed();
    let total_system_time = system_start.elapsed();

    // 3. Calculate Performance Metrics
    let records_per_sec = total_records as f64 / total_system_time.as_secs_f64();
    let mb_per_sec = (total_bytes as f64 / 1_048_576.0) / total_system_time.as_secs_f64();

    println!("-----------------------------------------------");
    println!("Queue Time (Main Thread):   {:?}", queue_time);
    println!("Wait Time (Background I/O): {:?}", commit_wait_time);
    println!("Total System Write Time:    {:?}", total_system_time);
    println!("-----------------------------------------------");
    println!("Throughput (Records/sec):   {:.0} ops/sec", records_per_sec);
    println!("Throughput (MB/sec):        {:.2} MB/s", mb_per_sec);
    println!("-----------------------------------------------\n");
}
