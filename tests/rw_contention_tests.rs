use lorealdb::db_engine::DBEngine;
use pyo3::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

// Payload generators for mixed sizes
fn gen_small() -> String {
    "{\"status\": \"active\", \"role\": \"user\"}".to_string()
}

fn gen_medium() -> String {
    let mut pairs = Vec::new();
    for i in 0..50 {
        pairs.push(format!("\"field_{}\": {}", i, i));
    }
    format!("{{{}}}", pairs.join(", "))
}

fn gen_large() -> String {
    let mut arr = Vec::new();
    for i in 0..100 {
        arr.push(format!("{{\"item_id\": {}, \"active\": true}}", i));
    }
    format!(
        "{{\"payload_type\": \"bulk\", \"data\": [{}]}}",
        arr.join(", ")
    )
}

#[test]
fn benchmark_read_write_contention() {
    Python::initialize();
    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("contention_test.redb");

    let engine = Arc::new(DBEngine::new(db_path.to_str().unwrap()).expect("Failed to init engine"));

    let total_writes = 20_000;
    let total_reads = 10_000;

    // 1. Pre-populate a few records so readers have guaranteed data to fetch immediately
    println!("Pre-populating initial data...");
    Python::attach(|py| {
        for i in 0..100 {
            engine
                .insert_json(&format!("doc_{}", i), &gen_small())
                .unwrap();
        }
    });

    // Wait for initial data to commit
    std::thread::sleep(Duration::from_millis(500));

    // 2. Pre-generate the mixed write payloads to isolate DB timing from string generation
    let mut write_payloads = Vec::with_capacity(total_writes);
    for i in 0..total_writes {
        let payload = match i % 3 {
            0 => gen_small(),
            1 => gen_medium(),
            _ => gen_large(),
        };
        write_payloads.push((format!("doc_{}", i), payload));
    }

    println!(
        "Starting Contention Test: {} Writes vs {} Reads...",
        total_writes, total_reads
    );
    let system_start = Instant::now();

    // 3. Spawn Reader Threads
    // We use 4 threads to simulate concurrent FastAPI web workers making GET requests
    let mut read_handles = vec![];
    for thread_id in 0..4 {
        let engine_clone = Arc::clone(&engine);

        let handle = std::thread::spawn(move || {
            let reads_per_thread = total_reads / 4;
            let mut max_read_latency = Duration::from_nanos(0);
            let mut total_read_time = Duration::from_nanos(0);

            Python::attach(|py| {
                for i in 0..reads_per_thread {
                    // Fetch a mix of old records (guaranteed to exist) and new records
                    let target_id = format!("doc_{}", (i * 17 + thread_id) % 5000);

                    let read_start = Instant::now();
                    // Perform the read
                    let _ = engine_clone.get(py, &target_id);
                    let elapsed = read_start.elapsed();

                    total_read_time += elapsed;
                    if elapsed > max_read_latency {
                        max_read_latency = elapsed;
                    }

                    // Tiny sleep to simulate real-world request spacing and prevent
                    // this tight loop from immediately consuming all CPU time.
                    std::thread::sleep(Duration::from_micros(100));
                }
            });

            (total_read_time, max_read_latency)
        });
        read_handles.push(handle);
    }

    // 4. Jam the Write Channel
    let write_start = Instant::now();
    for (id, payload) in write_payloads {
        engine
            .insert_json(&id, &payload)
            .expect("Write queue failed");
    }
    let queue_time = write_start.elapsed();

    // 5. Collect Reader Metrics
    let mut overall_max_read_latency = Duration::from_nanos(0);
    let mut aggregate_read_time = Duration::from_nanos(0);

    for handle in read_handles {
        let (total_time, max_latency) = handle.join().expect("Reader panicked");
        aggregate_read_time += total_time;
        if max_latency > overall_max_read_latency {
            overall_max_read_latency = max_latency;
        }
    }

    // 6. Polling Loop for Final Write
    let final_id = format!("doc_{}", total_writes - 1);
    loop {
        let is_committed = Python::attach(|py| engine.get(py, &final_id).unwrap().is_some());

        if is_committed {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let total_time = system_start.elapsed();
    let avg_read_latency = aggregate_read_time / (total_reads as u32);

    println!("-----------------------------------------------");
    println!("Main Thread Write Q Time: {:?}", queue_time);
    println!("Total System Time:        {:?}", total_time);
    println!("-----------------------------------------------");
    println!("Avg Read Latency:         {:?}", avg_read_latency);
    println!("Max Read Latency (Spike): {:?}", overall_max_read_latency);
    println!("-----------------------------------------------");

    // Test assertion: If a single read transaction takes more than 100ms,
    // redb lock contention is starving the readers.
    assert!(
        overall_max_read_latency < Duration::from_millis(100),
        "Read starvation detected! A read took {:?}",
        overall_max_read_latency
    );
}
