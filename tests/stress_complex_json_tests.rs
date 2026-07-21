use lorealdb::db_engine::DBEngine;
use pyo3::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

// ---------------------------------------------------------
// Payload Generators
// ---------------------------------------------------------

fn generate_deeply_nested_object(depth: usize) -> String {
    let mut json = String::from("\"end_value\"");
    for i in 0..depth {
        json = format!("{{\"level_{}\": {}}}", depth - i, json);
    }
    json
}

fn generate_deeply_nested_array(depth: usize) -> String {
    let mut json = String::from("\"inner_item\"");
    for _ in 0..depth {
        json = format!("[{}]", json);
    }
    format!("{{\"deep_array\": {}}}", json)
}

fn generate_wide_json(key_count: usize) -> String {
    let mut pairs = Vec::with_capacity(key_count);
    for i in 0..key_count {
        pairs.push(format!("\"key_{}\": {}", i, i));
    }
    format!("{{{}}}", pairs.join(", "))
}

fn generate_large_llm_response() -> String {
    let citations: Vec<String> = (0..50)
        .map(|i| {
            format!(
                "{{\"id\": \"ref_{}\", \"url\": \"https://example.com/page_{}\", \"confidence\": 0.99, \"snippets\": [\"Matched text A\", \"Matched text B\"]}}",
                i, i
            )
        })
        .collect();

    format!(
            "{{\n  \"model\": \"gpt-4-turbo\",\n  \"choices\": [{{\n    \"message\": {{\n      \"role\": \"assistant\",\n      \"content\": \"This is a very long generated response...\",\n      \"metadata\": {{\n        \"citations\": [{}],\n        \"tokens_used\": 4500,\n        \"finish_reason\": \"stop\"\n      }}\n    }}\n  }}]\n}}",
            citations.join(",\n        ")

    )
}

#[test]
fn benchmark_complex_workload_and_concurrency() {
    // 1. Initialize Python and DB
    Python::initialize();
    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("production_stress_test.redb");

    // Wrap the engine in an Arc so we can share it across reader/writer threads
    let engine = Arc::new(DBEngine::new(db_path.to_str().unwrap()).expect("Failed to init engine"));

    // 2. Pre-generate payloads to avoid timing string creation
    println!("Generating complex payloads...");
    let deep_obj = generate_deeply_nested_object(150); // 150 levels deep
    let deep_arr = generate_deeply_nested_array(150);
    let wide_obj = generate_wide_json(1000); // 1000 flat keys
    let llm_resp = generate_large_llm_response();

    let payloads = vec![deep_obj, deep_arr, wide_obj, llm_resp];
    let total_operations = 50_000;

    println!("Starting Concurrent Read/Write Stress Test...");
    let system_start = Instant::now();

    // 3. Spawn Reader Threads (Simulating concurrent API GET requests)
    let mut read_handles = vec![];
    for thread_id in 0..4 {
        let engine_clone = Arc::clone(&engine);

        let handle = std::thread::spawn(move || {
            // Each thread does 12,500 reads
            let reads_per_thread = total_operations / 4;

            Python::attach(|py| {
                for i in 0..reads_per_thread {
                    // Pseudo-randomly pick an ID that might or might not exist yet
                    // using deterministic math to avoid requiring the `rand` crate.
                    let target_id_num = (i * 997 + thread_id) % total_operations;
                    let target_id = format!("doc_{}", target_id_num);

                    // We don't unwrap the inner value, because the writer might not
                    // have inserted it yet. We just want to stress the read transaction lock.
                    let _ = engine_clone
                        .get(py, &target_id)
                        .expect("Read transaction panicked");
                }
            });
        });
        read_handles.push(handle);
    }

    // 4. Run Writer in the main thread (Simulating API POST requests)
    let write_start = Instant::now();
    for i in 0..total_operations {
        let id = format!("doc_{}", i);
        // Round-robin through the 4 complex payloads
        let payload = &payloads[i % 4];

        engine
            .insert_json(&id, payload)
            .expect("Queue failed to accept insert");
    }
    let queue_time = write_start.elapsed();

    // 5. Wait for readers to finish their spam
    for handle in read_handles {
        handle.join().expect("Reader thread panicked!");
    }

    // 6. Polling Loop: Wait for the background worker to flush the final write
    let final_id = format!("doc_{}", total_operations - 1);
    let commit_start = Instant::now();
    loop {
        let is_committed = Python::attach(|py| engine.get(py, &final_id).unwrap().is_some());

        if is_committed {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let total_time = system_start.elapsed();

    println!("-----------------------------------------------");
    println!(
        "Total Operations:      {} Writes, {} Reads",
        total_operations, total_operations
    );
    println!("Main Thread Write Q:   {:?}", queue_time);
    println!("Background I/O Wait:   {:?}", commit_start.elapsed());
    println!("Total System Time:     {:?}", total_time);
    println!("-----------------------------------------------");
    println!(
        "Mixed Throughput:      {:.0} ops/sec",
        (total_operations * 2) as f64 / total_time.as_secs_f64()
    );
}
