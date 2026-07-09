use lorealdb::DBEngine;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_100k_inserts_and_reads() {
    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("fiori_test.redb");

    let engine = DBEngine::new(&db_path).expect("Failed to initialize engine");

    // 1. Prepare data in memory first so we isolate DB write time
    let mut data_store = Vec::with_capacity(100_000);
    for i in 0..100_000 {
        let id = format!("ticket_{}", i);
        let payload = format!("{{\"status\": \"open\", \"id\": \"{}\"}}", id).into_bytes();
        data_store.push((id, payload));
    }

    // Convert into slice of references for the engine
    let mut batch: Vec<(&str, &mut [u8])> = data_store
        .iter_mut()
        .map(|(id, payload)| (id.as_str(), payload.as_mut_slice()))
        .collect();

    let start_write = Instant::now();

    // 2. Perform the single-transaction batch insert
    engine.insert_many(&mut batch).expect("Batch insert failed");

    println!(
        "100k batch-transaction inserts completed in: {:?}",
        start_write.elapsed()
    );

    // 3. Validate a specific record for byte equality
    let test_id = "ticket_88888";
    let expected_payload = format!("{{\"status\": \"open\", \"id\": \"{}\"}}", test_id);

    let retrieved = engine
        .get(test_id)
        .expect("Read transaction failed")
        .expect("Key not found");

    assert_eq!(
        retrieved,
        expected_payload.as_bytes(),
        "Byte equality assertion failed!"
    );
}

#[test]
fn test_json_validation_rejection() {
    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("fiori_validation_test.redb");
    let engine = DBEngine::new(&db_path).expect("Failed to initialize engine");

    let id = "ticket_malformed";

    // Notice the missing closing brace and quote
    let mut bad_payload = String::from("{\"status\": \"open, \"id\": 123").into_bytes();

    let result = engine.insert(id, &mut bad_payload);

    assert!(
        result.is_err(),
        "Engine should have rejected malformed JSON!"
    );

    println!("Successfully rejected bad JSON: {:?}", result.unwrap_err());
}
