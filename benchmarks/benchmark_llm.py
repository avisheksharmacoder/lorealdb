import time
import json
import os
from typing import List, Tuple
from lorealdb import DBEngineWriteOptimized

# --- CONFIGURATION ---
DB_PATH = "llm_benchmark.redb"
JSON_FILE = "llm_response_large.json"

# Workload sizes per your SSD constraints
BATCH_TARGET = 10_000
SINGLE_INSERTS = 1_000
GET_SINGLE = 1_000
GET_MANY = 5_000
PREFIX_SCAN = 5_000

# We will chunk the 10,000 inserts into batches of 100
RECORDS_PER_BATCH = 100
BATCH_LOOPS = BATCH_TARGET // RECORDS_PER_BATCH  # 100 loops

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print(f"🗑️  Deleted old {DB_PATH}")

print(f"📂 Loading heavy LLM payload from {JSON_FILE}...")
with open(JSON_FILE, "r") as f:
    source_data = json.load(f)

# Handle the file whether it's a single JSON object or a list with one item
if isinstance(source_data, list):
    single_record = source_data[0]
else:
    single_record = source_data

# Pre-encode to isolate disk speed from Python JSON parsing overhead
single_test_payload = json.dumps(single_record).encode("utf-8")

# Calculate payload sizes for MB/s throughput metrics
record_bytes = len(single_test_payload)
total_batch_mb = (record_bytes * BATCH_TARGET) / (1024 * 1024)

print(f"📦 Loaded 1 base record (Size: {record_bytes / 1024:.2f} KB).")
print(
    f"🔄 Preparing to write ~{total_batch_mb:.2f} MB of raw data to disk in {BATCH_LOOPS} batches..."
)

db = DBEngineWriteOptimized(DB_PATH)


print("\n" + "=" * 55)
print(f" 1. SINGLE INSERTS ({SINGLE_INSERTS:,} records)")
print("=" * 55)
start_time = time.perf_counter()

for i in range(SINGLE_INSERTS):
    db.insert_raw(f"single_{i}", single_test_payload)

duration = time.perf_counter() - start_time
mb_written = (record_bytes * SINGLE_INSERTS) / (1024 * 1024)
print(f"✅ Time: {duration:.4f}s")
print(
    f"⚡ Speed: {SINGLE_INSERTS / duration:,.0f} records/s  |  {mb_written / duration:.2f} MB/s"
)


print("\n" + "=" * 55)
print(f" 2. BATCH INSERTS ({BATCH_TARGET:,} total)")
print("=" * 55)
start_time = time.perf_counter()

for loop_idx in range(BATCH_LOOPS):
    # We intentionally name the first 5,000 records with a specific prefix
    # so we can target them perfectly in the Prefix Scan test later.
    prefix = (
        "scan_target_"
        if loop_idx < (PREFIX_SCAN // RECORDS_PER_BATCH)
        else "llm_batch_"
    )

    # Generate the batch using the exact same payload repeatedly
    batch: List[Tuple[str, bytes]] = [
        (f"{prefix}{loop_idx:04d}_{i:03d}", single_test_payload)
        for i in range(RECORDS_PER_BATCH)
    ]
    db.insert_many_raw(batch)

    if loop_idx > 0 and loop_idx % 20 == 0:
        print(f"   ... {loop_idx * RECORDS_PER_BATCH:,} records synced ...")

duration = time.perf_counter() - start_time
print(f"✅ Time: {duration:.4f}s")
print(
    f"⚡ Speed: {BATCH_TARGET / duration:,.0f} records/s  |  {total_batch_mb / duration:.2f} MB/s"
)


print("\n" + "=" * 55)
print(f" 3. GET SINGLE ({GET_SINGLE:,} individual reads)")
print("=" * 55)
start_time = time.perf_counter()

for i in range(GET_SINGLE):
    # Fetching from the standard batch prefix
    loop = i // RECORDS_PER_BATCH
    idx = i % RECORDS_PER_BATCH
    # Adjusting for the fact that the first 50 loops used the 'scan_target_' prefix
    actual_loop = loop + (PREFIX_SCAN // RECORDS_PER_BATCH)
    _ = db.get(f"llm_batch_{actual_loop:04d}_{idx:03d}")

duration = time.perf_counter() - start_time
print(f"✅ Time: {duration:.4f}s")
print(f"⚡ Speed: {GET_SINGLE / duration:,.0f} reads/s")


print("\n" + "=" * 55)
print(f" 4. GET MANY ({GET_MANY:,} records in one call)")
print("=" * 55)
# Generate 5,000 IDs to fetch simultaneously
fetch_ids = []
for i in range(GET_MANY):
    loop = i // RECORDS_PER_BATCH
    idx = i % RECORDS_PER_BATCH
    actual_loop = loop + (PREFIX_SCAN // RECORDS_PER_BATCH)
    fetch_ids.append(f"llm_batch_{actual_loop:04d}_{idx:03d}")

start_time = time.perf_counter()
results = db.get_many(fetch_ids)
duration = time.perf_counter() - start_time

mb_read = sum(len(b) for b in results.values() if b is not None) / (1024 * 1024)
print(f"✅ Time: {duration:.4f}s")
print(f"⚡ Throughput: {mb_read / duration:.2f} MB/s read into Python dictionary")


print("\n" + "=" * 55)
print(f" 5. PREFIX SCAN (Targeting {PREFIX_SCAN:,} records)")
print("=" * 55)
start_time = time.perf_counter()
scanned_results = db.scan_prefix("scan_target_")
duration = time.perf_counter() - start_time

mb_scanned = sum(len(b) for b in scanned_results.values()) / (1024 * 1024)
print(f"✅ Found {len(scanned_results):,} large records.")
print(f"✅ Time: {duration:.4f}s")
print(f"⚡ Throughput: {mb_scanned / duration:.2f} MB/s scanned into memory")

print("\n🎉 LLM PAYLOAD BENCHMARK COMPLETE!")
