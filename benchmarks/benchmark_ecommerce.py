import time
import json
import os
from typing import List, Tuple
from lorealdb import DBEngineWriteOptimized

# --- CONFIGURATION ---
DB_PATH = "benchmark_test.redb"
JSON_FILE = "sample_json.json"
TARGET_BATCH_RECORDS = 1_000_000  # 1 Million records for the batch test
SINGLE_INSERTS = 10_000

# We explicitly define the batch size so we aren't limited by the small JSON file
RECORDS_PER_BATCH = 1_000
BATCH_LOOPS = TARGET_BATCH_RECORDS // RECORDS_PER_BATCH

# 1. Clean up old database if it exists to ensure a fresh test
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print(f"🗑️  Deleted old {DB_PATH}")

# 2. Load and prep the JSON data
print(f"📂 Loading data from {JSON_FILE}...")
with open(JSON_FILE, "r") as f:
    source_data = json.load(f)

if not isinstance(source_data, list):
    raise ValueError("The JSON file must contain a list of JSON objects/dictionaries.")

num_source_records = len(source_data)
print(f"📦 Found {num_source_records:,} distinct records in the source file.")
print(
    f"🔄 Will cycle these records to run {BATCH_LOOPS:,} batch loops of {RECORDS_PER_BATCH:,} to reach {TARGET_BATCH_RECORDS:,} records."
)

# PRE-ENCODE: We encode to bytes now so Python's JSON parser doesn't slow down the Rust benchmark
encoded_source_data = [json.dumps(record).encode("utf-8") for record in source_data]
single_test_payload = encoded_source_data[0]

print(f"\n🚀 Initializing DBEngineWriteOptimized at {DB_PATH}...")
db = DBEngineWriteOptimized(DB_PATH)


print("\n" + "=" * 50)
print(f" 1. TESTING SINGLE INSERTS ({SINGLE_INSERTS:,} records)")
print("=" * 50)
start_time = time.perf_counter()

for i in range(SINGLE_INSERTS):
    # Just reuse the first payload for the single insert tests
    db.insert_raw(f"single_{i}", single_test_payload)

end_time = time.perf_counter()
duration = end_time - start_time
print(f"✅ Inserted {SINGLE_INSERTS:,} single records.")
print(f"⏱️  Time: {duration:.4f} seconds")
print(f"⚡ Speed: {SINGLE_INSERTS / duration:,.0f} records/second")


print("\n" + "=" * 50)
print(f" 2. TESTING BATCH INSERTS ({TARGET_BATCH_RECORDS:,} total)")
print("=" * 50)
start_time = time.perf_counter()

for loop_idx in range(BATCH_LOOPS):
    # Zip our generated unique IDs with the cycled payloads using modulo arithmetic
    batch: List[Tuple[str, bytes]] = [
        (f"batch_{loop_idx}_{i}", encoded_source_data[i % num_source_records])
        for i in range(RECORDS_PER_BATCH)
    ]
    db.insert_many_raw(batch)

    # Print progress every 100 loops (100k records)
    if loop_idx > 0 and loop_idx % 100 == 0:
        print(f"   ... inserted {loop_idx * RECORDS_PER_BATCH:,} records so far ...")

end_time = time.perf_counter()
duration = end_time - start_time
print(f"✅ Inserted {TARGET_BATCH_RECORDS:,} batch records.")
print(f"⏱️  Time: {duration:.4f} seconds")
print(f"⚡ Speed: {TARGET_BATCH_RECORDS / duration:,.0f} records/second")


print("\n" + "=" * 50)
print(" 3. TESTING INDIVIDUAL GET (100,000 records)")
print("=" * 50)
start_time = time.perf_counter()

# Reading the first 100k records sequentially
for i in range(100_000):
    _ = db.get(f"batch_{i // RECORDS_PER_BATCH}_{i % RECORDS_PER_BATCH}")

end_time = time.perf_counter()
duration = end_time - start_time
print(f"✅ Fetched 100,000 individual records.")
print(f"⏱️  Time: {duration:.4f} seconds")
print(f"⚡ Speed: {100_000 / duration:,.0f} reads/second")


print("\n" + "=" * 50)
print(" 4. TESTING GET_MANY (100,000 records)")
print("=" * 50)
# Generate the list of IDs in Python first
fetch_ids = [
    f"batch_{i // RECORDS_PER_BATCH}_{i % RECORDS_PER_BATCH}" for i in range(100_000)
]

start_time = time.perf_counter()
results = db.get_many(fetch_ids)
end_time = time.perf_counter()
duration = end_time - start_time
print(f"✅ Fetched 100,000 records in one get_many call.")
print(f"⏱️  Time: {duration:.4f} seconds")


print("\n" + "=" * 50)
print(" 5. TESTING PREFIX SCAN")
print("=" * 50)
# Scanning for loop index 50, which should return exactly RECORDS_PER_BATCH (1,000) records
start_time = time.perf_counter()
scanned_results = db.scan_prefix("batch_50_")
end_time = time.perf_counter()
duration = end_time - start_time
print(f"✅ Prefix scan found {len(scanned_results):,} records.")
print(f"⏱️  Time: {duration:.6f} seconds")


print("\n" + "=" * 50)
print(f" 6. TESTING DELETES (5,000 records)")
print("=" * 50)
start_time = time.perf_counter()

# Deleting half of the single inserts we made in step 1
for i in range(5000):
    db.delete(f"single_{i}")

end_time = time.perf_counter()
duration = end_time - start_time
print(f"✅ Deleted 5,000 records.")
print(f"⏱️  Time: {duration:.4f} seconds")
print(f"⚡ Speed: {5000 / duration:,.0f} deletes/second")

print("\n🎉 1 MILLION RECORD BENCHMARK COMPLETE!")
