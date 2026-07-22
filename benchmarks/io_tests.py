import os
import tempfile
import time

from lorealdb import DBEngine


def benchmark_background_io_throughput():
    # Use tempfile to replicate tempdir()
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "io_benchmark.redb")
        engine = DBEngine(db_path)

        total_records = 100_000
        data_store = []
        total_bytes = 0

        # Generate realistic JSON payloads
        for i in range(total_records):
            id_str = f"ticket_{i}"
            payload = (
                f'{{"status": "open", "priority": "high", "id": "{id_str}", '
                f'"description": "This is a simulated payload to test the I/O '
                f'throughput and metadata indexing of the embedded database engine."}}'
            )
            # Encode to utf-8 to get the exact byte length for MB/s calculation
            total_bytes += len(payload.encode("utf-8"))
            data_store.append((id_str, payload))

        print("\n--- Starting Background I/O Write Benchmark ---")
        print(f"Total Records:      {total_records}")
        print(f"Total Payload Size: {total_bytes / 1_048_576.0:.2f} MB")

        system_start = time.perf_counter()

        # 1. Measure Queue Time (Main Thread blocking time)
        queue_start = time.perf_counter()

        # insert_many_json takes a list of (id, json_string) tuples
        engine.insert_many_json(data_store)

        queue_time = time.perf_counter() - queue_start

        # 2. Measure Background Commit Time (Disk I/O and Indexing)
        commit_start = time.perf_counter()
        last_id = f"ticket_{total_records - 1}"

        # Poll the DB until the very last record appears
        while True:
            if engine.get(last_id) is not None:
                break
            # Yield the CPU briefly (2ms) to prevent starving the background worker thread
            time.sleep(0.002)

        commit_wait_time = time.perf_counter() - commit_start
        total_system_time = time.perf_counter() - system_start

        # 3. Calculate Performance Metrics
        records_per_sec = total_records / total_system_time
        mb_per_sec = (total_bytes / 1_048_576.0) / total_system_time

        print("-----------------------------------------------")
        # Format to 4 decimal places for milliseconds readability
        print(f"Queue Time (Main Thread):   {queue_time:.4f}s")
        print(f"Wait Time (Background I/O): {commit_wait_time:.4f}s")
        print(f"Total System Write Time:    {total_system_time:.4f}s")
        print("-----------------------------------------------")
        print(f"Throughput (Records/sec):   {records_per_sec:.0f} ops/sec")
        print(f"Throughput (MB/sec):        {mb_per_sec:.2f} MB/s")
        print("-----------------------------------------------\n")


if __name__ == "__main__":
    benchmark_background_io_throughput()

# Results for reference.
# python io_tests.py

# --- Starting Background I/O Write Benchmark ---
# Total Records:      100000
# Total Payload Size: 17.92 MB
# -----------------------------------------------
# Queue Time (Main Thread):   2.4097s
# Wait Time (Background I/O): 0.5875s
# Total System Write Time:    2.9973s
# -----------------------------------------------
# Throughput (Records/sec):   33364 ops/sec
# Throughput (MB/sec):        5.98 MB/s
# -----------------------------------------------
