import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from benchmarks.benchmark_ecommerce import results
from lorealdb import DBEngine


# Payload generators for mixed sizes
def gen_small() -> str:
    return '{"status": "active", "role": "user"}'


def gen_medium() -> str:
    pairs = [f'"field_{i}": {i}' for i in range(50)]
    return f"{{{', '.join(pairs)}}}"


def gen_large() -> str:
    arr = [f'{{"item_id": {i}, "active": true}}' for i in range(100)]
    return f'{{"payload_type": "bulk", "data": [{", ".join(arr)}]}}'


def reader_worker(engine: DBEngine, thread_id: int, num_reads: int):
    max_read_latency = 0.0
    total_read_time = 0.0

    for i in range(num_reads):
        # Fetch a mix of old records (guaranteed to exist) and new records
        target_id = f"doc_{(i * 17 + thread_id) % 5000}"

        read_start = time.perf_counter()
        # Perform the read
        engine.get(target_id)
        elapsed = time.perf_counter() - read_start

        total_read_time += elapsed
        if elapsed > max_read_latency:
            max_read_latency = elapsed

        # Tiny sleep to simulate real-world request spacing and prevent
        # this tight loop from immediately consuming all CPU time.
        time.sleep(0.0001)  # 100 microseconds

    return total_read_time, max_read_latency


def benchmark_read_write_contention():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "contention_test.redb")
        engine = DBEngine(db_path)

        total_writes = 20_000
        total_reads = 10_000

        # 1. Pre-populate a few records so readers have guaranteed data to fetch immediately
        print("Pre-populating initial data...")
        for i in range(100):
            engine.insert_json(f"doc_{i}", gen_small())

        # Wait for initial data to commit in the background worker
        time.sleep(0.5)

        # 2. Pre-generate the mixed write payloads to isolate DB timing from string generation
        write_payloads = []
        for i in range(total_writes):
            mod = i % 3
            if mod == 0:
                payload = gen_small()
            elif mod == 1:
                payload = gen_medium()
            else:
                payload = gen_large()

            write_payloads.append((f"doc_{i}", payload))

        print(
            f"Starting Contention Test: {total_writes} Writes vs {total_reads} Reads..."
        )
        system_start = time.perf_counter()

        # 3. Spawn Reader Threads
        # We use 4 threads to simulate concurrent FastAPI web workers making GET requests
        reads_per_thread = total_reads // 4

        # Use ThreadPoolExecutor to easily gather the results
        executor = ThreadPoolExecutor(max_workers=4)
        futures = []
        for thread_id in range(4):
            futures.append(
                executor.submit(reader_worker, engine, thread_id, reads_per_thread)
            )

        # 4. Jam the Write Channel
        write_start = time.perf_counter()
        for doc_id, payload in write_payloads:
            engine.insert_json(doc_id, payload)
        queue_time = time.perf_counter() - write_start

        # 5. Collect Reader Metrics
        overall_max_read_latency = 0.0
        aggregate_read_time = 0.0

        for future in futures:
            total_time, max_latency = future.result()
            aggregate_read_time += total_time
            if max_latency > overall_max_read_latency:
                overall_max_read_latency = max_latency

        executor.shutdown()

        # 6. Polling Loop for Final Write
        final_id = f"doc_{total_writes - 1}"
        while True:
            if engine.get(final_id) is not None:
                break
            time.sleep(0.01)  # 10ms poll

        total_time = time.perf_counter() - system_start
        avg_read_latency = aggregate_read_time / total_reads

        # Format times for readability
        def format_time(seconds: float) -> str:
            if seconds >= 1.0:
                return f"{seconds:.4f}s"
            elif seconds >= 0.001:
                return f"{seconds * 1000:.3f}ms"
            else:
                return f"{seconds * 1_000_000:.2f}µs"

        print("-----------------------------------------------")
        print(f"Main Thread Write Q Time: {format_time(queue_time)}")
        print(f"Total System Time:        {format_time(total_time)}")
        print("-----------------------------------------------")
        print(f"Avg Read Latency:         {format_time(avg_read_latency)}")
        print(f"Max Read Latency (Spike): {format_time(overall_max_read_latency)}")
        print("-----------------------------------------------")

        # Test assertion: If a single read transaction takes more than 100ms,
        # redb lock contention is starving the readers.
        assert overall_max_read_latency < 0.1, (
            f"Read starvation detected! A read took {format_time(overall_max_read_latency)}"
        )


if __name__ == "__main__":
    benchmark_read_write_contention()

# Test results

# python rw_contention_tests.py
# Pre-populating initial data...
# Starting Contention Test: 20000 Writes vs 10000 Reads...
# -----------------------------------------------
# Main Thread Write Q Time: 903.067ms
# Total System Time:        11.1760s
# -----------------------------------------------
# Avg Read Latency:         60.79µs
# Max Read Latency (Spike): 19.932ms
# -----------------------------------------------
