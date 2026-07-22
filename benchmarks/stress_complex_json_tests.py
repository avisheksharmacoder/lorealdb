import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

from lorealdb import DBEngine

# ---------------------------------------------------------
# Payload Generators
# ---------------------------------------------------------


def generate_deeply_nested_object(depth: int) -> str:
    json_str = '"end_value"'
    for i in range(depth):
        json_str = f'{{"level_{depth - i}": {json_str}}}'
    return json_str


def generate_deeply_nested_array(depth: int) -> str:
    json_str = '"inner_item"'
    for _ in range(depth):
        json_str = f"[{json_str}]"
    return f'{{"deep_array": {json_str}}}'


def generate_wide_json(key_count: int) -> str:
    pairs = [f'"key_{i}": {i}' for i in range(key_count)]
    return f"{{{', '.join(pairs)}}}"


def generate_large_llm_response() -> str:
    citations = []
    for i in range(50):
        citations.append(
            f'{{"id": "ref_{i}", "url": "https://example.com/page_{i}", '
            f'"confidence": 0.99, "snippets": ["Matched text A", "Matched text B"]}}'
        )

    citations_str = ",\n        ".join(citations)
    return f"""{{
  "model": "gpt-4-turbo",
  "choices": [{{
    "message": {{
      "role": "assistant",
      "content": "This is a very long generated response...",
      "metadata": {{
        "citations": [{citations_str}],
        "tokens_used": 4500,
        "finish_reason": "stop"
      }}
    }}
  }}]
}}"""


# ---------------------------------------------------------
# Reader Worker
# ---------------------------------------------------------


def reader_worker(
    engine: DBEngine, thread_id: int, num_reads: int, total_operations: int
):
    for i in range(num_reads):
        # Pseudo-randomly pick an ID that might or might not exist yet
        # using deterministic math to match the Rust implementation.
        target_id_num = (i * 997 + thread_id) % total_operations
        target_id = f"doc_{target_id_num}"

        # We just want to stress the read transaction lock.
        engine.get(target_id)


def benchmark_complex_workload_and_concurrency():
    # 1. Initialize DB
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "production_stress_test.redb")
        engine = DBEngine(db_path)

        # 2. Pre-generate payloads to avoid timing string creation
        print("Generating complex payloads...")
        deep_obj = generate_deeply_nested_object(100)  # Changed from 150
        deep_arr = generate_deeply_nested_array(100)  # Changed from 150
        wide_obj = generate_wide_json(1000)  # 1000 flat keys
        llm_resp = generate_large_llm_response()

        payloads = [deep_obj, deep_arr, wide_obj, llm_resp]
        total_operations = 50_000

        print("Starting Concurrent Read/Write Stress Test...")
        system_start = time.perf_counter()

        # 3. Spawn Reader Threads (Simulating concurrent API GET requests)
        reads_per_thread = total_operations // 4
        executor = ThreadPoolExecutor(max_workers=4)
        futures = []
        for thread_id in range(4):
            futures.append(
                executor.submit(
                    reader_worker, engine, thread_id, reads_per_thread, total_operations
                )
            )

        # 4. Run Writer in the main thread (Simulating API POST requests)
        write_start = time.perf_counter()
        for i in range(total_operations):
            doc_id = f"doc_{i}"
            # Round-robin through the 4 complex payloads
            payload = payloads[i % 4]
            engine.insert_json(doc_id, payload)

        queue_time = time.perf_counter() - write_start

        # 5. Wait for readers to finish their spam
        for future in futures:
            future.result()
        executor.shutdown()

        # 6. Polling Loop: Wait for the background worker to flush the final write
        final_id = f"doc_{total_operations - 1}"
        commit_start = time.perf_counter()
        while True:
            if engine.get(final_id) is not None:
                break
            time.sleep(0.01)

        total_time = time.perf_counter() - system_start

        def format_time(seconds: float) -> str:
            if seconds >= 1.0:
                return f"{seconds:.4f}s"
            elif seconds >= 0.001:
                return f"{seconds * 1000:.3f}ms"
            else:
                return f"{seconds * 1_000_000:.2f}µs"

        print("-----------------------------------------------")
        print(
            f"Total Operations:      {total_operations} Writes, {total_operations} Reads"
        )
        print(f"Main Thread Write Q:   {format_time(queue_time)}")
        print(
            f"Background I/O Wait:   {format_time(time.perf_counter() - commit_start)}"
        )
        print(f"Total System Time:     {format_time(total_time)}")
        print("-----------------------------------------------")
        print(
            f"Mixed Throughput:      {(total_operations * 2) / total_time:.0f} ops/sec"
        )


if __name__ == "__main__":
    benchmark_complex_workload_and_concurrency()


# Test results.

# python stress_complex_json_tests.py
# Generating complex payloads...
# Starting Concurrent Read/Write Stress Test...
# -----------------------------------------------
# Total Operations:      50000 Writes, 50000 Reads
# Main Thread Write Q:   75.4220s
# Background I/O Wait:   40.8361s
# Total System Time:     124.4312s
# -----------------------------------------------
# Mixed Throughput:      804 ops/sec
