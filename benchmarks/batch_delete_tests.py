import os
import tempfile
import time

from benchmarks.benchmark_ecommerce import results
from lorealdb import DBEngine


def benchmark_batch_delete_and_index_cleanup():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "delete_benchmark.redb")
        engine = DBEngine(db_path)
        total_records = 10_000

        # 1. Prepare and Insert 10,000 records
        print(f"Inserting {total_records} records for delete benchmark...")
        insert_payloads = []
        for i in range(total_records):
            doc_id = f"user_{i}"
            # JSON payload with predictable metadata for index verification
            payload = f'{{"type": "account", "status": "pending", "id": {i}}}'
            insert_payloads.append((doc_id, payload))

        engine.insert_many_json(insert_payloads)

        # 2. Wait for inserts to finish (Polling)
        final_insert_id = f"user_{total_records - 1}"
        while True:
            if engine.get(final_insert_id) is not None:
                break
            time.sleep(0.005)

        # 3. Verify indexes are populated BEFORE deletion (Sanity Check)
        indexed_records = engine.filter_by_metadata("status", "pending")
        assert len(indexed_records) == total_records, (
            f"Pre-delete index check failed: expected {total_records} records, "
            f"found {len(indexed_records)}"
        )

        # 4. Execute 10,000 Deletes
        print(f"Starting batch deletion of {total_records} records...")
        delete_start = time.perf_counter()

        for i in range(total_records):
            doc_id = f"user_{i}"
            engine.delete(doc_id)

        queue_time = time.perf_counter() - delete_start

        # 5. Enqueue a sync token to know when deletes are finished
        # Since the channel is FIFO, this insert will only process AFTER all 10,000 deletes.
        sync_id = "sync_token_after_delete"
        engine.insert_json(sync_id, '{"status": "sync"}')

        commit_start = time.perf_counter()
        while True:
            if engine.get(sync_id) is not None:
                break
            time.sleep(0.005)

        total_delete_time = time.perf_counter() - delete_start

        # 6. Post-Delete Verification
        sample_doc = engine.get("user_0")
        assert sample_doc is None, (
            "Document user_0 still exists in DOCUMENTS_TABLE after delete!"
        )

        leftover_indexes = engine.filter_by_metadata("status", "pending")
        assert len(leftover_indexes) == 0, (
            f"Metadata indexes were not properly cleaned up! "
            f"Found {len(leftover_indexes)} orphaned entries."
        )

        # 7. Output Metrics
        def format_time(seconds: float) -> str:
            if seconds >= 1.0:
                return f"{seconds:.4f}s"
            elif seconds >= 0.001:
                return f"{seconds * 1000:.3f}ms"
            else:
                return f"{seconds * 1_000_000:.2f}µs"

        print("-----------------------------------------------")
        print(f"Total Deletes:        {total_records}")
        print(f"Main Thread Queue Q:  {format_time(queue_time)}")
        print(
            f"Background I/O Wait:  {format_time(time.perf_counter() - commit_start)}"
        )
        print(f"Total System Time:    {format_time(total_delete_time)}")
        print("-----------------------------------------------")
        print(f"Delete Throughput:    {total_records / total_delete_time:.0f} ops/sec")
        print("Assertion Check:      PASSED (Documents and Indexes completely purged)")


if __name__ == "__main__":
    benchmark_batch_delete_and_index_cleanup()


# Test results

# python batch_delete_tests.py
# Inserting 10000 records for delete benchmark...
# Starting batch deletion of 10000 records...
# -----------------------------------------------
# Total Deletes:        10000
# Main Thread Queue Q:  4.861ms
# Background I/O Wait:  211.841ms
# Total System Time:    216.303ms
# -----------------------------------------------
# Delete Throughput:    46231 ops/sec
# Assertion Check:      PASSED (Documents and Indexes completely purged)
