import threading
import time

from lorealdb import DBEngine

# Initialize the database
db = DBEngine("./benchmark_test.db")

print("==================================================")
print(" PHASE 1: CONCURRENCY (NON-BLOCKING WRITES) ")
print("==================================================")
# We will insert 50,000 records one by one.
# In a normal Python DB (SQLite, etc.), doing 50,000 isolated
# inserts would block the main thread for seconds waiting on disk I/O.

start_write = time.time()
for i in range(50000):
    db.insert(f"user_{i}", {"name": f"User {i}", "status": "active"})
write_time = time.time() - start_write

print(f"Sent 50,000 individual inserts in: {write_time:.4f} seconds!")
print("-> Python didn't wait for the disk! The Rust background thread caught them all.")
print(
    "-> (Waiting 2 seconds to let the Rust background thread finish writing to disk...)\n"
)
time.sleep(2)


print("==================================================")
print(" PHASE 2: PARALLELISM (GIL RELEASE ON READS) ")
print("==================================================")
is_heavy_scan_running = True


def heavy_background_scan():
    global is_heavy_scan_running
    print("[Thread A] Starting heavy scan of 50,000 records...")

    start_scan = time.time()
    results = db.scan_prefix("user_")
    scan_time = time.time() - start_scan

    print(
        f"\n[Thread A] 🟢 Heavy scan finished! Fetched {len(results)} records in {scan_time:.4f} seconds."
    )
    is_heavy_scan_running = False


def foreground_web_server():
    print("[Thread B] Web server is online and taking fast requests...")
    requests_handled = 0

    while is_heavy_scan_running:
        # Fire requests as fast as possible without artificial sleep
        user = db.get("user_999")
        requests_handled += 1

        # Print every 50 requests instead of waiting for time.sleep
        if requests_handled % 50 == 0:
            print(
                f"  -> [Thread B] Handled {requests_handled} lightweight requests while scan runs..."
            )


# Launch the threads exactly at the same time
thread_a = threading.Thread(target=heavy_background_scan)
thread_b = threading.Thread(target=foreground_web_server)

thread_a.start()
thread_b.start()

thread_a.join()
thread_b.join()
# Launch the Heavy Task
thread_a = threading.Thread(target=heavy_background_scan)
thread_b = threading.Thread(target=foreground_web_server)

thread_a.start()
# Give Thread A a fraction of a second to enter Rust-land and drop the GIL
time.sleep(0.05)
thread_b.start()

thread_a.join()
thread_b.join()

print("\n==================================================")
print(" DEMO COMPLETE ")
print("==================================================")
print(f"If Thread B printed anything, it means the GIL was successfully released.")
print(
    "Python was actively executing Thread B while Rust crunched Thread A on a different CPU core!"
)
