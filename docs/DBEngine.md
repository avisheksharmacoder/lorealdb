# LorealDB `DBEngine` Python API Reference

The `DBEngine` is a high-performance, read-optimized database engine built in Rust and exposed to Python. It stores JSON documents as raw bytes and automatically creates a deep metadata index to allow for fast querying and filtering.

> **Note on Data Types:**
> The engine handles serialization and deserialization entirely in Rust. You can pass native Python dictionaries (or raw JSON strings using specific methods) directly to the API. When reading data, the engine automatically returns native Python dictionaries.

# Architecture of `DBEngine`

The `DBEngine` class is highly optimized out of the box. For embedded use cases, this is the only engine you will ever need. We stripped away all the complex configurations, technical nuances, and hectic installation processes typical of other Rust-native document databases.

_Fun fact:_ Lorealdb is Python-native first. The database engine is designed to abstract almost every technical hurdle you hate dealing with when writing applications.

## How data storage happens under the hood

To keep your Python application running at maximum speed, `DBEngine` separates the work you do from the work the disk does:

1. **Serialization:** You send a Python dictionary or a JSON string to `DBEngine`.
2. **Fast Validation:** It converts the payload to bytes and validates it at extreme speeds using `simdjson`.
3. **Non-blocking Handoff:** It immediately pushes the payload to a thread-safe background queue and gives control back to your Python app. **(Your app never waits for disk I/O).**
4. **Micro-batching:** Every 10 milliseconds, a dedicated Rust background worker drains the queue.
5. **Storage & Indexing:** The worker stores the raw bytes in a document table and automatically flattens/indexes every field into a metadata table for instant lookups.
6. **Reset:** The queue is cleared and ready for the next spike in workloads.

## Architectural Shifts (For the AI Systems)

If you are building high-throughput systems—like an AI-native backend juggling heavy JSON payloads and agentic workflows—you already know the standard bottlenecks. Scaling normally requires deploying Redis for queuing, spinning up Celery workers for background writes, and performing a delicate dance with `asyncio` to prevent disk I/O from locking the main thread.

`DBEngine` shifts that entire infrastructure topology directly into the PyO3 C-extension layer:

### 1. Zero-Infrastructure Load Balancing (Spike Absorption)

You do not need an external message broker to handle write-heavy spikes. The engine implements a bounded Rust `crossbeam` channel (capacity: 10,000) entirely in memory. If your API endpoints get hit with a sudden burst of traffic, the channel acts as a built-in shock absorber. The Rust background thread drains it at a steady state (batching every 10ms), protecting the underlying disk from I/O thrashing without requiring a separate load-balancing service.

### 2. Eliminating the `asyncio` Event Loop Tax

Python's `asyncio` is excellent for network calls, but local disk I/O is notoriously hostile to the event loop. By exposing synchronous Python methods (`def insert`) that instantly hand off memory to a Rust thread, `DBEngine` bypasses the need for asynchronous function coloring entirely. You achieve the non-blocking performance of `async` without writing a single `await`, completely eliminating the overhead of routing disk operations through `AnyIO` threadpools.

### 3. Starlette Threadpool Synergy (GIL-Free Reads)

When you define synchronous endpoints (`def`) in FastAPI, Starlette dispatches them to a worker threadpool. In a standard database setup, the Python Global Interpreter Lock (GIL) creates a bottleneck, forcing those threads to wait in line during reads. `DBEngine` explicitly calls `py.detach()` on all read operations, dropping the GIL before touching the `redb` storage engine. This allows your FastAPI workers to execute concurrent database reads simultaneously at bare-metal Rust speeds.

## Two Rules for DBEngine

Because writes happen in the background to keep your app fast, you must remember two things:

- **Writes take ~10ms:** If you insert a document and immediately try to fetch it on the very next line of code, it might not be in the database yet.
- **Always close the engine:** You **must** call `engine.close_engine()` before your application shuts down. If your script exits abruptly, any data sitting in the queue waiting for that 10ms window will be lost.

## What this means for you as a Python developer

1. **Store data at blazing non-blocking speeds:** No `async`/`await` required, the background thread handles the heavy lifting.
2. **Retrieve data with true multi-threading:** The Python GIL is released during reads, allowing high-throughput frameworks to stay concurrent.

```python
from lorealdb import DBEngine

# 1. Initialize
engine = DBEngine("my_local_db.redb")

# 2. Store (Non-blocking)
engine.insert(id="usr_123", payload={"name": "Alice", "role": "admin"})

# 3. Retrieve (GIL released)
user = engine.get("usr_123")

# 4. Always close before exiting!
engine.close_engine()

```

## Initialization

### `DBEngine(path)`

Creates a new database or opens an existing one at the specified file path.

- **Arguments:**
  - `path` (str): The file path where the database will be stored (e.g., `"my_database.redb"`).

- **Returns:** A `DBEngine` instance.

```python
from lorealdb import DBEngine

# Initialize or open the database
db = DBEngine("my_database.redb")
```

---

## Write Operations

### `insert(id, payload)`

Serializes and inserts a single Python dictionary into the database. It recursively traverses the dictionary to automatically index nested keys and values for fast metadata filtering.

- **Arguments:**
- `id` (str): The unique identifier for the document.
- `payload` (dict): The data payload as a native Python dictionary.

- **Returns:** `None` (Raises a `RuntimeError` if serialization fails or the write queue is full).

```python
data = {"name": "Alice", "role": "admin"}
db.insert("doc_1", data)

```

### `insert_json(id, json_payload)`

Validates, parses, and inserts a raw JSON string. This is highly optimized for inserting data from frameworks that already output JSON strings (like Pydantic's `model_dump_json()`).

- **Arguments:**
- `id` (str): The unique identifier for the document.
- `json_payload` (str): A valid JSON string.

- **Returns:** `None` (Raises a `RuntimeError` if the JSON is invalid).

```python
import json
json_string = json.dumps({"name": "David", "role": "guest"})
db.insert_json("doc_2", json_string)

```

### `insert_many(records)`

Validates and inserts multiple dictionary documents in a single transaction. If _any_ document in the batch fails serialization, the entire batch will raise an error and will not be inserted.

- **Arguments:**
- `records` (list of tuples): A list containing tuples of `(id: str, payload: dict)`.

- **Returns:** `None`

```python
batch_data = [
    ("doc_3", {"name": "Bob", "role": "user"}),
    ("doc_4", {"name": "Charlie", "role": "editor"})
]
db.insert_many(batch_data)

```

### `insert_many_json(records)`

Validates and inserts multiple raw JSON strings in a single batch transaction.

- **Arguments:**
- `records` (list of tuples): A list containing tuples of `(id: str, json_payload: str)`.

- **Returns:** `None`

```python
batch_json = [
    ("doc_5", '{"name": "Eve", "role": "admin"}'),
    ("doc_6", '{"name": "Frank", "role": "user"}')
]
db.insert_many_json(batch_json)

```

### `upsert(id, payload)`

Updates an existing document or inserts it if it doesn't exist. It safely removes the old metadata index links and creates new ones based on the updated dictionary.

- **Arguments:**
- `id` (str): The unique identifier for the document.
- `payload` (dict): The new data payload as a native Python dictionary.

- **Returns:** `None`

```python
updated_data = {"name": "Alice", "role": "super_admin"}
db.upsert("doc_1", updated_data)

```

### `delete(id)`

Removes a document from the database using its ID and purges all associated metadata indexes.

- **Arguments:**
- `id` (str): The unique identifier of the document to delete.

- **Returns:** `None`

```python
db.delete("doc_4")

```

---

## Read Operations

### `get(id)`

Retrieves a single document by its ID and deserializes it directly into a Python dictionary.

- **Arguments:**
- `id` (str): The unique identifier for the document.

- **Returns:** `dict` if the document is found, or `None` if it does not exist.

```python
result = db.get("doc_1")
if result:
    # result is already a standard Python dictionary
    print(result["name"])

```

### `get_many(ids)`

Retrieves multiple documents in a single read transaction.

- **Arguments:**
- `ids` (list of str): A list of document IDs to fetch.

- **Returns:** `list of tuples` - A list containing `(id, dict_or_none)` pairs.

```python
results = db.get_many(["doc_1", "doc_3", "missing_doc"])

for doc_id, data in results:
    if data:
        print(f"{doc_id}: {data}")
    else:
        print(f"{doc_id} not found.")

```

### `scan_prefix(prefix)`

Scans and retrieves all documents whose IDs start with a specific string prefix.

- **Arguments:**
- `prefix` (str): The string prefix to search for.

- **Returns:** `list of tuples` - A list containing `(id, dict)` pairs for all matching documents.

```python
db.insert("user:1", {"name": "Alice"})
db.insert("user:2", {"name": "Bob"})

# Fetch all users
users = db.scan_prefix("user:")
for doc_id, data in users:
    print(doc_id, data)

```

### `filter_by_metadata(index_key, index_value)`

Rapidly fetches documents based on deep or top-level JSON key-value pairs, utilizing the background multimap index. Because of the deep traversal, you can query nested keys using dot notation (e.g., `user.profile.role`).

- **Arguments:**
- `index_key` (str): The JSON key you are searching for (can use dot-notation for nested fields).
- `index_value` (str): The exact string value associated with that key.

- **Returns:** `list of tuples` - A list containing `(id, dict)` pairs for all matching documents.

```python
# Insert records with roles
db.insert("doc_10", {"user": {"name": "Grace", "role": "admin"}})
db.insert("doc_11", {"user": {"name": "Heidi", "role": "user"}})

# Find all documents where the nested "user.role" is "admin"
admins = db.filter_by_metadata("user.role", "admin")

for doc_id, data in admins:
    print(f"Found admin {doc_id}: {data}")

```
