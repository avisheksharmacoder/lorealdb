# LorealDB: Open-Source High-Performance Embedded Python Database Engine

**LorealDB** is an open-source, high-performance, read-optimized embedded database engine built using Redb, Rust and exposed natively to Python. It stores JSON documents as raw bytes and automatically creates a background metadata index, allowing for lightning-fast querying and filtering without the overhead of heavy runtime parsing.

## Features
* **Dead Simple:** No configuration, no servers, no ports. Just point it to a file path and start saving data.
* **Blazing Fast:** Built on a high-performance Rust core to read and write data at the physical limit of your SSD.
* **Crash Safe:** Your data is protected by strict transactional safety. If your script crashes, your data remains uncorrupted.

---

## Installation

Install the package using your favorite Python package manager:

`pip install lorealdb`

*Or if you prefer using `uv`:*

`uv pip install lorealdb`

---

## Quick Start

If you write basic Python scripts and want to store and query JSON data, use the standard `DBEngine`. 

> **Note:** `DBEngine` is read-optimized and strictly requires JSON data in bytes to automatically build a high-speed metadata index in the background.

```python
import json
from lorealdb import DBEngine

# 1. Connect to your database (it creates the file automatically)
db = DBEngine("my_data.redb")

# 2. Saving a complex Python dictionary (JSON)
# Data must be converted to JSON and then encoded to bytes.
app_settings = {"theme": "dark", "notifications": True, "version": 1.2}
json_bytes = json.dumps(app_settings).encode("utf-8")
db.insert("settings", json_bytes)

# 3. Reading and parsing JSON data back
settings_bytes = db.get("settings")
if settings_bytes:
    settings = json.loads(settings_bytes.decode("utf-8"))
    print(f"App Theme: {settings['theme']}")

# 4. Deleting a record
db.delete("settings")

```

# LorealDB `DBEngine` Python API Reference

The `DBEngine` is a high-performance, read-optimized database engine built in Rust and exposed to Python. It stores JSON documents as raw bytes and automatically creates a metadata index to allow for fast querying and filtering.

> **Note on Data Types:** > Because the engine processes data at high speeds using Rust's memory constructs, all JSON payloads must be passed as Python **bytes** (e.g., `json.dumps(data).encode('utf-8')`).

## Initialization

### `DBEngine(path)`

Creates a new database or opens an existing one at the specified file path.

* **Arguments:**
* `path` (str): The file path where the database will be stored (e.g., `"my_database.redb"`).


* **Returns:** A `DBEngine` instance.

```python
from lorealdb import DBEngine

# Initialize or open the database
db = DBEngine("my_database.redb")

```

---

## Write Operations

### `insert(id, payload)`

Parses, validates, and inserts a single JSON document into the database. It automatically indexes top-level flat string values for fast metadata filtering.

* **Arguments:**
* `id` (str): The unique identifier for the document.
* `payload` (bytes): The JSON data encoded as bytes.


* **Returns:** `None` (Raises a `RuntimeError` if the JSON is invalid).

```python
import json

data = {"name": "Alice", "role": "admin"}
payload_bytes = json.dumps(data).encode('utf-8')

db.insert("doc_1", payload_bytes)

```

### `insert_many(records)`

Validates and inserts multiple JSON documents in a single transaction. If *any* document in the batch contains invalid JSON, the entire batch will raise an error and will not be inserted.

* **Arguments:**
* `records` (list of tuples): A list containing tuples of `(id: str, payload: bytes)`.


* **Returns:** `None`

```python
batch_data = [
    ("doc_2", json.dumps({"name": "Bob", "role": "user"}).encode('utf-8')),
    ("doc_3", json.dumps({"name": "Charlie", "role": "editor"}).encode('utf-8'))
]

db.insert_many(batch_data)

```

### `upsert(id, payload)`

Updates an existing document or inserts it if it doesn't exist. It safely removes the old metadata index links and creates new ones based on the updated payload.

* **Arguments:**
* `id` (str): The unique identifier for the document.
* `payload` (bytes): The new JSON data encoded as bytes.


* **Returns:** `None`

```python
updated_data = {"name": "Alice", "role": "super_admin"}
db.upsert("doc_1", json.dumps(updated_data).encode('utf-8'))

```

### `delete(id)`

Removes a document from the database using its ID.

* **Arguments:**
* `id` (str): The unique identifier of the document to delete.


* **Returns:** `bool` - `True` if the record existed and was deleted, `False` if it wasn't found.

```python
was_deleted = db.delete("doc_4")
print(f"Deleted successfully: {was_deleted}")

```

---

## Read Operations

### `get(id)`

Retrieves a single document by its ID.

* **Arguments:**
* `id` (str): The unique identifier for the document.


* **Returns:** `bytes` if the document is found, or `None` if it does not exist.

```python
result = db.get("doc_1")
if result:
    parsed_json = json.loads(result.decode('utf-8'))
    print(parsed_json)

```

### `get_many(ids)`

Retrieves multiple documents in a single transaction, mapping them directly to a Python dictionary.

* **Arguments:**
* `ids` (list of str): A list of document IDs to fetch.


* **Returns:** `dict` - A dictionary mapping the `id` to its `bytes` payload (or `None` if missing).

```python
results = db.get_many(["doc_1", "doc_2", "missing_doc"])

for doc_id, byte_data in results.items():
    if byte_data:
        print(f"{doc_id}: {json.loads(byte_data.decode('utf-8'))}")
    else:
        print(f"{doc_id} not found.")

```

### `scan_prefix(prefix)`

Scans and retrieves all documents whose IDs start with a specific string prefix.

* **Arguments:**
* `prefix` (str): The string prefix to search for.


* **Returns:** `dict` - A dictionary mapping the matching `id`s to their `bytes` payloads.

```python
# Assuming IDs like 'user:1', 'user:2', 'post:1'
db.insert("user:1", b'{"name": "Alice"}')
db.insert("user:2", b'{"name": "Bob"}')

# Fetch all users
users = db.scan_prefix("user:")
for doc_id, data in users.items():
    print(doc_id, json.loads(data.decode('utf-8')))

```

### `filter_by_metadata(index_key, index_value)`

Rapidly fetches documents based on their top-level JSON key-value pairs, utilizing the background metadata index.

* **Arguments:**
* `index_key` (str): The JSON key you are searching for.
* `index_value` (str): The exact string value associated with that key.


* **Returns:** `dict` - A dictionary mapping the matching `id`s to their `bytes` payloads.

```python
# Insert some records with roles
db.insert("doc_10", b'{"name": "Eve", "role": "admin"}')
db.insert("doc_11", b'{"name": "Frank", "role": "admin"}')

# Find all documents where "role" is "admin"
admins = db.filter_by_metadata("role", "admin")

for doc_id, data in admins.items():
    print(f"Found admin {doc_id}: {json.loads(data.decode('utf-8'))}")

```