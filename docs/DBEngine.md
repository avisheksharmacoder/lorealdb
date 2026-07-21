# LorealDB `DBEngine` Python API Reference

The `DBEngine` is a high-performance, read-optimized database engine built in Rust and exposed to Python. It stores JSON documents as raw bytes and automatically creates a deep metadata index to allow for fast querying and filtering.

> **Note on Data Types:**
> The engine handles serialization and deserialization entirely in Rust. You can pass native Python dictionaries (or raw JSON strings using specific methods) directly to the API. When reading data, the engine automatically returns native Python dictionaries.

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
