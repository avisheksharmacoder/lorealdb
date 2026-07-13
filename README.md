# lorealdb

`lorealdb` is an ultra-fast, local-first key-value database built in Rust and designed for Python. It stores data safely inside a single file on your computer, making it perfect for caching, local AI workloads, and simple data persistence without the overhead of setting up a heavy database server.

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

## Quick Start (For Everyone)

If you write basic Python scripts and want to store text, configurations, or JSON data, use the standard `DBEngine`.

```python
import json
from lorealdb import DBEngine

# 1. Connect to your database (it creates the file automatically)
db = DBEngine("my_data.redb")

# 2. Saving a simple text string
# (Note: Data must be converted to bytes using .encode())
db.insert("user_101", "Avishek".encode("utf-8"))

# 3. Saving a complex Python dictionary (JSON)
app_settings = {"theme": "dark", "notifications": True, "version": 1.2}
json_bytes = json.dumps(app_settings).encode("utf-8")
db.insert("settings", json_bytes)

# 4. Reading data back
name_bytes = db.get("user_101")
if name_bytes:
    print(f"User Name: {name_bytes.decode('utf-8')}")

# 5. Reading and parsing JSON data back
settings_bytes = db.get("settings")
if settings_bytes:
    settings = json.loads(settings_bytes.decode("utf-8"))
    print(f"App Theme: {settings['theme']}")

# 6. Deleting a record
db.delete("user_101")