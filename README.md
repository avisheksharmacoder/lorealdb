# LorealDB: Open-Source High-Performance Embedded Python Database Engine

**LorealDB** is an open-source, high-performance, read-optimized embedded database engine built using [Redb](https://github.com/cberner/redb), [simd json](https://github.com/simdjson/simdjson), [crossbeam channel](https://github.com/crossbeam-rs/crossbeam-channel), Rust and exposed natively to Python. It stores JSON documents as raw bytes and automatically creates a background metadata index, allowing for lightning-fast querying and filtering without the overhead of heavy runtime parsing.

### Not just another rust KV store wrapper.

**Lorealdb is created to end your data storage headache with just one single, simple, highly optimized, and highly rustified tool.**

## ✨ Features

- **Dead Simple:** No configuration, no servers, no ports. Just point it to a file path and start saving data.
- **Blazing Fast I/O:** Built natively in 100% Rust for zero-cost abstraction.
- **GIL-Free Reads:** Database reads release the Global Interpreter Lock, allowing true multi-threading in Python.
- **Asynchronous Writes:** Upserts are pushed to a high-speed Rust MPSC channel, never blocking your Python thread.
- **Auto batching:** Database operations are automatically executed at every 10ms from the MPSC channel.
- **SIMD-accelerated validation:** JSON data is validated at CPU vector speeds.
- **Native Types:** Pass Python `dict` objects in, get Python `dict` objects back. Rust handles the serialization invisibly.
- **Prefix Scanning:** Efficiently query ranges of documents using key prefixes.
- **Batch Operations:** Fetch hundreds of keys at once with zero order-scrambling and memory-safe pre-allocation.

---

## Installation

Install the package using your favorite Python package manager:

`pip install lorealdb`

_Or if you prefer using `uv`:_

`uv pip install lorealdb`

---

> **Note:** `DBEngine` is read-optimized. It natively accepts standard Python dictionaries, transparently converting them to raw bytes to automatically build a high-speed metadata index in the background.

## Full API Reference for [DBEngine](docs/DBEngine.md)

---

## Quick Start

```python
from lorealdb import DBEngine

# 1. Connect to your database (it creates the file automatically)
db = DBEngine("my_data.redb")

# 2. Saving a complex Python dictionary
# The Rust engine automatically handles serialization natively.
app_settings = {"theme": "dark", "notifications": True, "version": 1.2}
db.insert("settings", app_settings)

# 3. Reading the data back
# The engine automatically deserializes the bytes directly into a Python dictionary.
settings = db.get("settings")
if settings:
    print(f"App Theme: {settings['theme']}")

# 4. Deleting a record
db.delete("settings")
```
