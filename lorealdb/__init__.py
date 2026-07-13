# lorealdb/__init__.py

# This imports the compiled Rust extension so it acts as the Python module.
from .lorealdb import DBEngine, DBEngineWriteOptimized

__all__ = ["DBEngine", "DBEngineWriteOptimized"]
