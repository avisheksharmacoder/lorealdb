use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use redb::{Database, TableDefinition};
use std::collections::HashMap;
use std::sync::Arc;

// Define the key table and log table for storing logs.
// TableDefinition<K, V>, K is key, V is value. &str is ref to str, &[u8] has ref + size
// We dont need metadata tables, as this is focussed on just db writes.
const RAW_DOCUMENTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raw_documents");

// create the db engine.
#[pyclass]
pub struct DBEngineWriteOptimized {
    db: Arc<Database>,
}

// implement the methods.
#[pymethods]

impl DBEngineWriteOptimized {
    #[new]
    // Create the tables if they don't exist.
    pub fn new(path: &str) -> PyResult<Self> {
        // create the database object.
        let db = Database::create(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // open a write transaction and create the tables if they dont exist.
        // once created, commit the changes.
        let write_txn = db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        {
            write_txn
                .open_table(RAW_DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // return Self inside Ok()
        Ok(Self { db: Arc::new(db) })
    }

    // function to insert already validated single raw JSON bytes
    pub fn insert_raw(&self, id: &str, payload: &[u8]) -> PyResult<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            let mut raw_documents_table = write_txn
                .open_table(RAW_DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            raw_documents_table
                .insert(id, payload)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }

        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    // function to insert already validated many JSON bytes.
    pub fn insert_many_raw(&self, records: Vec<(String, Vec<u8>)>) -> PyResult<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            let mut raw_documents_table = write_txn
                .open_table(RAW_DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            for (id, payload) in records {
                raw_documents_table
                    .insert(id.as_str(), payload.as_slice())
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            }
        }

        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn get<'py>(&self, py: Python<'py>, id: &str) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let raw_documents_table = read_txn
            .open_table(RAW_DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        if let Some(access_guard) = raw_documents_table
            .get(id)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        {
            return Ok(Some(PyBytes::new_bound(py, access_guard.value())));
        } else {
            Ok(None)
        }
    }

    // Get all recoords from the documents table in a single call, using Rust Hashmap.
    // Hashmap helps us to map to a python dictionary directly.
    pub fn get_many<'py>(
        &self,
        py: Python<'py>,
        ids: Vec<String>,
    ) -> PyResult<HashMap<String, Option<Bound<'py, PyBytes>>>> {
        // create a read transaction.
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        //  Fetch the data of the Documents table.
        let raw_documents_table = read_txn
            .open_table(RAW_DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Preallocate the hashmap data capacity to prevent reallocation overhead.
        let mut raw_document_results = HashMap::with_capacity(ids.len());

        // populate the hashmap with the result items.
        for id in ids {
            if let Some(access_guard) = raw_documents_table
                .get(id.as_str())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            {
                // add the id and access_guard value, if found from the table.
                raw_document_results.insert(id, Some(PyBytes::new_bound(py, access_guard.value())));
            } else {
                // add the id and None.
                raw_document_results.insert(id, None);
            }
        }

        Ok(raw_document_results)
    }

    pub fn delete(&self, id: &str) -> PyResult<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let record_existed = {
            let mut raw_documents_table = write_txn
                .open_table(RAW_DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let removed_record = raw_documents_table
                .remove(id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            removed_record.is_some()
        };
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(record_existed)
    }

    // Prefix scanning for Document ID.
    // Scan IDs of the all the records where the id starts with a prefix string.
    // Return a dictionary mapping the id to its raw bytes.
    pub fn scan_prefix<'py>(
        &self,
        py: Python<'py>,
        prefix: &str,
    ) -> PyResult<HashMap<String, Bound<'py, PyBytes>>> {
        // Create a read transaction from the DB Engine.
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Get the Documents table.
        let raw_document_table = read_txn
            .open_table(RAW_DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // create a hashmap to return the results into.
        let mut raw_document_results = HashMap::new();

        // create an iterator starting from the prefix to the end of the db
        // use range to create the iterator.
        let table_iterator = raw_document_table
            .range(prefix..)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        for item in table_iterator {
            let (key_guard, value_guard) =
                item.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let current_key = key_guard.value();

            // in redb, keyguards are sorted alphabetically.
            // if we do not find a key, that means there are no keys that match the prefix.
            // So from here, we break out to save CPU.
            if !current_key.starts_with(prefix) {
                break;
            }

            // if we find keys, insert it into the hashmap, the id and the bytes.
            raw_document_results.insert(
                current_key.to_string(),
                PyBytes::new_bound(py, value_guard.value()),
            );
        }
        Ok(raw_document_results)
    }
}
