use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use redb::{Database, TableDefinition};
use simd_json::OwnedValue;
use std::sync::Arc;

// Define the key table and log table for storing logs.
// TableDefinition<K, V>, K is key, V is value. &str is ref to str, &[u8] has ref + size
const DOCUMENTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");
const LOGS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("logs");

// create the db engine.
#[pyclass]
pub struct DBEngine {
    db: Arc<Database>,
}

#[pymethods]
impl DBEngine {
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
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            write_txn
                .open_table(LOGS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // return Self inside Ok()
        Ok(Self { db: Arc::new(db) })
    }

    // Insert a new record into the Documents Table.
    pub fn insert(&self, id: &str, payload: &[u8]) -> PyResult<()> {
        // make a mutable copy for simd_json to parse.
        let mut buffer: Vec<u8> = payload.to_vec();

        // validate and parse JSON data at CPU vector speeds.
        // if json is not valid, raise error to user.
        let _parsed: OwnedValue = simd_json::to_owned_value(&mut buffer).map_err(|e| {
            PyRuntimeError::new_err(format!("Invalid JSON payload for id {}: {}", id, e))
        })?;

        // write to the disk if the json data is only valid.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            let mut tickets_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // re borrow the mutated payload variable, as an immutable
            // for redb function.
            tickets_table
                .insert(id, &*payload)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }

        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(())
    }

    // Insert many records into the Documents Table.
    pub fn insert_many(&self, records: Vec<(String, Vec<u8>)>) -> PyResult<()> {
        // Validate the entire batch data, before we try to open a DB Transaction.
        // if one of those item is not validated, we skip the insert.
        for (id, payload) in &records {
            // create a mutable vector for simd_json to use.
            let mut buffer: Vec<u8> = payload.to_vec();

            simd_json::to_owned_value(&mut buffer).map_err(|e| {
                PyRuntimeError::new_err(format!("Invalid json in batch for id {}: {}", id, e))
            })?;
        }

        // Write to disk, if all the items of the batch data are valid.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            let mut tickets_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            for (id, payload) in records {
                tickets_table
                    .insert(id.as_str(), payload.as_slice())
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            }
        }

        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    // function to insert already validated single raw JSON bytes
    pub fn insert_raw(&self, id: &str, payload: &[u8]) -> PyResult<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            let mut tickets_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            tickets_table
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
            let mut tickets_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            for (id, payload) in records {
                tickets_table
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

        let tickets_table = read_txn
            .open_table(DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        if let Some(access_guard) = tickets_table
            .get(id)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        {
            return Ok(Some(PyBytes::new_bound(py, access_guard.value())));
        } else {
            Ok(None)
        }
    }

    pub fn delete(&self, id: &str) -> PyResult<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let record_existed = {
            let mut tickets_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let removed_record = tickets_table
                .remove(id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            removed_record.is_some()
        };
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(record_existed)
    }
}

#[pymodule]
fn lorealdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DBEngine>();
    Ok(())
}
