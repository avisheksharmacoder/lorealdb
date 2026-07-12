use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use redb::{Database, MultimapTableDefinition, TableDefinition};
use simd_json::prelude::*;
use simd_json::OwnedValue;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

// Define the key table and log table for storing logs.
// TableDefinition<K, V>, K is key, V is value. &str is ref to str, &[u8] has ref + size
const DOCUMENTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");

// create the Multi map table definition for faster Metadata filtering.
const METADATA_TABLE: MultimapTableDefinition<&str, &str> =
    MultimapTableDefinition::new("metadata_index");

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
                .open_multimap_table(METADATA_TABLE)
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
        let _parsed_json: OwnedValue = simd_json::to_owned_value(&mut buffer).map_err(|e| {
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

            // open metadata index table as well, to optimize read for future.
            let mut metadata_index_table = write_txn
                .open_multimap_table(METADATA_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // re borrow the mutated payload variable, as an immutable
            // for redb function.
            tickets_table
                .insert(id, &*payload)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // we have to iterate through the JSON string and index all the string values
            // into the metadata index table.
            if let Some(json_object) = _parsed_json.as_object() {
                for (key, value) in json_object {
                    // for now index flat strings.
                    if let Some(value_str) = value.as_str() {
                        let key_value = format!("{}:{}", key, value_str);

                        // insert the record into the metadata index table.
                        // store in the key:Value : id format.
                        metadata_index_table
                            .insert(key_value.as_str(), id)
                            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                    }
                }
            }
        }

        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(())
    }

    // Insert many records into the Documents Table.
    pub fn insert_many(&self, records: Vec<(String, Vec<u8>)>) -> PyResult<()> {
        // create a mutable object to store the validated records, during the simd validation.
        // This is done to save the parsed json for metadata filtering later.
        let mut parsed_json_objects: Vec<(String, Vec<u8>, OwnedValue)> =
            Vec::with_capacity(records.len());

        // Validate the entire batch data, before we try to open a DB Transaction.
        // if one of those item is not validated, we skip the insert.
        for (id, payload) in records {
            // create a mutable vector for simd_json to use.
            let mut buffer: Vec<u8> = payload.to_vec();

            let _parsed_json = simd_json::to_owned_value(&mut buffer).map_err(|e| {
                PyRuntimeError::new_err(format!("Invalid json in batch for id {}: {}", id, e))
            })?;

            // insert the validated json into the mutable vector object.
            parsed_json_objects.push((id, payload, _parsed_json));
        }

        // Write to disk, if all the items of the batch data are valid.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            // fetch the documents table.
            let mut tickets_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // fetch the metadata index table as well, to optimize read operations for future.
            let mut metadata_index_table = write_txn
                .open_multimap_table(METADATA_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            for (id, payload, parsed_json) in parsed_json_objects {
                tickets_table
                    .insert(id.as_str(), payload.as_slice())
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                // we have to iterate through the JSON string from the validated_record object and
                // index all the string values into the metadata index table.
                if let Some(json_object) = parsed_json.as_object() {
                    for (key, value) in json_object {
                        // for now index flat strings.
                        if let Some(value_str) = value.as_str() {
                            let key_value = format!("{}:{}", key, value_str);

                            // insert the record into the metadata index table.
                            // store in the key:Value : id format.
                            metadata_index_table
                                .insert(key_value.as_str(), id.as_str())
                                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                        }
                    }
                }
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
        let tickets_table = read_txn
            .open_table(DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Preallocate the hashmap data capacity to prevent reallocation overhead.
        let mut document_results = HashMap::with_capacity(ids.len());

        // populate the hashmap with the result items.
        for id in ids {
            if let Some(access_guard) = tickets_table
                .get(id.as_str())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            {
                // add the id and access_guard value, if found from the table.
                document_results.insert(id, Some(PyBytes::new_bound(py, access_guard.value())));
            } else {
                // add the id and None.
                document_results.insert(id, None);
            }
        }

        Ok(document_results)
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
        let document_table = read_txn
            .open_table(DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // create a hashmap to return the results into.
        let mut results = HashMap::new();

        // create an iterator starting from the prefix to the end of the db
        // use range to create the iterator.
        let table_iterator = document_table
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
            results.insert(
                current_key.to_string(),
                PyBytes::new_bound(py, value_guard.value()),
            );
        }
        Ok(results)
    }

    // Function to implement basic metadata filtering.
    pub fn filter_by_metadata<'py>(
        &self,
        py: Python<'py>,
        index_key: &str,
        index_value: &str,
    ) -> PyResult<HashMap<String, Bound<'py, PyBytes>>> {
        // create a read transaction.
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // open the documents table.
        let documents_table = read_txn
            .open_table(DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // open the metadata index table for storing the fetched key and value pairs
        // from Python.
        let metadata_index_table = read_txn
            .open_multimap_table(METADATA_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // format the search term into key:value format.
        let search_term_formatted = format!("{}:{}", index_key, index_value);

        // create the results hashmap to send to Python after data processing.
        let mut results = HashMap::new();

        // fetch all the matching document IDs from the index table, which needs to be implemented at
        // insert time for the documents table. This introduces a write time slowness but this is a Read
        // Optimized DB Engine.
        let matching_id_iterator = metadata_index_table
            .get(search_term_formatted.as_str())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // fetch records that we actually need to send to Python, from the documents table
        // using the iterator created from the metadata index table.
        for document_id in matching_id_iterator {
            let id_guard = document_id.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let doc_id = id_guard.value();

            // now fetch the document from the documents table using the guard pattern.
            if let Some(doc_guard) = documents_table
                .get(doc_id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            {
                results.insert(
                    doc_id.to_string(),
                    PyBytes::new_bound(py, doc_guard.value()),
                );
            }
        }
        Ok(results)
    }
}

#[pymodule]
fn lorealdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = m.add_class::<DBEngine>();
    Ok(())
}
