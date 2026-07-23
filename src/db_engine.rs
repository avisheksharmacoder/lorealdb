use crossbeam_channel::bounded;
use crossbeam_channel::RecvTimeoutError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use redb::{Database, MultimapTable, MultimapTableDefinition, TableDefinition};
use simd_json::prelude::*;
use simd_json::OwnedValue;
use simd_json::StaticNode;

use pythonize::depythonize;
use serde_json::Value;

// import crossbeam-channel's Unbonded and Sender.
use crossbeam_channel::Sender;
use std::net::Shutdown::Write;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// helper function to index the keys and values of the json payload.
// it helps to deep nest all the fields for easy metadata filtering later.
// Unified helper function to traverse JSON and either insert or remove metadata indexes.
// Pass `is_insert = true` for adding indexes, and `false` for deleting them from the
// metadata indexing table.
fn process_json_index(
    prefix: String,
    value: &OwnedValue,
    id: &str,
    metadata_table: &mut MultimapTable<'_, &'static str, &'static str>,
    is_insert: bool,
) {
    // This is a quick inline closure to handle the DB operation.
    // We dont need to write the if/else block in every single data type match arm.
    let update_db =
        |index_key: String, table: &mut MultimapTable<'_, &'static str, &'static str>| {
            if is_insert {
                // if is_insert is True, insert the keys into the table.
                let _ = table.insert(index_key.as_str(), id);
            } else {
                // if is_insert is False, remove the keys from the table.
                let _ = table.remove(index_key.as_str(), id);
            }
        };

    // We use match value to find the type of json value in a KV pair,
    // and appropriately process the data type.
    match value {
        // if the value of the json doc is a json.
        OwnedValue::Object(obj) => {
            for (json_key, json_value) in obj.iter() {
                let new_prefix = if prefix.is_empty() {
                    json_key.to_string()
                } else {
                    // If json = {"a": "b"}, then prefix key = "a.b"
                    // final map stored in multimap table = "a.b" -> ["id"]
                    format!("{}.{}", prefix, json_key)
                };
                // recursively process the next json, if the value is a json again.
                process_json_index(new_prefix, json_value, id, metadata_table, is_insert);
            }
        }

        // if the json value is an array, we need loop again.
        OwnedValue::Array(arr) => {
            for (element_index, element_value) in arr.iter().enumerate() {
                let new_prefix = if prefix.is_empty() {
                    element_index.to_string()
                } else {
                    format!("{}.{}", prefix, element_index)
                };
                // recursively process the json in the array.
                process_json_index(new_prefix, element_value, id, metadata_table, is_insert);
            }
        }

        // if the json value is a String value.
        OwnedValue::String(string_value) => {
            // update_db takes the data, inserts the data into the multimap table
            // if is_insert = True.
            // Else, it will delete the data, if is_insert = False.
            update_db(format!("{}.{}", prefix, string_value), metadata_table);
        }

        // if the json value is a integer.
        OwnedValue::Static(StaticNode::I64(integer_value)) => {
            update_db(format!("{}.{}", prefix, integer_value), metadata_table);
        }

        // if the json value is a Float.
        OwnedValue::Static(StaticNode::F64(float_value)) => {
            update_db(format!("{}.{}", prefix, float_value), metadata_table);
        }

        // if the json value is a slice of bytes.
        OwnedValue::Static(StaticNode::U64(bytes_value)) => {
            update_db(format!("{}.{}", prefix, bytes_value), metadata_table);
        }

        // if the jsob value is a boolean.
        OwnedValue::Static(StaticNode::Bool(bool_value)) => {
            update_db(format!("{}.{}", prefix, bool_value), metadata_table);
        }
        _ => {}
    }
}

// Define the key table and log table for storing logs.
// TableDefinition<K, V>, K is key, V is value. &str is ref to str, &[u8] has ref + size
const DOCUMENTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");

// create the Multi map table definition for faster Metadata filtering.
const METADATA_TABLE: MultimapTableDefinition<&str, &str> =
    MultimapTableDefinition::new("metadata_index");

// We define the WriteOp Enum.
// This is the universal instruction set from Python to the Master Writer.
// The Shutdown operation to drain the queue before Python shuts down the process.
enum WriteOp {
    Insert { id: String, payload: Vec<u8> },
    Delete { id: String },
    Upsert { id: String, payload: Vec<u8> },
    ShutDown,
}

// create the Database engine.
// add the sender payload so, the index is sent to a background worker to process it.
// When sending data to the background worker, we need to send it in a vector of payloads,
// whether it is one or many. We then write to the metadata table in one batch and one fsync
// is only required. This takes more memory to hold a large amount of JSON payloads.
// We add the WriteOp to the DB Engine to use it.
#[pyclass]
pub struct DBEngine {
    db: Arc<Database>,
    write_txn: Sender<WriteOp>,
    // We add a handle for python to refer to before closing down.
    worker_handle: Option<thread::JoinHandle<()>>,
}

#[pymethods]
impl DBEngine {
    #[new]
    // Create the tables if they don't exist.
    pub fn new(path: &str) -> PyResult<Self> {
        // create the database object.
        let db = Database::create(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // open a write transaction and create the tables if they dont exist.
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

        // once created, commit the changes.
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Create the Arc db pointer for db here.
        let db_arc = Arc::new(db);
        let background_worker_db = db_arc.clone();

        // Create a bounded FIFO channel of size 10,000. If fastapi brings in 100,000K requests
        // the channel will not accept them and go into OOM. It will hold from the fastapi side.
        let (tx, rx) = bounded::<WriteOp>(10000);

        // spawn the background worker thread, for processing the metadata of the json separately.
        // We implement a master write, to handle the disk operations in 3 types.
        let worker_handle = thread::spawn(move || {
            // create the batch time of 10ms.
            let queue_batch_time = Duration::from_millis(10);

            // We pre-allocate the master batch size in the heap, so resizing does not happen
            // at odd times or during spikes with large amount of payloads.
            let mut master_batch = Vec::with_capacity(10000);

            loop {
                // We wait for the first batch of json payloads at 10ms timeout.
                match rx.recv_timeout(queue_batch_time) {
                    Ok(db_op) => {
                        master_batch.push(db_op);

                        // We drain remaning payloads from the queue that may be smaller than 10,000
                        while let Ok(pending_op) = rx.try_recv() {
                            master_batch.push(pending_op);
                            // limit to how many payloads can be put into the master branch.
                            if master_batch.len() >= 10000 {
                                break;
                            }
                        }

                        // Open one write transaction for the entire batch.
                        if let Ok(write_txn) = background_worker_db.begin_write() {
                            // add a shutdown flag.
                            let mut shutdown_signal = false;
                            // Open the metadata and documents table for storing json index.
                            // The metadata table is a multimap table.
                            if let (Ok(mut documents_table), Ok(mut metadata_indexing_table)) = (
                                write_txn.open_table(DOCUMENTS_TABLE),
                                write_txn.open_multimap_table(METADATA_TABLE),
                            ) {
                                for op in master_batch.drain(..) {
                                    match op {
                                        WriteOp::Insert { id, mut payload } => {
                                            let _ = documents_table
                                                .insert(id.as_str(), payload.as_slice());
                                            if let Ok(parsed_json) =
                                                simd_json::to_owned_value(&mut payload)
                                            {
                                                process_json_index(
                                                    String::new(),
                                                    &parsed_json,
                                                    &id,
                                                    &mut metadata_indexing_table,
                                                    true,
                                                );
                                            }
                                        }

                                        WriteOp::Delete { id } => {
                                            if let Ok(Some(db_record)) =
                                                documents_table.remove(id.as_str())
                                            {
                                                let mut old_payload = db_record.value().to_vec();
                                                if let Ok(parsed_json) =
                                                    simd_json::to_owned_value(&mut old_payload)
                                                {
                                                    process_json_index(
                                                        String::new(),
                                                        &parsed_json,
                                                        &id,
                                                        &mut metadata_indexing_table,
                                                        false,
                                                    );
                                                }
                                            }
                                        }

                                        WriteOp::Upsert { id, mut payload } => {
                                            // Upsert follows -> remove old metadata indexes, insert into doc, insert into metadata.
                                            if let Ok(Some(db_record)) =
                                                documents_table.remove(id.as_str())
                                            {
                                                let mut old_payload = db_record.value().to_vec();
                                                if let Ok(parsed_json) =
                                                    simd_json::to_owned_value(&mut old_payload)
                                                {
                                                    process_json_index(
                                                        String::new(),
                                                        &parsed_json,
                                                        &id,
                                                        &mut metadata_indexing_table,
                                                        false,
                                                    );
                                                }
                                            }
                                            // now insert the records again in both tables.
                                            let _ = documents_table
                                                .insert(id.as_str(), payload.as_slice());
                                            if let Ok(parsed_new_json) =
                                                simd_json::to_owned_value(&mut payload)
                                            {
                                                process_json_index(
                                                    String::new(),
                                                    &parsed_new_json,
                                                    &id,
                                                    &mut metadata_indexing_table,
                                                    true,
                                                );
                                            }
                                        }

                                        WriteOp::ShutDown => {
                                            shutdown_signal = true;
                                        }
                                    }
                                }
                            }

                            // commit the changes to the tables.
                            let _ = write_txn.commit();

                            // shutdown here.,
                            if shutdown_signal {
                                break;
                            }
                        }
                        // clearr the master batch vector for the next batch of payloads.
                        master_batch.clear();
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        break;
                    }
                }
            }
        });

        // return Self inside Ok()
        Ok(Self {
            db: db_arc,
            write_txn: tx,
            worker_handle: Some(worker_handle),
        })
    }

    // expose a close function call to python to confirm that all database
    // operations are completed, before the python process is killed.
    pub fn close_engine(&mut self) -> PyResult<()> {
        // send the shutdown
        self.write_txn.send(WriteOp::ShutDown).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to send shutdown signal {}", e))
        })?;

        // take the thread handle, wait for it to finish committing to redb.
        if let Some(handle) = self.worker_handle.take() {
            handle.join().map_err(|_| {
                PyRuntimeError::new_err("Background writer panicked during shut down {}")
            })?;
        }

        Ok(())
    }

    // Insert a new record into the Documents Table.
    // for single insert, we dont need to send the json payload to buffer channel
    // as the I/O time is big enough to negate the small indexing time.
    pub fn insert<'py>(
        &self,
        _py: Python<'py>,
        id: &str,
        payload: Bound<'py, PyDict>,
    ) -> PyResult<()> {
        // Serialize the PyDict payload from Python to rust bytes.
        let dict_rust_value: Value = depythonize(&payload).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "Error converting dict to Rust bytes for {}.{}",
                id, e
            ))
        })?;

        // convert the serde_json value to a rust bytes vector.
        let buffer = serde_json::to_vec(&dict_rust_value).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to encode bytes for {}.{}", id, e))
        })?;

        // create operation for insert and send it to the channel.
        let op = WriteOp::Insert {
            id: id.to_string(),
            payload: buffer,
        };
        self.write_txn
            .send(op)
            .map_err(|e| PyRuntimeError::new_err(format!("Write queue full or closed. {}", e)))?;

        Ok(())
    }

    // Insert a new json record into the documents table. The insert_json() method takes a json from python,
    // or pydantic model_dumps() or json() output, converts it into bytes, parses and validates it using
    // simd_json. It saves the record in the DOCUMENTS_TABLE and sends the parsed json to a different background
    // worker to process it and insert into metadata indexing table for fast reads, search.
    pub fn insert_json(&self, id: &str, json_payload: &str) -> PyResult<()> {
        // validate and parse JSON data using serde_json.
        // if json is not valid, raise error to user.
        if let Err(e) = serde_json::from_str::<serde::de::IgnoredAny>(json_payload) {
            return Err(PyRuntimeError::new_err(format!(
                "Validation error at {}.{}",
                id, e
            )));
        }

        let op = WriteOp::Insert {
            id: id.to_string(),
            payload: json_payload.as_bytes().to_vec(),
        };

        self.write_txn
            .send(op)
            .map_err(|e| PyRuntimeError::new_err(format!("Write queue full or closed. {}", e)))?;

        Ok(())
    }

    // Insert many records into the Documents Table.
    // All records are Pydantic dict, or normal dict from Python.
    // We assume these dictionaries are not validated from user side.
    // The user sends a list of tuples that have id, and its dictionary.
    pub fn insert_many<'py>(
        &self,
        _py: Python<'py>,
        records: Vec<(String, Bound<'py, PyDict>)>,
    ) -> PyResult<()> {
        // Validate the entire batch data, before we try to open a DB Transaction.
        // if one of those item is not validated, we skip the insert.
        for (id, dict_payload) in records {
            // we first need to serialize them from PyDict to rust bytes.
            // The final value here is a rust serde_json value.
            let pydict_rust_bytes: Value = depythonize(&dict_payload).map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to serialize dict for {}.{}", id, e))
            })?;

            // convert the serde_json value to a rust bytes vector.
            let buffer = serde_json::to_vec(&pydict_rust_bytes).map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to encode bytes for {}.{}", id, e))
            })?;

            let op = WriteOp::Insert {
                id: id.to_string(),
                payload: buffer,
            };

            self.write_txn.send(op).map_err(|e| {
                PyRuntimeError::new_err(format!("Write queue full or closed. {}", e))
            })?;
        }

        Ok(())
    }

    // We process a payload of validated or invalidated jsons.
    // These jsons can be a final output of pydantic dumps or dataclasses.
    // We need to validate the json payload irrespective of where it comes from.
    // As a user can send a list of good of bad unformatted json and we dont want our db to crash.

    pub fn insert_many_json<'py>(
        &self,
        _py: Python<'py>,
        records: Vec<(String, String)>,
    ) -> PyResult<()> {
        // Validate the entire batch data, before we try to open a DB Transaction.
        // if one of those item is not validated, we skip the insert and send a error message back to Python.
        for (id, json_payload) in records {
            // validate the json in place, using simd_json.
            if let Err(e) = serde_json::from_str::<serde::de::IgnoredAny>(&json_payload) {
                return Err(PyRuntimeError::new_err(format!(
                    "Invalid json in payload for id {}.{}",
                    id, e
                )));
            }

            let op = WriteOp::Insert {
                id: id.to_string(),
                payload: json_payload.as_bytes().to_vec(),
            };

            self.write_txn.send(op).map_err(|e| {
                PyRuntimeError::new_err(format!("Write queue full or closed. {}", e))
            })?;
        }

        Ok(())
    }

    // insert method definitions end here. /////////////////////////////////////////////////////////////

    pub fn get<'py>(&self, py: Python<'py>, id: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        let db_result: Result<Option<Vec<u8>>, String> = py.detach(|| {
            let read_txn = self.db.begin_read().map_err(|e| e.to_string())?;
            let documents_table = read_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| e.to_string())?;

            if let Ok(Some(access_guard)) = documents_table.get(id).map_err(|e| e.to_string()) {
                Ok(Some(access_guard.value().to_vec()))
            } else {
                Ok(None)
            }
        });
        // Once we have the GIL back, we convert the string error to PyRuntimeError via ?.
        let bytes_option = db_result.map_err(|e| PyRuntimeError::new_err(e))?;
        match bytes_option {
            Some(bytes) => {
                // serialize to json
                let json_value: Value = serde_json::from_slice(&bytes)
                    .map_err(|e| PyRuntimeError::new_err(format!("Json parsing error {}", e)))?;
                // convert to py any type.
                let py_any = pythonize::pythonize(py, &json_value).map_err(|e| {
                    PyRuntimeError::new_err(format!("Error while pythonizing {}", e))
                })?;
                // convert to dict.
                let py_dict = py_any.cast_into::<PyDict>().map_err(|_| {
                    PyRuntimeError::new_err("Database entry is not a valid JSON/Dictionary")
                })?;
                Ok(Some(py_dict))
            }
            None => Ok(None),
        }
    }

    // Get all recoords from the documents table in a single call, using Rust Hashmap.
    // Hashmap helps us to map to a python dictionary directly.
    pub fn get_many<'py>(
        &self,
        py: Python<'py>,
        ids: Vec<String>,
    ) -> PyResult<Vec<(String, Option<Bound<'py, PyDict>>)>> {
        let db_results: Result<Vec<(String, Option<Vec<u8>>)>, String> = py.detach(|| {
            // create a read transaction.
            let read_txn = self.db.begin_read().map_err(|e| e.to_string())?;

            //  Fetch the data of the Documents table.
            let documents_table = read_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| e.to_string())?;

            // Preallocate the hashmap data capacity to prevent reallocation overhead.
            let mut document_results = Vec::with_capacity(ids.len());

            // populate the hashmap with the result items.
            for id in ids {
                if let Some(access_guard) = documents_table
                    .get(id.as_str())
                    .map_err(|e| e.to_string())?
                {
                    // add the id and access_guard value, if found from the table.
                    document_results.push((id, Some(access_guard.value().to_vec())));
                } else {
                    // add the id and None.
                    document_results.push((id, None));
                }
            }

            Ok(document_results)
        });

        // reacquire the GIL and send back the Python objects.
        let final_results = db_results
            .map_err(|e| PyRuntimeError::new_err(e))?
            .into_iter()
            .map(|(id, result_value)| {
                match result_value {
                    Some(bytes) => {
                        // serialize to json
                        let json_value: Value = serde_json::from_slice(&bytes).map_err(|e| {
                            PyRuntimeError::new_err(format!("Json parsing error {}", e))
                        })?;
                        // convert to py any type.
                        let py_any = pythonize::pythonize(py, &json_value).map_err(|e| {
                            PyRuntimeError::new_err(format!("Error while pythonizing {}", e))
                        })?;
                        // convert to dict.
                        let py_dict = py_any.cast_into::<PyDict>().map_err(|_| {
                            PyRuntimeError::new_err("Database entry is not a valid JSON/Dictionary")
                        })?;
                        Ok((id, Some(py_dict)))
                    }
                    None => Ok((id, None)),
                }
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(final_results)
    }

    // Delete the record from the documents table.
    // Also, find all the indexes of the record in the metadata indexing
    // table and delete them in one transaction.
    pub fn delete(&self, id: &str) -> PyResult<()> {
        let op = WriteOp::Delete { id: id.to_string() };
        self.write_txn
            .send(op)
            .map_err(|e| PyRuntimeError::new_err(format!("Write queue full or closed. {}", e)))?;

        Ok(())
    }

    // Prefix scanning for Document ID.
    // Scan IDs of the all the records where the id starts with a prefix string.
    // Return a dictionary mapping the id to its raw bytes.
    pub fn scan_prefix<'py>(
        &self,
        py: Python<'py>,
        prefix: &str,
    ) -> PyResult<Vec<(String, Bound<'py, PyDict>)>> {
        let db_result: Result<Vec<(String, Vec<u8>)>, String> = py.detach(|| {
            // Create a read transaction from the DB Engine.
            let read_txn = self.db.begin_read().map_err(|e| e.to_string())?;

            // Get the Documents table.
            let documents_table = read_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| e.to_string())?;

            // create a hashmap to return the results into.
            let mut results = Vec::new();

            // create an iterator starting from the prefix to the end of the db
            // use range to create the iterator.
            let table_iterator = documents_table.range(prefix..).map_err(|e| e.to_string())?;

            for item in table_iterator {
                let (key_guard, value_guard) = item.map_err(|e| e.to_string())?;
                let current_key = key_guard.value();
                // we can break out if the CPU prefix no longer matches.
                if !current_key.starts_with(prefix) {
                    break;
                }
                results.push((current_key.to_string(), value_guard.value().to_vec()));
            }
            Ok(results)
        });
        // Reacquire GIL and map to pybytes.
        let final_results = db_result
            .map_err(|e| PyRuntimeError::new_err(e))?
            .into_iter()
            .map(|(k, v)| {
                // Deserialzie the bytes to json
                let json_value: Value = serde_json::from_slice(&v)
                    .map_err(|e| PyRuntimeError::new_err(format!("Json Parse Error {}", e)))?;
                // convert to py Any type.
                let py_any = pythonize::pythonize(py, &json_value)
                    .map_err(|e| PyRuntimeError::new_err(format!("Pythonize Error {}", e)))?;
                // convert to a dict.
                let py_dict = py_any.cast_into::<PyDict>().map_err(|_| {
                    PyRuntimeError::new_err("Database entry is not a valid JSON/Dictionary")
                })?;
                Ok((k, py_dict))
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(final_results)
    }

    // Function to implement upsert/update functionality.
    // We need to update a record in the documents table, with a get() and then insert().
    // We also need to update the metadata strings of that record in the metadata index table.
    pub fn upsert<'py>(&self, id: &str, payload: Bound<'py, PyDict>) -> PyResult<()> {
        // Serialize the PyDict payload from Python to a rust serde_json Value.
        let dict_rust_value: Value = depythonize(&payload).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "Error converting dict to Rust bytes for {}.{}",
                id, e
            ))
        })?;

        // Convert the serde_json value to a rust bytes vector.
        let buffer = serde_json::to_vec(&dict_rust_value).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to encode bytes for {}.{}", id, e))
        })?;

        let op = WriteOp::Upsert {
            id: id.to_string(),
            payload: buffer,
        };
        self.write_txn
            .send(op)
            .map_err(|e| PyRuntimeError::new_err(format!("Write queue full or closed. {}", e)))?;

        Ok(())
    }

    // Function to implement basic metadata filtering.
    pub fn filter_by_metadata<'py>(
        &self,
        py: Python<'py>,
        index_key: &str,
        index_value: &str,
    ) -> PyResult<Vec<(String, Bound<'py, PyDict>)>> {
        // format the search term into key:value format.
        let search_term_formatted = format!("{}.{}", index_key, index_value);

        let db_result: Result<Vec<(String, Vec<u8>)>, String> = py.detach(|| {
            let read_txn = self.db.begin_read().map_err(|e| e.to_string())?;

            // open the documents table.
            let documents_table = read_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| e.to_string())?;

            // open the metadata index table for storing the fetched key and value pairs
            // from Python.
            let metadata_index_table = read_txn
                .open_multimap_table(METADATA_TABLE)
                .map_err(|e| e.to_string())?;

            // create the results hashmap to send to Python after data processing.
            let mut results = Vec::new();

            // fetch all the matching document IDs from the index table, which needs to be implemented at
            // insert time for the documents table. This introduces a write time slowness but this is a Read
            // Optimized DB Engine.
            let matching_id_iterator = metadata_index_table
                .get(search_term_formatted.as_str())
                .map_err(|e| e.to_string())?;

            for document_id in matching_id_iterator {
                let id_guard = document_id.map_err(|e| e.to_string())?;
                let doc_id = id_guard.value();

                if let Some(doc_guard) = documents_table.get(doc_id).map_err(|e| e.to_string())? {
                    results.push((doc_id.to_string(), doc_guard.value().to_vec()));
                }
            }
            Ok(results)
        });
        let fiinal_results = db_result
            .map_err(|e| PyRuntimeError::new_err(e))?
            .into_iter()
            .map(|(k, v)| {
                // Deserialzie the bytes to json
                let json_value: Value = serde_json::from_slice(&v)
                    .map_err(|e| PyRuntimeError::new_err(format!("Json Parse Error {}", e)))?;
                // convert to py Any type.
                let py_any = pythonize::pythonize(py, &json_value)
                    .map_err(|e| PyRuntimeError::new_err(format!("Pythonize Error {}", e)))?;
                // convert to a dict.
                let py_dict = py_any.cast_into::<PyDict>().map_err(|_| {
                    PyRuntimeError::new_err("Database entry is not a valid JSON/Dictionary")
                })?;
                Ok((k, py_dict))
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(fiinal_results)
    }
}
