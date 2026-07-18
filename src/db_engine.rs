use crossbeam_channel::unbounded;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use redb::{Database, MultimapTable, MultimapTableDefinition, ReadableTable, TableDefinition};
use simd_json::prelude::*;
use simd_json::OwnedValue;
use simd_json::StaticNode;
use std::collections::HashMap;

use pythonize::depythonize;
use serde_json::Value;

// import crossbeam-channel's Unbonded and Sender.
use crossbeam_channel::Sender;
use std::sync::Arc;
use std::thread;

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
    let mut update_db = |index_key: String| {
        if is_insert {
            // if is_insert is True, insert the keys into the table.
            let _ = metadata_table.insert(index_key.as_str(), id);
        } else {
            // if is_insert is False, remove the keys from the table.
            let _ = metadata_table.remove(index_key.as_str(), id);
        }
    };

    // We use match value to find the type of json value in a KV pair,
    // and appropriately process the data type.
    match value {
        // if the value of the json doc is a json.
        OwnedValue::Object(obj) => {
            for (json_key, json_value) in obj.iter() {
                let new_prefix = if json_key.is_empty() {
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
                let new_prefix = format!("{}.{}", prefix, element_index);
                // recursively process the json in the array.
                process_json_index(new_prefix, element_value, id, metadata_table, is_insert);
            }
        }

        // if the json value is a String value.
        OwnedValue::String(string_value) => {
            // update_db takes the data, inserts the data into the multimap table
            // if is_insert = True.
            // Else, it will delete the data, if is_insert = False.
            update_db(format!("{}.{}", prefix, string_value));
        }

        // if the json value is a integer.
        OwnedValue::Static(StaticNode::I64(integer_value)) => {
            update_db(format!("{}.{}", prefix, integer_value));
        }

        // if the json value is a Float.
        OwnedValue::Static(StaticNode::F64(float_value)) => {
            update_db(format!("{}.{}", prefix, float_value));
        }

        // if the json value is a slice of bytes.
        OwnedValue::Static(StaticNode::U64(bytes_value)) => {
            update_db(format!("{}.{}", prefix, bytes_value));
        }

        // if the jsob value is a boolean.
        OwnedValue::Static(StaticNode::Bool(bool_value)) => {
            update_db(format!("{}.{}", prefix, bool_value));
        }
        _ => {}
    }
}
// the struct containing id and payload(JSON) to send to the
// cross beam channel for a different worker to handle metadata indexing
// It contains the id as string and the payload a Vector of bytes.
// ...................................................................
// The data needs a struct of its own, as it is crossing boundaries and
// the data needs to be owned first to travel across the channel and thread boundaries.
struct MetadataIndexPayload {
    id: String,
    json_payload_bytes: Vec<u8>,
}

// Define the key table and log table for storing logs.
// TableDefinition<K, V>, K is key, V is value. &str is ref to str, &[u8] has ref + size
const DOCUMENTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");

// create the Multi map table definition for faster Metadata filtering.
const METADATA_TABLE: MultimapTableDefinition<&str, &str> =
    MultimapTableDefinition::new("metadata_index");

// create the Database engine.
// add the sender payload so, the index is sent to a background worker to process it.
// When sending data to the background worker, we need to send it in a vector of payloads,
// whether it is one or many. We then write to the metadata table in one batch and one fsync
// is only required. This takes more memory to hold a large amount of JSON payloads.
#[pyclass]
pub struct DBEngine {
    db: Arc<Database>,
    indexing_transmitter: Sender<Vec<MetadataIndexPayload>>,
}

// A private method that will not be exposed to Python API.
impl DBEngine {
    // this function is used to send the json payload to the metadata filter processing
    // function through the background worker.
    fn dispatch_json_to_worker(&self, json_payload: Vec<MetadataIndexPayload>) -> PyResult<()> {
        // if the payload is empty, we don't send anything to the channel.
        if json_payload.is_empty() {
            return Ok(());
        }

        // send the json payload to the background worker.
        self.indexing_transmitter
            .send(json_payload)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(())
    }
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

        // Create the lock free channel to pass the json data around.
        let (tx, rx) = unbounded::<Vec<MetadataIndexPayload>>();

        // spawn the background worker thread, for processing the metadata of the json separately.
        thread::spawn(move || {
            // rx receiver now returns a vector of json payloads to process.
            while let Ok(mut json_payload) = rx.recv() {
                // we open one write transaction for the entire job, whether it has
                // one json payload or many. Simd json can validate it in memory.
                // Create the write transaction here.
                if let Ok(db_write_trx) = background_worker_db.begin_write() {
                    // Open the metadata documents table for storing json index.
                    // The metadata table is a multimap table.
                    if let Ok(mut metdata_table) = db_write_trx.open_multimap_table(METADATA_TABLE)
                    {
                        // Here, we process json payloads one by one from the vector.
                        for json_doc in &mut json_payload {
                            // We will use simd_json, validate all json before doing a write a commit to the
                            // metadata table.
                            // the mutable json bytes is a mutable reference which is 100% owwned in this thread.
                            // We can modify it here as well if needed.
                            match simd_json::to_owned_value(&mut json_doc.json_payload_bytes) {
                                // Call the indexing helper funnction to process the payload.
                                // The function will return the parsed payload here.
                                Ok(parsed_json_payload) => {
                                    // extract all the key hierarchies from the json
                                    process_json_index(
                                        String::new(),
                                        &parsed_json_payload,
                                        &json_doc.id,
                                        &mut metdata_table,
                                        true,
                                    );
                                }
                                // Return error if any error happens.
                                Err(e) => {
                                    println!(
                                        "Metadata indexing failed for {}: {}",
                                        &json_doc.id, e
                                    );
                                }
                            }
                        }
                    }

                    // commit the changes to the metadata indexing table.
                    let _ = db_write_trx.commit();
                }
            }
        });

        // return Self inside Ok()
        Ok(Self {
            db: db_arc,
            indexing_transmitter: tx,
        })
    }

    // Insert a new record into the Documents Table.
    // for single insert, we dont need to send the json payload to buffer channel
    // as the I/O time is big enough to negate the small indexing time.
    pub fn insert<'py>(
        &self,
        _py: Python<'py>,
        id: &str,
        dict_payload: Bound<'py, PyDict>,
    ) -> PyResult<()> {
        // Serialize the PyDict payload from Python to rust bytes.
        let dict_rust_value: Value = depythonize(&dict_payload).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "Error converting dict to Rust bytes for {}.{}",
                id, e
            ))
        })?;

        // validate and parse JSON data at CPU vector speeds.
        // if json is not valid, raise error to user.
        let buffer = simd_json::to_vec(&dict_rust_value).map_err(|e| {
            PyRuntimeError::new_err(format!("Invalid json payload for {}.{}", id, e))
        })?;

        // Insert the json payloads to the documents table once they are validated.
        // Create a write transaction for documents table.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            // open the documents table to write the json payload.
            let mut documents_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // insert the json payload.
            documents_table
                .insert(id, buffer.as_slice())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }

        // commit the changes to the documents table.
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Create the metadata index payload and send to the background worker
        // to create the indexes and store in the metadata table.
        // We pass the json bufefgr
        let indexing_json_job = MetadataIndexPayload {
            id: id.to_string(),
            json_payload_bytes: buffer,
        };

        self.dispatch_json_to_worker(vec![indexing_json_job])?;

        Ok(())
    }

    // Insert a new json record into the documents table. The insert_json() method takes a json from python,
    // or pydantic model_dumps() or json() output, converts it into bytes, parses and validates it using
    // simd_json. It saves the record in the DOCUMENTS_TABLE and sends the parsed json to a different background
    // worker to process it and insert into metadata indexing table for fast reads, search.
    pub fn insert_json(&self, id: &str, json_payload: &str) -> PyResult<()> {
        // create a bytes payload for simd_json to validate it in the memory.
        let mut buffer = json_payload.as_bytes().to_vec();

        // validate and parse JSON data at CPU vector speeds.
        // if json is not valid, raise error to user.
        if let Err(e) = simd_json::to_owned_value(&mut buffer) {
            return Err(PyRuntimeError::new_err(format!(
                "Validation error for json payload id {}.{}",
                id, e
            )));
        }

        // Save the json payload to the documents table.
        // open a write txn for the documents table.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            // Get the documents table.
            let mut documents_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // insert the json payload as bytes to the documents table.
            documents_table
                .insert(id, json_payload.as_bytes())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }
        // commit the changes to the database.
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Now, here we trigger the background worker for the metadata processing to store the
        // keys in the indexing table.
        // we have to make a copy of the json bytes from Python, for simd_json to validate. it needs
        // a mutable ref.
        let index_job = MetadataIndexPayload {
            id: id.to_string(),
            json_payload_bytes: json_payload.as_bytes().to_vec(),
        };

        // now send the json payload to the background worker.
        // since the dispatch function now expects a vector of json payloads to process,
        // we send the json payload wrapped into a Vector.
        self.dispatch_json_to_worker(vec![index_job])?;

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
        // create a vector to store all the valid json jobs.
        let mut valid_jobs = Vec::with_capacity(records.len());

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

            // push the validated json payload to the valid_jobs vector.
            valid_jobs.push(MetadataIndexPayload {
                id,
                json_payload_bytes: buffer,
            });
        }

        // Create the write transaction to write the payload to the documents table.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            // get the documents table from database.
            let mut documents_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // insert the payloads one by one.
            for json_data in &valid_jobs {
                documents_table
                    .insert(
                        json_data.id.as_str(),
                        json_data.json_payload_bytes.as_slice(),
                    )
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            }
        }

        // commit the changes to the database.
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // send the json_payload to the background worker.
        self.dispatch_json_to_worker(valid_jobs)?;

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
        // create a vector to store all the valid json bytes.
        let mut valid_jobs = Vec::with_capacity(records.len());

        // Validate the entire batch data, before we try to open a DB Transaction.
        // if one of those item is not validated, we skip the insert and send a error message back to Python.
        for (id, json_payload) in records {
            // convert the json value to a rust bytes.
            let mut buffer = json_payload.into_bytes();

            // validate the json in place, using simd_json.
            if let Err(e) = simd_json::to_owned_value(&mut buffer) {
                return Err(PyRuntimeError::new_err(format!(
                    "Invalid json in payload for id {}.{}",
                    id, e
                )));
            }

            // push the validated json payload to the valid_jobs vector.
            valid_jobs.push(MetadataIndexPayload {
                id,
                json_payload_bytes: buffer,
            });
        }

        // Create the write transaction to write the payload to the documents table.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            // get the documents table from database.
            let mut documents_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // insert the payloads one by one.
            for json_data in &valid_jobs {
                documents_table
                    .insert(
                        json_data.id.as_str(),
                        json_data.json_payload_bytes.as_slice(),
                    )
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            }
        }

        // commit the changes to the database.
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // send the json_payload to the background worker.
        self.dispatch_json_to_worker(valid_jobs)?;

        Ok(())
    }

    // insert method definitions end here. /////////////////////////////////////////////////////////////

    pub fn get<'py>(&self, py: Python<'py>, id: &str) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let documents_table = read_txn
            .open_table(DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        if let Some(access_guard) = documents_table
            .get(id)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        {
            return Ok(Some(PyBytes::new(py, access_guard.value())));
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
        let documents_table = read_txn
            .open_table(DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Preallocate the hashmap data capacity to prevent reallocation overhead.
        let mut document_results = HashMap::with_capacity(ids.len());

        // populate the hashmap with the result items.
        for id in ids {
            if let Some(access_guard) = documents_table
                .get(id.as_str())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            {
                // add the id and access_guard value, if found from the table.
                document_results.insert(id, Some(PyBytes::new(py, access_guard.value())));
            } else {
                // add the id and None.
                document_results.insert(id, None);
            }
        }

        Ok(document_results)
    }

    // Delete the record from the documents table.
    // Also, find all the indexes of the record in the metadata indexing
    // table and delete them in one transaction.
    pub fn delete(&self, id: &str) -> PyResult<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let record_existed = {
            let mut documents_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let mut metadata_indexing_table = write_txn
                .open_multimap_table(METADATA_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let removed_record = documents_table
                .remove(id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // if the document existed, clean up its metadata as well.
            if let Some(record) = removed_record {
                // generate the bytes of the deleted document using simd_json.
                // We need the json bytes to generate the indexes of the document.
                // to delete them from the metadata indexing table.
                let mut buffer = record.value().to_vec();
                if let Ok(parsed_json) = simd_json::to_owned_value(&mut buffer) {
                    // call the process json index function to delete the indexes.
                    process_json_index(
                        String::new(),
                        &parsed_json,
                        id,
                        &mut metadata_indexing_table,
                        false,
                    );
                }
                true
            } else {
                false
            }
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
        let documents_table = read_txn
            .open_table(DOCUMENTS_TABLE)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // create a hashmap to return the results into.
        let mut results = HashMap::new();

        // create an iterator starting from the prefix to the end of the db
        // use range to create the iterator.
        let table_iterator = documents_table
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
                PyBytes::new(py, value_guard.value()),
            );
        }
        Ok(results)
    }

    // Function to implement upsert/update functionality.
    // We need to update a record in the documents table, with a get() and then insert().
    // We also need to update the metadata strings of that record in the metadata index table.
    pub fn upsert<'py>(&self, id: &str, payload: &[u8]) -> PyResult<()> {
        // make a mutable copy for simd_json to parse.
        let mut buffer: Vec<u8> = payload.to_vec();

        // validate and parse JSON data at CPU vector speeds.
        // if json is not valid, raise error to user.
        let new_parsed_json: OwnedValue = simd_json::to_owned_value(&mut buffer).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "Invalid JSON payload for upsert operation, id {}: {}",
                id, e
            ))
        })?;

        // open a write transaction from the DB.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        {
            // Open the documents table and the metadata indexing table as well.
            let mut documents_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let mut metadata_indexing_table = write_txn
                .open_multimap_table(METADATA_TABLE)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // safely extract the old metadata and drop the guard immediately.
            let old_data_opt = documents_table
                .get(id)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
                .map(|guard| guard.value().to_vec());

            // check if an existing record is available or not.
            if let Some(mut old_buffer) = old_data_opt {
                if let Ok(old_parsed_json) = simd_json::to_owned_value(&mut old_buffer) {
                    if let Some(old_json_object) = old_parsed_json.as_object() {
                        for (key, value) in old_json_object {
                            if let Some(value_str) = value.as_str() {
                                let old_key_value = format!("{}:{}", key, value_str);

                                // remove the value for this specific id only from the metadata indexing table.
                                let _ = metadata_indexing_table.remove(old_key_value.as_str(), id);
                            }
                        }
                    }
                }
            }

            // now insert new payload into the table.
            documents_table
                .insert(id, payload)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            // now populate the metadata indexing table with the new parsed json data.
            if let Some(new_json_object) = new_parsed_json.as_object() {
                for (key, value) in new_json_object {
                    if let Some(value_str) = value.as_str() {
                        let new_key_value = format!("{}:{}", key, value_str);

                        // store the new key value and id into metadata indexingg table
                        metadata_indexing_table
                            .insert(new_key_value.as_str(), id)
                            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                    }
                }
            }
        }
        // commit the write transaction.
        write_txn
            .commit()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(())
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
                results.insert(doc_id.to_string(), PyBytes::new(py, doc_guard.value()));
            }
        }
        Ok(results)
    }
}
