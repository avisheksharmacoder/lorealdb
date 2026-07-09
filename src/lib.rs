use redb::{Database, TableDefinition};
use simd_json::OwnedValue;
use std::path::Path;
use std::sync::Arc;

// Define the key table and log table for storing logs.
// TableDefinition<K, V>, K is key, V is value. &str is ref to str, &[u8] has ref + size
const DOCUMENTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");
const LOGS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("logs");

// create the db engiine.
pub struct DBEngine {
    db: Arc<Database>,
}

impl DBEngine {
    // method to create the tables.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, redb::Error> {
        // create the database object.
        let db = Database::create(path)?;

        // open a write transaction and create the tables if they dont exist.
        // once created, commit the changes.
        let write_txn = db.begin_write()?;
        {
            write_txn.open_table(DOCUMENTS_TABLE)?;
            write_txn.open_table(LOGS_TABLE)?;
        }
        write_txn.commit()?;

        // return Self inside Ok()
        Ok(Self { db: Arc::new(db) })
    }

    pub fn insert(&self, id: &str, payload: &mut [u8]) -> Result<(), String> {
        // validate and parse JSON data at CPU vector speeds.
        // if json is not valid, raise error to user.
        let _parsed: OwnedValue = simd_json::to_owned_value(payload)
            .map_err(|e| format!("Invalid JSON payload for id {}: {}", id, e))?;

        // write to the disk if the json data is only valid.
        let write_txn = self.db.begin_write().map_err(|e| e.to_string())?;

        {
            let mut tickets_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| e.to_string())?;

            // re borrow the mutated payload variable, as an immutable
            // for redb function.
            tickets_table
                .insert(id, &*payload)
                .map_err(|e| e.to_string())?;
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn insert_many(&self, records: &mut [(&str, &mut [u8])]) -> Result<(), String> {
        // Validate the entire batch data, before we try to open a DB Transaction.
        // if one of those item is not validated, we skip the insert.

        for (id, payload) in records.iter_mut() {
            simd_json::to_owned_value(*payload)
                .map_err(|e| format!("Invalid json in batch for id {}: {}", id, e))?;
        }

        // Write to disk, if all the items of the batch data are valid.
        let write_txn = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut tickets_table = write_txn
                .open_table(DOCUMENTS_TABLE)
                .map_err(|e| e.to_string())?;
            for (id, payload) in records {
                tickets_table
                    .insert(*id, &**payload)
                    .map_err(|e| e.to_string())?;
            }
        }
        write_txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<Option<Vec<u8>>, redb::Error> {
        let read_txn = self.db.begin_read()?;
        let tickets_table = read_txn.open_table(DOCUMENTS_TABLE)?;

        if let Some(access_guard) = tickets_table.get(id)? {
            return Ok(Some(access_guard.value().to_vec()));
        } else {
            Ok(None)
        }
    }

    pub fn delete(&self, id: &str) -> Result<bool, redb::Error> {
        let write_txn = self.db.begin_write()?;
        let record_existed = {
            let mut tickets_table = write_txn.open_table(DOCUMENTS_TABLE)?;
            tickets_table.remove(id)?.is_some()
        };
        write_txn.commit()?;
        Ok(record_existed)
    }
}
