use redb::{Database, TableDefinition};
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

    pub fn insert(&self, id: &str, payload: &[u8]) -> Result<(), redb::Error> {
        let write_txn = self.db.begin_write()?;

        {
            let mut tickets_table = write_txn.open_table(DOCUMENTS_TABLE)?;
            tickets_table.insert(id, payload)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn insert_many(&self, records: &[(&str, &[u8])]) -> Result<(), redb::Error> {
        let write_txn = self.db.begin_write()?;
        {
            let mut tickets_table = write_txn.open_table(DOCUMENTS_TABLE)?;
            for (id, payload) in records {
                tickets_table.insert(*id, *payload)?;
            }
        }
        write_txn.commit()?;
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
