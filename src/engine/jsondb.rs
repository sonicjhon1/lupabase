use super::memorydb::MemoryDB;
use crate::{Deserialize, Error, Result, prelude::*, utils::*};
use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub struct JsonDB {
    db_dir: PathBuf,
}

impl Database for JsonDB {
    const NAME: &'static str = "JsonDB";

    fn new(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref();

        create_dir_all(dir)
            .map_err(|e| Error::IOCreateDirFailure {
                path: dir.display().to_string(),
                reason: e,
            })
            .expect("JsonDB directories creation should succeed.");

        Self { db_dir: dir.into() }
    }
}

impl DatabaseOpsCustom for JsonDB {}
impl DatabaseOps for JsonDB {}

impl DatabaseIO for JsonDB {
    const EXTENSION: &str = "jsondb";

    fn dir(&self) -> PathBuf {
        self.db_dir.clone()
    }

    fn try_write_storage(&self, data: impl serde::Serialize, path: impl AsRef<Path>) -> Result<()> {
        let serialized =
            serde_json::to_vec(&data).map_err(|e| Error::SerializationFailure(Box::new(e)))?;

        return try_write_file(&serialized, path);
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        let file_data = try_read_file(&path)?;

        serde_json::from_slice(&file_data).map_err(|e| backup_failed_parse(self, path, e))
    }
}

impl DatabaseTransaction for JsonDB {
    type TransactionDB = JsonDBTransaction;

    fn try_rollback_with_path<T: DatabaseRecord>(
        &self,
        transaction: &Self::TransactionDB,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let records_before = transaction.records_before.get_all_with_path::<T>(&path)?;
        return self.try_write_storage(records_before, path);
    }
}

#[derive(Clone, Debug)]
pub struct JsonDBTransaction {
    dir: PathBuf,
    records_before: MemoryDB,
    records_after: MemoryDB,
}

impl Database for JsonDBTransaction {
    const NAME: &'static str = "JsonDB-Transaction";

    fn new(dir: impl AsRef<Path>) -> Self {
        let memory_db = MemoryDB::new(&dir);
        return Self {
            dir: dir.as_ref().to_path_buf(),
            records_before: memory_db.clone(),
            records_after: memory_db,
        };
    }
}

impl DatabaseOpsCustom for JsonDBTransaction {}
impl DatabaseOps for JsonDBTransaction {}

impl DatabaseIO for JsonDBTransaction {
    const EXTENSION: &str = "jsontransactdb";

    fn dir(&self) -> PathBuf {
        self.dir.clone()
    }

    fn try_write_storage(&self, data: impl serde::Serialize, path: impl AsRef<Path>) -> Result<()> {
        return self.records_after.try_write_storage(data, path);
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        return self.records_after.try_read_storage::<O>(path);
    }
}
