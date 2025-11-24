use super::memorydb::MemoryDB;
use crate::{Deserialize, Error, Result, Serialize, prelude::*, utils::*};
use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub struct CborDB {
    db_dir: PathBuf,
}

impl Database for CborDB {
    const NAME: &str = "CborDB";

    fn new(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref();

        create_dir_all(dir)
            .map_err(|e| Error::IOCreateDirFailure {
                path: dir.display().to_string(),
                reason: e,
            })
            .expect("CborDB directories creation should succeed.");

        Self { db_dir: dir.into() }
    }
}

impl DatabaseOpsCustom for CborDB {}
impl DatabaseOps for CborDB {}

impl DatabaseIO for CborDB {
    const EXTENSION: &str = "cbordb";

    fn dir(&self) -> PathBuf { self.db_dir.clone() }

    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()> {
        let serialized =
            minicbor_serde::to_vec(&data).map_err(|e| Error::SerializationFailure(Box::new(e)))?;

        return try_write_file(&serialized, path);
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        let file_data = try_read_file(&path)?;

        minicbor_serde::from_slice(&file_data).map_err(|e| backup_failed_parse(self, path, e))
    }
}

impl DatabaseTransaction for CborDB {
    type TransactionDB = CborDBTransaction;

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
pub struct CborDBTransaction {
    dir: PathBuf,
    records_before: MemoryDB,
    records_after: MemoryDB,
}

impl Database for CborDBTransaction {
    const NAME: &str = "CborDB-Transaction";

    fn new(dir: impl AsRef<Path>) -> Self {
        let memory_db = MemoryDB::new(&dir);
        return Self {
            dir: dir.as_ref().to_path_buf(),
            records_before: memory_db.clone(),
            records_after: memory_db,
        };
    }
}

impl DatabaseOpsCustom for CborDBTransaction {}
impl DatabaseOps for CborDBTransaction {}

impl DatabaseIO for CborDBTransaction {
    const EXTENSION: &str = "cbortransactdb";

    fn dir(&self) -> PathBuf { self.dir.clone() }

    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()> {
        return self.records_after.try_write_storage(data, path);
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        return self.records_after.try_read_storage::<O>(path);
    }
}
