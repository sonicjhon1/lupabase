use super::memorydb::MemoryDB;
use crate::{Deserialize, Error, Result, Serialize, prelude::*, utils::*};
use std::{
    borrow::Borrow,
    fs::create_dir_all,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub struct JsonDB {
    db_dir: PathBuf,
}

impl Database for JsonDB {
    const NAME: &str = "JsonDB";

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

impl DatabaseOps for JsonDB {}

impl DatabaseOpsCustom for JsonDB {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()>
    where
        Self: Database + Sized, {
        return try_populate_storage::<Self, O>(self, default_data, path);
    }
}

impl DatabaseIO for JsonDB {
    const EXTENSION: &str = "jsondb";

    fn dir(&self) -> PathBuf { self.db_dir.clone() }

    fn try_copy_storage(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<()> {
        return try_copy_file(source, destination);
    }

    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()> {
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
}

#[derive(Clone, Debug)]
pub struct JsonDBTransaction {
    dir: PathBuf,
    records_before: MemoryDB,
    records_after: MemoryDB,
}

impl Database for JsonDBTransaction {
    const NAME: &str = "JsonDB-Transaction";

    fn new(dir: impl AsRef<Path>) -> Self {
        return Self {
            dir: dir.as_ref().to_path_buf(),
            records_before: MemoryDB::new(&dir),
            records_after: MemoryDB::new(&dir),
        };
    }
}

impl DatabaseOps for JsonDBTransaction {}

impl DatabaseOpsCustom for JsonDBTransaction {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()>
    where
        Self: Database + Sized, {
        try_populate_storage::<_, O>(&self.records_before, default_data.borrow(), &path)?;
        try_populate_storage::<_, O>(&self.records_after, default_data, &path)
    }
}

impl DatabaseIO for JsonDBTransaction {
    const EXTENSION: &str = "jsontransactdb";

    fn dir(&self) -> PathBuf { self.dir.clone() }

    fn try_copy_storage(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<()> {
        return self.records_after.try_copy_storage(source, destination);
    }

    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()> {
        return self.records_after.try_write_storage(data, path);
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        return self.records_after.try_read_storage::<O>(path);
    }
}

impl DatabaseTransactionOps for JsonDBTransaction {}

impl DatabaseTransactionIO for JsonDBTransaction {
    fn try_read_storage_before<O: for<'a> Deserialize<'a>>(
        &self,
        transaction_path: impl AsRef<Path>,
    ) -> Result<O> {
        self.records_before.try_read_storage::<O>(transaction_path)
    }
}
