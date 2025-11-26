use super::memorydb::MemoryDB;
use crate::{Deserialize, Error, Result, Serialize, prelude::*, utils::*};
use std::{
    borrow::Borrow,
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

impl DatabaseOps for CborDB {}

impl DatabaseOpsCustom for CborDB {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return try_populate_storage::<Self, O>(self, default_data, path);
    }
}

impl DatabaseIO for CborDB {
    const EXTENSION: &str = "cbordb";

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
        return Self {
            dir: dir.as_ref().to_path_buf(),
            records_before: MemoryDB::new(&dir),
            records_after: MemoryDB::new(&dir),
        };
    }
}

impl DatabaseOps for CborDBTransaction {}

impl DatabaseOpsCustom for CborDBTransaction {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        try_populate_storage::<_, O>(&self.records_before, default_data.borrow(), &path)?;
        try_populate_storage::<_, O>(&self.records_after, default_data, &path)
    }
}

impl DatabaseIO for CborDBTransaction {
    const EXTENSION: &str = "cbortransactdb";

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

impl DatabaseTransactionOps for CborDBTransaction {}

impl DatabaseTransactionIO for CborDBTransaction {
    fn try_read_storage_before<O: for<'a> Deserialize<'a>>(
        &self,
        transaction_path: impl AsRef<Path>,
    ) -> Result<O> {
        self.records_before.try_read_storage::<O>(transaction_path)
    }
}
