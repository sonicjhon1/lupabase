use super::memorydb::MemoryDB;
use crate::{Deserialize, Result, Serialize, prelude::*, utils::*};
use std::{
    borrow::Borrow,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub struct TransactionDB<S> {
    dir: PathBuf,
    records_before: MemoryDB<S>,
    records_after: MemoryDB<S>,
}

impl<S: BytesSerde> Database for TransactionDB<S> {
    const NAME: &str = "TransactionDB";
    const SERDE_FORMAT: &str = S::FORMAT;

    fn new(dir: impl AsRef<Path>) -> Self {
        return Self {
            dir: dir.as_ref().to_path_buf(),
            records_before: MemoryDB::new(&dir),
            records_after: MemoryDB::new(&dir),
        };
    }
}

impl<S: BytesSerde> DatabaseOps for TransactionDB<S> {}

impl<S: BytesSerde> DatabaseOpsCustom for TransactionDB<S> {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        try_populate_storage::<_, O>(&self.records_before, default_data.borrow(), &path)?;
        try_populate_storage::<_, O>(&self.records_after, default_data, &path)
    }
}

impl<S: BytesSerde> DatabaseIO for TransactionDB<S> {
    const EXTENSION: &str = S::FORMAT;

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

impl<S: BytesSerde> DatabaseTransactionOps for TransactionDB<S> {}

impl<S: BytesSerde> DatabaseTransactionIO for TransactionDB<S> {
    fn try_read_storage_before<O: for<'a> Deserialize<'a>>(
        &self,
        transaction_path: impl AsRef<Path>,
    ) -> Result<O> {
        self.records_before.try_read_storage::<O>(transaction_path)
    }
}
