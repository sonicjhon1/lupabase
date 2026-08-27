use crate::{Deserialize, Result, Serialize, prelude::*, utils::*};
use std::{borrow::Borrow, path::Path};

#[derive(Clone, Debug)]
pub struct TransactionDB<S> {
    records_before: HashMapDB<S>,
    records_after: HashMapDB<S>,
}

impl<S> TransactionDB<S> {
    /// Creates a new instance of [`TransactionDB`]
    pub fn new() -> Self { return Self::default(); }
}

impl<S> Default for TransactionDB<S> {
    fn default() -> Self {
        return Self {
            records_before: HashMapDB::new(),
            records_after: HashMapDB::new(),
        };
    }
}

impl<S: BytesSerde> Database for TransactionDB<S> {
    const NAME: &str = "TransactionDB";
    const SERDE_FORMAT_EXTENSION: &str = S::FORMAT;

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

impl<S: BytesSerde> DatabasePartitioned for TransactionDB<S> {
    fn partition_path<T: DatabaseRecordPartitioned>(&self) -> RecordPartitionPath<'_> {
        RecordPartitionPath::Borrowed(Path::new(T::PARTITION))
    }
}

impl<S: BytesSerde> DatabaseSnapshot for TransactionDB<S> {
    type SnapshotDB = HashMapDB<S>;

    fn snapshot(&self) -> &Self::SnapshotDB { &self.records_before }
}

impl<S: BytesSerde> OperationCrudPartitioned for TransactionDB<S> {}

impl<S: BytesSerde> OperationCrud for TransactionDB<S> {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        try_populate_storage::<_, O>(&self.records_before, default_data.borrow(), &path)?;
        try_populate_storage::<_, O>(&self.records_after, default_data, &path)
    }
}

impl<S: BytesSerde> DatabaseProcedure for TransactionDB<S> {}
