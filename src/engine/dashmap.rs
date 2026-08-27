use crate::{Deserialize, Error, Result, Serialize, prelude::*, utils::try_populate_storage};
use std::{
    borrow::Borrow,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::warn;

type Key = PathBuf;
type Value = Vec<u8>;
type Store = Arc<dashmap::DashMap<Key, Value>>;

#[derive(Clone, Debug)]
pub struct DashMapDB<S> {
    store: Store,
    _serde_marker: PhantomData<S>,
}

impl<S> DashMapDB<S> {
    /// Creates a new instance of [`DashMapDB`]
    pub fn new() -> Self { return Self::default(); }

    pub const fn inner(&self) -> &Store { &self.store }
}

impl<S> Default for DashMapDB<S> {
    fn default() -> Self {
        return Self {
            store: Default::default(),
            _serde_marker: PhantomData,
        };
    }
}

impl<S: BytesSerde> Database for DashMapDB<S> {
    const NAME: &str = "DashMapDB";
    const SERDE_FORMAT_EXTENSION: &str = S::FORMAT;

    //TODO: This is not the same as DiskDB::try_copy_storage.
    fn try_copy_storage(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<()> {
        let content = self
            .store
            .get(source.as_ref())
            .ok_or_else(|| Error::DBNotFound {
                file_path: source.as_ref().to_path_buf(),
            })?
            .clone();

        self.store
            .insert(destination.as_ref().to_path_buf(), content);

        Ok(())
    }

    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()> {
        let serialized = S::try_serialize_as_bytes(data)?;

        self.store.insert(path.as_ref().to_path_buf(), serialized);
        return Ok(());
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        let path = path.as_ref();

        let bytes = self.store.get(path).ok_or_else(|| Error::DBNotFound {
            file_path: path.to_path_buf(),
        })?;

        S::try_deserialize_from_bytes(&bytes).map_err(|e| {
            warn!(
                "Failed deserialize partition at [{}], caused by: [{e}]",
                path.display()
            );

            return Error::DBCorrupt {
                file_path: path.to_path_buf(),
                reason: Error::DeserializationFailure(Box::new(e)).to_string(),
            };
        })
    }
}

impl<S: BytesSerde> DatabasePartitioned for DashMapDB<S> {
    fn partition_path<T: DatabaseRecordPartitioned>(&self) -> RecordPartitionPath<'_> {
        RecordPartitionPath::Borrowed(Path::new(T::PARTITION))
    }
}

impl<S: BytesSerde> OperationCrudPartitioned for DashMapDB<S> {}

impl<S: BytesSerde> OperationCrud for DashMapDB<S> {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return try_populate_storage::<Self, O>(self, default_data, path);
    }
}

impl<S: BytesSerde> DatabaseProcedure for DashMapDB<S> {}

#[cfg(feature = "transaction")]
impl<S: BytesSerde> DatabaseTransaction for DashMapDB<S> {
    type TransactionDB = TransactionDB<S>;

    fn transact(&self) -> Result<Self::TransactionDB> { return Ok(Self::TransactionDB::new()); }
}

#[cfg(feature = "transaction")]
impl<S: BytesSerde> DatabaseTransactionPartitioned for DashMapDB<S> {}
