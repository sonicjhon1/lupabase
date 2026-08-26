use crate::{Deserialize, Error, Result, Serialize, prelude::*, utils::try_populate_storage};
use std::{borrow::Borrow, marker::PhantomData, path::Path, sync::Arc};
use tracing::warn;

type Key = arctic::key::BoxedStr<arctic::key::NonNull>;
type Value = arctic::concurrent::value::Arc<Vec<u8>>;
type Store = Arc<arctic::ConcurrentMap<Key, Value>>;

#[derive(Clone)]
pub struct ArcticMapDB<S> {
    store: Store,
    _serde_marker: PhantomData<S>,
}

impl<S> ArcticMapDB<S> {
    /// Creates a new instance of [`ArcticMapDB`]
    pub fn new() -> Self { return Self::default(); }

    pub const fn inner(&self) -> &Store { &self.store }
}

impl<S> Default for ArcticMapDB<S> {
    fn default() -> Self {
        return Self {
            store: Default::default(),
            _serde_marker: PhantomData,
        };
    }
}

impl<S: BytesSerde> Database for ArcticMapDB<S> {
    const NAME: &str = "ArcticMapDB";
    const SERDE_FORMAT_EXTENSION: &str = S::FORMAT;

    //TODO: This is not the same as DiskDB::try_copy_storage.
    fn try_copy_storage(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<()> {
        let source_path = source.as_ref();
        let source_path_str = source_path.to_string_lossy();
        let source_key = arctic::key::Str::<arctic::key::NonNull>::new(&source_path_str).unwrap();

        let content = self
            .store
            .get(source_key)
            .ok_or_else(|| Error::DBNotFound {
                file_path: source.as_ref().to_path_buf(),
            })?
            .clone();

        let destination_path = destination.as_ref();
        let destination_path_str = destination_path.to_string_lossy();
        let destination_key =
            arctic::key::Str::<arctic::key::NonNull>::new(&destination_path_str).unwrap();
        let destination_value = arctic::concurrent::value::Arc::from(Arc::new(content));

        self.store.upsert(destination_key, destination_value);

        Ok(())
    }

    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()> {
        let serialized = S::try_serialize_as_bytes(data)?;

        let path = path.as_ref();
        let path_str = path.to_string_lossy();
        let key = arctic::key::Str::<arctic::key::NonNull>::new(&path_str).unwrap();
        let value = arctic::concurrent::value::Arc::from(Arc::new(serialized));

        let _ = self.store.upsert(key, value);
        return Ok(());
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        let path = path.as_ref();
        let path_str = path.to_string_lossy();
        let key = arctic::key::Str::<arctic::key::NonNull>::new(&path_str).unwrap();

        let bytes = self.store.get(key).ok_or_else(|| Error::DBNotFound {
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

impl<S: BytesSerde> DatabasePartitioned for ArcticMapDB<S> {
    fn partition_path<T: DatabaseRecordPartitioned>(&self) -> RecordPartitionPath<'_> {
        RecordPartitionPath::Borrowed(Path::new(T::PARTITION))
    }
}

impl<S: BytesSerde> OperationPartitioned for ArcticMapDB<S> {}

impl<S: BytesSerde> Operation for ArcticMapDB<S> {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return try_populate_storage::<Self, O>(self, default_data, path);
    }
}

impl<S: BytesSerde> DatabaseProcedure for ArcticMapDB<S> {}

#[cfg(feature = "transaction")]
impl<S: BytesSerde> DatabaseTransaction for ArcticMapDB<S> {
    type TransactionDB = TransactionDB<S>;

    fn transact(&self) -> Result<Self::TransactionDB> { return Ok(Self::TransactionDB::new()); }
}

#[cfg(feature = "transaction")]
impl<S: BytesSerde> DatabaseTransactionPartitioned for ArcticMapDB<S> {}
