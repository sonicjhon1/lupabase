use crate::{Deserialize, Error, Result, Serialize, prelude::*, utils::try_populate_storage};
use hashbrown::HashMap;
use parking_lot::RwLock;
use std::{
    borrow::Borrow,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::warn;

#[derive(Clone, Debug)]
pub struct MemoryDB<S> {
    dir: PathBuf,
    store: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,
    _serde_marker: PhantomData<S>,
}

impl<S: BytesSerde> Database for MemoryDB<S> {
    const NAME: &str = "MemoryDB";
    const SERDE_FORMAT: &str = S::FORMAT;

    fn new(dir: impl AsRef<Path>) -> Self {
        return Self {
            dir: dir.as_ref().to_path_buf(),
            store: Default::default(),
            _serde_marker: PhantomData,
        };
    }
}

impl<S: BytesSerde> DatabaseOps for MemoryDB<S> {}

impl<S: BytesSerde> DatabaseOpsCustom for MemoryDB<S> {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return try_populate_storage::<Self, O>(self, default_data, path);
    }
}

impl<S: BytesSerde> DatabaseIO for MemoryDB<S> {
    const EXTENSION: &str = S::FORMAT;

    fn dir(&self) -> PathBuf { self.dir.clone() }

    fn try_copy_storage(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<()> {
        let mut guard = self.store.write();

        let content = guard
            .get(source.as_ref())
            .cloned()
            .ok_or_else(|| Error::DBNotFound {
                file_path: source.as_ref().to_path_buf(),
            })?;

        let _ = guard.insert(destination.as_ref().to_path_buf(), content);

        Ok(())
    }

    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()> {
        let serialized = S::try_serialize_as_bytes(data)?;

        let mut guard = self.store.write();
        let _ = guard.insert(path.as_ref().to_path_buf(), serialized);
        return Ok(());
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        let path = path.as_ref();

        let guard = self.store.read();
        let bytes = guard.get(path).ok_or_else(|| Error::DBNotFound {
            file_path: path.to_path_buf(),
        })?;

        S::try_deserialize_from_bytes(bytes).map_err(|e| {
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

impl<S: BytesSerde> DatabaseTransaction for MemoryDB<S> {
    type TransactionDB = TransactionDB<S>;
}
