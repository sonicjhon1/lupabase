use crate::{Deserialize, Error, Result, Serialize, prelude::*, utils::try_populate_storage};
use hashbrown::HashMap;
use parking_lot::RwLock;
use std::{
    borrow::Borrow,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::warn;

#[derive(Clone, Debug)]
pub struct MemoryDB {
    dir: PathBuf,
    store: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,
}

impl Database for MemoryDB {
    const NAME: &str = "MemoryDB";

    fn new(dir: impl AsRef<Path>) -> Self {
        return Self {
            dir: dir.as_ref().to_path_buf(),
            store: Default::default(),
        };
    }
}

impl DatabaseOps for MemoryDB {}

impl DatabaseOpsCustom for MemoryDB {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return try_populate_storage::<Self, O>(self, default_data, path);
    }
}

impl DatabaseIO for MemoryDB {
    const EXTENSION: &str = "memorydb";

    fn dir(&self) -> PathBuf { self.dir.clone() }

    fn try_copy_storage(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<()> {
        let mut guard = self.store.write();

        let content = guard
            .get(source.as_ref())
            .ok_or_else(|| {
                return Error::DBNotFound {
                    file_path: source.as_ref().to_path_buf(),
                };
            })?
            .clone();

        let _ = guard
            .entry(destination.as_ref().to_path_buf())
            .insert(content.clone());

        Ok(())
    }

    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()> {
        let serialized =
            minicbor_serde::to_vec(data).map_err(|e| Error::SerializationFailure(Box::new(e)))?;

        let mut guard = self.store.write();
        let _ = guard.insert(path.as_ref().to_path_buf(), serialized);
        return Ok(());
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        let path = path.as_ref();

        let guard = self.store.read();
        let data = guard.get(path).ok_or_else(|| Error::DBNotFound {
            file_path: path.to_path_buf(),
        })?;

        minicbor_serde::from_slice(data).map_err(|e| {
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
