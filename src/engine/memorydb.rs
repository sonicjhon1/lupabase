use crate::{Deserialize, Error, Result, prelude::*};
use hashbrown::HashMap;
use parking_lot::RwLock;
use std::{
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

impl DatabaseOpsCustom for MemoryDB {}
impl DatabaseOps for MemoryDB {}

impl DatabaseIO for MemoryDB {
    const EXTENSION: &str = "memorydb";

    fn dir(&self) -> PathBuf {
        self.dir.clone()
    }

    fn try_write_storage(&self, data: impl serde::Serialize, path: impl AsRef<Path>) -> Result<()> {
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
