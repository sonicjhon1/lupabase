use crate::{Deserialize, Error, Result, database::*};
use hashbrown::HashMap;
use parking_lot::RwLock;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct MemoryDB {
    dir: PathBuf,
    store: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,
}

impl Database for MemoryDB {
    const NAME: &'static str = "MemoryDB";

    fn new(dir: impl AsRef<Path>) -> Self {
        return Self {
            dir: dir.as_ref().to_path_buf(),
            store: Arc::new(RwLock::new(HashMap::new())),
        };
    }

    fn dir(&self) -> PathBuf {
        self.dir.clone()
    }

    fn file_path(&self, file_name: impl AsRef<Path>) -> PathBuf {
        self.dir.join(file_name)
    }
}

impl DatabaseOps for MemoryDB {}

impl DatabaseIO for MemoryDB {
    fn try_initialize_file<T: DatabaseRecord>(
        &self,
        default_records: impl AsRef<[T]>,
    ) -> Result<()> {
        let partition_path = self.file_path(T::PARTITION);

        match self.try_read_file::<T, Vec<T>>() {
            Ok(_) => {}
            Err(Error::DBNotFound { file_path }) => {
                warn!(
                    "Couldn't find [{}]. Trying to populate {}.",
                    file_path.display(),
                    Self::NAME
                );

                self.try_write_file::<T>(default_records.as_ref())?;
            }
            Err(e) => return Err(e),
        };

        info!("Found [{}].", partition_path.display());
        return Ok(());
    }

    fn try_write_file<T: DatabaseRecord>(&self, data: impl serde::Serialize) -> Result<()> {
        let partition_path = self.file_path(T::PARTITION);
        let serialized =
            minicbor_serde::to_vec(data).map_err(|e| Error::SerializationFailure(Box::new(e)))?;

        let mut guard = self.store.write();
        let _ = guard.insert(partition_path, serialized);
        return Ok(());
    }

    fn try_read_file<T: DatabaseRecord, O: for<'a> Deserialize<'a>>(&self) -> Result<O> {
        let partition_path = self.file_path(T::PARTITION);

        let guard = self.store.read();
        let data = guard
            .get(&partition_path)
            .ok_or_else(|| Error::DBNotFound {
                file_path: partition_path.clone(),
            })?;

        minicbor_serde::from_slice(data).map_err(|e| {
            warn!(
                "Failed deserialize partition at [{}], caused by: [{e}]",
                partition_path.display()
            );

            return Error::DBCorrupt {
                file_path: partition_path,
                reason: Error::DeserializationFailure(Box::new(e)).to_string(),
            };
        })
    }
}
