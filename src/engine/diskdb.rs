use crate::{Deserialize, Error, Result, Serialize, prelude::*, utils::*};
use std::{
    borrow::Borrow,
    fs::create_dir_all,
    marker::PhantomData,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub struct DiskDB<S> {
    db_dir: PathBuf,
    _serde_marker: PhantomData<S>,
}

impl<S: BytesSerde> Database for DiskDB<S> {
    const NAME: &str = "DiskDB";
    const SERDE_FORMAT: &str = S::FORMAT;

    fn new(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref();

        create_dir_all(dir)
            .map_err(|e| Error::IOCreateDirFailure {
                path: dir.display().to_string(),
                reason: e,
            })
            .expect("DiskDB directories creation should succeed.");

        Self {
            db_dir: dir.into(),
            _serde_marker: PhantomData,
        }
    }
}

impl<S: BytesSerde> DatabaseOps for DiskDB<S> {}

impl<S: BytesSerde> DatabaseOpsCustom for DiskDB<S> {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return try_populate_storage::<Self, O>(self, default_data, path);
    }
}

impl<S: BytesSerde> DatabaseIO for DiskDB<S> {
    const EXTENSION: &str = S::FORMAT;

    fn dir(&self) -> PathBuf { self.db_dir.clone() }

    fn try_copy_storage(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<()> {
        return try_copy_file(source, destination);
    }

    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()> {
        let serialized = S::try_serialize_as_bytes(data)?;

        return try_write_file(&serialized, path);
    }

    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O> {
        let bytes = try_read_file(&path)?;

        return S::try_deserialize_from_bytes(&bytes)
            .map_err(|e| backup_failed_parse(self, path, e));
    }
}

impl<S: BytesSerde> DatabaseTransaction for DiskDB<S> {
    type TransactionDB = TransactionDB<S>;
}
