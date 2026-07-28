use crate::{Deserialize, Error, Result, Serialize, prelude::*, utils::*};
use std::{
    borrow::Borrow,
    fs::create_dir_all,
    marker::PhantomData,
    path::{Path, PathBuf},
};
use tracing::warn;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct DiskDB<S> {
    db_dir: PathBuf,
    _serde_marker: PhantomData<S>,
}

impl<S: BytesSerde> DiskDB<S> {
    /// Creates a new instance of [`DiskDB`] with the specified base directory where files will be stored
    pub fn new(dir: impl AsRef<Path>) -> Self {
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

    /// Returns the storage's base directory used for all I/O
    pub fn dir(&self) -> PathBuf { self.db_dir.clone() }

    /// Returns the absolute path of the storage's base directory
    ///
    /// This method attempts to convert the relative directory returned by [`Self::dir`] into an absolute path.
    /// If obtaining an absolute path fails, it falls back to returning the original directory.
    pub fn dir_absolute(&self) -> PathBuf {
        let dir = self.dir();

        return std::path::absolute(&dir).unwrap_or(dir);
    }

    /// Returns a storage path with the provided file name
    pub fn file_path(&self, file_name: impl AsRef<Path>) -> PathBuf {
        self.dir()
            .join(file_name)
            .with_added_extension(Self::SERDE_FORMAT_EXTENSION)
    }

    /// Returns the absolute storage path with the provided file name    
    ///
    /// This method attempts to convert the relative path into an absolute path.
    /// If obtaining an absolute path fails, it falls back to returning the original path.
    pub fn file_path_absolute(&self, file_name: impl AsRef<Path>) -> PathBuf {
        let path = self
            .dir()
            .join(file_name)
            .with_added_extension(Self::SERDE_FORMAT_EXTENSION);

        return std::path::absolute(&path).unwrap_or(path);
    }
}

impl<S: BytesSerde> Database for DiskDB<S> {
    const NAME: &str = "DiskDB";
    const SERDE_FORMAT_EXTENSION: &str = S::FORMAT;

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
        let path = path.as_ref();
        let bytes = try_read_file(path)?;

        return S::try_deserialize_from_bytes(&bytes).map_err(|e| {
            warn!(
                "Failed deserialize partition at [{}], caused by: [{e}]",
                path.display()
            );

            match self.try_backup_storage_with_path(path, "DeserializationFailure") {
                Ok(_) => Error::DBCorrupt {
                    file_path: path.to_path_buf(),
                    reason: Error::DeserializationFailure(Box::new(e)).to_string(),
                },
                Err(e) => e,
            }
        });
    }
}

impl<S: BytesSerde> DatabasePartitioned for DiskDB<S> {
    fn partition_path<T: DatabaseRecordPartitioned>(&self) -> std::borrow::Cow<'_, Path> {
        std::borrow::Cow::Owned(self.file_path(T::PARTITION))
    }
}

impl<S: BytesSerde> OperationBackup for DiskDB<S> {
    fn try_backup_storage_with_path(
        &self,
        path: impl AsRef<Path>,
        reason: impl AsRef<str>,
    ) -> Result<PathBuf> {
        let path = path.as_ref();

        let backup_path = path.with_added_extension(format!(
            "{}-{}.bak",
            chrono::Local::now().timestamp(),
            reason.as_ref()
        ));

        self.try_copy_storage(path, &backup_path)?;

        return Ok(backup_path);
    }
}

impl<S: BytesSerde> OperationBackupPartitioned for DiskDB<S> {}

impl<S: BytesSerde> OperationPartitioned for DiskDB<S> {}

impl<S: BytesSerde> Operation for DiskDB<S> {
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return try_populate_storage::<Self, O>(self, default_data, path);
    }
}

#[cfg(feature = "transaction")]
impl<S: BytesSerde> DatabaseTransaction for DiskDB<S> {
    type TransactionDB = TransactionDB<S>;

    fn transact(&self) -> Result<Self::TransactionDB> { return Ok(Self::TransactionDB::new()); }
}

#[cfg(feature = "transaction")]
impl<S: BytesSerde> DatabaseTransactionPartitioned for DiskDB<S> {}
