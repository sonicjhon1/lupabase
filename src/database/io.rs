use crate::{Deserialize, Result, Serialize};
use std::path::{Path, PathBuf};

/// Provides operations for database I/O
pub trait DatabaseIO {
    /// The extension for the storage's path
    const EXTENSION: &str;

    /// Returns the storage's base directory used for all I/O
    fn dir(&self) -> PathBuf;

    /// Returns the absolute path of the storage's base directory
    ///
    /// This method attempts to convert the relative directory returned by [`DatabaseIO::dir`] into an absolute path.
    /// If obtaining an absolute path fails, it falls back to returning the original directory.
    fn dir_absolute(&self) -> PathBuf { std::path::absolute(self.dir()).unwrap_or(self.dir()) }

    /// Returns a storage path with the provided file name
    fn file_path(&self, file_name: impl AsRef<Path>) -> PathBuf {
        self.dir()
            .join(file_name)
            .with_added_extension(Self::EXTENSION)
    }

    /// Attemps to copy the storage to the destination
    ///
    /// # Errors
    /// - I/O
    fn try_copy_storage(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<()>;

    /// Attempts to backup the storage, returning the backed-up storage path
    ///
    /// # Errors
    /// - I/O
    fn try_backup_storage(
        &self,
        path: impl AsRef<Path>,
        reason: impl AsRef<str>,
    ) -> Result<PathBuf> {
        let path = path.as_ref();

        let backup_path = path.with_added_extension(format!(
            "{}-{}.bak",
            &chrono::Local::now().timestamp(),
            reason.as_ref()
        ));

        self.try_copy_storage(path, &backup_path)?;

        return Ok(backup_path);
    }

    /// Attempts to write the provided data to storage
    ///
    /// # Errors
    /// - I/O
    /// - Parsing failure
    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()>;

    /// Attempts to read data from storage and deserialize it into the specified type of data
    ///
    /// # Errors
    /// - I/O
    /// - Parsing failure
    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O>;
}
