use crate::{Deserialize, Result, Serialize};
use std::path::Path;

/// Represents a database that provides operations for managing the underlying storage,
/// this trait provides the minimum operations needed for a database.
pub trait Database {
    /// The name of the Database
    const NAME: &str;

    /// The extension of the Database's storage's path
    const SERDE_FORMAT_EXTENSION: &str;

    /// Attemps to copy the storage to the destination
    ///
    /// # Errors
    /// - I/O
    fn try_copy_storage(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<()>;

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
