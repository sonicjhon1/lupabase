use crate::{Result, database::*};
use std::path::{Path, PathBuf};

/// Provides backup operations with arbritary paths for [`Database`]
pub trait OperationBackup: Database {
    /// Attempts to backup the storage, returning the backed-up storage path
    ///
    /// # Errors
    /// - I/O
    fn try_backup_storage_with_path(
        &self,
        path: impl AsRef<Path>,
        reason: impl AsRef<str>,
    ) -> Result<PathBuf>;
}
