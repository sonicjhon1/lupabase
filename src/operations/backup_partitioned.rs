use crate::{Result, database::*, operations::OperationBackup, record::DatabaseRecordPartitioned};
use std::path::PathBuf;

/// Provides backup operations by using [`DatabaseRecordPartitioned::PARTITION`] for [`Database`]
pub trait OperationBackupPartitioned: DatabasePartitioned + OperationBackup {
    /// Attempts to backup the storage, returning the backed-up storage path
    ///
    /// # Errors
    /// - I/O
    fn try_backup_storage<T: DatabaseRecordPartitioned>(
        &self,
        reason: impl AsRef<str>,
    ) -> Result<PathBuf> {
        self.try_backup_storage_with_path(self.partition_path::<T>(), reason)
    }
}
