use crate::{Result, database::*, record::*, transaction::*};
use std::path::Path;

/// Provides transaction database operations
pub trait DatabaseTransactionOps: Database + DatabaseTransactionIO {
    /// Reads all [`DatabaseRecord`] with the given path from the stored snapshot in the transaction
    ///
    /// See [`DatabaseTransactionIO::try_read_storage_before`] for details and the list of possible errors.
    fn get_all_before_with_path<T: DatabaseRecord>(
        &self,
        transaction_path: impl AsRef<Path>,
    ) -> Result<Vec<T>> {
        return self.try_read_storage_before::<Vec<T>>(transaction_path);
    }

    /// Reads all [`DatabaseRecord`] from the given path from the stored snapshot in the transaction
    ///
    /// See [`DatabaseTransactionOps::get_all_before_with_path`] for details and the list of possible errors.
    fn get_all_before<T: DatabaseRecordPartitioned>(&self) -> Result<Vec<T>> {
        self.get_all_before_with_path::<T>(self.file_path(T::PARTITION))
    }
}
