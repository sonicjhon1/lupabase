use crate::{Deserialize, Result, database::*};
use std::path::Path;

/// Provides I/O operations for transaction database
pub trait DatabaseTransactionIO: Database {
    /// Attempts to read data from the stored snapshot in the transaction and deserialize it into the
    /// specified type of data
    ///
    /// # Errors
    /// - I/O
    /// - Invalid path
    /// - Parsing failure
    fn try_read_storage_before<O: for<'a> Deserialize<'a>>(
        &self,
        transaction_path: impl AsRef<Path>,
    ) -> Result<O>;
}
