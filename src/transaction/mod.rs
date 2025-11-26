mod operation;
pub use operation::*;
mod io;
pub use io::*;

use crate::{Deserialize, Error, Result, Serialize, database::*, record::*};
use std::path::Path;

// TODO: Lock file when transaction started
/// Provides atomic transactional support for databases.
pub trait DatabaseTransaction: Database {
    /// The transactional database type
    ///
    /// This associated type represents a temporary, mutable snapshot of the database state.
    type TransactionDB: Database + DatabaseTransactionOps + DatabaseTransactionIO;

    /// Begins a new transaction (usually is Infallible)
    ///
    /// # Errors
    /// - I/O
    fn transact(&self) -> Result<Self::TransactionDB> { return Ok(Self::TransactionDB::new("")); }

    /// Commits the current transaction in the given path
    ///
    /// See [`DatabaseTransaction::try_commit`] for details and the list of possible errors.
    fn try_commit_with_path<O: Serialize + for<'a> Deserialize<'a>>(
        &self,
        transaction: &Self::TransactionDB,
        transaction_path: impl AsRef<Path>,
        database_path: impl AsRef<Path>,
    ) -> Result<()> {
        let records = transaction.try_read_storage::<O>(&transaction_path)?;
        match self.try_write_storage(records, &database_path) {
            Ok(()) => Ok(()),
            Err(e) => {
                let database_path = database_path.as_ref().to_path_buf();

                self.try_rollback_with_path::<O>(transaction, transaction_path, &database_path)
                    .map_err(|e| Error::DBTransactionRollbackFailure {
                        file_path: database_path.clone(),
                        reason: e.to_string(),
                    })?;

                return Err(Error::DBTransactionCommitFailure {
                    file_path: database_path,
                    reason: e.to_string(),
                });
            }
        }
    }

    /// Commits the current transaction
    ///
    /// # Errors
    /// - I/O
    fn try_commit<T: DatabaseRecordPartitioned>(
        &self,
        transaction: &Self::TransactionDB,
    ) -> Result<()> {
        return self.try_commit_with_path::<Vec<T>>(
            transaction,
            transaction.file_path(T::PARTITION),
            self.file_path(T::PARTITION),
        );
    }

    /// Rolls back the current transaction in the given path
    ///
    /// See [`DatabaseTransaction::try_rollback`] for details and the list of possible errors.
    fn try_rollback_with_path<O: Serialize + for<'a> Deserialize<'a>>(
        &self,
        transaction: &Self::TransactionDB,
        transaction_path: impl AsRef<Path>,
        database_path: impl AsRef<Path>,
    ) -> Result<()> {
        let records_before = transaction.try_read_storage_before::<O>(&transaction_path)?;

        return self.try_write_storage(records_before, database_path);
    }

    /// Rolls back the current transaction
    ///
    /// # Errors
    /// - I/O
    fn try_rollback<T: DatabaseRecordPartitioned>(
        &self,
        transaction: &Self::TransactionDB,
    ) -> Result<()> {
        self.try_rollback_with_path::<Vec<T>>(
            transaction,
            transaction.file_path(T::PARTITION),
            self.file_path(T::PARTITION),
        )
    }
}
