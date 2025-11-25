use crate::{Deserialize, Error, Result, Serialize, record::*, record_utils::*, utils::*};
use std::{
    borrow::Borrow,
    path::{Path, PathBuf},
};

/// Represents a database that provides operations for managing records,
/// built upon the functionality provided by [`DatabaseOps`] and [`DatabaseIO`]
pub trait Database: DatabaseOps + DatabaseIO {
    /// The name of the Database
    const NAME: &str;

    /// Creates a new instance of [`Database`] with the specified base directory where files will be stored
    fn new(dir: impl AsRef<Path>) -> Self;
}

/// Provides common database operations using [`DatabaseRecordPartitioned::PARTITION`] as path for [`DatabaseOpsCustom`]
///
/// See [`DatabaseOpsCustom`] for details and the list of possible errors.
pub trait DatabaseOps: DatabaseOpsCustom {
    /// Retrieves all [`DatabaseRecordPartitioned`] from storage
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifiers
    fn get_all<T: DatabaseRecordPartitioned>(&self) -> Result<Vec<T>> {
        return self.get_all_with_path(self.file_path(T::PARTITION));
    }

    /// Inserts a single [`DatabaseRecordPartitioned`] into storage.
    /// The record is wrapped into a slice and passed to [`DatabaseOps::insert_all`].
    ///
    /// See [`DatabaseOps::insert_all`] for details and the list of possible errors.
    fn insert<T: DatabaseRecordPartitioned>(&self, updated_record: T) -> Result<()> {
        return self.insert_with_path(updated_record, self.file_path(T::PARTITION));
    }

    /// Inserts multiple [`DatabaseRecordPartitioned`] into storage
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifier is found among the new records
    fn insert_all<T: DatabaseRecordPartitioned>(&self, new_records: impl AsRef<[T]>) -> Result<()> {
        return self.insert_all_with_path(new_records, self.file_path(T::PARTITION));
    }

    /// Updates a single [`DatabaseRecordPartitioned`] in storage.
    /// The record is wrapped into a slice and passed to [`DatabaseOps::update_all`].
    ///
    /// See [`DatabaseOps::update_all`] for details and the list of possible errors.
    fn update<T: DatabaseRecordPartitioned>(&self, updated_record: T) -> Result<()> {
        return self.update_with_path(updated_record, self.file_path(T::PARTITION));
    }

    /// Updates multiple [`DatabaseRecordPartitioned`] in storage
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifier is found among the updated records
    /// - Unmatched unique identifier is found
    fn update_all<T: DatabaseRecordPartitioned>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        return self.update_all_with_path(updated_records, self.file_path(T::PARTITION));
    }

    /// Replace all [`DatabaseRecordPartitioned`] in storage with the provided [`DatabaseRecordPartitioned`]
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifier is found among the updated records
    fn replace_all<T: DatabaseRecordPartitioned>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        return self.replace_all_with_path(updated_records, self.file_path(T::PARTITION));
    }

    /// Attempts to initialize the provided default [`DatabaseRecordPartitioned`] into storage
    ///
    /// This method should check if the file already exists and validates its contents,
    /// returning an error if the file content is corrupted. If the file does not exist,
    /// it creates the database file using the provided default [`DatabaseRecordPartitioned`].
    ///
    /// # Errors
    /// - I/O
    /// - Parsing failure
    fn try_initialize_storage<
        T: DatabaseRecordPartitioned,
        O: Serialize + for<'a> Deserialize<'a>,
    >(
        &self,
        default_data: O,
    ) -> Result<()>
    where
        Self: Database + Sized, {
        return self
            .try_initialize_storage_with_path::<O>(default_data, self.file_path(T::PARTITION));
    }
}

/// Provides common database operations with arbritary paths for [`DatabaseIO`]
///
/// This trait supplies generic implementations for initializing, retrieving,
/// inserting, updating, and replacing records in the database.
pub trait DatabaseOpsCustom: DatabaseIO {
    /// Read all [`DatabaseRecord`] from the given path
    ///
    /// See [`DatabaseOps::get_all`] for details and the list of possible errors.
    fn get_all_with_path<T: DatabaseRecord>(&self, path: impl AsRef<Path>) -> Result<Vec<T>> {
        return self.try_read_storage::<Vec<T>>(path);
    }

    /// Inserts a single [`DatabaseRecord`] into the given path
    ///
    /// See [`DatabaseOps::insert`] for details and the list of possible errors.
    fn insert_with_path<T: DatabaseRecord>(
        &self,
        updated_record: T,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return self.insert_all_with_path([updated_record], path);
    }

    /// Inserts multiple [`DatabaseRecord`] into the given path
    ///
    /// See [`DatabaseOps::insert_all`] for details and the list of possible errors.
    fn insert_all_with_path<T: DatabaseRecord>(
        &self,
        new_records: impl AsRef<[T]>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let records = self.get_all_with_path(&path)?;
        let new_records = new_records.as_ref();

        check_is_all_new_records(&records, new_records, &path)?;

        return self.try_write_storage(
            records
                .iter()
                .chain(new_records.iter())
                .collect::<Vec<&T>>(),
            path,
        );
    }

    /// Updates a single [`DatabaseRecord`] into the given path
    ///
    /// See [`DatabaseOps::update`] for details and the list of possible errors.
    fn update_with_path<T: DatabaseRecord>(
        &self,
        updated_record: T,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return self.update_all_with_path([updated_record], path);
    }

    /// Updates multiple [`DatabaseRecord`] into the given path
    ///
    /// See [`DatabaseOps::update_all`] for details and the list of possible errors.
    fn update_all_with_path<T: DatabaseRecord>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let mut records = self.get_all_with_path(&path)?;
        let updated_records: Vec<T> = updated_records.into_iter().collect();

        check_is_all_existing_records(&records, &updated_records, &path)?;

        updated_records.into_iter().for_each(|ur| {
            let record = records
                .find_by_unique_mut(&ur.unique_value())
                .expect("All records should exist as it was checked before.");
            *record = ur;
        });
        return self.try_write_storage(records, path);
    }

    /// Replace all [`DatabaseRecord`] into the given path with the provided [`DatabaseRecord`]
    ///
    /// See [`DatabaseOps::replace_all`] for details and the list of possible errors.
    fn replace_all_with_path<T: DatabaseRecord>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let mut records: Vec<T> = vec![];

        for ur in updated_records.into_iter() {
            if let Some(duplicate_record) = records.find_by_unique(&ur.unique_value()) {
                return Err(Error::DBOperationFailure {
                    path: path.as_ref().display().to_string(),
                    reason: format!(
                        "Found duplicated unique value in records when replacing: [{:?}].",
                        duplicate_record.unique_value()
                    ),
                });
            }

            records.push(ur);
        }

        return self.try_write_storage(records, path);
    }

    /// Attempts to initialize the provided default data into the given storage path
    ///
    /// See [`DatabaseOps::try_initialize_storage`] for details and the list of possible errors.
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()>
    where
        Self: Database + Sized;
}

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

    /// Attempts to write the provided data (usually some form of [`DatabaseRecord`]) to storage
    ///
    /// # Errors
    /// - I/O
    /// - Parsing failure
    fn try_write_storage(&self, data: impl Serialize, path: impl AsRef<Path>) -> Result<()>;

    /// Attempts to read data from storage and deserialize it into the specified type of data
    /// (usually some form of [`DatabaseRecord`])
    ///
    /// # Errors
    /// - I/O
    /// - Parsing failure
    fn try_read_storage<O: for<'a> Deserialize<'a>>(&self, path: impl AsRef<Path>) -> Result<O>;
}

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

pub trait DatabaseTransactionOps: Database + DatabaseTransactionIO {
    fn get_all_before_with_path<T: DatabaseRecord>(
        &self,
        transaction_path: impl AsRef<Path>,
    ) -> Result<Vec<T>> {
        return self.try_read_storage_before::<Vec<T>>(transaction_path);
    }

    fn get_all_before<T: DatabaseRecordPartitioned>(&self) -> Result<Vec<T>> {
        self.get_all_before_with_path::<T>(self.file_path(T::PARTITION))
    }
}

pub trait DatabaseTransactionIO: Database {
    fn try_read_storage_before<O: for<'a> Deserialize<'a>>(
        &self,
        transaction_path: impl AsRef<Path>,
    ) -> Result<O>;
}
