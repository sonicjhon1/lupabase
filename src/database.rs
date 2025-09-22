use crate::{Deserialize, Error, Result, Serialize, utils::*};
use itertools::Itertools;
use std::{
    fmt::Debug,
    hash::Hash,
    path::{Path, PathBuf},
};

/// Unique identifier for [`DatabaseRecord`]
///
/// This type must implement [`Hash`], [`Eq`], and [`Debug`] to ensure proper comparison and debugging capabilities.
pub trait IntoUnique: Hash + Eq + Debug + Serialize {}
impl<T: Hash + Eq + Debug + Serialize> IntoUnique for T {}

/// Represents a Record that can be stored
///
/// Record must implement both [`Serialize`] and [`Deserialize`]
pub trait DatabaseRecord: Serialize + for<'a> Deserialize<'a> {
    /// The type of the unique identifier that implements [`IntoUnique`]
    type Unique: IntoUnique;

    /// Specifies the partition where records of this type are stored
    const PARTITION: &'static str;

    /// Returns the unique identifier of the record
    ///
    /// This method should provide a value that uniquely identifies records of the same type,
    /// essential for [`DatabaseOps`].
    fn unique_value(&self) -> Self::Unique;
}

/// Represents a database that provides operations for managing records,
/// built upon the functionality provided by [`DatabaseOps`] and [`DatabaseIO`]
pub trait Database: DatabaseOps + DatabaseIO {
    const NAME: &'static str;

    /// Creates a new instance of [`Database`] with the specified base directory where files will be stored.
    fn new(dir: impl AsRef<Path>) -> Self;
}

/// Provides common database operations built on top of [`DatabaseIO`]
///
/// This trait supplies generic implementations for initializem retrieving,
/// inserting, updating, and replacing records in the database.
pub trait DatabaseOps: DatabaseIO {
    /// Read all [`DatabaseRecord`] from the given path
    ///
    /// See [`DatabaseOps::get_all`] for details and the list of possible errors.
    fn get_all_with_path<T: DatabaseRecord>(&self, path: impl AsRef<Path>) -> Result<Vec<T>> {
        return self.try_read_storage::<Vec<T>>(path);
    }

    /// Retrieves all [`DatabaseRecord`] from storage
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifiers
    fn get_all<T: DatabaseRecord>(&self) -> Result<Vec<T>> {
        return self.get_all_with_path(self.file_path(T::PARTITION));
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

    /// Inserts a single [`DatabaseRecord`] into storage.
    /// The record is wrapped into a slice and passed to [`DatabaseOps::insert_all`].
    ///
    /// See [`DatabaseOps::insert_all`] for details and the list of possible errors.
    fn insert<T: DatabaseRecord>(&self, updated_record: T) -> Result<()> {
        return self.insert_with_path(updated_record, self.file_path(T::PARTITION));
    }

    /// Inserts multiple [`DatabaseRecord`] into the given path
    ///
    /// See [`DatabaseOps::insert_all`] for details and the list of possible errors.
    fn insert_all_with_path<T: DatabaseRecord>(
        &self,
        new_records: impl AsRef<[T]>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let records = self.get_all::<T>()?;
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

    /// Inserts multiple [`DatabaseRecord`] into storage
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifier is found among the new records
    fn insert_all<T: DatabaseRecord>(&self, new_records: impl AsRef<[T]>) -> Result<()> {
        return self.insert_all_with_path(new_records, self.file_path(T::PARTITION));
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

    /// Updates a single [`DatabaseRecord`] in storage.
    /// The record is wrapped into a slice and passed to [`DatabaseOps::update_all`].
    ///
    /// See [`DatabaseOps::update_all`] for details and the list of possible errors.
    fn update<T: DatabaseRecord>(&self, updated_record: T) -> Result<()> {
        return self.update_with_path(updated_record, self.file_path(T::PARTITION));
    }

    /// Updates multiple [`DatabaseRecord`] into the given path
    ///
    /// See [`DatabaseOps::update_all`] for details and the list of possible errors.
    fn update_all_with_path<T: DatabaseRecord>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let mut records = self.get_all::<T>()?;
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

    /// Updates multiple [`DatabaseRecord`] in storage
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifier is found among the updated records
    /// - Unmatched unique identifier is found
    fn update_all<T: DatabaseRecord>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        return self.update_all_with_path(updated_records, self.file_path(T::PARTITION));
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

    /// Replace all [`DatabaseRecord`] in storage with the provided [`DatabaseRecord`]
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifier is found among the updated records
    fn replace_all<T: DatabaseRecord>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        return self.replace_all_with_path(updated_records, self.file_path(T::PARTITION));
    }

    /// Attempts to initialize the provided default [`DatabaseRecord`] into the given storage path
    ///
    /// See [`DatabaseIO::try_initialize_file`] for details and the list of possible errors.
    fn try_initialize_storage_with_path<T: DatabaseRecord>(
        &self,
        default_records: impl AsRef<[T]>,
        path: impl AsRef<Path>,
    ) -> Result<()>
    where
        Self: Database + Sized,
    {
        return try_populate_storage(self, default_records, path);
    }

    /// Attempts to initialize the provided default [`DatabaseRecord`] into storage
    ///
    /// This method should check if the file already exists and validates its contents,
    /// returning an error if the file content is corrupted. If the file does not exist,
    /// it creates the database file using the provided default [`DatabaseRecord`].
    ///
    /// # Errors
    /// - I/O
    /// - Parsing failure
    fn try_initialize_storage<T: DatabaseRecord>(
        &self,
        default_records: impl AsRef<[T]>,
    ) -> Result<()>
    where
        Self: Database + Sized,
    {
        return self
            .try_initialize_storage_with_path(default_records, self.file_path(T::PARTITION));
    }
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
    fn dir_absolute(&self) -> PathBuf {
        std::path::absolute(self.dir()).unwrap_or(self.dir())
    }

    /// Returns a storage path with the provided file name
    fn file_path(&self, file_name: impl AsRef<Path>) -> PathBuf {
        self.dir()
            .join(file_name)
            .with_added_extension(Self::EXTENSION)
    }

    fn try_backup_storage(
        &self,
        path: impl AsRef<Path>,
        reason: impl AsRef<str>,
    ) -> Result<PathBuf> {
        let path = path.as_ref();

        let backup_path = path.with_extension(format!(
            "-{}-{}.bak",
            &chrono::Local::now().timestamp(),
            reason.as_ref()
        ));

        if let Err(e) = std::fs::copy(path, &backup_path) {
            return Err(Error::IOCopyFailure {
                path_from: path.display().to_string(),
                path_destination: backup_path.display().to_string(),
                reason: e,
            });
        };

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
///
/// This trait builds on the basic database functionality (inherited from the [`Database`] trait)
/// by introducing methods that allow multiple operations to be executed as a single transaction.
/// Transactions allow changes to be grouped so that they can be either fully committed or fully
/// rolled back, ensuring data consistency even in the face of failures.
///
/// # Transactional Context
///
/// Implementors of this trait provide a transactional context through an associated type,
/// `TransactionDB`. This context captures the state of the database at the beginning of a
/// transaction and is used to perform modifications that are not immediately persisted.
/// When the transaction is committed, all changes are applied atomically; if any operation
/// fails, the transaction can be rolled back to restore the previous state.
///
/// # Associated Types
///
/// * `TransactionDB` - A type representing the transactional context. This type must implement
///   both [`DatabaseOps`] (for performing standard database operations) and [`DatabaseIO`]
///   (for file-based input/output), ensuring that all necessary operations can be executed
///   within the transaction.
pub trait DatabaseTransaction: Database {
    /// The transactional database type.
    ///
    /// This associated type represents a temporary, mutable snapshot of the database state.
    /// It must implement [`Database`] so that it can support all required operations and
    /// file interactions during a transaction.
    type TransactionDB: Database;

    /// Begins a new transaction.
    ///
    /// This method initiates a transaction by capturing the current state of the database in a
    /// transactional context. The returned `TransactionDB` instance can be used to perform a
    /// series of operations that will either be committed atomically or discarded entirely.
    ///
    /// # Returns
    ///
    /// * `Result<Self::TransactionDB>` - On success, a transactional database instance is returned.
    ///   If the transaction cannot be initiated, an error is returned.
    fn transact(&self) -> Result<Self::TransactionDB> {
        return Ok(Self::TransactionDB::new(""));
    }

    /// Commits the current transaction.
    ///
    /// This method attempts to persist all changes made within the transactional context by
    /// writing the modified state to the underlying database storage. If the write operation
    /// fails, a rollback is initiated to revert the database to its previous state.
    ///
    /// # Type Parameters
    ///
    /// * `T` — The type of the records involved in the transaction. This type must implement
    ///   the [`DatabaseRecord`] trait.
    ///
    /// # Parameters
    ///
    /// * `transaction` — A reference to the transactional database instance that holds the
    ///   pending changes.
    ///
    /// # Returns
    ///
    /// * `Result<()>` — Returns `Ok(())` if the commit operation succeeds. If the commit fails,
    ///   an error is returned, and the transaction is rolled back to maintain data consistency.
    fn try_commit<T: DatabaseRecord>(&self, transaction: &Self::TransactionDB) -> Result<()> {
        let records = transaction.get_all::<T>()?;
        match self.replace_all::<T>(records) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.try_rollback::<T>(transaction).map_err(|e| {
                    Error::DBTransactionRollbackFailure {
                        file_path: T::PARTITION.into(),
                        reason: e.to_string(),
                    }
                })?;

                return Err(Error::DBTransactionCommitFailure {
                    file_path: T::PARTITION.into(),
                    reason: e.to_string(),
                });
            }
        }
    }

    /// Rolls back the current transaction in the given path
    ///
    /// See [`DatabaseTransaction::try_rollback`] for details and the list of possible errors.
    fn try_rollback_with_path<T: DatabaseRecord>(
        &self,
        transaction: &Self::TransactionDB,
        path: impl AsRef<Path>,
    ) -> Result<()>;

    /// Rolls back the current transaction
    ///
    /// In the event of an error or a decision not to persist the changes, this method reverts
    /// the database to its state prior to the start of the transaction. All modifications made
    /// in the transactional context are discarded.
    ///
    /// # Type Parameters
    ///
    /// * `T` — The type of the records involved in the transaction, which must implement
    ///   the [`DatabaseRecord`] trait.
    ///
    /// # Parameters
    ///
    /// * `transaction` — A reference to the transactional database instance whose changes
    ///   are to be rolled back.
    ///
    /// # Returns
    ///
    /// * `Result<()>` — Returns `Ok(())` if the rollback operation completes successfully.
    ///   Otherwise, an error is returned indicating the failure to revert the changes.
    fn try_rollback<T: DatabaseRecord>(&self, transaction: &Self::TransactionDB) -> Result<()> {
        self.try_rollback_with_path::<T>(transaction, self.file_path(T::PARTITION))
    }
}

/// Provide utility methods for DatabaseRecord.
pub trait DatabaseRecordsUtils<T: DatabaseRecord> {
    fn as_uniques(&self) -> Vec<T::Unique>;
    fn find_intersecting_uniques_from(
        &self,
        other_records: &[T],
    ) -> Vec<<T as DatabaseRecord>::Unique>;
    fn find_non_intersecting_uniques_from(
        &self,
        other_records: &[T],
    ) -> Vec<<T as DatabaseRecord>::Unique>;
    fn find_by_unique(&self, unique: &T::Unique) -> Option<&T>;
    fn find_by_unique_mut(&mut self, unique_value: &T::Unique) -> Option<&mut T>;
}

impl<T: DatabaseRecord> DatabaseRecordsUtils<T> for [T] {
    /// Returns a vector containing the unique value of each record.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::database::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #    const PARTITION: &'static str = "";
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let records = vec![Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let uniques = records.as_uniques();
    /// assert_eq!(uniques, vec![1, 2, 3]);
    /// ```
    fn as_uniques(&self) -> Vec<<T as DatabaseRecord>::Unique> {
        self.iter().map(|r| r.unique_value()).collect()
    }
    /// Returns the `Unique` values that are present in both `self` and `other_records`.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::database::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #    const PARTITION: &'static str = "";
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let a = &[Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let b = [Record { id: 2 }, Record { id: 3 }, Record { id: 4 }];
    /// let intersecting = a.find_intersecting_uniques_from(&b);
    /// assert_eq!(intersecting, vec![2, 3]);
    /// ```
    fn find_intersecting_uniques_from(
        &self,
        other_records: &[T],
    ) -> Vec<<T as DatabaseRecord>::Unique> {
        return self
            .as_uniques()
            .into_iter()
            .chain(other_records.as_uniques())
            .duplicates()
            .collect();
    }
    /// Returns the `Unique` values from `other_records` that are not present in `self`.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::database::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #    const PARTITION: &'static str = "";
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let a = &[Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let b = [Record { id: 2 }, Record { id: 3 }, Record { id: 4 }];
    /// let non_intersecting = a.find_non_intersecting_uniques_from(&b);
    /// assert_eq!(non_intersecting, vec![4]);
    /// ```
    fn find_non_intersecting_uniques_from(
        &self,
        other_records: &[T],
    ) -> Vec<<T as DatabaseRecord>::Unique> {
        let all_that_exists = self.find_intersecting_uniques_from(other_records);
        return other_records
            .as_uniques()
            .into_iter()
            .filter(|ou| !all_that_exists.contains(ou))
            .collect();
    }
    /// Returns a reference to the record with the specified unique value, if it exists.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::database::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #    const PARTITION: &'static str = "";
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let records = &[Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let record = records.find_by_unique(&2).unwrap();
    /// assert_eq!(record.id, 2);
    /// ```
    fn find_by_unique(&self, unique_value: &T::Unique) -> Option<&T> {
        self.iter().find(|r| &r.unique_value() == unique_value)
    }
    /// Returns a mutable reference to the record with the specified unique value, if it exists.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::database::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #    const PARTITION: &'static str = "";
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let mut records = [Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let record = records.find_by_unique_mut(&2).unwrap();
    /// assert_eq!(record.id, 2);
    /// ```
    fn find_by_unique_mut(&mut self, unique_value: &T::Unique) -> Option<&mut T> {
        self.iter_mut().find(|r| &r.unique_value() == unique_value)
    }
}
