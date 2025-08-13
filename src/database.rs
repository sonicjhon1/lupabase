use crate::{Deserialize, Error, Result, Serialize};
use itertools::Itertools;
use std::{
    fmt::Debug,
    hash::Hash,
    path::{Path, PathBuf},
};

pub trait IntoUnique: Hash + Eq + Debug + Serialize {}
impl<T: Hash + Eq + Debug + Serialize> IntoUnique for T {}

/// Represents a record that can be stored in the database.
///
/// This trait enforces that any record is both serializable and deserializable,
/// and that it can provide a unique identifier for itself. The unique identifier is
/// critical for performing operations like finding, inserting, updating, or deleting a specific record.
///
/// # Associated Types
///
/// * `Unique` - The type of the unique identifier for the record. This type must implement
///   `Hash`, `Eq`, and `Debug` to ensure proper comparison and debugging capabilities.
///
/// # Constants
///
/// * `PARTITION` - A static string that specifies the partition where records
///   of this type are stored. This allows the database to organize records by type or category.
pub trait DatabaseRecord: Serialize + for<'a> Deserialize<'a> {
    type Unique: IntoUnique;
    const PARTITION: &'static str;

    /// Returns the unique identifier of the record.
    ///
    /// This method should provide a value that uniquely identifies the record among all records
    /// of the same type. It is essential for operations that require matching, updating, or deleting
    /// specific records in the database.
    fn unique_value(&self) -> Self::Unique;
}

/// Represents a database that provides operations for reading, writing,
/// and managing records, built upon the functionality provided by `DatabaseOps` and `DatabaseIO`.
///
/// A type implementing this trait can be instantiated with a base directory, and provides
/// methods to retrieve the database directory (both relative and absolute) as well as to build
/// full file paths within that directory.
pub trait Database: DatabaseOps + DatabaseIO {
    const NAME: &'static str;

    /// Creates a new instance of the database with the specified base directory.
    ///
    /// # Parameters
    ///
    /// * `dir` - A value that can be referenced as a `Path`, representing the base directory
    ///   where the database files will be stored.
    ///
    /// # Returns
    ///
    /// * `Self` - A new instance of the database.
    fn new(dir: impl AsRef<Path>) -> Self;
    /// Returns the base directory of the database as a `PathBuf`.
    ///
    /// This directory is used as the root for all database file operations.
    fn dir(&self) -> PathBuf;
    /// Returns the absolute path of the database's base directory.
    ///
    /// This method attempts to convert the relative directory returned by `dir()` into an absolute path.
    /// If obtaining an absolute path fails, it falls back to returning the original directory.
    fn dir_absolute(&self) -> PathBuf {
        std::path::absolute(self.dir()).unwrap_or(self.dir())
    }
    /// Constructs a full file path by joining the database's base directory with the provided file name.
    ///
    /// # Parameters
    ///
    /// * `file_name` - A value that can be referenced as a `Path`, representing the name (or relative path)
    ///   of the file within the database directory.
    ///
    /// # Returns
    ///
    /// * `PathBuf` - The full file path corresponding to the given file name within the database directory.
    fn file_path(&self, file_name: impl AsRef<Path>) -> PathBuf;
}

/// Provides common database operations built on top of the `DatabaseIO` functionality.
///
/// This trait supplies generic implementations for retrieving, inserting, updating,
/// and replacing records in the database. It leverages helper methods such as
/// `try_read_file` and `try_write_file` from the `DatabaseIO` trait and uses the
/// unique identifier provided by the `DatabaseRecord` trait to detect conflicts.
pub trait DatabaseOps: DatabaseIO {
    /// Retrieves all records of type `T` from the database.
    ///
    /// This method reads and returns a vector of records using the underlying file I/O mechanism.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The underlying file read or write operations fail.
    /// - Duplicate unique identifiers are detected in the records.
    fn get_all<T: DatabaseRecord>(&self) -> Result<Vec<T>> {
        return self.try_read_file::<T, Vec<T>>();
    }
    /// Inserts a single record of type `T` into the database.
    ///
    /// The record is wrapped in a slice and passed to `insert_all`.
    /// If the insertion fails (for example, due to a duplicate unique identifier),
    /// an error is returned.
    fn insert<T: DatabaseRecord>(&self, updated_record: T) -> Result<()> {
        return self.insert_all([updated_record]);
    }
    /// Inserts multiple records into the database.
    ///
    /// This method retrieves the current records and compares them with the new records
    /// using their unique identifiers. If any new record has a unique identifier that already
    /// exists in the current records, the method returns an error indicating the duplicate values.
    /// Otherwise, it concatenates the existing records and the new records (as references) and writes
    /// the combined collection back to the database using the underlying file I/O mechanism.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The underlying file read or write operations fail.
    /// - A duplicate unique identifier is found among the new records.
    fn insert_all<T: DatabaseRecord>(&self, new_records: impl AsRef<[T]>) -> Result<()> {
        let records = self.get_all::<T>()?;
        let new_records = new_records.as_ref();

        let duplicates = &records.find_intersecting_uniques_from(new_records);
        if !duplicates.is_empty() {
            return Err(Error::DBOperationFailure {
                partition: T::PARTITION.into(),
                reason: format!(
                    "Found duplicate Unique value(s) in record(s) when inserting: [{duplicates:#?}].",
                ),
            });
        };

        return self.try_write_file::<T>(
            records
                .iter()
                .chain(new_records.iter())
                .collect::<Vec<&T>>(),
        );
    }
    /// Updates a single record of type `T` in the database.
    ///
    /// The record is wrapped in a slice and passed to `update_all`.
    /// If the update fails (for example, due to a missing matching record),
    /// an error is returned.
    fn update<T: DatabaseRecord>(&self, updated_record: T) -> Result<()> {
        return self.update_all(vec![updated_record]);
    }
    /// Updates multiple records in the database.
    ///
    /// This method retrieves all current records and attempts to update each one with a corresponding
    /// record from the provided collection. Each updated record is matched by its unique identifier.
    /// If any record in the updated collection does not have a matching record in the current database,
    /// the method returns an error indicating the missing unique identifier(s).
    /// Otherwise, the records are updated in place and the new collection is written back to the database.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The current records cannot be retrieved.
    /// - A record in the updated collection does not match any record in the current database.
    /// - The underlying file read or write operations fail.
    fn update_all<T: DatabaseRecord>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        let mut records = self.get_all::<T>()?;
        let updated_records: Vec<T> = updated_records.into_iter().collect();

        let non_matching = &records.find_non_intersecting_uniques_from(&updated_records);
        if !non_matching.is_empty() {
            return Err(Error::DBOperationFailure {
                partition: T::PARTITION.into(),
                reason: format!(
                    "Found non-matching Unique value(s) in record(s) when updating: [{non_matching:?}].",
                ),
            });
        };

        updated_records.into_iter().for_each(|ur| {
            let record = records
                .find_by_unique_mut(&ur.unique_value())
                .expect("All records should exist as it was checked before.");
            *record = ur;
        });
        return self.try_write_file::<T>(records);
    }
    /// Replaces all records with the provided updated records.
    ///
    /// This function iterates through the updated records and ensures that each record has a unique
    /// identifier within the provided collection. If any duplicate unique identifier is encountered,
    /// it returns an error. Otherwise, the new collection of records is written to the database using
    /// the underlying file I/O mechanism.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A duplicate unique identifier is found among the updated records.
    /// - The underlying file read or write operations fail.
    fn replace_all<T: DatabaseRecord>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        let mut records: Vec<T> = vec![];

        for ur in updated_records.into_iter() {
            if let Some(duplicate_record) = records.find_by_unique(&ur.unique_value()) {
                return Err(Error::DBOperationFailure {
                    partition: T::PARTITION.into(),
                    reason: format!(
                        "Found duplicated unique value in records when replacing: [{:?}].",
                        duplicate_record.unique_value()
                    ),
                });
            }

            records.push(ur);
        }

        return self.try_write_file::<T>(records);
    }
}

/// Defines operations for file-based database I/O.
///
/// This trait provides methods to initialize, write to, and read from a database file.
pub trait DatabaseIO {
    /// Attempts to initialize the database file with the provided default records.
    ///
    /// This method checks if the file already exists and validates its contents,
    /// returning an error if the file content is corrupted. If the file does not exist,
    /// it creates the database file using the provided default records.
    ///
    /// The `default_records` parameter is generic over any type that can be referenced as
    /// a slice of records (`AsRef<[T]>`), which allows for flexibility in the types of
    /// collections that can be passed in (e.g., a `Vec<T>` or a slice `&[T]`).
    ///
    /// # Type Parameters
    ///
    /// - `T`: The record type, which must implement the `DatabaseRecord` trait.
    ///
    /// # Parameters
    ///
    /// * `default_records` - A collection of default records to initialize the file with.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Returns `Ok(())` if the file is successfully initialized,
    ///   or an `Err(Error)` if the initialization fails.
    fn try_initialize_file<T: DatabaseRecord>(
        &self,
        default_records: impl AsRef<[T]>,
    ) -> Result<()>;
    /// Attempts to write the provided data to the database file.
    ///
    /// This method serializes the given `data` and writes it to the file. The data
    /// must implement the `Serialize` trait to be converted into a storable format.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the records, which must implement `DatabaseRecord`.
    ///
    /// # Parameters
    ///
    /// * `data` - The data to serialize and write to the file.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Returns `Ok(())` if the write operation succeeds,
    ///   or an error if it fails.
    fn try_write_file<T: DatabaseRecord>(&self, data: impl Serialize) -> Result<()>;
    /// Attempts to read data from the database file and deserialize it into the specified type.
    ///
    /// This method reads the file's content and attempts to deserialize it into an instance
    /// of type `O`. The output type must implement `Deserialize` so that the file content
    /// can be correctly parsed.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the records, which must implement `DatabaseRecord`.
    /// * `O` - The output type into which the file data should be deserialized.
    ///   This type must implement `Deserialize`.
    ///
    /// # Returns
    ///
    /// * `Result<O>` - On success, returns the deserialized data; otherwise, returns an error.
    fn try_read_file<T: DatabaseRecord, O: for<'a> Deserialize<'a>>(&self) -> Result<O>;
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
    /// Rolls back the current transaction.
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
    fn try_rollback<T: DatabaseRecord>(&self, transaction: &Self::TransactionDB) -> Result<()>;
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
