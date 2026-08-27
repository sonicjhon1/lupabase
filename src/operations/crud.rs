use crate::{
    Deserialize, Error, Result, Serialize,
    database::*,
    record::{utils::*, *},
    utils::*,
};
use std::{borrow::Borrow, path::Path};

/// Provides common database operations with arbitrary paths for [`Database`].
///
/// This trait supplies generic implementations for initializing, retrieving,
/// inserting, updating, upserting, and replacing records in the database.
pub trait OperationCrud: Database {
    /// Reads all [`DatabaseRecord`] from the given path.
    ///
    /// # Errors
    /// - I/O
    fn get_all_with_path<T: DatabaseRecord>(&self, path: impl AsRef<Path>) -> Result<Vec<T>> {
        return self.try_read_storage::<Vec<T>>(path);
    }

    /// Inserts a single [`DatabaseRecord`] into the given path.
    ///
    /// The record is wrapped into a slice and passed to
    /// [`OperationCrud::insert_all_with_path`].
    ///
    /// # Errors
    /// - I/O
    /// - A duplicate unique identifier is found among the existing or new records
    fn insert_with_path<T: DatabaseRecord>(
        &self,
        updated_record: T,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return self.insert_all_with_path([updated_record], path);
    }

    /// Inserts multiple [`DatabaseRecord`] into the given path.
    ///
    /// # Errors
    /// - I/O
    /// - A duplicate unique identifier is found among the existing or new records
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

    /// Updates a single [`DatabaseRecord`] in the given path.
    ///
    /// The record is wrapped into a slice and passed to
    /// [`OperationCrud::update_all_with_path`].
    ///
    /// # Errors
    /// - I/O
    /// - A record with the given unique identifier does not exist
    fn update_with_path<T: DatabaseRecord>(
        &self,
        updated_record: T,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return self.update_all_with_path([updated_record], path);
    }

    /// Updates multiple [`DatabaseRecord`] in the given path.
    ///
    /// # Errors
    /// - I/O
    /// - A record with one of the given unique identifiers does not exist
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

    /// Updates or inserts a single [`DatabaseRecord`] into the given path.
    ///
    /// The record is wrapped into a slice and passed to
    /// [`OperationCrud::upsert_all_with_path`].
    ///
    /// # Errors
    /// - I/O
    fn upsert_with_path<T: DatabaseRecord>(
        &self,
        upserted_record: T,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        return self.upsert_all_with_path([upserted_record], path);
    }

    /// Updates or inserts multiple [`DatabaseRecord`] into the given path.
    ///
    /// # Errors
    /// - I/O
    fn upsert_all_with_path<T: DatabaseRecord>(
        &self,
        upserted_records: impl IntoIterator<Item = T>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let mut records = self.get_all_with_path(&path)?;

        for upserted_record in upserted_records.into_iter() {
            if let Some(record) = records.find_by_unique_mut(&upserted_record.unique_value()) {
                *record = upserted_record;
            } else {
                records.push(upserted_record);
            }
        }

        return self.try_write_storage(records, path);
    }

    /// Replaces all [`DatabaseRecord`] in the given path with the provided records.
    ///
    /// # Errors
    /// - I/O
    /// - A duplicate unique identifier is found among the provided records
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

    /// Attempts to initialize the provided default data into the given storage path.
    ///
    /// # Errors
    /// - I/O
    fn try_initialize_storage_with_path<O: Serialize + for<'a> Deserialize<'a> + Borrow<O>>(
        &self,
        default_data: O,
        path: impl AsRef<Path>,
    ) -> Result<()>;
}
