use crate::{
    Deserialize, Error, Result, Serialize,
    database::*,
    record::{utils::*, *},
    utils::*,
};
use std::{borrow::Borrow, path::Path};

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
