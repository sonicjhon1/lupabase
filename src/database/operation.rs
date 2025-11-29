use crate::{Deserialize, Result, Serialize, database::*, record::*};

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
    fn insert<T: DatabaseRecordPartitioned>(&self, new_record: T) -> Result<()> {
        return self.insert_with_path(new_record, self.file_path(T::PARTITION));
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

    /// Updates or inserts a single [`DatabaseRecordPartitioned`] into storage.
    /// The record is wrapped into a slice and passed to [`DatabaseOps::upsert_all`].
    ///
    /// See [`DatabaseOps::upsert_all`] for details and the list of possible errors.
    fn upsert<T: DatabaseRecordPartitioned>(&self, upserted_record: T) -> Result<()> {
        return self.upsert_with_path(upserted_record, self.file_path(T::PARTITION));
    }

    /// Updates or inserts multiple [`DatabaseRecordPartitioned`] into storage
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifier is found among the upserted records
    fn upsert_all<T: DatabaseRecordPartitioned>(
        &self,
        upserted_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        return self.upsert_all_with_path(upserted_records, self.file_path(T::PARTITION));
    }

    /// Replace all [`DatabaseRecordPartitioned`] in storage with the provided [`DatabaseRecordPartitioned`]
    ///
    /// # Errors
    /// - I/O
    /// - Duplicate unique identifier is found among the updated records
    fn replace_all<T: DatabaseRecordPartitioned>(
        &self,
        replaced_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        return self.replace_all_with_path(replaced_records, self.file_path(T::PARTITION));
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
    ) -> Result<()> {
        return self
            .try_initialize_storage_with_path::<O>(default_data, self.file_path(T::PARTITION));
    }
}
