use crate::{
    Deserialize, Result, Serialize, database::DatabasePartitioned, operations::OperationCrud,
    record::*,
};

/// Provides common database operations by using [`DatabaseRecordPartitioned::PARTITION`]
pub trait OperationCrudPartitioned: DatabasePartitioned + OperationCrud {
    /// Retrieves all [`DatabaseRecordPartitioned`] from storage.
    ///
    /// # Errors
    /// - I/O
    fn get_all<T: DatabaseRecordPartitioned>(&self) -> Result<Vec<T>> {
        return self.get_all_with_path(self.partition_path::<T>());
    }

    /// Inserts a single [`DatabaseRecordPartitioned`] into storage.
    ///
    /// The record is passed to [`OperationCrud::insert_with_path`] using the
    /// partition path for the record type.
    ///
    /// # Errors
    /// - I/O
    /// - A duplicate unique identifier is found among the existing or new records
    fn insert<T: DatabaseRecordPartitioned>(&self, new_record: T) -> Result<()> {
        return self.insert_with_path(new_record, self.partition_path::<T>());
    }

    /// Inserts multiple [`DatabaseRecordPartitioned`] into storage.
    ///
    /// The records are passed to [`OperationCrud::insert_all_with_path`] using the
    /// partition path for the record type.
    ///
    /// # Errors
    /// - I/O
    /// - A duplicate unique identifier is found among the existing or new records
    fn insert_all<T: DatabaseRecordPartitioned>(&self, new_records: impl AsRef<[T]>) -> Result<()> {
        return self.insert_all_with_path(new_records, self.partition_path::<T>());
    }

    /// Updates a single [`DatabaseRecordPartitioned`] in storage.
    ///
    /// The record is passed to [`OperationCrud::update_with_path`] using the
    /// partition path for the record type.
    ///
    /// # Errors
    /// - I/O
    /// - A record with the given unique identifier does not exist
    fn update<T: DatabaseRecordPartitioned>(&self, updated_record: T) -> Result<()> {
        return self.update_with_path(updated_record, self.partition_path::<T>());
    }

    /// Updates multiple [`DatabaseRecordPartitioned`] in storage.
    ///
    /// The records are passed to [`OperationCrud::update_all_with_path`] using the
    /// partition path for the record type.
    ///
    /// # Errors
    /// - I/O
    /// - A record with one of the given unique identifiers does not exist
    fn update_all<T: DatabaseRecordPartitioned>(
        &self,
        updated_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        return self.update_all_with_path(updated_records, self.partition_path::<T>());
    }

    /// Updates or inserts a single [`DatabaseRecordPartitioned`] into storage.
    ///
    /// The record is passed to [`OperationCrud::upsert_with_path`] using the
    /// partition path for the record type.
    ///
    /// # Errors
    /// - I/O
    fn upsert<T: DatabaseRecordPartitioned>(&self, upserted_record: T) -> Result<()> {
        return self.upsert_with_path(upserted_record, self.partition_path::<T>());
    }

    /// Updates or inserts multiple [`DatabaseRecordPartitioned`] into storage.
    ///
    /// The records are passed to [`OperationCrud::upsert_all_with_path`] using the
    /// partition path for the record type.
    ///
    /// # Errors
    /// - I/O
    fn upsert_all<T: DatabaseRecordPartitioned>(
        &self,
        upserted_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        return self.upsert_all_with_path(upserted_records, self.partition_path::<T>());
    }

    /// Replaces all [`DatabaseRecordPartitioned`] in storage with the provided records.
    ///
    /// The records are passed to [`OperationCrud::replace_all_with_path`] using the
    /// partition path for the record type.
    ///
    /// # Errors
    /// - I/O
    /// - A duplicate unique identifier is found among the provided records
    fn replace_all<T: DatabaseRecordPartitioned>(
        &self,
        replaced_records: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        return self.replace_all_with_path(replaced_records, self.partition_path::<T>());
    }

    /// Attempts to initialize the provided default data into storage.
    ///
    /// This method uses the partition path for the record type and delegates to
    /// [`OperationCrud::try_initialize_storage_with_path`]. If the storage already
    /// exists, its contents are validated. If it does not exist, it is initialized
    /// with the provided default data.
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
            .try_initialize_storage_with_path::<O>(default_data, self.partition_path::<T>());
    }
}
