use crate::{Result, operations::OperationCrudPartitioned, record::*};

/// Provides composable, variadic database operations through
/// [`DatabaseRecordOperatablePartitioned`].
///
/// This trait extends [`OperationCrudPartitioned`] with methods that accept a
/// [`DatabaseRecordOperatablePartitioned`] implementation, allowing operations
/// to be composed across one or more [`DatabaseRecordPartitioned`] types.
pub trait OperationOperatablePartitioned: OperationCrudPartitioned + Sized {
    /// Retrieves all records represented by `R`.
    ///
    /// See [`OperationCrudPartitioned::get_all`] for details and the list of
    /// possible errors.
    fn get_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
    ) -> Result<R::Collection> {
        return R::get_all(self);
    }

    /// Inserts a single record represented by `R`.
    ///
    /// See [`OperationCrudPartitioned::insert`] for details and the list of
    /// possible errors.
    fn insert_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        new_record: R,
    ) -> Result<()> {
        return R::insert(self, new_record);
    }

    /// Inserts multiple records represented by `R`.
    ///
    /// See [`OperationCrudPartitioned::insert_all`] for details and the list of
    /// possible errors.
    fn insert_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        new_records: R::Collection,
    ) -> Result<()> {
        return R::insert_all(self, new_records);
    }

    /// Updates a single record represented by `R`.
    ///
    /// See [`OperationCrudPartitioned::update`] for details and the list of
    /// possible errors.
    fn update_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        updated_record: R,
    ) -> Result<()> {
        return R::update(self, updated_record);
    }

    /// Updates multiple records represented by `R`.
    ///
    /// See [`OperationCrudPartitioned::update_all`] for details and the list of
    /// possible errors.
    fn update_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        updated_records: R::Collection,
    ) -> Result<()> {
        return R::update_all(self, updated_records);
    }

    /// Updates or inserts a single record represented by `R`.
    ///
    /// See [`OperationCrudPartitioned::upsert`] for details and the list of
    /// possible errors.
    fn upsert_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        upserted_record: R,
    ) -> Result<()> {
        return R::upsert(self, upserted_record);
    }

    /// Updates or inserts multiple records represented by `R`.
    ///
    /// See [`OperationCrudPartitioned::upsert_all`] for details and the list of
    /// possible errors.
    fn upsert_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        upserted_records: R::Collection,
    ) -> Result<()> {
        return R::upsert_all(self, upserted_records);
    }

    /// Replaces all records represented by `R`.
    ///
    /// See [`OperationCrudPartitioned::replace_all`] for details and the list
    /// of possible errors.
    fn replace_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        replaced_records: R::Collection,
    ) -> Result<()> {
        return R::replace_all(self, replaced_records);
    }

    /// Attempts to initialize storage for all records represented by `R`.
    ///
    /// See [`OperationCrudPartitioned::try_initialize_storage`] for details and
    /// the list of possible errors.
    fn try_initialize_storage_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        default_data: R::Collection,
    ) -> Result<()> {
        return R::try_initialize_storage(self, default_data);
    }
}

impl<DB: OperationCrudPartitioned> OperationOperatablePartitioned for DB {}
