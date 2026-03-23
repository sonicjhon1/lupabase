use crate::{Result, database::*, record::*};

/// Provides a composable variadic database operations for [`DatabaseOps`]
///
/// See [`DatabaseOps`] for details and the list of possible errors.
pub trait DatabaseOpsOperatable: DatabaseOps + Sized {
    /// See [`DatabaseOps::get_all`] for details and the list of possible errors.
    fn get_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
    ) -> Result<R::Collection> {
        return R::get_all(self);
    }

    /// See [`DatabaseOps::insert`] for details and the list of possible errors.
    fn insert_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        new_record: R,
    ) -> Result<()> {
        return R::insert(self, new_record);
    }

    /// See [`DatabaseOps::insert_all`] for details and the list of possible errors.
    fn insert_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        new_records: R::Collection,
    ) -> Result<()> {
        return R::insert_all(self, new_records);
    }

    /// See [`DatabaseOps::update`] for details and the list of possible errors.
    fn update_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        updated_record: R,
    ) -> Result<()> {
        return R::update(self, updated_record);
    }

    /// See [`DatabaseOps::update_all`] for details and the list of possible errors.
    fn update_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        updated_records: R::Collection,
    ) -> Result<()> {
        return R::update_all(self, updated_records);
    }

    /// See [`DatabaseOps::upsert`] for details and the list of possible errors.
    fn upsert_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        upserted_record: R,
    ) -> Result<()> {
        return R::upsert(self, upserted_record);
    }

    /// See [`DatabaseOps::upsert_all`] for details and the list of possible errors.
    fn upsert_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        upserted_records: R::Collection,
    ) -> Result<()> {
        return R::upsert_all(self, upserted_records);
    }

    /// See [`DatabaseOps::replace_all`] for details and the list of possible errors.
    fn replace_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        replaced_records: R::Collection,
    ) -> Result<()> {
        return R::replace_all(self, replaced_records);
    }

    /// See [`DatabaseOps::try_initialize_storage`] for details and the list of possible errors.
    fn try_initialize_storage_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        default_data: R::Collection,
    ) -> Result<()> {
        return R::try_initialize_storage(self, default_data);
    }
}

impl<DB: DatabaseOps> DatabaseOpsOperatable for DB {}
