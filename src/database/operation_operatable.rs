use crate::{Result, database::*, record::*};

pub trait DatabaseOpsOperatable: DatabaseOps + Sized {
    fn get_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
    ) -> Result<R::Collection> {
        return R::get_all(self);
    }

    fn insert_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        new_record: R,
    ) -> Result<()> {
        return R::insert(self, new_record);
    }

    fn insert_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        new_records: R::Collection,
    ) -> Result<()> {
        return R::insert_all(self, new_records);
    }

    fn update_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        updated_record: R,
    ) -> Result<()> {
        return R::update(self, updated_record);
    }

    fn update_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        updated_records: R::Collection,
    ) -> Result<()> {
        return R::update_all(self, updated_records);
    }

    fn upsert_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        upserted_record: R,
    ) -> Result<()> {
        return R::upsert(self, upserted_record);
    }

    fn upsert_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        upserted_records: R::Collection,
    ) -> Result<()> {
        return R::upsert_all(self, upserted_records);
    }

    fn replace_all_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        replaced_records: R::Collection,
    ) -> Result<()> {
        return R::replace_all(self, replaced_records);
    }

    fn try_initialize_storage_with_operatable<R: DatabaseRecordOperatablePartitioned>(
        &self,
        default_data: R::Collection,
    ) -> Result<()> {
        return R::try_initialize_storage(self, default_data);
    }
}

impl<DB: DatabaseOps> DatabaseOpsOperatable for DB {}
