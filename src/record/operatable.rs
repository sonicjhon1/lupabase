use crate::{error::Result, prelude::OperationCrudPartitioned, record::DatabaseRecordPartitioned};

/// Provides composable, variadic database operations for [`DatabaseRecordPartitioned`].
///
/// This trait provides a uniform interface for performing CRUD operations over
/// one or more [`DatabaseRecordPartitioned`] types. Implementations may compose
/// multiple record types into a tuple, allowing the same operation to be
/// performed across their respective partitions.
pub trait DatabaseRecordOperatablePartitioned {
    /// The collection type containing all records represented by this operation.
    type Collection;

    /// Retrieves all records from the corresponding partition(s).
    ///
    /// See [`OperationCrudPartitioned::get_all`] for details and the list of
    /// possible errors.
    fn get_all(db: &impl OperationCrudPartitioned) -> Result<Self::Collection>;

    /// Inserts a single record into the corresponding partition(s).
    ///
    /// See [`OperationCrudPartitioned::insert`] for details and the list of
    /// possible errors.
    fn insert(db: &impl OperationCrudPartitioned, new_record: Self) -> Result<()>;

    /// Inserts multiple records into the corresponding partition(s).
    ///
    /// See [`OperationCrudPartitioned::insert_all`] for details and the list of
    /// possible errors.
    fn insert_all(db: &impl OperationCrudPartitioned, new_records: Self::Collection) -> Result<()>;

    /// Updates a single record in the corresponding partition(s).
    ///
    /// See [`OperationCrudPartitioned::update`] for details and the list of
    /// possible errors.
    fn update(db: &impl OperationCrudPartitioned, updated_record: Self) -> Result<()>;

    /// Updates multiple records in the corresponding partition(s).
    ///
    /// See [`OperationCrudPartitioned::update_all`] for details and the list of
    /// possible errors.
    fn update_all(
        db: &impl OperationCrudPartitioned,
        updated_records: Self::Collection,
    ) -> Result<()>;

    /// Updates or inserts a single record in the corresponding partition(s).
    ///
    /// See [`OperationCrudPartitioned::upsert`] for details and the list of
    /// possible errors.
    fn upsert(db: &impl OperationCrudPartitioned, upserted_record: Self) -> Result<()>;

    /// Updates or inserts multiple records in the corresponding partition(s).
    ///
    /// See [`OperationCrudPartitioned::upsert_all`] for details and the list of
    /// possible errors.
    fn upsert_all(
        db: &impl OperationCrudPartitioned,
        upserted_records: Self::Collection,
    ) -> Result<()>;

    /// Replaces all records in the corresponding partition(s).
    ///
    /// See [`OperationCrudPartitioned::replace_all`] for details and the list of
    /// possible errors.
    fn replace_all(
        db: &impl OperationCrudPartitioned,
        replaced_records: Self::Collection,
    ) -> Result<()>;

    /// Attempts to initialize storage for the corresponding partition(s).
    ///
    /// See [`OperationCrudPartitioned::try_initialize_storage`] for details and
    /// the list of possible errors.
    fn try_initialize_storage(
        db: &impl OperationCrudPartitioned,
        default_data: Self::Collection,
    ) -> Result<()>;
}

impl<R1> DatabaseRecordOperatablePartitioned for R1
where
    R1: DatabaseRecordPartitioned,
{
    type Collection = Vec<R1>;

    fn get_all(db: &impl OperationCrudPartitioned) -> Result<Self::Collection> {
        return db.get_all();
    }

    fn insert(db: &impl OperationCrudPartitioned, new_record: Self) -> Result<()> {
        return db.insert(new_record);
    }

    fn insert_all(db: &impl OperationCrudPartitioned, new_records: Self::Collection) -> Result<()> {
        return db.insert_all(new_records);
    }

    fn update(db: &impl OperationCrudPartitioned, updated_record: Self) -> Result<()> {
        return db.update(updated_record);
    }

    fn update_all(
        db: &impl OperationCrudPartitioned,
        updated_records: Self::Collection,
    ) -> Result<()> {
        return db.update_all(updated_records);
    }

    fn upsert(db: &impl OperationCrudPartitioned, upserted_record: Self) -> Result<()> {
        return db.upsert(upserted_record);
    }

    fn upsert_all(
        db: &impl OperationCrudPartitioned,
        upserted_records: Self::Collection,
    ) -> Result<()> {
        return db.upsert_all(upserted_records);
    }

    fn replace_all(
        db: &impl OperationCrudPartitioned,
        replaced_records: Self::Collection,
    ) -> Result<()> {
        return db.replace_all(replaced_records);
    }

    fn try_initialize_storage(
        db: &impl OperationCrudPartitioned,
        default_data: Self::Collection,
    ) -> Result<()> {
        return db.try_initialize_storage::<R1, Vec<R1>>(default_data);
    }
}

impl<R1, RN> DatabaseRecordOperatablePartitioned for (R1, RN)
where
    R1: DatabaseRecordPartitioned,
    RN: DatabaseRecordOperatablePartitioned,
{
    type Collection = (Vec<R1>, RN::Collection);

    fn get_all(db: &impl OperationCrudPartitioned) -> Result<Self::Collection> {
        return Ok((db.get_all::<R1>()?, RN::get_all(db)?));
    }

    fn insert(db: &impl OperationCrudPartitioned, new_record: Self) -> Result<()> {
        db.insert(new_record.0)?;
        return RN::insert(db, new_record.1);
    }

    fn insert_all(db: &impl OperationCrudPartitioned, new_records: Self::Collection) -> Result<()> {
        db.insert_all(new_records.0)?;
        return RN::insert_all(db, new_records.1);
    }

    fn update(db: &impl OperationCrudPartitioned, updated_record: Self) -> Result<()> {
        db.update(updated_record.0)?;
        return RN::update(db, updated_record.1);
    }

    fn update_all(
        db: &impl OperationCrudPartitioned,
        updated_records: Self::Collection,
    ) -> Result<()> {
        db.update_all(updated_records.0)?;
        return RN::update_all(db, updated_records.1);
    }

    fn upsert(db: &impl OperationCrudPartitioned, upserted_record: Self) -> Result<()> {
        db.upsert(upserted_record.0)?;
        return RN::upsert(db, upserted_record.1);
    }

    fn upsert_all(
        db: &impl OperationCrudPartitioned,
        upserted_records: Self::Collection,
    ) -> Result<()> {
        db.upsert_all(upserted_records.0)?;
        return RN::upsert_all(db, upserted_records.1);
    }

    fn replace_all(
        db: &impl OperationCrudPartitioned,
        replaced_records: Self::Collection,
    ) -> Result<()> {
        db.replace_all(replaced_records.0)?;
        return RN::replace_all(db, replaced_records.1);
    }

    fn try_initialize_storage(
        db: &impl OperationCrudPartitioned,
        default_data: Self::Collection,
    ) -> Result<()> {
        db.try_initialize_storage::<R1, Vec<R1>>(default_data.0)?;
        return RN::try_initialize_storage(db, default_data.1);
    }
}
