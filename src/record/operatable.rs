use crate::{error::Result, prelude::Database, record::DatabaseRecordPartitioned};

pub trait OperatablePartitioned {
    type Collection;

    fn get_all(db: &impl Database) -> Result<Self::Collection>;

    fn insert(db: &impl Database, new_record: Self) -> Result<()>;

    fn insert_all(db: &impl Database, new_records: Self::Collection) -> Result<()>;

    fn update(db: &impl Database, updated_record: Self) -> Result<()>;

    fn update_all(db: &impl Database, updated_records: Self::Collection) -> Result<()>;

    fn replace_all(db: &impl Database, replaced_records: Self::Collection) -> Result<()>;

    fn try_initialize_storage(db: &impl Database, default_data: Self::Collection) -> Result<()>;
}

impl<R1> OperatablePartitioned for R1
where
    R1: DatabaseRecordPartitioned,
{
    type Collection = Vec<R1>;

    fn get_all(db: &impl Database) -> Result<Self::Collection> { return db.get_all(); }

    fn insert(db: &impl Database, new_record: Self) -> Result<()> { return db.insert(new_record); }

    fn insert_all(db: &impl Database, new_records: Self::Collection) -> Result<()> {
        return db.insert_all(new_records);
    }

    fn update(db: &impl Database, updated_record: Self) -> Result<()> {
        return db.update(updated_record);
    }

    fn update_all(db: &impl Database, updated_records: Self::Collection) -> Result<()> {
        return db.update_all(updated_records);
    }

    fn replace_all(db: &impl Database, replaced_records: Self::Collection) -> Result<()> {
        return db.replace_all(replaced_records);
    }

    fn try_initialize_storage(db: &impl Database, default_data: Self::Collection) -> Result<()> {
        return db.try_initialize_storage::<R1, Vec<R1>>(default_data);
    }
}

impl<R1, RN> OperatablePartitioned for (R1, RN)
where
    R1: DatabaseRecordPartitioned,
    RN: OperatablePartitioned,
{
    type Collection = (Vec<R1>, RN::Collection);

    fn get_all(db: &impl Database) -> Result<Self::Collection> {
        return Ok((db.get_all::<R1>()?, RN::get_all(db)?));
    }

    fn insert(db: &impl Database, new_record: Self) -> Result<()> {
        db.insert(new_record.0)?;
        return RN::insert(db, new_record.1);
    }

    fn insert_all(db: &impl Database, new_records: Self::Collection) -> Result<()> {
        db.insert_all(new_records.0)?;
        return RN::insert_all(db, new_records.1);
    }

    fn update(db: &impl Database, updated_record: Self) -> Result<()> {
        db.update(updated_record.0)?;
        return RN::update(db, updated_record.1);
    }

    fn update_all(db: &impl Database, updated_records: Self::Collection) -> Result<()> {
        db.update_all(updated_records.0)?;
        return RN::update_all(db, updated_records.1);
    }

    fn replace_all(db: &impl Database, replaced_records: Self::Collection) -> Result<()> {
        db.replace_all(replaced_records.0)?;
        return RN::replace_all(db, replaced_records.1);
    }

    fn try_initialize_storage(db: &impl Database, default_data: Self::Collection) -> Result<()> {
        db.try_initialize_storage::<R1, Vec<R1>>(default_data.0)?;
        return RN::try_initialize_storage(db, default_data.1);
    }
}
