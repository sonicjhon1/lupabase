use crate::{error::Result, prelude::Database, record::DatabaseRecordPartitioned};

pub trait OperatablePartitioned {
    type Collection;

    fn get_all(db: impl Database) -> Result<Self::Collection>;

    fn insert(db: impl Database, new_record: Self) -> Result<()>;

    fn insert_all(db: impl Database, new_records: Self::Collection) -> Result<()>;

    fn update(db: impl Database, updated_record: Self) -> Result<()>;

    fn update_all(db: impl Database, updated_records: Self::Collection) -> Result<()>;

    fn replace_all(db: impl Database, replaced_records: Self::Collection) -> Result<()>;

    fn try_initialize_storage(db: impl Database, default_data: Self::Collection) -> Result<()>;
}

impl<R1> OperatablePartitioned for R1
where
    R1: DatabaseRecordPartitioned,
{
    type Collection = Vec<R1>;

    fn get_all(db: impl Database) -> Result<Self::Collection> {
        todo!()
    }

    fn insert(db: impl Database, new_record: Self) -> Result<()> {
        todo!()
    }

    fn insert_all(db: impl Database, new_records: Self::Collection) -> Result<()> {
        todo!()
    }

    fn update(db: impl Database, updated_record: Self) -> Result<()> {
        todo!()
    }

    fn update_all(db: impl Database, updated_records: Self::Collection) -> Result<()> {
        todo!()
    }

    fn replace_all(db: impl Database, replaced_records: Self::Collection) -> Result<()> {
        todo!()
    }

    fn try_initialize_storage(db: impl Database, default_data: Self::Collection) -> Result<()> {
        todo!()
    }
}

impl<R1, R2> OperatablePartitioned for (R1, R2)
where
    R1: DatabaseRecordPartitioned,
    R2: DatabaseRecordPartitioned,
{
    type Collection = (Vec<R1>, Vec<R2>);

    fn get_all(db: impl Database) -> Result<Self::Collection> {
        return Ok((db.get_all::<R1>()?, db.get_all::<R2>()?));
    }

    fn insert(db: impl Database, new_record: Self) -> Result<()> {
        db.insert(new_record.0)?;
        return db.insert(new_record.1);
    }

    fn insert_all(db: impl Database, new_records: Self::Collection) -> Result<()> {
        db.insert_all(new_records.0)?;
        return db.insert_all(new_records.1);
    }

    fn update(db: impl Database, updated_record: Self) -> Result<()> {
        db.update(updated_record.0)?;
        return db.update(updated_record.1);
    }

    fn update_all(db: impl Database, updated_records: Self::Collection) -> Result<()> {
        db.update_all(updated_records.0)?;
        return db.update_all(updated_records.1);
    }

    fn replace_all(db: impl Database, replaced_records: Self::Collection) -> Result<()> {
        db.replace_all(replaced_records.0)?;
        return db.replace_all(replaced_records.1);
    }

    fn try_initialize_storage(db: impl Database, default_data: Self::Collection) -> Result<()> {
        db.try_initialize_storage::<R1, Vec<R1>>(default_data.0)?;
        return db.try_initialize_storage::<R2, Vec<R2>>(default_data.1);
    }
}

impl<R1, R2, R3> OperatablePartitioned for (R1, R2, R3)
where
    R1: DatabaseRecordPartitioned,
    R2: DatabaseRecordPartitioned,
    R3: DatabaseRecordPartitioned,
{
    type Collection = (Vec<R1>, Vec<R2>, Vec<R3>);

    fn get_all(db: impl Database) -> Result<Self::Collection> {
        return Ok((
            db.get_all::<R1>()?,
            db.get_all::<R2>()?,
            db.get_all::<R3>()?,
        ));
    }

    fn insert(db: impl Database, new_record: Self) -> Result<()> {
        db.insert(new_record.0)?;
        db.insert(new_record.1)?;
        return db.insert(new_record.2);
    }

    fn insert_all(db: impl Database, new_records: Self::Collection) -> Result<()> {
        db.insert_all(new_records.0)?;
        db.insert_all(new_records.1)?;
        return db.insert_all(new_records.2);
    }

    fn update(db: impl Database, updated_record: Self) -> Result<()> {
        db.update(updated_record.0)?;
        db.update(updated_record.1)?;
        return db.update(updated_record.2);
    }

    fn update_all(db: impl Database, updated_records: Self::Collection) -> Result<()> {
        db.update_all(updated_records.0)?;
        db.update_all(updated_records.1)?;
        return db.update_all(updated_records.2);
    }

    fn replace_all(db: impl Database, replaced_records: Self::Collection) -> Result<()> {
        db.replace_all(replaced_records.0)?;
        db.replace_all(replaced_records.1)?;
        return db.replace_all(replaced_records.2);
    }

    fn try_initialize_storage(db: impl Database, default_data: Self::Collection) -> Result<()> {
        db.try_initialize_storage::<R1, Vec<R1>>(default_data.0)?;
        db.try_initialize_storage::<R2, Vec<R2>>(default_data.1)?;
        return db.try_initialize_storage::<R3, Vec<R3>>(default_data.2);
    }
}
