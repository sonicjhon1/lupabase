use insta::assert_debug_snapshot;
use lupabase::{prelude::*, record::DatabaseRecord};
use serde::{Deserialize, Serialize};
use std::{error::Error, fs, num::NonZero, path::PathBuf};

const TMP_DIR: &str = "./files/basics/";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TestRecord {
    pub id: NonZero<u64>,
    pub data: String,
}

impl DatabaseRecord for TestRecord {
    type Unique = NonZero<u64>;

    fn unique_value(&self) -> Self::Unique { self.id }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TestRecordPartitioned {
    pub id: NonZero<u64>,
    pub data: String,
}

impl DatabaseRecord for TestRecordPartitioned {
    type Unique = NonZero<u64>;

    fn unique_value(&self) -> Self::Unique { self.id }
}

impl DatabaseRecordPartitioned for TestRecordPartitioned {
    const PARTITION: &str = "TestRecordPartitioned";
}

impl TestRecordPartitioned {
    fn new(id: &mut u64) -> Self {
        *id += 1;

        Self {
            id: NonZero::try_from(*id).expect("ID should not be Zero"),
            data: format!("My data of {id}"),
        }
    }
}

#[test]
fn basics_cbor() -> Result<(), Box<dyn Error>> {
    let working_dir = PathBuf::from(TMP_DIR).join("./cbor/");
    fs::remove_dir_all(&working_dir)?;

    let db = CborDB::new(working_dir);

    db.try_initialize_storage::<TestRecordPartitioned, Vec<TestRecordPartitioned>>(vec![])?;
    assert_debug_snapshot!("DB should be empty", db.get_all::<TestRecordPartitioned>()?);

    let id = &mut 0_u64;

    {
        db.insert(TestRecordPartitioned::new(id))?;
        assert_debug_snapshot!("DB inserted", db.get_all::<TestRecordPartitioned>()?);
    }

    {
        db.insert_all([
            TestRecordPartitioned::new(id),
            TestRecordPartitioned::new(id),
        ])?;
        assert_debug_snapshot!("DB inserted all", db.get_all::<TestRecordPartitioned>()?);
    }

    {
        let mut record = TestRecordPartitioned::new(id);
        db.insert(record.clone())?;
        record.data = String::from("Data has been updated!");
        db.update(record)?;
        assert_debug_snapshot!("DB updated", db.get_all::<TestRecordPartitioned>()?);
    }

    {
        let mut record_1 = TestRecordPartitioned::new(id);
        let mut record_2 = TestRecordPartitioned::new(id);
        let mut record_3 = TestRecordPartitioned::new(id);
        db.insert_all([record_1.clone(), record_2.clone(), record_3.clone()])?;
        record_1.data = String::from("Data 1 has been updated!");
        record_2.data = String::from("Data 2 has been updated!");
        record_3.data = String::from("Data 3 has been updated!");
        // Updating out of order should be fine!
        db.update_all([record_1, record_3, record_2])?;
        assert_debug_snapshot!("DB updated all", db.get_all::<TestRecordPartitioned>()?);
    }

    {
        let current_records = db.get_all::<TestRecordPartitioned>()?;
        db.replace_all::<TestRecordPartitioned>([])?;
        assert_debug_snapshot!(
            "DB replaced all empty",
            db.get_all::<TestRecordPartitioned>()?
        );
        db.replace_all(current_records)?;
        assert_debug_snapshot!(
            "DB replaced all restored",
            db.get_all::<TestRecordPartitioned>()?
        );
    }

    Ok(())
}
