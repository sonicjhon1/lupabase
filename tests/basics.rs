pub mod tests_records;
pub mod tests_utils;

use insta::assert_debug_snapshot;
use lupabase::prelude::*;
use std::{error::Error, fs};
use tests_records::*;
use tests_utils::*;

#[test]
fn basics_cbor() -> Result<(), Box<dyn Error>> {
    basics_tester::<CborDB>()?;

    Ok(())
}

#[test]
fn basics_json() -> Result<(), Box<dyn Error>> {
    basics_tester::<JsonDB>()?;

    Ok(())
}

#[test]
fn basics_memory() -> Result<(), Box<dyn Error>> {
    basics_tester::<MemoryDB>()?;

    Ok(())
}

fn basics_tester<DB: Database>() -> Result<(), Box<dyn Error>> {
    init_tracing_for_tests();

    let db_name = DB::NAME;

    let (working_dir, _temp_dir_drop_guard) = create_temp_working_dir("basics", db_name);

    let _ = fs::remove_dir_all(&working_dir);

    let db = DB::new(working_dir);

    {
        span_and_info!("Partitioned");

        {
            span_and_info!("Initialize");

            db.try_initialize_storage::<TestRecordPartitioned, Vec<TestRecordPartitioned>>(vec![])?;
            assert_debug_snapshot!(
                format!("{db_name} initialized should be empty"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        let id = &mut 0_u64;

        {
            span_and_info!("Operation", "Insert");

            db.insert(TestRecordPartitioned::new(id))?;
            assert_debug_snapshot!(
                format!("{db_name} inserted"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Inserting all");

            db.insert_all([
                TestRecordPartitioned::new(id),
                TestRecordPartitioned::new(id),
            ])?;
            assert_debug_snapshot!(
                format!("{db_name} inserted all"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Updating");

            let mut record = TestRecordPartitioned::new(id);
            db.insert(record.clone())?;
            record.data = String::from("Data has been updated!");
            db.update(record)?;
            assert_debug_snapshot!(
                format!("{db_name} updated"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Updating all");

            let mut record_1 = TestRecordPartitioned::new(id);
            let mut record_2 = TestRecordPartitioned::new(id);
            let mut record_3 = TestRecordPartitioned::new(id);
            db.insert_all([record_1.clone(), record_2.clone(), record_3.clone()])?;
            record_1.data = String::from("Data 1 has been updated!");
            record_2.data = String::from("Data 2 has been updated!");
            record_3.data = String::from("Data 3 has been updated!");
            // Updating out of order should be fine!
            db.update_all([record_1, record_3, record_2])?;
            assert_debug_snapshot!(
                format!("{db_name} updated all"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Upserting");

            let mut record = TestRecordPartitioned::new(id);
            db.upsert(record.clone())?;
            record.data = String::from("Data has been upserted!");
            db.upsert(record)?;
            assert_debug_snapshot!(
                format!("{db_name} upserted"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Upserting all");

            let mut record_1 = TestRecordPartitioned::new(id);
            let mut record_2 = TestRecordPartitioned::new(id);
            let mut record_3 = TestRecordPartitioned::new(id);
            // We will upsert record_3 later
            db.upsert_all([record_1.clone(), record_2.clone()])?;
            record_1.data = String::from("Data 1 has been upserted!");
            record_2.data = String::from("Data 2 has been upserted!");
            record_3.data = String::from("Data 3 has been upserted!");
            // Upserting out of order should be fine!
            // Upserted record_3
            db.upsert_all([record_1, record_3, record_2])?;
            assert_debug_snapshot!(
                format!("{db_name} upserted all"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Replacing all");

            let current_records = db.get_all::<TestRecordPartitioned>()?;
            db.replace_all::<TestRecordPartitioned>([])?;
            assert_debug_snapshot!(
                format!("{db_name} replaced all empty"),
                db.get_all::<TestRecordPartitioned>()?
            );
            db.replace_all(current_records)?;
            assert_debug_snapshot!(
                format!("{db_name} replaced all restored"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Re-Initialize");

            db.try_initialize_storage::<TestRecordPartitioned, Vec<TestRecordPartitioned>>(vec![])?;
            assert_debug_snapshot!(
                format!("{db_name} reinitialized"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }
    }

    {
        span_and_info!("Collection");

        let db_file_path = db.file_path("TestRecords");

        {
            span_and_info!("Initialize");

            db.try_initialize_storage_with_path(vec![] as Vec<TestRecord>, &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Storage should be empty"),
                db.try_read_storage::<Vec<TestRecord>>(&db_file_path)?
            );
        }

        let id = &mut 0_u64;

        {
            span_and_info!("Operation", "Read Write");

            let mut records = db.try_read_storage::<Vec<TestRecord>>(&db_file_path)?;
            records.push(TestRecord::new(id));
            records.push(TestRecord::new(id));
            db.try_write_storage(records, &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Storage written"),
                db.try_read_storage::<Vec<TestRecord>>(&db_file_path)?
            );
        }

        {
            span_and_info!("Operation", "Backup");

            let backup_path = db.try_backup_storage(&db_file_path, "Manual backup")?;
            assert_debug_snapshot!(
                format!("{db_name} Storage backup"),
                db.try_read_storage::<Vec<TestRecord>>(backup_path)?
            );
        }

        {
            span_and_info!("Re-Initialize");

            db.try_initialize_storage_with_path(vec![] as Vec<TestRecord>, &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Storage reinitialized"),
                db.try_read_storage::<Vec<TestRecord>>(db_file_path)?
            );
        }
    }

    {
        span_and_info!("Single");

        let db_file_path = db.file_path("TestRecord");

        let id = &mut 0_u64;
        let default_record = TestRecord::new(id);

        {
            span_and_info!("Initialize");

            db.try_initialize_storage_with_path(default_record.clone(), &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Single Storage should contain the record"),
                db.try_read_storage::<TestRecord>(&db_file_path)?
            );
        }

        {
            span_and_info!("Operation", "Read Write");

            let mut record = db.try_read_storage::<TestRecord>(&db_file_path)?;
            record.data = String::from("Modified the data");
            db.try_write_storage(record, &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Single Storage written"),
                db.try_read_storage::<TestRecord>(&db_file_path)?
            );
        }

        {
            span_and_info!("Operation", "Backup");

            let backup_path = db.try_backup_storage(&db_file_path, "Manual backup")?;
            assert_debug_snapshot!(
                format!("{db_name} Single Storage backup"),
                db.try_read_storage::<TestRecord>(backup_path)?
            );
        }

        {
            span_and_info!("Re-Initialize");

            db.try_initialize_storage_with_path(default_record, &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Single Storage reinitialized"),
                db.try_read_storage::<TestRecord>(db_file_path)?
            );
        }
    }

    Ok(())
}
