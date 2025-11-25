pub mod tests_records;
pub mod tests_utils;

use insta::assert_debug_snapshot;
use lupabase::prelude::*;
use std::{error::Error, fs};
use tests_records::*;
use tests_utils::*;

#[test]
fn transactions_cbor() -> Result<(), Box<dyn Error>> {
    transactions_tester::<CborDB>()?;

    Ok(())
}

#[test]
fn transactions_json() -> Result<(), Box<dyn Error>> {
    transactions_tester::<JsonDB>()?;

    Ok(())
}

//TODO: MemoryDB transactions
// #[test]
// fn transactions_memory() -> Result<(), Box<dyn Error>> {
//     transactions_tester::<MemoryDB>()?;

//     Ok(())
// }

fn transactions_tester<DB: DatabaseTransaction>() -> Result<(), Box<dyn Error>> {
    init_tracing_for_tests();

    let db_name = DB::NAME;
    let tx_name = DB::TransactionDB::NAME;

    let (working_dir, _temp_dir_drop_guard) = create_temp_working_dir("transactions", db_name);

    let _ = fs::remove_dir_all(&working_dir);

    let db = DB::new(working_dir);

    {
        span_and_info!("Transaction Partitioned");

        let id = &mut 0;

        db.try_initialize_storage::<TestRecordPartitioned, Vec<TestRecordPartitioned>>(vec![])?;

        let tx = db.transact()?;

        {
            span_and_info!("Transaction", "Initialize");

            let current_records = db.get_all::<TestRecordPartitioned>()?;
            tx.try_initialize_storage::<TestRecordPartitioned, Vec<TestRecordPartitioned>>(
                current_records,
            )?;

            assert_debug_snapshot!(
                format!("{tx_name} transaction initialized"),
                tx.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Insert");

            tx.insert(TestRecordPartitioned::new(id))?;
            assert_debug_snapshot!(
                format!("{tx_name} transaction inserted"),
                tx.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Inserting all");

            tx.insert_all([
                TestRecordPartitioned::new(id),
                TestRecordPartitioned::new(id),
            ])?;
            assert_debug_snapshot!(
                format!("{tx_name} transaction inserted all"),
                tx.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Updating");

            let mut record = TestRecordPartitioned::new(id);
            tx.insert(record.clone())?;
            record.data = String::from("Data has been updated!");
            tx.update(record)?;
            assert_debug_snapshot!(
                format!("{tx_name} transaction updated"),
                tx.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Updating all");

            let mut record_1 = TestRecordPartitioned::new(id);
            let mut record_2 = TestRecordPartitioned::new(id);
            let mut record_3 = TestRecordPartitioned::new(id);
            tx.insert_all([record_1.clone(), record_2.clone(), record_3.clone()])?;
            record_1.data = String::from("Data 1 has been updated!");
            record_2.data = String::from("Data 2 has been updated!");
            record_3.data = String::from("Data 3 has been updated!");
            // Updating out of order should be fine!
            tx.update_all([record_1, record_3, record_2])?;
            assert_debug_snapshot!(
                format!("{tx_name} transaction updated all"),
                tx.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Operation", "Replacing all");

            let current_records = tx.get_all::<TestRecordPartitioned>()?;
            tx.replace_all::<TestRecordPartitioned>([])?;
            assert_debug_snapshot!(
                format!("{tx_name} transaction replaced all empty"),
                tx.get_all::<TestRecordPartitioned>()?
            );
            tx.replace_all(current_records)?;
            assert_debug_snapshot!(
                format!("{tx_name} transaction replaced all restored"),
                tx.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("TransactionOperation", "Get all before");

            assert_debug_snapshot!(
                format!("{tx_name} transaction get all before"),
                tx.get_all_before::<TestRecordPartitioned>()?
            );

            // DB's records should be synced to TX's get_all_before records
            assert!(
                db.get_all::<TestRecordPartitioned>()?
                    == tx.get_all_before::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Re-Initialize");

            tx.try_initialize_storage::<TestRecordPartitioned, Vec<TestRecordPartitioned>>(vec![])?;
            assert_debug_snapshot!(
                format!("{tx_name} transaction reinitialized"),
                tx.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Transaction", "Commit and Rollback");

            let db_records_before = db.get_all::<TestRecordPartitioned>()?;
            let tx_records_current = tx.get_all::<TestRecordPartitioned>()?;

            db.try_commit::<TestRecordPartitioned>(&tx)?;
            assert_debug_snapshot!(
                format!("{db_name} commited"),
                db.get_all::<TestRecordPartitioned>()?
            );

            // DB's records should be synced with TX's records post-commit
            assert!(db.get_all::<TestRecordPartitioned>()? == tx_records_current);

            db.try_rollback::<TestRecordPartitioned>(&tx)?;
            assert_debug_snapshot!(
                format!("{db_name} rolled back"),
                db.get_all::<TestRecordPartitioned>()?
            );

            // DB's records should be rolled back to the pre-commit records
            assert!(db.get_all::<TestRecordPartitioned>()? == db_records_before);
        }
    }

    {
        span_and_info!("Transaction Collection");

        let id = &mut 0_u64;

        let db_file_path = db.file_path("TestRecords");
        db.try_initialize_storage_with_path(vec![] as Vec<TestRecord>, &db_file_path)?;

        let tx = db.transact()?;
        let tx_file_path = tx.file_path("TestRecords");

        {
            span_and_info!("Transaction", "Initialize");

            let current_records = db.try_read_storage::<Vec<TestRecord>>(&db_file_path)?;
            tx.try_initialize_storage_with_path(current_records, &tx_file_path)?;

            assert_debug_snapshot!(
                format!("{tx_name} Storage transaction initialized"),
                tx.try_read_storage::<Vec<TestRecord>>(&tx_file_path)?
            );
        }

        {
            span_and_info!("Operation", "Read Write");

            let mut records = tx.try_read_storage::<Vec<TestRecord>>(&tx_file_path)?;
            records.push(TestRecord::new(id));
            records.push(TestRecord::new(id));
            tx.try_write_storage(records, &tx_file_path)?;
            assert_debug_snapshot!(
                format!("{tx_name} Storage transaction written"),
                tx.try_read_storage::<Vec<TestRecord>>(&tx_file_path)?
            );
        }

        {
            span_and_info!("Operation", "Backup");

            let backup_path = tx.try_backup_storage(&tx_file_path, "Manual backup")?;
            assert_debug_snapshot!(
                format!("{tx_name} Storage transaction backup"),
                tx.try_read_storage::<Vec<TestRecord>>(backup_path)?
            );
        }

        {
            span_and_info!("TransactionOperation", "Read before");

            assert_debug_snapshot!(
                format!("{tx_name} Storage transaction read before"),
                tx.try_read_storage_before::<Vec<TestRecord>>(&tx_file_path)?
            );

            // DB's records should be synced to TX's try_read_storage_before records
            assert!(
                db.try_read_storage::<Vec<TestRecord>>(&db_file_path)?
                    == tx.try_read_storage_before::<Vec<TestRecord>>(&tx_file_path)?
            );
        }

        {
            span_and_info!("Re-Initialize");

            tx.try_initialize_storage_with_path(vec![] as Vec<TestRecord>, &tx_file_path)?;
            assert_debug_snapshot!(
                format!("{tx_name} Storage transaction reinitialized"),
                tx.try_read_storage::<Vec<TestRecord>>(&tx_file_path)?
            );
        }

        {
            span_and_info!("Transaction", "Commit and Rollback");

            let db_records_before = db.try_read_storage::<Vec<TestRecord>>(&db_file_path)?;
            let tx_records_current = tx.try_read_storage::<Vec<TestRecord>>(&tx_file_path)?;

            db.try_commit_with_path::<Vec<TestRecord>>(&tx, &tx_file_path, &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Storage commited"),
                db.try_read_storage::<Vec<TestRecord>>(&db_file_path)?
            );

            // DB's records should be synced with TX's records post-commit
            assert!(db.try_read_storage::<Vec<TestRecord>>(&db_file_path)? == tx_records_current);

            db.try_rollback_with_path::<Vec<TestRecord>>(&tx, &tx_file_path, &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Storage rolled back"),
                db.try_read_storage::<Vec<TestRecord>>(&db_file_path)?
            );

            // DB's records should be rolled back to the pre-commit records
            assert!(db.try_read_storage::<Vec<TestRecord>>(&db_file_path)? == db_records_before);
        }
    }

    {
        span_and_info!("Transaction Single");

        let id = &mut 0_u64;
        let default_record = TestRecord::new(id);

        let db_file_path = db.file_path("TestRecord");
        db.try_initialize_storage_with_path(default_record.clone(), &db_file_path)?;

        let tx = db.transact()?;
        let tx_file_path = tx.file_path("TestRecord");

        {
            span_and_info!("Transaction", "Initialize");

            let current_records = db.try_read_storage::<TestRecord>(&db_file_path)?;
            tx.try_initialize_storage_with_path(current_records, &tx_file_path)?;

            assert_debug_snapshot!(
                format!("{tx_name} Single Storage transaction initialized"),
                tx.try_read_storage::<TestRecord>(&tx_file_path)?
            );
        }

        {
            span_and_info!("Operation", "Read Write");

            let mut record = tx.try_read_storage::<TestRecord>(&tx_file_path)?;
            record.data = String::from("Modified the data");
            tx.try_write_storage(record, &tx_file_path)?;
            assert_debug_snapshot!(
                format!("{tx_name} Single Storage transaction written"),
                tx.try_read_storage::<TestRecord>(&tx_file_path)?
            );
        }

        {
            span_and_info!("Operation", "Backup");

            let backup_path = tx.try_backup_storage(&tx_file_path, "Manual backup")?;
            assert_debug_snapshot!(
                format!("{tx_name} Single Storage transaction backup"),
                tx.try_read_storage::<TestRecord>(backup_path)?
            );
        }

        {
            span_and_info!("TransactionOperation", "Read before");

            assert_debug_snapshot!(
                format!("{tx_name} Single Storage transaction read before"),
                tx.try_read_storage_before::<TestRecord>(&tx_file_path)?
            );

            // DB's record should be synced to TX's try_read_storage_before record
            assert!(
                db.try_read_storage::<TestRecord>(&db_file_path)?
                    == tx.try_read_storage_before::<TestRecord>(&tx_file_path)?
            );
        }

        {
            span_and_info!("Re-Initialize");

            tx.try_initialize_storage_with_path(default_record, &tx_file_path)?;
            assert_debug_snapshot!(
                format!("{tx_name} Single Storage transaction reinitialized"),
                tx.try_read_storage::<TestRecord>(&tx_file_path)?
            );
        }

        {
            span_and_info!("Transaction", "Commit and Rollback");

            let db_records_before = db.try_read_storage::<TestRecord>(&db_file_path)?;
            let tx_records_current = tx.try_read_storage::<TestRecord>(&tx_file_path)?;

            db.try_commit_with_path::<TestRecord>(&tx, &tx_file_path, &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Single Storage commited"),
                db.try_read_storage::<TestRecord>(&db_file_path)?
            );

            // DB's record should be synced with TX's record post-commit
            assert!(db.try_read_storage::<TestRecord>(&db_file_path)? == tx_records_current);

            db.try_rollback_with_path::<TestRecord>(&tx, &tx_file_path, &db_file_path)?;
            assert_debug_snapshot!(
                format!("{db_name} Single Storage rolled back"),
                db.try_read_storage::<TestRecord>(&db_file_path)?
            );

            // DB's record should be rolled back to the pre-commit record
            assert!(db.try_read_storage::<TestRecord>(&db_file_path)? == db_records_before);
        }
    }

    Ok(())
}
