pub mod tests_records;
pub mod tests_utils;

use insta::assert_debug_snapshot;
use lupabase::prelude::*;
use std::{error::Error, fs};
use tests_records::*;
use tests_utils::*;

#[test]
fn variadics_cbor() -> Result<(), Box<dyn Error>> {
    variadics_tester::<CborDB>()?;

    Ok(())
}

#[test]
fn variadics_json() -> Result<(), Box<dyn Error>> {
    variadics_tester::<JsonDB>()?;

    Ok(())
}

#[test]
fn variadics_memory() -> Result<(), Box<dyn Error>> {
    variadics_tester::<MemoryDB>()?;

    Ok(())
}

type Partition1232 = (
    TestRecordPartitioned,
    (
        TestRecordPartitioned2,
        (TestRecordPartitioned3, TestRecordPartitioned2),
    ),
);

type Partition123 = (
    TestRecordPartitioned,
    (TestRecordPartitioned2, TestRecordPartitioned3),
);

type Partition132 = (
    TestRecordPartitioned,
    (TestRecordPartitioned3, TestRecordPartitioned2),
);

type Partition12 = (TestRecordPartitioned, TestRecordPartitioned2);

fn variadics_tester<DB: Database>() -> Result<(), Box<dyn Error>> {
    init_tracing_for_tests();

    let db_name = DB::NAME;

    let (working_dir, _temp_dir_drop_guard) = create_temp_working_dir("variadics", db_name);

    let _ = fs::remove_dir_all(&working_dir);

    let db = DB::new(working_dir);

    {
        span_and_info!("Partitioned");

        {
            span_and_info!("Initialize");

            db.try_initialize_storage_with_operatable::<Partition1232>(Default::default())?;
            assert_debug_snapshot!(
                format!("{db_name} initialized should be empty"),
                db.get_all_with_operatable::<Partition123>()?
            );
        }

        let id = &mut 0_u64;

        {
            span_and_info!("Operation", "Insert");

            db.insert_with_operatable((
                TestRecordPartitioned::new(id),
                TestRecordPartitioned3::new(id),
            ))?;
            assert_debug_snapshot!(
                format!("{db_name} inserted"),
                db.get_all_with_operatable::<Partition123>()?
            );
        }

        {
            span_and_info!("Operation", "Inserting all");

            db.insert_all_with_operatable::<Partition123>((
                vec![TestRecordPartitioned::new(id)],
                (
                    vec![
                        TestRecordPartitioned2::new(id),
                        TestRecordPartitioned2::new(id),
                    ],
                    vec![
                        TestRecordPartitioned3::new(id),
                        TestRecordPartitioned3::new(id),
                        TestRecordPartitioned3::new(id),
                    ],
                ),
            ))?;
            assert_debug_snapshot!(
                format!("{db_name} inserted all"),
                db.get_all_with_operatable::<Partition123>()?
            );
        }

        {
            span_and_info!("Operation", "Updating");

            let mut record = (
                TestRecordPartitioned::new(id),
                TestRecordPartitioned2::new(id),
            );
            db.insert_with_operatable::<Partition12>(record.clone())?;
            record.0.data = String::from("Data 1 has been updated!");
            record.1.data = String::from("Data 2 has been updated!");
            db.update_with_operatable::<Partition12>(record)?;
            assert_debug_snapshot!(
                format!("{db_name} updated"),
                db.get_all_with_operatable::<Partition123>()?
            );
        }

        {
            span_and_info!("Operation", "Updating all");

            let mut record_1 = vec![
                TestRecordPartitioned::new(id),
                TestRecordPartitioned::new(id),
                TestRecordPartitioned::new(id),
            ];
            let mut record_2 = vec![
                TestRecordPartitioned2::new(id),
                TestRecordPartitioned2::new(id),
                TestRecordPartitioned2::new(id),
            ];
            let mut record_3 = vec![
                TestRecordPartitioned3::new(id),
                TestRecordPartitioned3::new(id),
                TestRecordPartitioned3::new(id),
            ];
            db.insert_all_with_operatable::<Partition123>((
                record_1.clone(),
                (record_2.clone(), record_3.clone()),
            ))?;
            record_1[0].data = String::from("Data 1 has been updated!");
            record_2[1].data = String::from("Data 2 has been updated!");
            record_3[2].data = String::from("Data 3 has been updated!");
            // Updating out of order should be fine!
            db.update_all_with_operatable::<Partition132>((record_1, (record_3, record_2)))?;
            assert_debug_snapshot!(
                format!("{db_name} updated all"),
                db.get_all_with_operatable::<Partition123>()?
            );

            let (r1, (r2, r3)) = db.get_all_with_operatable::<Partition123>()?;
            let (rr1, (rr3, rr2)) = db.get_all_with_operatable::<Partition132>()?;

            assert_eq!(r1, rr1);
            assert_eq!(r2, rr2);
            assert_eq!(r3, rr3);
        }

        {
            span_and_info!("Operation", "Replacing all");

            let current_records = db.get_all_with_operatable::<Partition123>()?;
            db.replace_all_with_operatable::<Partition132>((vec![], (vec![], vec![])))?;
            assert_debug_snapshot!(
                format!("{db_name} replaced all empty"),
                db.get_all_with_operatable::<Partition123>()?
            );
            db.replace_all_with_operatable::<Partition123>(current_records)?;
            assert_debug_snapshot!(
                format!("{db_name} replaced all restored"),
                db.get_all_with_operatable::<Partition123>()?
            );
        }

        {
            span_and_info!("Re-Initialize");

            db.try_initialize_storage_with_operatable::<Partition123>(Default::default())?;
            assert_debug_snapshot!(
                format!("{db_name} reinitialized"),
                db.get_all_with_operatable::<Partition123>()?
            );
        }
    }

    //TODO: Collection and Single wrapper
    // {
    //     span_and_info!("Collection");
    // }

    // {
    //     span_and_info!("Single");
    // }

    Ok(())
}
