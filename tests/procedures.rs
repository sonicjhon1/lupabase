pub mod tests_records;
pub mod tests_utils;

use insta::assert_debug_snapshot;
use lupabase::{
    prelude::*,
    procedures::{
        FilterAlwaysMatches, FilterNeverMatches, FilterUnique, ProcedureCollectionCreate,
        ProcedureCollectionGet, ProcedureCollectionInsertFirst, ProcedureCollectionRemoveFirst,
        ProcedureCollectionRemoveMany, ProcedureCollectionUpdateFirst,
    },
};
use std::{error::Error, path::PathBuf};
use tests_records::*;
use tests_utils::*;

// TODO: Maps / Memory based procedures
// #[test]
// fn procedures_arcticmap() -> Result<(), Box<dyn Error>> {
//     #[cfg(feature = "arcticmap")]
//     {
//         #[cfg(feature = "cbor")]
//         tester(|_| ArcticMapDB::<CborSerde>::new())?;
//         #[cfg(feature = "json")]
//         tester(|_| ArcticMapDB::<JsonSerde>::new())?;
//         #[cfg(feature = "postcard")]
//         tester(|_| ArcticMapDB::<PostcardSerde>::new())?;
//     }
//     Ok(())
// }

// #[test]
// fn procedures_dashmap() -> Result<(), Box<dyn Error>> {
//     #[cfg(feature = "dashmap")]
//     {
//         #[cfg(feature = "cbor")]
//         tester(|_| DashMapDB::<CborSerde>::new())?;
//         #[cfg(feature = "json")]
//         tester(|_| DashMapDB::<JsonSerde>::new())?;
//         #[cfg(feature = "postcard")]
//         tester(|_| DashMapDB::<PostcardSerde>::new())?;
//     }

//     Ok(())
// }

#[test]
fn procedures_disk() -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "disk")]
    {
        #[cfg(feature = "cbor")]
        tester(DiskDB::<CborSerde>::new)?;
        #[cfg(feature = "json")]
        tester(DiskDB::<JsonSerde>::new)?;
        #[cfg(feature = "postcard")]
        tester(DiskDB::<PostcardSerde>::new)?;
    }

    Ok(())
}

// #[test]
// fn procedures_hashmap() -> Result<(), Box<dyn Error>> {
//     #[cfg(feature = "hashmap")]
//     {
//         #[cfg(feature = "cbor")]
//         tester(|_| HashMapDB::<CborSerde>::new())?;
//         #[cfg(feature = "json")]
//         tester(|_| HashMapDB::<JsonSerde>::new())?;
//         #[cfg(feature = "postcard")]
//         tester(|_| HashMapDB::<PostcardSerde>::new())?;
//     }

//     Ok(())
// }

fn tester<DB: Database + DatabasePartitioned + DatabaseProcedure + OperationCrudPartitioned>(
    db_fn: impl FnOnce(PathBuf) -> DB,
) -> Result<(), Box<dyn Error>> {
    init_tracing_for_tests();

    let (working_dir, _working_dir_guard) = create_temp_working_dir("basics");
    let _ = std::fs::remove_dir_all(&working_dir);
    let db = db_fn(working_dir);

    let db_name = &format!("{}-{}", DB::SERDE_FORMAT_EXTENSION, DB::NAME);

    {
        span_and_info!("Collection");

        {
            span_and_info!("Initialize");

            db.try_initialize_storage::<TestRecordPartitioned, Vec<TestRecordPartitioned>>(vec![])?;
        }

        let id = &mut 0_u64;

        {
            span_and_info!("Procedure", "Create");

            {
                db.execute(ProcedureCollectionCreate::new(
                    TestRecord::new(id),
                    db.partition_path::<TestRecordPartitioned>(),
                ))?;

                db.execute(ProcedureCollectionCreate::new_partitioned(
                    TestRecordPartitioned::new(id),
                    &db,
                ))?;
            }

            {
                db.execute(
                    ProcedureCollectionCreate::new_partitioned(TestRecordPartitioned::new(id), &db)
                        .with_filter(FilterNeverMatches),
                )?;

                db.execute(
                    ProcedureCollectionCreate::new_partitioned(TestRecordPartitioned::new(id), &db)
                        .with_filter(FilterAlwaysMatches),
                )
                .expect_err("DB should reject");
            }

            {
                let record = TestRecordPartitioned::new(id);

                db.execute(ProcedureCollectionCreate::new_partitioned(
                    record.clone(),
                    &db,
                ))?;

                db.execute(ProcedureCollectionCreate::new_partitioned(record, &db))
                    .expect_err("DB should reject");
            }

            assert_debug_snapshot!(
                format!("{db_name} Create"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Procedure", "Get");

            assert_debug_snapshot!(
                format!("{db_name} Get"),
                db.execute(ProcedureCollectionGet::<'_, TestRecord, _>::new(
                    db.partition_path::<TestRecordPartitioned>(),
                ))?
            );

            assert_debug_snapshot!(
                format!("{db_name} Get Partitioned"),
                db.execute(
                    ProcedureCollectionGet::<'_, TestRecordPartitioned, _>::new_partitioned(&db),
                )?
            );

            assert_debug_snapshot!(
                format!("{db_name} Get FilterAlwaysMatches"),
                db.execute(
                    ProcedureCollectionGet::<'_, TestRecordPartitioned, _>::new_partitioned(&db)
                        .with_filter(FilterAlwaysMatches),
                )?
            );

            assert_debug_snapshot!(
                format!("{db_name} Get FilterNeverMatches"),
                db.execute(
                    ProcedureCollectionGet::<'_, TestRecordPartitioned, _>::new_partitioned(&db)
                        .with_filter(FilterNeverMatches),
                )?
            );
        }

        {
            span_and_info!("Procedure", "Insert First");

            {
                db.execute(ProcedureCollectionInsertFirst::new(
                    TestRecord::new(id),
                    db.partition_path::<TestRecordPartitioned>(),
                ))?;

                db.execute(ProcedureCollectionInsertFirst::new_partitioned(
                    TestRecordPartitioned::new(id),
                    &db,
                ))?;
            }

            {
                db.execute(
                    ProcedureCollectionInsertFirst::new_partitioned(
                        TestRecordPartitioned::new(id),
                        &db,
                    )
                    .with_filter(FilterNeverMatches),
                )?;

                db.execute(
                    ProcedureCollectionInsertFirst::new_partitioned(
                        TestRecordPartitioned::new(id),
                        &db,
                    )
                    .with_filter(FilterAlwaysMatches),
                )?;
            }

            {
                let record = TestRecordPartitioned::new(id);

                db.execute(ProcedureCollectionInsertFirst::new_partitioned(
                    record.clone(),
                    &db,
                ))?;

                db.execute(ProcedureCollectionInsertFirst::new_partitioned(record, &db))?;
            }

            assert_debug_snapshot!(
                format!("{db_name} Insert First"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Procedure", "Remove First");

            {
                let record = TestRecordPartitioned::new(id);
                let record_unique = record.unique_value();

                db.execute(ProcedureCollectionCreate::new_partitioned(record, &db))?;

                db.execute(ProcedureCollectionRemoveFirst {
                    _record_marker: std::marker::PhantomData::<TestRecordPartitioned>,
                    partition_path: db.partition_path::<TestRecordPartitioned>(),
                    filter: FilterUnique::new(record_unique),
                })?;
            }

            {
                let record = TestRecordPartitioned::new(id);
                let record_unique = record.unique_value();

                db.execute(
                    ProcedureCollectionCreate::new_partitioned(record.clone(), &db)
                        .with_filter(FilterNeverMatches),
                )?;

                db.execute(
                    ProcedureCollectionCreate::new_partitioned(record, &db)
                        .with_filter(FilterNeverMatches),
                )?;

                db.execute(ProcedureCollectionRemoveFirst {
                    _record_marker: std::marker::PhantomData::<TestRecordPartitioned>,
                    partition_path: db.partition_path::<TestRecordPartitioned>(),
                    filter: FilterUnique::new(record_unique),
                })?;

                db.execute(ProcedureCollectionRemoveFirst {
                    _record_marker: std::marker::PhantomData::<TestRecordPartitioned>,
                    partition_path: db.partition_path::<TestRecordPartitioned>(),
                    filter: FilterUnique::new(record_unique),
                })?;

                db.execute(ProcedureCollectionRemoveFirst {
                    _record_marker: std::marker::PhantomData::<TestRecordPartitioned>,
                    partition_path: db.partition_path::<TestRecordPartitioned>(),
                    filter: FilterUnique::new(record_unique),
                })
                .expect_err("DB should reject");
            }

            assert_debug_snapshot!(
                format!("{db_name} Remove First"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Procedure", "Remove Many");

            {
                let record = TestRecordPartitioned::new(id);
                let record_unique = record.unique_value();

                db.execute(ProcedureCollectionCreate::new_partitioned(record, &db))?;

                db.execute(ProcedureCollectionRemoveMany {
                    _record_marker: std::marker::PhantomData::<TestRecordPartitioned>,
                    partition_path: db.partition_path::<TestRecordPartitioned>(),
                    filter: FilterUnique::new(record_unique),
                })?;
            }

            {
                let record = TestRecordPartitioned::new(id);
                let record_unique = record.unique_value();

                db.execute(
                    ProcedureCollectionCreate::new_partitioned(record.clone(), &db)
                        .with_filter(FilterNeverMatches),
                )?;

                db.execute(
                    ProcedureCollectionCreate::new_partitioned(record, &db)
                        .with_filter(FilterNeverMatches),
                )?;

                db.execute(ProcedureCollectionRemoveMany {
                    _record_marker: std::marker::PhantomData::<TestRecordPartitioned>,
                    partition_path: db.partition_path::<TestRecordPartitioned>(),
                    filter: FilterUnique::new(record_unique),
                })?;

                db.execute(ProcedureCollectionRemoveMany {
                    _record_marker: std::marker::PhantomData::<TestRecordPartitioned>,
                    partition_path: db.partition_path::<TestRecordPartitioned>(),
                    filter: FilterUnique::new(record_unique),
                })?;
            }

            assert_debug_snapshot!(
                format!("{db_name} Remove Many"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }

        {
            span_and_info!("Procedure", "Update First");

            {
                let mut record = TestRecord::new(id);

                db.execute(ProcedureCollectionInsertFirst::new(
                    record.clone(),
                    db.partition_path::<TestRecordPartitioned>(),
                ))?;

                record.data = "Updated".to_string();

                db.execute(ProcedureCollectionUpdateFirst::new(
                    record,
                    db.partition_path::<TestRecordPartitioned>(),
                ))?;
            }

            {
                let mut record = TestRecordPartitioned::new(id);

                db.execute(ProcedureCollectionInsertFirst::new_partitioned(
                    record.clone(),
                    &db,
                ))?;

                record.data = "Updated".to_string();

                db.execute(ProcedureCollectionUpdateFirst::new_partitioned(record, &db))?;
            }

            {
                let mut record = TestRecordPartitioned::new(id);
                record.data = "Not updated".to_string();

                db.execute(
                    ProcedureCollectionInsertFirst::new_partitioned(record.clone(), &db)
                        .with_filter(FilterNeverMatches),
                )?;

                db.execute(
                    ProcedureCollectionInsertFirst::new_partitioned(record.clone(), &db)
                        .with_filter(FilterNeverMatches),
                )?;

                record.data = "Updated 1".to_string();

                db.execute(ProcedureCollectionUpdateFirst::new_partitioned(
                    record.clone(),
                    &db,
                ))?;

                record.data = "Updated 2".to_string();

                db.execute(ProcedureCollectionUpdateFirst::new_partitioned(
                    record.clone(),
                    &db,
                ))?;
            }

            {
                let mut record_1 = TestRecordPartitioned::new(id);
                record_1.data = "Not updated".to_string();

                db.execute(ProcedureCollectionInsertFirst::new_partitioned(
                    record_1.clone(),
                    &db,
                ))?;

                let record_2 = TestRecordPartitioned::new(id);

                db.execute(
                    ProcedureCollectionUpdateFirst::new_partitioned(record_2.clone(), &db)
                        .with_filter(FilterAlwaysMatches),
                )?;

                db.execute(
                    ProcedureCollectionUpdateFirst::new_partitioned(record_2.clone(), &db)
                        .with_filter(FilterNeverMatches),
                )
                .expect_err("DB should reject");
            }

            {
                let mut record = TestRecordPartitioned::new(id);
                record.data = "Not updated".to_string();

                db.execute(ProcedureCollectionUpdateFirst::new_partitioned(
                    record.clone(),
                    &db,
                ))
                .expect_err("DB should reject");
            }

            assert_debug_snapshot!(
                format!("{db_name} Update First"),
                db.get_all::<TestRecordPartitioned>()?
            );
        }
    }

    Ok(())
}
