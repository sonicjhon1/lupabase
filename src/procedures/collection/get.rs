use crate::{
    database::DatabasePartitioned,
    prelude::Database,
    procedures::{CollectionFilter, FilterAlwaysMatches, Procedure},
    record::{DatabaseRecord, DatabaseRecordPartitioned, RecordPartitionPath},
};
use std::marker::PhantomData;

/// Retrieves records from a [`RecordPartitionPath`], optionally filtering the results with a [`CollectionFilter`]
#[derive(Debug)]
pub struct ProcedureCollectionGet<'p, R, F> {
    pub _record_marker: PhantomData<R>,
    pub partition_path: RecordPartitionPath<'p>,
    pub filter: F,
}

impl<'p, R> ProcedureCollectionGet<'p, R, FilterAlwaysMatches>
where
    R: DatabaseRecord,
{
    /// Creates the procedure for the specified partition
    pub const fn new(partition_path: RecordPartitionPath<'p>) -> Self {
        Self {
            _record_marker: PhantomData,
            filter: FilterAlwaysMatches,
            partition_path,
        }
    }

    /// Creates the procedure using the partition associated with the record type
    pub fn new_partitioned<DB>(db: &'p DB) -> Self
    where
        R: DatabaseRecordPartitioned,
        DB: DatabasePartitioned, {
        ProcedureCollectionGet::new(db.partition_path::<R>())
    }
}

impl<'p, R, F> ProcedureCollectionGet<'p, R, F> {
    /// Replaces the [`CollectionFilter`] used by the procedure
    pub fn with_filter<NewFilter>(
        self,
        filter: NewFilter,
    ) -> ProcedureCollectionGet<'p, R, NewFilter> {
        ProcedureCollectionGet {
            _record_marker: PhantomData,
            partition_path: self.partition_path,
            filter,
        }
    }
}

impl<'p, DB, R, F> Procedure<DB> for ProcedureCollectionGet<'p, R, F>
where
    DB: Database,
    R: DatabaseRecord,
    F: CollectionFilter<R>,
{
    type Output = Vec<R>;

    fn execute(self, db: &DB) -> crate::Result<Self::Output> {
        Ok(db
            .try_read_storage::<Vec<R>>(self.partition_path)?
            .into_iter()
            .filter(|record| self.filter.matches(record))
            .collect())
    }
}
