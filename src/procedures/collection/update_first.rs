use crate::{
    Error,
    database::DatabasePartitioned,
    procedures::{CollectionFilter, FilterUnique, Procedure},
    record::{DatabaseRecord, DatabaseRecordPartitioned, RecordPartitionPath},
};

/// Updates the first record in a [`RecordPartitionPath`] that matches the [`CollectionFilter`]
#[derive(Debug)]
pub struct ProcedureCollectionUpdateFirst<'p, R, F> {
    pub record: R,
    pub partition_path: RecordPartitionPath<'p>,
    pub filter: F,
}

impl<'p, R> ProcedureCollectionUpdateFirst<'p, R, FilterUnique<R::Unique>>
where
    R: DatabaseRecord,
{
    /// Creates the procedure for the specified partition
    pub fn new(record: R, partition_path: RecordPartitionPath<'p>) -> Self {
        Self {
            filter: FilterUnique::new(record.unique_value()),
            record,
            partition_path,
        }
    }

    /// Creates the procedure using the partition associated with the record type
    pub fn new_partitioned<DB>(record: R, db: &'p DB) -> Self
    where
        R: DatabaseRecordPartitioned,
        DB: DatabasePartitioned, {
        ProcedureCollectionUpdateFirst::new(record, db.partition_path::<R>())
    }
}

impl<'p, R, F> ProcedureCollectionUpdateFirst<'p, R, F> {
    /// Replaces the [`CollectionFilter`] used by the procedure
    pub fn with_filter<NewFilter>(
        self,
        filter: NewFilter,
    ) -> ProcedureCollectionUpdateFirst<'p, R, NewFilter> {
        ProcedureCollectionUpdateFirst {
            record: self.record,
            partition_path: self.partition_path,
            filter,
        }
    }
}

impl<'p, R, F> Procedure for ProcedureCollectionUpdateFirst<'p, R, F>
where
    R: DatabaseRecord,
    F: CollectionFilter<R>,
{
    type Output = ();

    fn execute<DB: crate::prelude::Database + ?Sized>(
        self,
        db: &DB,
    ) -> crate::Result<Self::Output> {
        let mut records = db.try_read_storage::<Vec<R>>(&self.partition_path)?;

        *records
            .iter_mut()
            .find(|record| self.filter.matches(record))
            .ok_or_else(|| Error::DBOperationFailure {
                path: self.partition_path.display().to_string(),
                reason: "Matching record does not exist".to_string(),
            })? = self.record;

        db.try_write_storage(records, self.partition_path)
    }
}
