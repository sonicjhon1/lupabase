use crate::{
    database::Database,
    procedures::{CollectionFilter, Procedure},
    record::{DatabaseRecord, RecordPartitionPath},
};
use std::marker::PhantomData;

/// Removes all records from a [`RecordPartitionPath`] that match the [`CollectionFilter`]
#[derive(Debug)]
pub struct ProcedureCollectionRemoveMany<'p, R, F> {
    pub _record_marker: PhantomData<R>,
    pub partition_path: RecordPartitionPath<'p>,
    pub filter: F,
}

impl<'p, R, F> ProcedureCollectionRemoveMany<'p, R, F> {
    /// Replaces the [`CollectionFilter`] used by the procedure
    pub fn with_filter<NewFilter>(
        self,
        filter: NewFilter,
    ) -> ProcedureCollectionRemoveMany<'p, R, NewFilter> {
        ProcedureCollectionRemoveMany {
            _record_marker: PhantomData,
            partition_path: self.partition_path,
            filter,
        }
    }
}

impl<'p, DB, R, F> Procedure<DB> for ProcedureCollectionRemoveMany<'p, R, F>
where
    DB: Database,
    R: DatabaseRecord,
    F: CollectionFilter<R>,
{
    type Output = ();

    fn execute(self, db: &DB) -> crate::Result<Self::Output> {
        let mut records = db.try_read_storage::<Vec<R>>(&self.partition_path)?;

        records.retain(|record| !self.filter.matches(record));

        db.try_write_storage(records, self.partition_path)
    }
}
