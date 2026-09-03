use crate::{
    Error,
    database::Database,
    procedures::{CollectionFilter, Procedure},
    record::{DatabaseRecord, RecordPartitionPath},
};
use std::marker::PhantomData;

/// Removes the first record from a [`RecordPartitionPath`] that matches the [`CollectionFilter`]
#[derive(Debug)]
pub struct ProcedureCollectionRemoveFirst<'p, R, F> {
    pub _record_marker: PhantomData<R>,
    pub partition_path: RecordPartitionPath<'p>,
    pub filter: F,
}

impl<'p, R, F> ProcedureCollectionRemoveFirst<'p, R, F> {
    /// Replaces the [`CollectionFilter`] used by the procedure
    pub fn with_filter<NewFilter>(
        self,
        filter: NewFilter,
    ) -> ProcedureCollectionRemoveFirst<'p, R, NewFilter> {
        ProcedureCollectionRemoveFirst {
            _record_marker: PhantomData,
            partition_path: self.partition_path,
            filter,
        }
    }
}

impl<'p, DB, R, F> Procedure<DB> for ProcedureCollectionRemoveFirst<'p, R, F>
where
    DB: Database,
    R: DatabaseRecord,
    F: CollectionFilter<R>,
{
    type Output = ();

    fn execute(self, db: &DB) -> crate::Result<Self::Output> {
        let mut records = db.try_read_storage::<Vec<R>>(&self.partition_path)?;

        let index = records
            .iter()
            .position(|record| self.filter.matches(record))
            .ok_or_else(|| Error::DBOperationFailure {
                path: self.partition_path.display().to_string(),
                reason: "Matching record does not exist".to_string(),
            })?;

        records.remove(index);
        db.try_write_storage(records, self.partition_path)
    }
}
