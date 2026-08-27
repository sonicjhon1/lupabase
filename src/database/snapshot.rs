use crate::{database::Database, operations::OperationCrud};

/// Provides snapshotable database
pub trait DatabaseSnapshot {
    type SnapshotDB: Database + OperationCrud;

    fn snapshot(&self) -> &Self::SnapshotDB;
}
