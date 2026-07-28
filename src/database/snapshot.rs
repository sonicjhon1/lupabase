use crate::{database::Database, operations::Operation};

/// Provides snapshotable database
pub trait DatabaseSnapshot {
    type SnapshotDB: Database + Operation;

    fn snapshot(&self) -> &Self::SnapshotDB;
}
