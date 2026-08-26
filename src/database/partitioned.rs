use crate::record::{DatabaseRecordPartitioned, RecordPartitionPath};

/// Provides partitioned database
pub trait DatabasePartitioned {
    /// Returns the [`DatabaseRecordPartitioned`]'s partition storage path
    fn partition_path<T: DatabaseRecordPartitioned>(&self) -> RecordPartitionPath<'_>;
}
