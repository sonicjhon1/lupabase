use crate::record::DatabaseRecordPartitioned;
use std::{borrow::Cow, path::Path};

/// Provides partitioned database
pub trait DatabasePartitioned {
    /// Returns the [`DatabaseRecordPartitioned`]'s partition storage path
    fn partition_path<T: DatabaseRecordPartitioned>(&self) -> Cow<'_, Path>;
}
