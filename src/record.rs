use crate::{Deserialize, Serialize};
use std::{fmt::Debug, hash::Hash};

/// Unique identifier for [`DatabaseRecord`]
///
/// This type must implement [`Hash`], [`Eq`], and [`Debug`] to ensure proper comparison and debugging capabilities.
pub trait IntoUnique: Hash + Eq + Debug + Serialize {}
impl<T: Hash + Eq + Debug + Serialize> IntoUnique for T {}

/// Represents a Record that can be stored
///
/// Record must implement both [`Serialize`] and [`Deserialize`]
pub trait DatabaseRecord: Serialize + for<'a> Deserialize<'a> {
    /// The type of the unique identifier that implements [`IntoUnique`]
    type Unique: IntoUnique;

    /// Returns the unique identifier of the record
    ///
    /// This method should provide a value that uniquely identifies records of the same type,
    /// essential for [`DatabaseOps`].
    fn unique_value(&self) -> Self::Unique;
}

/// Represents a Record that has a built-in partition
pub trait DatabaseRecordPartitioned: DatabaseRecord {
    /// Specifies the partition where records of this type are stored
    const PARTITION: &'static str;
}
