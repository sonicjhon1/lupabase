use crate::{procedures::CollectionFilter, record::DatabaseRecord};

/// A [`CollectionFilter`] that matches records with a specific unique value
#[derive(Debug)]
pub struct FilterUnique<U>(U);

impl<U> FilterUnique<U> {
    /// Creates a filter for the specified unique value
    pub const fn new(unique: U) -> Self { Self(unique) }
}

impl<R> CollectionFilter<R> for FilterUnique<R::Unique>
where
    R: DatabaseRecord,
{
    fn matches(&self, record: &R) -> bool { record.unique_value() == self.0 }
}
