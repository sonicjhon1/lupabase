use crate::{procedures::CollectionFilter, record::DatabaseRecord};

/// A [`CollectionFilter`] that inverts the result of another filter
#[derive(Debug)]
pub struct FilterInvert<F>(F);

impl<F> FilterInvert<F> {
    /// Creates a filter that inverts the given filter
    pub const fn new(filter: F) -> Self { Self(filter) }
}

impl<R, F> CollectionFilter<R> for FilterInvert<F>
where
    R: DatabaseRecord,
    F: CollectionFilter<R>,
{
    fn matches(&self, record: &R) -> bool { !self.0.matches(record) }
}
