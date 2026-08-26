/// Filters a collection of records based on a matching predicate
pub trait CollectionFilter<T> {
    /// Determines whether a record matches the filter
    fn matches(&self, record: &T) -> bool;
}
