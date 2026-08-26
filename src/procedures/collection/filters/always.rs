use crate::procedures::CollectionFilter;

/// A [`CollectionFilter`] that matches every record
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct FilterAlwaysMatches;

impl<T> CollectionFilter<T> for FilterAlwaysMatches {
    fn matches(&self, _record: &T) -> bool { true }
}
