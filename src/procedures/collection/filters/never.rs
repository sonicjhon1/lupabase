use crate::procedures::CollectionFilter;

/// A [`CollectionFilter`] that never matches any record
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct FilterNeverMatches;

impl<T> CollectionFilter<T> for FilterNeverMatches {
    fn matches(&self, _record: &T) -> bool { false }
}
