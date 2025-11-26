use crate::record::*;
use itertools::Itertools;

/// Provide utility methods for DatabaseRecord.
pub trait DatabaseRecordsUtils<T: DatabaseRecord> {
    fn as_uniques(&self) -> Vec<T::Unique>;
    fn find_intersecting_uniques_from(
        &self,
        other_records: &[T],
    ) -> Vec<<T as DatabaseRecord>::Unique>;
    fn find_non_intersecting_uniques_from(
        &self,
        other_records: &[T],
    ) -> Vec<<T as DatabaseRecord>::Unique>;
    fn find_by_unique(&self, unique: &T::Unique) -> Option<&T>;
    fn find_by_unique_mut(&mut self, unique_value: &T::Unique) -> Option<&mut T>;
}

impl<T: DatabaseRecord> DatabaseRecordsUtils<T> for [T] {
    /// Returns a vector containing the unique value of each record.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::prelude::*;
    /// # use lupabase::record::utils::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let records = vec![Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let uniques = records.as_uniques();
    /// assert_eq!(uniques, vec![1, 2, 3]);
    /// ```
    fn as_uniques(&self) -> Vec<<T as DatabaseRecord>::Unique> {
        self.iter().map(|r| r.unique_value()).collect()
    }
    /// Returns the `Unique` values that are present in both `self` and `other_records`.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::prelude::*;
    /// # use lupabase::record::utils::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let a = &[Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let b = [Record { id: 2 }, Record { id: 3 }, Record { id: 4 }];
    /// let intersecting = a.find_intersecting_uniques_from(&b);
    /// assert_eq!(intersecting, vec![2, 3]);
    /// ```
    fn find_intersecting_uniques_from(
        &self,
        other_records: &[T],
    ) -> Vec<<T as DatabaseRecord>::Unique> {
        return self
            .as_uniques()
            .into_iter()
            .chain(other_records.as_uniques())
            .duplicates()
            .collect();
    }
    /// Returns the `Unique` values from `other_records` that are not present in `self`.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::prelude::*;
    /// # use lupabase::record::utils::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let a = &[Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let b = [Record { id: 2 }, Record { id: 3 }, Record { id: 4 }];
    /// let non_intersecting = a.find_non_intersecting_uniques_from(&b);
    /// assert_eq!(non_intersecting, vec![4]);
    /// ```
    fn find_non_intersecting_uniques_from(
        &self,
        other_records: &[T],
    ) -> Vec<<T as DatabaseRecord>::Unique> {
        let all_that_exists = self.find_intersecting_uniques_from(other_records);
        return other_records
            .as_uniques()
            .into_iter()
            .filter(|ou| !all_that_exists.contains(ou))
            .collect();
    }
    /// Returns a reference to the record with the specified unique value, if it exists.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::prelude::*;
    /// # use lupabase::record::utils::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let records = &[Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let record = records.find_by_unique(&2).unwrap();
    /// assert_eq!(record.id, 2);
    /// ```
    fn find_by_unique(&self, unique_value: &T::Unique) -> Option<&T> {
        self.iter().find(|r| &r.unique_value() == unique_value)
    }
    /// Returns a mutable reference to the record with the specified unique value, if it exists.
    ///
    /// # Example
    /// ```rust
    /// # use lupabase::prelude::*;
    /// # use lupabase::record::utils::*;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Record { id: u8 }
    /// #
    /// # impl DatabaseRecord for Record {
    /// #    type Unique = u8;
    /// #
    /// #    fn unique_value(&self) -> Self::Unique { self.id }
    /// # }
    /// let mut records = [Record { id: 1 }, Record { id: 2 }, Record { id: 3 }];
    /// let record = records.find_by_unique_mut(&2).unwrap();
    /// assert_eq!(record.id, 2);
    /// ```
    fn find_by_unique_mut(&mut self, unique_value: &T::Unique) -> Option<&mut T> {
        self.iter_mut().find(|r| &r.unique_value() == unique_value)
    }
}
