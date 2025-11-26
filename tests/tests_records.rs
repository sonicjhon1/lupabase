use lupabase::{prelude::*, record::DatabaseRecord};
use serde::{Deserialize, Serialize};
use std::num::NonZero;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TestRecord {
    pub id: NonZero<u64>,
    pub data: String,
}

impl DatabaseRecord for TestRecord {
    type Unique = NonZero<u64>;

    fn unique_value(&self) -> Self::Unique { self.id }
}

impl TestRecord {
    pub fn new(id: &mut u64) -> Self {
        *id += 1;

        Self {
            id: NonZero::try_from(*id).expect("ID should not be Zero"),
            data: format!("My data of {id}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TestRecordPartitioned {
    pub id: NonZero<u64>,
    pub data: String,
}

impl DatabaseRecord for TestRecordPartitioned {
    type Unique = NonZero<u64>;

    fn unique_value(&self) -> Self::Unique { self.id }
}

impl DatabaseRecordPartitioned for TestRecordPartitioned {
    const PARTITION: &str = "TestRecordPartitioned";
}

impl TestRecordPartitioned {
    pub fn new(id: &mut u64) -> Self {
        *id += 1;

        Self {
            id: NonZero::try_from(*id).expect("ID should not be Zero"),
            data: format!("My data of {id}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TestRecordPartitioned2 {
    pub id: NonZero<u64>,
    pub data: String,
}

impl DatabaseRecord for TestRecordPartitioned2 {
    type Unique = NonZero<u64>;

    fn unique_value(&self) -> Self::Unique { self.id }
}

impl DatabaseRecordPartitioned for TestRecordPartitioned2 {
    const PARTITION: &str = "TestRecordPartitioned2";
}

impl TestRecordPartitioned2 {
    pub fn new(id: &mut u64) -> Self {
        *id += 1;

        Self {
            id: NonZero::try_from(*id).expect("ID should not be Zero"),
            data: format!("My data of {id}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TestRecordPartitioned3 {
    pub id: NonZero<u64>,
    pub data: String,
}

impl DatabaseRecord for TestRecordPartitioned3 {
    type Unique = NonZero<u64>;

    fn unique_value(&self) -> Self::Unique { self.id }
}

impl DatabaseRecordPartitioned for TestRecordPartitioned3 {
    const PARTITION: &str = "TestRecordPartitioned3";
}

impl TestRecordPartitioned3 {
    pub fn new(id: &mut u64) -> Self {
        *id += 1;

        Self {
            id: NonZero::try_from(*id).expect("ID should not be Zero"),
            data: format!("My data of {id}"),
        }
    }
}
