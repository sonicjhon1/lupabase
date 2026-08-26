use std::{borrow::Cow, path::Path};

pub type RecordPartition<'p> = &'p str;

pub type RecordPartitionPath<'p> = Cow<'p, Path>;
