# lupabase

[![Docs](https://docs.rs/lupabase/badge.svg)](https://docs.rs/lupabase/latest/lupabase/)
[![Crates.io](https://img.shields.io/crates/v/lupabase.svg)](https://crates.io/crates/lupabase)
[![Downloads](https://img.shields.io/crates/d/lupabase.svg)](https://crates.io/crates/lupabase)
[![License](https://img.shields.io/badge/license-MIT%2FApache-blue.svg)](https://github.com/sonicjhon1/lupabase#license)

Lupabase is a **blazingly fast** (work-in-progress) database, written entirely in Rust. It focuses on simplicity, portability with flexible storage backends.

## Features
- Multiple [`Database`](crate::prelude::Database) engines built-in: 
    - [`MemoryDB`](crate::prelude::MemoryDB): In-memory, non-persistent storage. Extremely fast, backed by [`RwLock`](parking_lot::RwLock) and [`HashMap`](hashbrown::HashMap)
    - [`JsonDB`](crate::prelude::JsonDB): Persists records to disk using the [`JSON`](https://docs.rs/serde_json) format
    - [`CborDB`](crate::prelude::CborDB): Persists records to disk using the [`CBOR`](https://docs.rs/minicbor-serde) format

- Flexible database record:
  - [`DatabaseRecord`](crate::prelude::DatabaseRecord): A minimal, general-purpose record type for database operations that support custom paths. 
    Suitable record that needs to be stored at custom file paths.
  - [`DatabaseRecordPartitioned`](crate::prelude::DatabaseRecordPartitioned): A partitioned (named) record type that enables full feature support across all
database operations. This includes everything supported by [`DatabaseRecord`](crate::prelude::DatabaseRecord). Recommended ✅

- [`DatabaseRecordsUtils`](crate::record_utils::DatabaseRecordsUtils) for database records utilities: 
    - Filter records by unique key
    - Detect intersecting and non-intersecting records
    - Easily extract unique identifiers

- [`DatabaseTransaction`](crate::prelude::DatabaseTransaction) for ACID-like transactions: 
    - Start transactions
    - Commit stored transations
    - Rollback transactions to a previous snapshot

- Raw database I/O with [`serde`](https://docs.rs/serde) through [`DatabaseIO`](crate::prelude::DatabaseIO):
  - Store any type that implements [`Serialize`](serde_core::Serialize)
  - Retrieve any type that implements [`Deserialize`](serde_core::Deserialize)

## This is not
- A standalone database server
- A relational database

## Example/Usage
```rust 
use lupabase::prelude::*;
use serde::{Serialize, Deserialize};

// Setup Record
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct User {
    id: u32,
    name: String,
}

impl DatabaseRecordPartitioned for User {
    const PARTITION: &str = "users";
}

impl DatabaseRecord for User {
    type Unique = u32;

    fn unique_value(&self) -> Self::Unique {
        self.id
    }
}

// Create a new Database (This example uses in-memory database that is wiped on exit)
let db = memorydb::MemoryDB::new("Test");

let users = vec![
    User { id: 0, name: "Alice".into() },
    User { id: 1, name: "Bob".into() },
];

// Initialize the Database
db.try_initialize_storage::<User, _>(users.clone()).unwrap();

// Query
let users_db = db.get_all::<User>().unwrap();
assert_eq!(users_db, users);

// Initialize the Database with a custom storage path
// The ::<Vec<User>> parameter is optional if the default_record is inferable / not empty
db.try_initialize_storage_with_path::<Vec<User>>(vec![], "custom_path").unwrap();

// Query from custom storage path
db.insert_with_path(User { id: 2, name: "Lupa".into() }, "custom_path").unwrap();

let users_custom_db = db.get_all_with_path::<User>("custom_path").unwrap();
assert_eq!(users_custom_db.len(), 1);
assert_eq!(users_custom_db[0].unique_value(), 2);

// Additional helpers
use lupabase::record_utils::*;

let uniques = users_db.as_uniques();
println!("Unique IDs: {:?}", uniques);

if let Some(user) = users_db.find_by_unique(&1) {
    println!("Found user: {}", user.name);
}
```

## Roadmap
- Variadic / multiple path (partition) function call
- Concurrency-safe multi-threaded transactions
- Improved tests and docs
- Examples

## Status
This project is **under active development and not yet production-ready**. Although it is in active use for my own projects, you should expect breaking changes if you use this library in the current status.

## License
All code in this repository is dual-licensed under either:

* MIT License ([LICENSE-MIT](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))

at your option.


### Your contributions

Any contribution submitted for inclusion in the work by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
