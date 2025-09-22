# lupabase
Lupabase is a **blazingly fast** (work-in-progress) database engine, written entirely in Rust. It focuses on simplicity, portability with flexible storage backends.

## Features
- Two storage backends: `Database`
    - MemoryDB: super fast, data is lost on restart
    - JsonDB: persists to disk in human-readable json format

- Record Utilities: `DatabaseRecordsUtils`
    - Filter records by unique key
    - Detect intersecting and non-intersecting records
    - Easily extract unique identifiers

- Transactions: `DatabaseTransaction`
    - ACID-like transaction system (Work in progress)

- Tightly integrated with Serde: `DatabaseRecord`

### It is not
- A standalone database server
- A relational database

## Roadmap
- Variadic / multiple table function call
- Concurrency-safe multi-threaded transactions
- Tests


## Example/Usage
```rust 
use lupabase::prelude::*;
use lupabase::record_utils::*;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
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

let users = vec![
    User { id: 1, name: "Alice".into() },
    User { id: 2, name: "Bob".into() },
];

let uniques = users.as_uniques();
println!("Unique IDs: {:?}", uniques);

if let Some(user) = users.find_by_unique(&1) {
    println!("Found user: {}", user.name);
}
```

## Status
This project is **under active development and not yet production-ready**. Although it is in active use for my own projects, you should expect breaking changes if you use this library in the current status.

## License
All source code is licensed under MIT OR Apache-2.0.
All contributions are to be licensed as MIT OR Apache-2.0.