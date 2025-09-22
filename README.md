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

## Status
This project is **under active development and not yet production-ready**. Although it is in active use for my own projects, you should expect breaking changes if you use this library in the current status.

## License
All source code is licensed under MIT OR Apache-2.0.
All contributions are to be licensed as MIT OR Apache-2.0.