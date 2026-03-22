mod operation;
pub use operation::*;
mod operation_path;
pub use operation_path::*;
mod operation_operatable;
pub use operation_operatable::*;
mod io;
pub use io::*;

use std::path::Path;

/// Represents a database that provides operations for managing records,
/// built upon the functionality provided by [`DatabaseOps`] and [`DatabaseIO`]
pub trait Database: DatabaseOps + DatabaseIO {
    /// The name of the Database
    const NAME: &str;

    /// The format of the Database's Serializer
    const SERDE_FORMAT: &str;

    /// Creates a new instance of [`Database`] with the specified base directory where files will be stored
    fn new(dir: impl AsRef<Path>) -> Self;
}
