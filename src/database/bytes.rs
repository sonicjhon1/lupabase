use crate::{Deserialize, Result, Serialize};

/// Provides bytes operations for database
pub trait DatabaseBytes {
    /// Attempts to serialize the provided data as bytes
    ///
    /// # Errors
    /// - Parsing failure
    fn try_as_bytes(data: impl Serialize) -> Result<Vec<u8>>;

    /// Attempts to deserialize the provided data from bytes
    ///
    /// # Errors
    /// - Parsing failure
    fn try_from_bytes<'de, O: Deserialize<'de>>(bytes: &'de [u8]) -> Result<O>;
}
