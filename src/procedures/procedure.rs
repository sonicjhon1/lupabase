use crate::{Result, database::Database};

/// Represents an operation that can be executed against a [`Database`]
pub trait Procedure {
    /// The value returned by the procedure
    type Output;

    /// Executes the procedure using the provided database
    fn execute<DB: Database + ?Sized>(self, db: &DB) -> Result<Self::Output>;
}
