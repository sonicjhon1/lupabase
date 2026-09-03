use crate::{Result, database::Database};

/// Represents an operation that can be executed against a [`Database`]
pub trait Procedure<DB: Database> {
    /// The value returned by the procedure
    type Output;

    /// Executes the procedure using the provided database
    fn execute(self, db: &DB) -> Result<Self::Output>;
}
