use crate::{Result, database::Database, procedures::Procedure};

/// Provides [`Procedure`] support for the database
pub trait DatabaseProcedure: Database {
    /// Executes the provided [`Procedure`] using this database
    fn execute<P>(&self, procedure: P) -> Result<P::Output>
    where
        P: Procedure, {
        procedure.execute(self)
    }
}
