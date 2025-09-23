use std::path::PathBuf;

use derive_more::{Display, Error, From};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Error, From, Display)]
pub enum Error {
    #[from]
    #[display("To be documented: [{reason}]")]
    TBD { reason: String },

    // -- I/O
    #[display("Directory creation at [{path}] failed, caused by: [{reason}]")]
    IOCreateDirFailure {
        path: String,
        reason: std::io::Error,
    },

    #[display("Copy from [{path_from}] to [{path_destination}] failed, caused by: [{reason}]")]
    IOCopyFailure {
        path_from: String,
        path_destination: String,
        reason: std::io::Error,
    },

    #[display("Write to file at [{path}] failed, caused by: [{reason}]")]
    IOWriteFailure {
        path: String,
        reason: std::io::Error,
    },

    // -- Serde
    #[display("Serialization failed, caused by: [{_0}]")]
    SerializationFailure(Box<dyn std::error::Error + Send + Sync>),

    #[display("Deserialization failed, caused by: [{_0}]")]
    DeserializationFailure(Box<dyn std::error::Error + Send + Sync>),

    // -- DB
    #[display("Database file at [{}] not found.", std::path::absolute(file_path).unwrap().display())]
    DBNotFound { file_path: PathBuf },

    #[display("Database file at [{}] is corrupt, caused by: [{reason}]", std::path::absolute(file_path).unwrap().display())]
    DBCorrupt { file_path: PathBuf, reason: String },

    #[display("Database file at [{}] is inaccessible, caused by: [{reason}]", std::path::absolute(file_path).unwrap().display())]
    DBInaccessible {
        file_path: PathBuf,
        reason: std::io::Error,
    },

    #[display("Database operation failed: [{}], caused by: [{reason}]", std::path::absolute(path).unwrap().display())]
    DBOperationFailure { path: String, reason: String },

    #[display("Database transaction commit failed: [{}], caused by: [{reason}]", std::path::absolute(file_path).unwrap().display())]
    DBTransactionCommitFailure { file_path: PathBuf, reason: String },

    #[display("Database transaction rollback failed: [{}], caused by: [{reason}]", std::path::absolute(file_path).unwrap().display())]
    DBTransactionRollbackFailure { file_path: PathBuf, reason: String },
}
