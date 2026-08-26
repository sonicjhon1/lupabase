#![doc = include_str!("../README.md")]

pub mod database;
pub mod engine;
pub mod error;
pub mod operations;
pub mod prelude;
pub mod procedures;
pub mod record;
pub mod serdes;
mod utils;

pub use error::{Error, Result};
pub(crate) use serde_core::{Deserialize, Serialize};
