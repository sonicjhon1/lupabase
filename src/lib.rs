#![feature(error_generic_member_access)]
#![doc = include_str!("../README.md")]

pub mod database;
pub mod engine;
pub mod error;
pub mod prelude;
pub mod record;
pub mod record_utils;
mod utils;

pub use error::{Error, Result};
pub(crate) use serde::{Deserialize, Serialize};
