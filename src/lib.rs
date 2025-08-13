#![feature(path_add_extension)]
#![feature(error_generic_member_access)]
#![doc = include_str!("../README.md")]

pub mod database;
pub mod engine;
mod error;

pub use error::{Error, Result};
pub(crate) use serde::{Deserialize, Serialize};
