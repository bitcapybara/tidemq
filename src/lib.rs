//! TideMQ service building blocks. Only the executable selects process-wide facilities.

pub mod allocator;
pub mod budget;
pub mod config;
pub mod error;
pub mod observability;

pub use error::{Error, Result};
