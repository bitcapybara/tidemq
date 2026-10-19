//! TideMQ service building blocks. Only the executable selects process-wide facilities.

pub mod allocator;
pub mod archive;
pub mod budget;
pub mod config;
pub mod error;
pub mod object_store;
pub mod observability;
pub mod replication;
pub mod transport;
pub mod wal;

pub use error::{Error, Result};
