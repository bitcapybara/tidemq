//! TideMQ service building blocks. Only the executable selects process-wide facilities.

pub mod allocator;
pub mod archive;
pub mod broker;
pub mod budget;
pub mod config;
pub mod error;
pub mod gateway;
pub mod metadata;
pub mod object_store;
pub mod observability;
pub mod ownership;
pub mod producer;
pub mod replication;
pub mod retention;
pub mod runtime;
pub mod server;
pub mod subscription;
pub mod transport;
pub mod wal;

pub use error::{Error, Result};
