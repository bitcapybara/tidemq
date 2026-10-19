use std::path::PathBuf;

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid storage configuration: {reason}"))]
    Configuration { reason: &'static str },
    #[snafu(display("Storage IO failed for {}: {source}", path.display()))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Storage encoding failed: {source}"))]
    Encode { source: postcard::Error },
    #[snafu(display("Storage decoding failed: {source}"))]
    Decode { source: postcard::Error },
    #[snafu(display("Corrupt journal at byte {offset}: {reason}"))]
    Corrupt { offset: u64, reason: &'static str },
    #[snafu(display("Storage limit exceeded: {name}"))]
    Limit { name: &'static str },
    #[snafu(display("Journal writer is unavailable: {reason}"))]
    Writer { reason: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}
