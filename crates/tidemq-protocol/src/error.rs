use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid protocol value: {reason}"))]
    Invalid { reason: &'static str },
    #[snafu(display("Protocol limit exceeded: {name}"))]
    Limit { name: &'static str },
    #[snafu(display("Protocol arithmetic overflow: {name}"))]
    Overflow { name: &'static str },
    #[snafu(display("Failed to obtain identity entropy"))]
    Entropy { source: getrandom::Error },
    #[snafu(display("Invalid UUID"))]
    Uuid { source: uuid::Error },
    #[snafu(display("Failed to reserve protocol buffer"))]
    Reserve {
        source: std::collections::TryReserveError,
    },
    #[snafu(display("Failed to decode protocol message"))]
    Decode { source: prost::DecodeError },
    #[snafu(display("Failed to encode protocol message"))]
    Encode { source: prost::EncodeError },
}
