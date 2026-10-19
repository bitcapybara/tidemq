use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum Error {
    #[snafu(display("Invalid transport configuration: {reason}"))]
    Configuration { reason: &'static str },
    #[snafu(display("Transport authentication rejected: {reason}"))]
    Authentication { reason: &'static str },
    #[snafu(display("Transport authorization rejected: {reason}"))]
    Authorization { reason: &'static str },
    #[snafu(display("Transport admission exhausted: {resource}"))]
    Admission { resource: &'static str },
    #[snafu(display("Transport deadline expired during {operation}"))]
    Deadline { operation: &'static str },
    #[snafu(display("Transport connection is closed"))]
    Closed,
    #[snafu(display("Transport lane cannot resume after a partial operation"))]
    Poisoned,
    #[snafu(display("Transport stream prelude is invalid"))]
    Prelude,
    #[snafu(display("Transport IO failed during {operation}"))]
    Io {
        operation: &'static str,
        source: std::io::Error,
    },
    #[snafu(display("QUIC connection failed during {operation}"))]
    Connection {
        operation: &'static str,
        source: s2n_quic::connection::Error,
    },
    #[snafu(display("QUIC stream failed during {operation}"))]
    Stream {
        operation: &'static str,
        source: s2n_quic::stream::Error,
    },
    #[snafu(display("Transport protocol validation failed"))]
    Protocol { source: tidemq_protocol::Error },
    #[snafu(display("Transport memory admission failed"))]
    Memory { source: crate::Error },
    #[snafu(display("Transport buffer allocation failed"))]
    Allocation {
        source: std::collections::TryReserveError,
    },
    #[snafu(display("QUIC endpoint setup failed during {operation}"))]
    Setup {
        operation: &'static str,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub(super) fn setup<E>(operation: &'static str, source: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::Setup {
        operation,
        source: Box::new(source),
    }
}
