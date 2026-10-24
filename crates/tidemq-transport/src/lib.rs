//! Authenticated, bounded QUIC lanes. Transport receipt never means durable commit.

pub mod budget;
pub mod rpc;

mod auth;
mod endpoint;
mod error;
mod frame;
mod io;
mod node_auth;
mod policy;
mod session;
mod telemetry;

pub use auth::{
    Authorization, ClientCredentials, CredentialGrant, NodeGrant, ServerAuthorization,
    TokenAuthenticator, TokenGrant,
};
pub use endpoint::{Client, ClientTls, Server, ServerTls, TlsIdentity};
pub use error::{Error, Result};
pub use frame::{FrameReceiver, FrameSender, FramedStream, ReceivedFrame};
pub use node_auth::{AuthorizedNode, NodeAuthorizations};
pub use policy::{PeerTarget, TransportConfig};
pub use session::{Connection, Session, StreamPurpose};
pub use telemetry::{RecoverySnapshot, TransportMetrics, TransportSnapshot};
