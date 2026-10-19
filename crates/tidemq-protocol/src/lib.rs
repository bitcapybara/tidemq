//! Shared identities, bounded framing, and versioned TideMQ wire messages.

mod error;
pub mod framing;
pub mod identity;
pub mod limits;
pub mod producer;
pub mod state;

pub use error::{Error, Result};
pub use identity::Id;
pub use limits::ProtocolLimits;

pub use generated::tidemq::{common::v1 as common, internal::v1 as internal, public::v1 as public};

mod generated {
    pub mod tidemq {
        pub mod common {
            pub mod v1 {
                include!("generated/tidemq.common.v1.rs");
            }
        }
        pub mod public {
            pub mod v1 {
                include!("generated/tidemq.public.v1.rs");
            }
        }
        pub mod internal {
            pub mod v1 {
                include!("generated/tidemq.internal.v1.rs");
            }
        }
    }
}
