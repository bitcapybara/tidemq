use crate::{Error, Result};
use tidemq_protocol::public;

#[derive(Clone, Debug)]
pub struct ResourceLimits(pub(crate) public::ResourceLimits);

#[derive(Clone, Debug)]
pub struct ResourceLimitsBuilder {
    limits: public::ResourceLimits,
    invalid: bool,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self(public::ResourceLimits::standard())
    }
}

impl ResourceLimits {
    pub fn builder() -> ResourceLimitsBuilder {
        ResourceLimitsBuilder {
            limits: public::ResourceLimits::default(),
            invalid: false,
        }
    }
}

macro_rules! limit {
    ($name:ident, $ty:ty) => {
        pub fn $name(mut self, value: $ty) -> Self {
            self.limits.$name = value;
            self.invalid |= value == 0;
            self
        }
    };
}

impl ResourceLimitsBuilder {
    limit!(topics, u32);
    limit!(subscriptions, u32);
    limit!(principals, u32);
    limit!(credentials, u32);
    limit!(connections, u32);
    limit!(members, u32);
    limit!(backlog_bytes, u64);
    limit!(inflight_bytes, u64);
    limit!(inflight_messages, u64);
    limit!(publish_bytes_per_second, u64);
    limit!(consume_bytes_per_second, u64);
    limit!(cold_reads, u64);

    pub fn build(self) -> Result<ResourceLimits> {
        if self.invalid {
            return Err(Error::Configuration {
                reason: "configured resource limits must be greater than zero",
            });
        }
        self.limits.validate().map_err(|_| Error::Configuration {
            reason: "resource limits must be positive and within their finite maxima",
        })?;
        Ok(ResourceLimits(self.limits))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyUpdate {
    Applied,
    Superseded,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_limits_preserve_unspecified_values_and_reject_invalid_explicit_values() {
        let partial = ResourceLimits::builder().connections(1).build().unwrap();
        assert_eq!(partial.0.connections, 1);
        assert_eq!(partial.0.backlog_bytes, 0);
        assert!(ResourceLimits::builder().connections(0).build().is_err());
        assert!(ResourceLimits::builder().members(65_537).build().is_err());
        assert!(ResourceLimits::default().0.backlog_bytes > 0);
    }
}
