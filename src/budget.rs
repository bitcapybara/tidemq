pub use tidemq_transport::budget::{
    Error, MemoryBudget, MemoryLimits, MemoryPermit, Result, class_name,
};

impl From<&crate::config::MemoryConfig> for MemoryLimits {
    fn from(config: &crate::config::MemoryConfig) -> Self {
        Self {
            total_bytes: u64::from(config.total_bytes),
            control_reserved_bytes: u64::from(config.control_reserved_bytes),
        }
    }
}
