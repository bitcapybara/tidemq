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

pub(crate) fn encoded_size(value: &impl serde::Serialize, limit: u64) -> std::io::Result<u64> {
    let mut counter = EncodingCounter { bytes: 0, limit };
    postcard::to_io(value, &mut counter)
        .map_err(|source| std::io::Error::other(source.to_string()))?;
    Ok(counter.bytes)
}

struct EncodingCounter {
    bytes: u64,
    limit: u64,
}
impl std::io::Write for EncodingCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = u64::try_from(bytes.len())
            .map_err(|_| std::io::Error::other("Encoded byte count overflow"))?;
        let next = self
            .bytes
            .checked_add(count)
            .filter(|next| *next <= self.limit)
            .ok_or_else(|| std::io::Error::other("Encoded byte capacity is exhausted"))?;
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
