use anyhow::{Context, Result, ensure};
use serde::Serialize;
use tidemq_sdk::{Bytes, Id, public};

const MAX_MESSAGES: u64 = 272;
const TAG: &[u8] = b"TDMQDEV1";

#[derive(Serialize)]
pub struct Ledger {
    pub expected: u64,
    pub committed: u64,
    pub received: u64,
    pub acknowledged: u64,
    pub frontier: u64,
    #[serde(skip)]
    run: Id,
    #[serde(skip)]
    body_bytes: usize,
    #[serde(skip)]
    seen: Vec<bool>,
    #[serde(skip)]
    acked: Vec<bool>,
}

impl Ledger {
    pub fn new(expected: u64, body_bytes: usize, run: Id) -> Result<Self> {
        ensure!(
            (1..=MAX_MESSAGES).contains(&expected),
            "Invalid ledger size"
        );
        ensure!((32..=65_536).contains(&body_bytes), "Invalid body size");
        let mut seen = Vec::new();
        seen.try_reserve_exact(usize::try_from(expected)?)?;
        seen.resize(usize::try_from(expected)?, false);
        let mut acked = Vec::new();
        acked.try_reserve_exact(seen.len())?;
        acked.resize(seen.len(), false);
        Ok(Self {
            expected,
            committed: 0,
            received: 0,
            acknowledged: 0,
            frontier: 0,
            run,
            body_bytes,
            seen,
            acked,
        })
    }

    pub fn messages(&self, first: u64, count: u64) -> Result<Vec<public::Message>> {
        let end = first.checked_add(count).context("Message range overflow")?;
        ensure!(
            count > 0 && count <= 8 && end <= self.expected,
            "Invalid batch range"
        );
        let mut messages = Vec::new();
        messages.try_reserve_exact(usize::try_from(count)?)?;
        for identity in first..end {
            let mut body = Vec::new();
            body.try_reserve_exact(self.body_bytes)?;
            body.extend_from_slice(self.run.as_bytes());
            body.extend_from_slice(&identity.to_be_bytes());
            body.extend_from_slice(TAG);
            body.resize(self.body_bytes, 0x5a);
            messages.push(public::Message {
                key: Bytes::copy_from_slice(&identity.to_be_bytes()),
                body: body.into(),
            });
        }
        Ok(messages)
    }

    pub fn commit(&mut self, first: u64, count: u32) -> Result<()> {
        let end = first
            .checked_add(u64::from(count))
            .context("Receipt overflow")?;
        ensure!(
            first == self.committed && end <= self.expected && count > 0,
            "Receipt must cover the next original batch"
        );
        self.committed = end;
        Ok(())
    }

    pub fn observe(&mut self, offset: u64, key: &[u8], body: &[u8]) -> Result<()> {
        let identity = u64::from_be_bytes(key.try_into().context("Invalid key shape")?);
        ensure!(
            offset == identity && offset < self.committed,
            "Delivery identity or offset mismatch"
        );
        ensure!(body.len() == self.body_bytes, "Delivery body size mismatch");
        ensure!(
            body.get(..16) == Some(self.run.as_bytes().as_slice()),
            "Delivery run mismatch"
        );
        ensure!(
            body.get(16..24) == Some(identity.to_be_bytes().as_slice()),
            "Body identity mismatch"
        );
        ensure!(body.get(24..32) == Some(TAG), "Delivery tag mismatch");
        ensure!(
            body.get(32..)
                .is_some_and(|tail| tail.iter().all(|byte| *byte == 0x5a)),
            "Delivery body content mismatch"
        );
        let slot = self
            .seen
            .get_mut(usize::try_from(offset)?)
            .context("Offset outside ledger")?;
        ensure!(!*slot, "Duplicate delivery offset or identity");
        *slot = true;
        self.received = self
            .received
            .checked_add(1)
            .context("Receive count overflow")?;
        Ok(())
    }

    pub fn ack(&mut self, offset: u64, returned_frontier: u64) -> Result<()> {
        let index = usize::try_from(offset)?;
        ensure!(
            self.seen.get(index) == Some(&true),
            "Cannot ACK an unseen delivery"
        );
        ensure!(self.acked.get(index) == Some(&false), "Duplicate ACK");
        let mut next = self.frontier;
        while next < self.expected {
            if next != offset && self.acked.get(usize::try_from(next)?) != Some(&true) {
                break;
            }
            next = next.checked_add(1).context("ACK frontier overflow")?;
        }
        ensure!(returned_frontier == next, "Durable ACK frontier mismatch");
        *self.acked.get_mut(index).context("ACK outside ledger")? = true;
        self.acknowledged = self
            .acknowledged
            .checked_add(1)
            .context("ACK count overflow")?;
        self.frontier = next;
        Ok(())
    }

    pub fn complete(&self, target: u64) -> Result<()> {
        ensure!(
            self.committed == target
                && self.received == target
                && self.acknowledged == target
                && self.frontier == target,
            "The complete original message range must be committed, delivered and durably acknowledged"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reordered_parallel_delivery_preserves_identity_uniqueness_and_contiguous_ack() {
        let mut ledger = Ledger::new(3, 32, Id::random().unwrap()).unwrap();
        let messages = ledger.messages(0, 3).unwrap();
        ledger.commit(0, 3).unwrap();
        for (offset, frontier) in [(2_u64, 0), (0, 1), (1, 3)] {
            let message = messages.get(offset as usize).unwrap();
            ledger.observe(offset, &message.key, &message.body).unwrap();
            ledger.ack(offset, frontier).unwrap();
        }
        ledger.complete(3).unwrap();
        let message = messages.first().unwrap();
        assert!(ledger.observe(0, &message.key, &message.body).is_err());
        assert!(ledger.ack(0, 3).is_err());
    }

    #[test]
    fn corruption_wrong_offsets_and_premature_ack_fail() {
        let mut ledger = Ledger::new(2, 64, Id::random().unwrap()).unwrap();
        let messages = ledger.messages(0, 2).unwrap();
        let message = messages.first().unwrap();
        assert!(ledger.observe(0, &message.key, &message.body).is_err());
        ledger.commit(0, 2).unwrap();
        assert!(ledger.observe(1, &message.key, &message.body).is_err());
        assert!(ledger.ack(0, 1).is_err());
        let mut body = message.body.to_vec();
        *body.last_mut().unwrap() = 0;
        assert!(ledger.observe(0, &message.key, &body).is_err());
        ledger.observe(0, &message.key, &message.body).unwrap();
        assert!(ledger.ack(0, 2).is_err());
        assert!(ledger.complete(2).is_err());
    }
}
