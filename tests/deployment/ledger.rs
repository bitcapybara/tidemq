use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use serde::Serialize;
use tidemq_protocol::Id;
use tidemq_sdk::Message;

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
    message_ids: Vec<Option<String>>,
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
            message_ids: vec![None; seen.len()],
            seen,
            acked,
        })
    }

    pub fn messages(&self, first: u64, count: u64) -> Result<Vec<Message>> {
        let end = first.checked_add(count).context("Message range overflow")?;
        ensure!(
            count > 0 && count <= 8 && end <= self.expected,
            "Invalid batch range"
        );
        let mut messages = Vec::new();
        messages.try_reserve_exact(usize::try_from(count)?)?;
        for identity in first..end {
            let body = self.payload(identity)?;
            messages.push(Message::new(body).key(Bytes::copy_from_slice(&identity.to_be_bytes())));
        }
        Ok(messages)
    }

    fn payload(&self, identity: u64) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        body.try_reserve_exact(self.body_bytes)?;
        body.extend_from_slice(self.run.as_bytes());
        body.extend_from_slice(&identity.to_be_bytes());
        body.extend_from_slice(TAG);
        body.resize(self.body_bytes, 0x5a);
        Ok(body)
    }

    pub fn commit(&mut self, first: u64, receipts: &[String]) -> Result<()> {
        let end = first
            .checked_add(u64::try_from(receipts.len())?)
            .context("Receipt overflow")?;
        ensure!(
            first == self.committed && end <= self.expected && !receipts.is_empty(),
            "Receipts must cover the next original business range"
        );
        for (index, receipt) in receipts.iter().enumerate() {
            let id = receipt.clone();
            ensure!(
                !self.message_ids.contains(&Some(id.clone())),
                "Duplicate committed message identity"
            );
            let index = usize::try_from(first)?
                .checked_add(index)
                .context("Receipt index overflow")?;
            *self
                .message_ids
                .get_mut(index)
                .context("Receipt outside ledger")? = Some(id);
        }
        self.committed = end;
        Ok(())
    }

    pub fn observe(&mut self, message_id: &str, key: &[u8], body: &[u8]) -> Result<u64> {
        let identity = u64::from_be_bytes(key.try_into().context("Invalid key shape")?);
        ensure!(
            identity < self.committed
                && self
                    .message_ids
                    .get(usize::try_from(identity)?)
                    .and_then(Option::as_deref)
                    == Some(message_id),
            "Delivery message identity differs from its original committed receipt"
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
            .get_mut(usize::try_from(identity)?)
            .context("Business identity outside ledger")?;
        ensure!(!*slot, "Duplicate delivery identity");
        *slot = true;
        self.received = self
            .received
            .checked_add(1)
            .context("Receive count overflow")?;
        Ok(identity)
    }

    pub fn ack(&mut self, offset: u64) -> Result<()> {
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
    fn reordered_messages_keep_business_prefix_separate_from_birth_offsets() {
        let mut ledger = Ledger::new(3, 64, Id::new_v7().unwrap()).unwrap();
        let ids = ["topic:shard-a:9", "topic:shard-b:9", "topic:shard-a:42"].map(str::to_owned);
        ledger.commit(0, &ids).unwrap();
        for (identity, frontier) in [(2_u64, 0), (0, 1), (1, 3)] {
            let body = ledger.payload(identity).unwrap();
            assert_eq!(
                ledger
                    .observe(
                        ids.get(identity as usize).unwrap(),
                        &identity.to_be_bytes(),
                        &body
                    )
                    .unwrap(),
                identity
            );
            ledger.ack(identity).unwrap();
            assert_eq!(ledger.frontier, frontier);
        }
        ledger.complete(3).unwrap();
        assert!(ledger.ack(0).is_err());
        assert!(
            ledger
                .observe(
                    ids.first().unwrap(),
                    &0_u64.to_be_bytes(),
                    &ledger.payload(0).unwrap()
                )
                .is_err()
        );
    }

    #[test]
    fn foreign_identity_corrupt_content_and_unseen_ack_are_rejected() {
        let mut ledger = Ledger::new(2, 64, Id::new_v7().unwrap()).unwrap();
        let body = ledger.payload(0).unwrap();
        let key = 0_u64.to_be_bytes();
        assert!(ledger.observe("a", &key, &body).is_err());
        ledger.commit(0, &["a".into(), "b".into()]).unwrap();
        assert!(ledger.observe("b", &key, &body).is_err());
        assert!(ledger.ack(0).is_err());
        let mut corrupt = body.clone();
        *corrupt.last_mut().unwrap() = 0;
        assert!(ledger.observe("a", &key, &corrupt).is_err());
        ledger.observe("a", &key, &body).unwrap();
        ledger.ack(0).unwrap();
        assert!(ledger.complete(2).is_err());
    }
}
