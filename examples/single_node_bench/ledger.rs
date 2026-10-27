use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use serde::Serialize;
use tidemq_protocol::Id;
use tidemq_sdk::Message;

const TAG: &[u8] = b"TDMQPER1";

#[derive(Default, Serialize)]
pub struct Entry {
    pub sequence: usize,
    pub started_ns: Option<u64>,
    pub committed_ns: Option<u64>,
    pub received_ns: Option<u64>,
    pub acknowledged_ns: Option<u64>,
    pub committed_id: Option<String>,
    pub received_id: Option<String>,
}

#[derive(Serialize)]
pub struct Ledger {
    pub run_id: Id,
    pub entries: Vec<Entry>,
    #[serde(skip)]
    committed_ids: BTreeSet<String>,
    #[serde(skip)]
    received_ids: BTreeSet<String>,
}

impl Ledger {
    pub fn new(count: usize, run_id: Id) -> Result<Self> {
        ensure!(count <= 200_000, "Ledger exceeds 200000 messages");
        let mut entries = Vec::new();
        entries.try_reserve_exact(count)?;
        entries.extend((0..count).map(|sequence| Entry {
            sequence,
            ..Default::default()
        }));
        Ok(Self {
            run_id,
            entries,
            committed_ids: BTreeSet::new(),
            received_ids: BTreeSet::new(),
        })
    }

    pub fn start(&mut self, sequence: usize, at: u64) -> Result<()> {
        let entry = self
            .entries
            .get_mut(sequence)
            .context("Send outside ledger")?;
        ensure!(
            entry.started_ns.is_none(),
            "Business identity submitted twice"
        );
        entry.started_ns = Some(at);
        Ok(())
    }

    pub fn commit(&mut self, sequence: usize, id: String, at: u64) -> Result<()> {
        let entry = self
            .entries
            .get_mut(sequence)
            .context("Commit outside ledger")?;
        ensure!(entry.started_ns.is_some(), "Commit without send start");
        ensure!(entry.committed_id.is_none(), "Duplicate commit sequence");
        ensure!(
            self.committed_ids.insert(id.clone()),
            "Duplicate committed identity"
        );
        ensure!(
            entry.received_id.as_ref().is_none_or(|seen| *seen == id),
            "Receipt differs from delivery identity"
        );
        entry.committed_id = Some(id);
        entry.committed_ns = Some(at);
        Ok(())
    }

    pub fn receive(&mut self, sequence: usize, id: String, at: u64) -> Result<()> {
        let entry = self
            .entries
            .get_mut(sequence)
            .context("Delivery outside ledger")?;
        ensure!(entry.started_ns.is_some(), "Delivery without send start");
        ensure!(entry.received_id.is_none(), "Duplicate delivery sequence");
        ensure!(
            self.received_ids.insert(id.clone()),
            "Duplicate delivery identity"
        );
        ensure!(
            entry.committed_id.as_ref().is_none_or(|sent| *sent == id),
            "Delivery differs from committed identity"
        );
        // A delivery can reach the client before the send receipt does.
        entry.received_id = Some(id);
        entry.received_ns = Some(at);
        Ok(())
    }

    pub fn ack(&mut self, sequence: usize, at: u64) -> Result<()> {
        let entry = self
            .entries
            .get_mut(sequence)
            .context("ACK outside ledger")?;
        ensure!(entry.received_ns.is_some(), "ACK without delivery");
        ensure!(entry.acknowledged_ns.is_none(), "Duplicate ACK sequence");
        entry.acknowledged_ns = Some(at);
        Ok(())
    }

    pub fn complete(&self) -> Result<()> {
        for entry in &self.entries {
            let started = entry.started_ns.context("Missing send sequence")?;
            let committed = entry.committed_ns.context("Missing commit sequence")?;
            let received = entry.received_ns.context("Missing delivery sequence")?;
            let acked = entry.acknowledged_ns.context("Missing ACK sequence")?;
            ensure!(
                committed >= started && received >= started && acked >= received,
                "Invalid monotonic timestamps"
            );
            ensure!(
                entry.committed_id.is_some() && entry.committed_id == entry.received_id,
                "Message identity mismatch"
            );
        }
        Ok(())
    }
}

pub fn message(run: Id, sequence: usize, size: usize) -> Result<Message> {
    ensure!((32..=65_536).contains(&size), "Invalid message size");
    let mut body = Vec::new();
    body.try_reserve_exact(size)?;
    body.extend_from_slice(run.as_bytes());
    body.extend_from_slice(&u64::try_from(sequence)?.to_be_bytes());
    body.extend_from_slice(TAG);
    body.resize(size, 0x5a);
    Ok(Message::new(body).key(Bytes::copy_from_slice(
        &u64::try_from(sequence)?.to_be_bytes(),
    )))
}

pub fn sequence(run: Id, key: Option<&[u8]>, body: &[u8], size: usize) -> Result<usize> {
    let key: [u8; 8] = key
        .context("Missing delivery key")?
        .try_into()
        .context("Invalid key size")?;
    ensure!(body.len() == size, "Payload size mismatch");
    ensure!(
        body.get(..16) == Some(run.as_bytes().as_slice()),
        "Delivery belongs to another run"
    );
    ensure!(
        body.get(16..24) == Some(key.as_slice()) && body.get(24..32) == Some(TAG),
        "Payload identity mismatch"
    );
    ensure!(
        body.get(32..)
            .is_some_and(|tail| tail.iter().all(|byte| *byte == 0x5a)),
        "Payload content mismatch"
    );
    Ok(usize::try_from(u64::from_be_bytes(key))?)
}

#[derive(Debug, Serialize)]
pub struct Latency {
    pub samples: usize,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
}

pub fn latency(mut values: Vec<u64>) -> Result<Option<Latency>> {
    if values.is_empty() {
        return Ok(None);
    }
    values.sort_unstable();
    let percentile = |percent: usize| -> Result<u64> {
        let rank = values
            .len()
            .checked_mul(percent)
            .context("Percentile rank overflow")?
            .div_ceil(100);
        Ok(*values
            .get(rank.checked_sub(1).context("Invalid percentile rank")?)
            .context("Percentile outside samples")?)
    };
    Ok(Some(Latency {
        samples: values.len(),
        p50_ns: percentile(50)?,
        p95_ns: percentile(95)?,
        p99_ns: percentile(99)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_uses_nearest_rank_without_interpolation() {
        assert!(latency(Vec::new()).unwrap().is_none());
        let value = latency((1..=100).rev().collect()).unwrap().unwrap();
        assert_eq!(
            (value.samples, value.p50_ns, value.p95_ns, value.p99_ns),
            (100, 50, 95, 99)
        );
        assert_eq!(latency(vec![7]).unwrap().unwrap().p99_ns, 7);
    }

    #[test]
    fn delivery_before_receipt_is_valid_but_gaps_duplicates_and_wrong_ids_fail() {
        let mut ledger = Ledger::new(2, Id::new_v7().unwrap()).unwrap();
        ledger.start(1, 5).unwrap();
        ledger.receive(1, "id-1".into(), 6).unwrap();
        ledger.ack(1, 7).unwrap();
        assert!(ledger.commit(1, "wrong".into(), 8).is_err());
        ledger.commit(1, "id-1".into(), 8).unwrap();
        assert!(ledger.complete().is_err());
        assert!(ledger.receive(1, "id-1".into(), 9).is_err());
        assert!(ledger.ack(1, 9).is_err());
        assert!(ledger.start(1, 9).is_err());
        assert!(ledger.start(2, 9).is_err());
        ledger.start(0, 4).unwrap();
        ledger.commit(0, "id-0".into(), 5).unwrap();
        ledger.receive(0, "id-0".into(), 8).unwrap();
        ledger.ack(0, 9).unwrap();
        ledger.complete().unwrap();
    }

    #[test]
    fn payload_checks_run_sequence_size_and_content() {
        let run = Id::new_v7().unwrap();
        let mut body = Vec::from(run.as_bytes().as_slice());
        body.extend_from_slice(&3_u64.to_be_bytes());
        body.extend_from_slice(TAG);
        body.resize(64, 0x5a);
        assert_eq!(
            sequence(run, Some(&3_u64.to_be_bytes()), &body, 64).unwrap(),
            3
        );
        assert!(sequence(run, None, &body, 64).is_err());
        assert!(sequence(run, Some(&4_u64.to_be_bytes()), &body, 64).is_err());
        assert!(sequence(Id::new_v7().unwrap(), Some(&3_u64.to_be_bytes()), &body, 64).is_err());
        *body.last_mut().unwrap() = 0;
        assert!(sequence(run, Some(&3_u64.to_be_bytes()), &body, 64).is_err());
        assert!(message(run, 0, 31).is_err());
        assert!(message(run, 0, 65_537).is_err());
    }
}
