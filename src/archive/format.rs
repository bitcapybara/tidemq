use std::{collections::BTreeSet, fmt, sync::Arc};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::{Id, common::TrafficClass, state::OffsetRange};
use tidemq_transport::budget::{MemoryBudget, MemoryPermit};

use super::{Error, Result};

const MAGIC: &[u8; 8] = b"TIDEPK02";
const FOOTER_MAGIC: &[u8; 8] = b"TIDEEND2";
const HEADER_BYTES: usize = 80;
const RECORD_BYTES: usize = 72;
const INDEX_BYTES: usize = 84;
const FOOTER_BYTES: usize = 92;

#[cfg(test)]
#[path = "../../tests/scalable_support/archive_context.rs"]
mod context_tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackIdentity {
    pub cluster_id: Id,
    pub group_id: u64,
    pub object_id: Id,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionIndex {
    pub topic_id: Id,
    pub partition_id: u32,
    pub offsets: OffsetRange,
    pub byte_start: u64,
    pub byte_end: u64,
    pub checksum: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Descriptor {
    pub identity: PackIdentity,
    pub total_bytes: u64,
    pub checksum: [u8; 32],
    pub partitions: Vec<PartitionIndex>,
}

#[derive(Clone, Copy, Debug)]
pub struct PackLimits {
    pub max_bytes: usize,
    pub max_partitions: usize,
    pub max_records: usize,
    pub max_message_bytes: usize,
}

impl Default for PackLimits {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            max_partitions: 1024,
            max_records: 100_000,
            max_message_bytes: 256 * 1024,
        }
    }
}

impl PackLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_bytes == 0
            || self.max_bytes > 512 * 1024 * 1024
            || self.max_partitions == 0
            || self.max_partitions > 1024
            || self.max_records == 0
            || self.max_records > 100_000
            || self.max_message_bytes == 0
            || self.max_message_bytes > 256 * 1024
        {
            return Err(Error::Invalid {
                reason: "pack limits exceed the supported hard bounds",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Record {
    pub offset: u64,
    pub key: Bytes,
    pub body: Bytes,
    pub context: Bytes,
}

/// A checked zero-copy view; one partition buffer keeps its byte admission alive.
#[derive(Clone)]
pub struct ArchivedRecord {
    pub offset: u64,
    buffer: Arc<RecordBuffer>,
    key_start: usize,
    context_start: usize,
    body_start: usize,
    body_end: usize,
}

struct RecordBuffer {
    bytes: Bytes,
    _index_memory: MemoryPermit,
}

impl ArchivedRecord {
    pub fn key(&self) -> &[u8] {
        self.buffer
            .bytes
            .get(self.key_start..self.context_start)
            .unwrap_or_default()
    }
    pub fn body(&self) -> &[u8] {
        self.buffer
            .bytes
            .get(self.body_start..self.body_end)
            .unwrap_or_default()
    }
    pub fn context(&self) -> &[u8] {
        self.buffer
            .bytes
            .get(self.context_start..self.body_start)
            .unwrap_or_default()
    }
}

impl fmt::Debug for ArchivedRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArchivedRecord")
            .field("offset", &self.offset)
            .field("key_bytes", &self.key().len())
            .field("body_bytes", &self.body().len())
            .finish()
    }
}

impl PartialEq for ArchivedRecord {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.key() == other.key()
            && self.body() == other.body()
            && self.context() == other.context()
    }
}
impl Eq for ArchivedRecord {}

impl fmt::Debug for Record {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Record")
            .field("offset", &self.offset)
            .field("key_bytes", &self.key.len())
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

pub struct CommittedPartition<'a> {
    pub group_id: u64,
    pub topic_id: Id,
    pub partition_id: u32,
    pub offsets: OffsetRange,
    pub committed_end: u64,
    pub records: &'a [Record],
}

#[derive(Clone)]
pub struct Pack {
    descriptor: Descriptor,
    bytes: Bytes,
}

impl fmt::Debug for Pack {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Pack")
            .field("descriptor", &self.descriptor)
            .finish()
    }
}

struct BudgetedBytes {
    bytes: Vec<u8>,
    _permit: MemoryPermit,
}

impl AsRef<[u8]> for BudgetedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Pack {
    /// Run large builds on a background blocking worker, outside the producer acknowledgement path.
    pub fn build(
        identity: PackIdentity,
        partitions: &[CommittedPartition<'_>],
        limits: PackLimits,
        budget: &MemoryBudget,
    ) -> Result<Self> {
        let total = encoded_size(identity, partitions, limits)?;
        let charged = u32::try_from(
            total
                .checked_mul(2)
                .and_then(|value| value.checked_add(64 * 1024))
                .ok_or(Error::Limit)?,
        )
        .map_err(|_| Error::Limit)?;
        let permit = budget
            .try_reserve(TrafficClass::Bulk, charged)
            .map_err(|source| Error::Memory { source })?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total)
            .map_err(|source| Error::Allocation { source })?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(identity.cluster_id.as_bytes());
        bytes.extend_from_slice(&identity.group_id.to_be_bytes());
        bytes.extend_from_slice(identity.object_id.as_bytes());
        let header_hash = blake3::hash(&bytes);
        bytes.extend_from_slice(header_hash.as_bytes());
        let mut indices = Vec::new();
        indices
            .try_reserve_exact(partitions.len())
            .map_err(|source| Error::Allocation { source })?;
        for partition in partitions {
            indices.push(encode_partition(&mut bytes, partition)?);
        }
        let index_start = bytes.len();
        for index in &indices {
            encode_index(&mut bytes, index);
        }
        let index_len = bytes.len().checked_sub(index_start).ok_or(Error::Limit)?;
        let index_hash = blake3::hash(bytes.get(index_start..).ok_or(Error::Limit)?);
        let footer_start = bytes.len();
        bytes.extend_from_slice(&(index_start as u64).to_be_bytes());
        bytes.extend_from_slice(&(index_len as u64).to_be_bytes());
        bytes.extend_from_slice(&(indices.len() as u32).to_be_bytes());
        bytes.extend_from_slice(index_hash.as_bytes());
        bytes.extend_from_slice(FOOTER_MAGIC);
        let footer_hash = blake3::hash(bytes.get(footer_start..).ok_or(Error::Limit)?);
        bytes.extend_from_slice(footer_hash.as_bytes());
        if bytes.len() != total {
            return Err(Error::Corrupt {
                reason: "pack encoder size mismatch",
            });
        }
        let descriptor = Descriptor {
            identity,
            total_bytes: total as u64,
            checksum: *blake3::hash(&bytes).as_bytes(),
            partitions: indices,
        };
        Ok(Self {
            descriptor,
            bytes: Bytes::from_owner(BudgetedBytes {
                bytes,
                _permit: permit,
            }),
        })
    }

    pub fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }
    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub fn validate(&self, limits: PackLimits) -> Result<()> {
        validate_pack(&self.descriptor, &self.bytes, limits)
    }
}

fn encoded_size(
    identity: PackIdentity,
    partitions: &[CommittedPartition<'_>],
    limits: PackLimits,
) -> Result<usize> {
    limits.validate()?;
    if identity.group_id == 0
        || partitions.is_empty()
        || partitions.len() > limits.max_partitions
        || limits.max_partitions > u32::MAX as usize
    {
        return Err(Error::Invalid {
            reason: "pack requires a group and bounded nonempty partitions",
        });
    }
    let mut keys = BTreeSet::new();
    let mut count = 0_usize;
    let mut bytes = HEADER_BYTES.checked_add(FOOTER_BYTES).ok_or(Error::Limit)?;
    for partition in partitions {
        if partition.group_id != identity.group_id
            || !keys.insert((partition.topic_id, partition.partition_id))
        {
            return Err(Error::Invalid {
                reason: "pack partitions must be unique and belong to one group",
            });
        }
        let end = partition
            .offsets
            .start
            .checked_add(partition.records.len() as u64)
            .ok_or(Error::Limit)?;
        if partition.records.is_empty()
            || end != partition.offsets.end
            || end > partition.committed_end
        {
            return Err(Error::Invalid {
                reason: "pack range must contain only contiguous committed records",
            });
        }
        count = count
            .checked_add(partition.records.len())
            .ok_or(Error::Limit)?;
        if count > limits.max_records {
            return Err(Error::Limit);
        }
        bytes = bytes.checked_add(INDEX_BYTES).ok_or(Error::Limit)?;
        for (sequence, record) in partition.records.iter().enumerate() {
            let expected = partition
                .offsets
                .start
                .checked_add(sequence as u64)
                .ok_or(Error::Limit)?;
            let payload = record
                .key
                .len()
                .checked_add(record.body.len())
                .and_then(|size| size.checked_add(record.context.len()))
                .ok_or(Error::Limit)?;
            if record.offset != expected
                || payload > limits.max_message_bytes
                || record.key.len() > u32::MAX as usize
                || record.body.len() > u32::MAX as usize
                || record.context.len() > u32::MAX as usize
            {
                return Err(Error::Invalid {
                    reason: "record offset or payload size is invalid",
                });
            }
            bytes = bytes
                .checked_add(RECORD_BYTES)
                .and_then(|size| size.checked_add(payload))
                .ok_or(Error::Limit)?;
            if bytes > limits.max_bytes {
                return Err(Error::Limit);
            }
        }
    }
    if bytes > limits.max_bytes || bytes > (u32::MAX as usize) / 2 {
        return Err(Error::Limit);
    }
    Ok(bytes)
}

fn encode_partition(
    bytes: &mut Vec<u8>,
    partition: &CommittedPartition<'_>,
) -> Result<PartitionIndex> {
    let start = bytes.len();
    for record in partition.records {
        let record_start = bytes.len();
        bytes.extend_from_slice(partition.topic_id.as_bytes());
        bytes.extend_from_slice(&partition.partition_id.to_be_bytes());
        bytes.extend_from_slice(&record.offset.to_be_bytes());
        bytes.extend_from_slice(&(record.key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&(record.body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&(record.context.len() as u32).to_be_bytes());
        let mut hash = blake3::Hasher::new();
        hash.update(b"tidemq-pack-record-v2\0");
        hash.update(bytes.get(record_start..).ok_or(Error::Limit)?);
        hash.update(&record.key);
        hash.update(&record.context);
        hash.update(&record.body);
        bytes.extend_from_slice(hash.finalize().as_bytes());
        bytes.extend_from_slice(&record.key);
        bytes.extend_from_slice(&record.context);
        bytes.extend_from_slice(&record.body);
    }
    Ok(PartitionIndex {
        topic_id: partition.topic_id,
        partition_id: partition.partition_id,
        offsets: partition.offsets,
        byte_start: start as u64,
        byte_end: bytes.len() as u64,
        checksum: *blake3::hash(bytes.get(start..).ok_or(Error::Limit)?).as_bytes(),
    })
}

fn encode_index(bytes: &mut Vec<u8>, index: &PartitionIndex) {
    bytes.extend_from_slice(index.topic_id.as_bytes());
    bytes.extend_from_slice(&index.partition_id.to_be_bytes());
    for number in [
        index.offsets.start,
        index.offsets.end,
        index.byte_start,
        index.byte_end,
    ] {
        bytes.extend_from_slice(&number.to_be_bytes());
    }
    bytes.extend_from_slice(&index.checksum);
}

impl Descriptor {
    pub fn validate(&self, limits: PackLimits) -> Result<()> {
        limits.validate()?;
        if self.identity.group_id == 0
            || self.partitions.is_empty()
            || self.partitions.len() > limits.max_partitions
            || self.total_bytes > limits.max_bytes as u64
        {
            return Err(Error::Limit);
        }
        let suffix = self
            .partitions
            .len()
            .checked_mul(INDEX_BYTES)
            .and_then(|size| size.checked_add(FOOTER_BYTES))
            .ok_or(Error::Limit)?;
        let index_start = self
            .total_bytes
            .checked_sub(suffix as u64)
            .ok_or(Error::Limit)?;
        let mut next = HEADER_BYTES as u64;
        let mut count = 0_u64;
        let mut keys = BTreeSet::new();
        for index in &self.partitions {
            let records = index
                .offsets
                .end
                .checked_sub(index.offsets.start)
                .ok_or(Error::Limit)?;
            count = count.checked_add(records).ok_or(Error::Limit)?;
            let minimum = records
                .checked_mul(RECORD_BYTES as u64)
                .ok_or(Error::Limit)?;
            if records == 0
                || count > limits.max_records as u64
                || index.byte_start != next
                || index.byte_end > index_start
                || index
                    .byte_end
                    .checked_sub(index.byte_start)
                    .ok_or(Error::Limit)?
                    < minimum
                || !keys.insert((index.topic_id, index.partition_id))
            {
                return Err(Error::Corrupt {
                    reason: "pack index is overlapping, empty, or outside the object",
                });
            }
            next = index.byte_end;
        }
        if next != index_start {
            return Err(Error::Corrupt {
                reason: "pack index leaves a gap",
            });
        }
        Ok(())
    }
}

pub fn validate_pack(descriptor: &Descriptor, bytes: &Bytes, limits: PackLimits) -> Result<()> {
    descriptor.validate(limits)?;
    if bytes.len() as u64 != descriptor.total_bytes
        || blake3::hash(bytes).as_bytes() != &descriptor.checksum
    {
        return Err(Error::Corrupt {
            reason: "whole pack checksum or length differs",
        });
    }
    let header = bytes.get(..48).ok_or(Error::Limit)?;
    let mut cursor = Cursor(header);
    if cursor.take(8)? != MAGIC
        || cursor.take(16)? != descriptor.identity.cluster_id.as_bytes()
        || cursor.u64()? != descriptor.identity.group_id
        || cursor.take(16)? != descriptor.identity.object_id.as_bytes()
        || bytes.get(48..HEADER_BYTES) != Some(blake3::hash(header).as_bytes().as_slice())
    {
        return Err(Error::Corrupt {
            reason: "pack header identity or checksum differs",
        });
    }
    let footer_start = bytes.len().checked_sub(FOOTER_BYTES).ok_or(Error::Limit)?;
    let footer = bytes.get(footer_start..).ok_or(Error::Limit)?;
    let mut cursor = Cursor(footer);
    let index_start = usize::try_from(cursor.u64()?).map_err(|_| Error::Limit)?;
    let index_len = usize::try_from(cursor.u64()?).map_err(|_| Error::Limit)?;
    let count = cursor.u32()? as usize;
    let index_hash = cursor.take(32)?;
    if cursor.take(8)? != FOOTER_MAGIC
        || cursor.take(32)? != blake3::hash(footer.get(..60).ok_or(Error::Limit)?).as_bytes()
        || count != descriptor.partitions.len()
        || index_start.checked_add(index_len) != Some(footer_start)
    {
        return Err(Error::Corrupt {
            reason: "pack footer differs",
        });
    }
    let index_bytes = bytes.get(index_start..footer_start).ok_or(Error::Limit)?;
    if index_hash != blake3::hash(index_bytes).as_bytes() {
        return Err(Error::Corrupt {
            reason: "pack index checksum differs",
        });
    }
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(index_len)
        .map_err(|source| Error::Allocation { source })?;
    for index in &descriptor.partitions {
        encode_index(&mut expected, index);
    }
    if expected != index_bytes {
        return Err(Error::Corrupt {
            reason: "pack descriptor differs from immutable index",
        });
    }
    for index in &descriptor.partitions {
        let start = usize::try_from(index.byte_start).map_err(|_| Error::Limit)?;
        let end = usize::try_from(index.byte_end).map_err(|_| Error::Limit)?;
        validate_records(index, bytes.get(start..end).ok_or(Error::Limit)?, limits)?;
    }
    Ok(())
}

pub(super) fn decode_records(
    index: &PartitionIndex,
    bytes: Bytes,
    limits: PackLimits,
    budget: &MemoryBudget,
) -> Result<Vec<ArchivedRecord>> {
    let count = validate_partition(index, &bytes, limits)?;
    let charged = count
        .checked_mul(std::mem::size_of::<ArchivedRecord>())
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or(Error::Limit)?;
    let permit = budget
        .try_reserve(
            TrafficClass::Bulk,
            u32::try_from(charged).map_err(|_| Error::Limit)?,
        )
        .map_err(|source| Error::Memory { source })?;
    let buffer = Arc::new(RecordBuffer {
        bytes,
        _index_memory: permit,
    });
    let mut records = Vec::new();
    records
        .try_reserve_exact(count)
        .map_err(|source| Error::Allocation { source })?;
    let mut cursor = Cursor(&buffer.bytes);
    for sequence in 0..count {
        let expected = index
            .offsets
            .start
            .checked_add(sequence as u64)
            .ok_or(Error::Limit)?;
        let record = decode_record(index, &buffer.bytes, &mut cursor, expected, limits)?;
        records.push(ArchivedRecord {
            offset: record.offset,
            buffer: buffer.clone(),
            key_start: record.key_start,
            context_start: record.context_start,
            body_start: record.body_start,
            body_end: record.body_end,
        });
    }
    cursor.finish()?;
    Ok(records)
}

fn validate_partition(index: &PartitionIndex, bytes: &[u8], limits: PackLimits) -> Result<usize> {
    if bytes.len() as u64
        != index
            .byte_end
            .checked_sub(index.byte_start)
            .ok_or(Error::Limit)?
        || blake3::hash(bytes).as_bytes() != &index.checksum
    {
        return Err(Error::Corrupt {
            reason: "partition range checksum or length differs",
        });
    }
    let count = usize::try_from(
        index
            .offsets
            .end
            .checked_sub(index.offsets.start)
            .ok_or(Error::Limit)?,
    )
    .map_err(|_| Error::Limit)?;
    if count > limits.max_records {
        return Err(Error::Limit);
    }
    Ok(count)
}

fn validate_records(index: &PartitionIndex, bytes: &[u8], limits: PackLimits) -> Result<()> {
    let count = validate_partition(index, bytes, limits)?;
    let mut cursor = Cursor(bytes);
    for sequence in 0..count {
        let expected = index
            .offsets
            .start
            .checked_add(sequence as u64)
            .ok_or(Error::Limit)?;
        decode_record(index, bytes, &mut cursor, expected, limits)?;
    }
    cursor.finish()
}

struct RecordView {
    offset: u64,
    key_start: usize,
    context_start: usize,
    body_start: usize,
    body_end: usize,
}

fn decode_record(
    index: &PartitionIndex,
    bytes: &[u8],
    cursor: &mut Cursor<'_>,
    expected: u64,
    limits: PackLimits,
) -> Result<RecordView> {
    let start = bytes
        .len()
        .checked_sub(cursor.0.len())
        .ok_or(Error::Limit)?;
    if cursor.take(16)? != index.topic_id.as_bytes() || cursor.u32()? != index.partition_id {
        return Err(Error::Corrupt {
            reason: "record partition identity differs",
        });
    }
    let offset = cursor.u64()?;
    let key_len = cursor.u32()? as usize;
    let body_len = cursor.u32()? as usize;
    let context_len = cursor.u32()? as usize;
    if offset != expected
        || key_len
            .checked_add(body_len)
            .and_then(|size| size.checked_add(context_len))
            .ok_or(Error::Limit)?
            > limits.max_message_bytes
    {
        return Err(Error::Corrupt {
            reason: "record offset or payload size differs",
        });
    }
    let checksum = cursor.take(32)?;
    let key_start = bytes
        .len()
        .checked_sub(cursor.0.len())
        .ok_or(Error::Limit)?;
    let key = cursor.take(key_len)?;
    let context_start = bytes
        .len()
        .checked_sub(cursor.0.len())
        .ok_or(Error::Limit)?;
    let context = cursor.take(context_len)?;
    let body_start = bytes
        .len()
        .checked_sub(cursor.0.len())
        .ok_or(Error::Limit)?;
    let body = cursor.take(body_len)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq-pack-record-v2\0");
    hash.update(
        bytes
            .get(start..start.checked_add(40).ok_or(Error::Limit)?)
            .ok_or(Error::Limit)?,
    );
    hash.update(key);
    hash.update(context);
    hash.update(body);
    if checksum != hash.finalize().as_bytes() {
        return Err(Error::Corrupt {
            reason: "record checksum differs",
        });
    }
    Ok(RecordView {
        offset,
        key_start,
        context_start,
        body_start,
        body_end: body_start.checked_add(body_len).ok_or(Error::Limit)?,
    })
}

pub(crate) fn view(bytes: &Bytes, start: usize, end: usize) -> Result<Bytes> {
    bytes.get(start..end).ok_or(Error::Limit)?;
    struct View {
        bytes: Bytes,
        start: usize,
        end: usize,
    }
    impl AsRef<[u8]> for View {
        fn as_ref(&self) -> &[u8] {
            // The private constructor validates the range and Bytes is immutable.
            self.bytes.get(self.start..self.end).unwrap_or_default()
        }
    }
    Ok(Bytes::from_owner(View {
        bytes: bytes.clone(),
        start,
        end,
    }))
}

struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn finish(&self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Error::Corrupt {
                reason: "partition range has trailing bytes",
            })
        }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let (value, rest) = self.0.split_at_checked(length).ok_or(Error::Corrupt {
            reason: "pack is truncated",
        })?;
        self.0 = rest;
        Ok(value)
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| Error::Limit)?,
        ))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| Error::Limit)?,
        ))
    }
}
