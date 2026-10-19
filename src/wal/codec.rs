use std::io::{self, Write};
use std::path::Path;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{Error, FRAME_HEADER_BYTES, JournalOptions, Result};

const MAGIC: [u8; 8] = *b"TIDEWAL1";
pub(crate) const HEADER_BYTES: u64 = FRAME_HEADER_BYTES as u64;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Inspection {
    pub file_bytes: u64,
    pub valid_bytes: u64,
    pub records: u64,
    pub incomplete_tail_bytes: u64,
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("Encoded storage value length overflow"))?;
        if end > self.limit {
            return Err(io::Error::other("Encoded storage value exceeds its limit"));
        }
        self.bytes
            .try_reserve(bytes.len())
            .map_err(io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn encode_value<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let mut output = BoundedBuffer {
        bytes: Vec::new(),
        limit,
    };
    output = postcard::to_io(value, output).map_err(|source| Error::Encode { source })?;
    Ok(output.bytes)
}

pub(crate) fn decode_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (value, remaining) =
        postcard::take_from_bytes(bytes).map_err(|source| Error::Decode { source })?;
    if !remaining.is_empty() {
        return Err(Error::Corrupt {
            offset: 0,
            reason: "encoded storage value has trailing bytes",
        });
    }
    Ok(value)
}

pub(crate) fn frame(payload: &[u8], sequence: u64) -> Result<Vec<u8>> {
    let length = u64::try_from(payload.len()).map_err(|_| Error::Limit {
        name: "record length",
    })?;
    let length_bytes = length.to_le_bytes();
    let sequence_bytes = sequence.to_le_bytes();
    let mut hash = blake3::Hasher::new();
    hash.update(&MAGIC);
    hash.update(&length_bytes);
    hash.update(&sequence_bytes);
    hash.update(payload);
    let capacity = payload
        .len()
        .checked_add(FRAME_HEADER_BYTES)
        .ok_or(Error::Limit {
            name: "frame length",
        })?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(capacity)
        .map_err(|_| Error::Limit {
            name: "frame allocation",
        })?;
    result.extend_from_slice(&MAGIC);
    result.extend_from_slice(&length_bytes);
    result.extend_from_slice(&sequence_bytes);
    let header_hash = blake3::hash(&result);
    result.extend_from_slice(header_hash.as_bytes());
    result.extend_from_slice(hash.finalize().as_bytes());
    result.extend_from_slice(payload);
    Ok(result)
}

/// Read-only validation. An incomplete final frame is reported, never repaired here.
pub async fn inspect(path: impl AsRef<Path>, options: &JournalOptions) -> Result<Inspection> {
    let (inspection, _) = scan(path.as_ref(), options, false).await?;
    Ok(inspection)
}

pub(crate) async fn scan(
    path: &Path,
    options: &JournalOptions,
    retain: bool,
) -> Result<(Inspection, Vec<Vec<u8>>)> {
    options.validate()?;
    let mut file = File::open(path).await.map_err(|e| Error::io(path, e))?;
    let file_bytes = file.metadata().await.map_err(|e| Error::io(path, e))?.len();
    if file_bytes > options.max_file_bytes {
        return Err(Error::Limit {
            name: "journal file",
        });
    }
    let mut report = Inspection {
        file_bytes,
        ..Inspection::default()
    };
    let mut records = Vec::new();
    while report.valid_bytes < file_bytes {
        let remaining = file_bytes.saturating_sub(report.valid_bytes);
        if remaining < HEADER_BYTES {
            break;
        }
        let mut header = [0_u8; FRAME_HEADER_BYTES];
        file.read_exact(&mut header)
            .await
            .map_err(|e| Error::io(path, e))?;
        let (length, sequence) = parse_header(&header, report.valid_bytes, options)?;
        let frame_bytes = HEADER_BYTES.checked_add(length).ok_or(Error::Limit {
            name: "frame length",
        })?;
        if frame_bytes > remaining {
            break;
        }
        if sequence != report.records {
            return Err(Error::Corrupt {
                offset: report.valid_bytes,
                reason: "record sequence is not contiguous",
            });
        }
        let size = usize::try_from(length).map_err(|_| Error::Limit {
            name: "record allocation",
        })?;
        let mut payload = Vec::new();
        payload.try_reserve_exact(size).map_err(|_| Error::Limit {
            name: "record allocation",
        })?;
        payload.resize(size, 0);
        file.read_exact(&mut payload)
            .await
            .map_err(|e| Error::io(path, e))?;
        validate_hash(&header, &payload, report.valid_bytes)?;
        report.valid_bytes = report
            .valid_bytes
            .checked_add(frame_bytes)
            .ok_or(Error::Limit {
                name: "journal offset",
            })?;
        report.records = report.records.checked_add(1).ok_or(Error::Limit {
            name: "journal record count",
        })?;
        if retain {
            records.push(payload);
        }
    }
    report.incomplete_tail_bytes = file_bytes.saturating_sub(report.valid_bytes);
    Ok((report, records))
}

fn parse_header(
    header: &[u8; FRAME_HEADER_BYTES],
    offset: u64,
    options: &JournalOptions,
) -> Result<(u64, u64)> {
    if header.get(..8) != Some(MAGIC.as_slice()) {
        return Err(Error::Corrupt {
            offset,
            reason: "invalid frame magic or format version",
        });
    }
    let prefix = header.get(..24).ok_or(Error::Corrupt {
        offset,
        reason: "frame header prefix is missing",
    })?;
    let checksum = header.get(24..56).ok_or(Error::Corrupt {
        offset,
        reason: "frame header checksum is missing",
    })?;
    if blake3::hash(prefix).as_bytes() != checksum {
        return Err(Error::Corrupt {
            offset,
            reason: "frame header checksum does not match",
        });
    }
    let length = read_u64(header.get(8..16), offset)?;
    let sequence = read_u64(header.get(16..24), offset)?;
    if length > options.max_record_bytes as u64 {
        return Err(Error::Corrupt {
            offset,
            reason: "frame length exceeds the configured limit",
        });
    }
    Ok((length, sequence))
}

fn read_u64(bytes: Option<&[u8]>, offset: u64) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .and_then(|b| b.try_into().ok())
        .ok_or(Error::Corrupt {
            offset,
            reason: "invalid integer in frame header",
        })?;
    Ok(u64::from_le_bytes(bytes))
}

fn validate_hash(header: &[u8; FRAME_HEADER_BYTES], payload: &[u8], offset: u64) -> Result<()> {
    let prefix = header.get(..24).ok_or(Error::Corrupt {
        offset,
        reason: "invalid checksum prefix",
    })?;
    let expected = header.get(56..FRAME_HEADER_BYTES).ok_or(Error::Corrupt {
        offset,
        reason: "invalid checksum field",
    })?;
    let mut hash = blake3::Hasher::new();
    hash.update(prefix);
    hash.update(payload);
    if hash.finalize().as_bytes() != expected {
        return Err(Error::Corrupt {
            offset,
            reason: "frame checksum does not match",
        });
    }
    Ok(())
}

pub(crate) fn unframe(bytes: &[u8], max_payload_bytes: usize) -> Result<&[u8]> {
    let header: [u8; FRAME_HEADER_BYTES] = bytes
        .get(..FRAME_HEADER_BYTES)
        .and_then(|part| part.try_into().ok())
        .ok_or(Error::Corrupt {
            offset: 0,
            reason: "snapshot or identity header is incomplete",
        })?;
    let options = JournalOptions {
        max_record_bytes: max_payload_bytes,
        ..JournalOptions::default()
    };
    let (length, sequence) = parse_header(&header, 0, &options)?;
    let payload = bytes.get(FRAME_HEADER_BYTES..).ok_or(Error::Corrupt {
        offset: 0,
        reason: "storage payload is missing",
    })?;
    if sequence != 0 || length != payload.len() as u64 {
        return Err(Error::Corrupt {
            offset: 0,
            reason: "snapshot or identity length is invalid",
        });
    }
    validate_hash(&header, payload, 0)?;
    Ok(payload)
}

pub(crate) async fn sync_directory(path: &Path) -> Result<()> {
    let directory = File::open(path).await.map_err(|e| Error::io(path, e))?;
    directory.sync_all().await.map_err(|e| Error::io(path, e))
}

pub(crate) async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or(Error::Configuration {
        reason: "storage file must have a parent directory",
    })?;
    let temporary = parent.join(format!(
        ".replace-{}",
        uuid::Uuid::from(tidemq_protocol::Id::random().map_err(|e| {
            Error::Writer {
                reason: e.to_string(),
            }
        })?)
    ));
    let result = async {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|e| Error::io(&temporary, e))?;
        file.write_all(bytes)
            .await
            .map_err(|e| Error::io(&temporary, e))?;
        file.sync_all()
            .await
            .map_err(|e| Error::io(&temporary, e))?;
        fs::rename(&temporary, path)
            .await
            .map_err(|e| Error::io(path, e))?;
        sync_directory(parent).await
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_file(&temporary).await;
    }
    result
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct Body {
        bytes: Bytes,
    }

    #[test]
    fn binary_byte_bodies_do_not_expand_to_json_arrays_and_trailing_bytes_are_rejected() {
        let body = Body {
            bytes: Bytes::from(vec![255; 65_536]),
        };
        let encoded = encode_value(&body, 65_552).unwrap();
        assert!(encoded.len() <= 65_552);
        assert_eq!(decode_value::<Body>(&encoded).unwrap(), body);
        let mut extra = encoded.clone();
        extra.push(0);
        assert!(matches!(
            decode_value::<Body>(&extra),
            Err(Error::Corrupt { .. })
        ));
        for length in [0, 1, 2, encoded.len() - 1] {
            assert!(decode_value::<Body>(encoded.get(..length).unwrap()).is_err());
        }
        assert!(encode_value(&body, 1024).is_err());
    }
}
