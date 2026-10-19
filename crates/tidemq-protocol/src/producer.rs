use crate::{Error, Id, Result, public::Message};

/// Routes a key with the version-one hash and a fixed, nonzero topic partition count.
/// Keep the count and algorithm unchanged for the lifetime of a topic.
pub fn partition_for_key(key: &[u8], partition_count: u32) -> Result<u32> {
    if partition_count == 0 {
        return Err(Error::Invalid {
            reason: "key routing requires a nonzero partition count",
        });
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq/key-routing/v1\0");
    hash.update(key);
    let digest = hash.finalize();
    let prefix = digest.as_bytes().get(..8).ok_or(Error::Invalid {
        reason: "key routing digest is incomplete",
    })?;
    let value = u64::from_be_bytes(prefix.try_into().map_err(|_| Error::Invalid {
        reason: "key routing digest prefix is invalid",
    })?);
    u32::try_from(value % u64::from(partition_count)).map_err(|_| Error::Overflow {
        name: "key routing partition",
    })
}

/// A stable digest of logical content, independent of retries and owner changes.
pub fn canonical_produce_digest(
    topic: Id,
    partition: u32,
    producer: Id,
    epoch: u64,
    first_sequence: u64,
    messages: &[Message],
) -> Result<[u8; 32]> {
    canonical_produce_digest_parts(
        topic,
        partition,
        producer,
        epoch,
        first_sequence,
        messages
            .iter()
            .map(|message| (message.key.as_ref(), message.body.as_ref())),
    )
}

/// The borrowed form lets storage and SDK callers hash without copying payloads.
pub fn canonical_produce_digest_parts<'a>(
    topic: Id,
    partition: u32,
    producer: Id,
    epoch: u64,
    first_sequence: u64,
    messages: impl ExactSizeIterator<Item = (&'a [u8], &'a [u8])>,
) -> Result<[u8; 32]> {
    if epoch == 0 || messages.len() == 0 {
        return Err(Error::Invalid {
            reason: "a producer batch requires a nonzero epoch and messages",
        });
    }
    let count = u64::try_from(messages.len()).map_err(|_| Error::Overflow {
        name: "batch message count",
    })?;
    first_sequence.checked_add(count).ok_or(Error::Overflow {
        name: "producer sequence",
    })?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq/produce-digest/v1\0");
    hash.update(topic.as_bytes());
    hash.update(&partition.to_be_bytes());
    hash.update(producer.as_bytes());
    hash.update(&epoch.to_be_bytes());
    hash.update(&first_sequence.to_be_bytes());
    hash.update(&count.to_be_bytes());
    let mut actual_count = 0_u64;
    for (key, body) in messages {
        hash_bytes(&mut hash, key)?;
        hash_bytes(&mut hash, body)?;
        actual_count = actual_count.checked_add(1).ok_or(Error::Overflow {
            name: "batch message count",
        })?;
    }
    if actual_count != count {
        return Err(Error::Invalid {
            reason: "batch iterator length changed",
        });
    }
    Ok(*hash.finalize().as_bytes())
}

fn hash_bytes(hash: &mut blake3::Hasher, bytes: &[u8]) -> Result<()> {
    let length = u64::try_from(bytes.len()).map_err(|_| Error::Overflow {
        name: "message bytes",
    })?;
    hash.update(&length.to_be_bytes());
    hash.update(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_routing_is_versioned_and_rejects_an_empty_partition_set() {
        assert!(partition_for_key(b"key", 0).is_err());
        for (key, expected) in [
            (b"".as_slice(), 10),
            (b"customer-42".as_slice(), 65),
            ([0, 255].as_slice(), 59),
        ] {
            assert_eq!(partition_for_key(key, 1).unwrap(), 0);
            assert_eq!(partition_for_key(key, 97).unwrap(), expected);
        }
    }

    fn id(byte: u8) -> Id {
        Id::from_slice(&[byte; 16]).unwrap()
    }

    fn digest(parts: &[(&[u8], &[u8])]) -> [u8; 32] {
        canonical_produce_digest_parts(id(1), 2, id(3), 4, 5, parts.iter().copied()).unwrap()
    }

    #[test]
    fn message_boundaries_identity_and_sequence_are_unambiguous() {
        let expected = digest(&[(b"ab", b"c")]);
        for parts in [
            vec![(b"a".as_slice(), b"bc".as_slice())],
            vec![
                (b"ab".as_slice(), b"c".as_slice()),
                (b"".as_slice(), b"".as_slice()),
            ],
        ] {
            assert_ne!(expected, digest(&parts));
        }
        for (topic, partition, producer, epoch, sequence) in [
            (id(9), 2, id(3), 4, 5),
            (id(1), 9, id(3), 4, 5),
            (id(1), 2, id(9), 4, 5),
            (id(1), 2, id(3), 9, 5),
            (id(1), 2, id(3), 4, 9),
        ] {
            assert_ne!(
                expected,
                canonical_produce_digest_parts(
                    topic,
                    partition,
                    producer,
                    epoch,
                    sequence,
                    [(b"ab".as_slice(), b"c".as_slice())].into_iter()
                )
                .unwrap()
            );
        }
    }

    #[test]
    fn wire_and_borrowed_content_match_and_invalid_ranges_fail() {
        let messages = [Message {
            key: b"ab".as_slice().into(),
            body: b"c".as_slice().into(),
        }];
        assert_eq!(
            digest(&[(b"ab", b"c")]),
            canonical_produce_digest(id(1), 2, id(3), 4, 5, &messages).unwrap()
        );
        assert!(canonical_produce_digest(id(1), 2, id(3), 0, 0, &messages).is_err());
        assert!(canonical_produce_digest(id(1), 2, id(3), 1, u64::MAX, &messages).is_err());
        assert!(canonical_produce_digest(id(1), 2, id(3), 1, 0, &[]).is_err());
    }
}
