use super::*;
use tidemq_protocol::topic::{SendIdentity, message_digest};

#[test]
fn message_values_keep_absent_keys_distinct_bound_properties_and_redact_debug() {
    let identity = SendIdentity {
        topic: Id::new_v7().unwrap(),
        producer: Id::new_v7().unwrap(),
        registration: 1,
        sequence: 1,
    };
    let absent = Message::new("private-payload").property("version", "private-value");
    let empty = absent.clone().key("");
    assert!(absent.wire.key.is_none());
    assert_eq!(empty.wire.key.as_deref(), Some(b"".as_slice()));
    let digest = |message: &Message| {
        message_digest(
            identity,
            message.wire.key.as_deref(),
            &message.wire.payload,
            &message.wire.properties,
        )
        .unwrap()
    };
    assert_ne!(digest(&absent), digest(&empty));
    assert!(absent.memory_bytes(256 * 1024).unwrap() > 4096);
    assert!(!format!("{absent:?}").contains("private-"));
    let mut excess = Message::new("");
    for index in 0..129 {
        excess = excess.property(format!("property-{index}"), "value");
    }
    assert!(excess.memory_bytes(256 * 1024).is_err());
}
