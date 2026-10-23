use tidemq::metadata::{PublisherCommand, PublishersBook, Rejection};
use tidemq_protocol::Id;

fn id() -> Id {
    Id::new_v7().unwrap()
}

#[test]
fn publisher_registration_is_fenced_and_renewals_cannot_revive_expired_or_closed_sessions() {
    let mut book = PublishersBook::default();
    let topic = id();
    let producer = id();
    let client = id();
    let credential = id();
    let operation = id();
    let open = PublisherCommand::Open {
        topic,
        producer,
        registration: 1,
        client,
        credential,
        now_ms: 1,
        credential_expires_ms: 1_000_000,
        digest: [1; 32],
    };
    let permit = book.apply(operation, &open).unwrap();
    assert_eq!(permit.registration, 1);
    assert_eq!(book.next_registration(), 2);
    assert_eq!(book.apply(operation, &open).unwrap(), permit);
    let renewal_operation = id();
    let heartbeat = PublisherCommand::Heartbeat {
        producer,
        registration: 1,
        client,
        credential,
        sequence: 1,
        now_ms: 10_000,
        credential_expires_ms: 1_000_000,
        digest: [2; 32],
    };
    let renewed = book.apply(renewal_operation, &heartbeat).unwrap();
    assert!(renewed.expires_ms > permit.expires_ms);
    assert_eq!(book.apply(renewal_operation, &heartbeat).unwrap(), renewed);
    let late = PublisherCommand::Heartbeat {
        producer,
        registration: 1,
        client,
        credential,
        sequence: 2,
        now_ms: 400_000,
        credential_expires_ms: 1_000_000,
        digest: [3; 32],
    };
    assert_eq!(book.apply(id(), &late), Err(Rejection::Superseded));
    assert!(!book.record(producer, 1).unwrap().permit_at(400_000).live);
    let close = PublisherCommand::Close {
        producer,
        registration: 1,
        client,
        credential,
    };
    assert!(!book.apply(id(), &close).unwrap().live);
    assert!(!book.apply(operation, &open).unwrap().live);
    let stolen = PublisherCommand::Close {
        producer,
        registration: 1,
        client: id(),
        credential,
    };
    assert_eq!(book.apply(id(), &stolen), Err(Rejection::InvalidEvidence));
    book.validate().unwrap();
    let restored: PublishersBook =
        postcard::from_bytes(&postcard::to_stdvec(&book).unwrap()).unwrap();
    assert_eq!(restored, book);
}
