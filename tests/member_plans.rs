use tidemq::metadata::{MemberHeartbeat, MemberJoin, MemberStatus, MembersBook, Rejection};
use tidemq_protocol::{
    Id,
    topic::{InitialPosition, SubscriptionKind},
};

fn id() -> Id {
    Id::new_v7().unwrap()
}

fn activate(book: &mut MembersBook, now: u64) {
    while !book.plan().active {
        let plan = book.plan().clone();
        for domain in &plan.domains {
            book.observe_installed(plan.version, plan.fenced, *domain, now)
                .unwrap();
        }
    }
}

fn join(
    book: &mut MembersBook,
    member: Id,
    client: Id,
    credential: Id,
    now: u64,
) -> (Id, MemberJoin) {
    let operation = id();
    let request = MemberJoin {
        member,
        expected_epoch: book.next_epoch(),
        client,
        credential,
        now_ms: now,
        credential_expires_ms: 1_000_000,
        max_messages: 32,
        max_bytes: 65_536,
        digest: [7; 32],
    };
    book.join(operation, &request).unwrap();
    (operation, request)
}

#[test]
fn retiring_members_allows_repeated_clean_shutdown_without_reviving_old_epochs() {
    let mut book = MembersBook::new(
        id(),
        SubscriptionKind::Queue,
        InitialPosition::Latest,
        1,
        vec![id()],
    )
    .unwrap();
    activate(&mut book, 0);
    let mut first = None;
    for index in 0..300 {
        let (operation, request) = join(&mut book, id(), id(), id(), index * 10 + 1);
        if first.is_none() {
            first = Some((operation, request.clone()));
        }
        assert!(
            book.retire(
                request.member,
                request.expected_epoch,
                request.client,
                request.credential
            )
            .is_err()
        );
        activate(&mut book, index * 10 + 2);
        book.leave(
            id(),
            request.member,
            request.expected_epoch,
            request.client,
            request.credential,
            index * 10 + 3,
        )
        .unwrap();
        activate(&mut book, index * 10 + 4);
        book.retire(
            request.member,
            request.expected_epoch,
            request.client,
            request.credential,
        )
        .unwrap();
        book.validate().unwrap();
    }
    let (operation, request) = first.unwrap();
    assert_eq!(book.join(operation, &request), Err(Rejection::Superseded));
    assert_eq!(book.next_epoch(), 301);
}

#[test]
fn joining_members_can_renew_a_long_running_domain_installation() {
    let mut book = MembersBook::new(
        id(),
        SubscriptionKind::Stream,
        InitialPosition::Latest,
        1,
        vec![id(); 1],
    )
    .unwrap();
    activate(&mut book, 0);
    let (_, request) = join(&mut book, id(), id(), id(), 1);
    let version = book.plan().version;
    book.heartbeat(
        id(),
        MemberHeartbeat {
            member: request.member,
            epoch: request.expected_epoch,
            client: request.client,
            credential: request.credential,
            sequence: 1,
            now_ms: 10_000,
            credential_expires_ms: 1_000_000,
            digest: [8; 32],
        },
    )
    .unwrap();
    let domain = book.plan().domains[0];
    book.observe_installed(version, false, domain, 16_000)
        .unwrap();
    assert_eq!(book.grants(16_000).unwrap()[0].expires_ms, 25_000);
    book.observe_installed(version, true, domain, 16_001)
        .unwrap();
    assert_eq!(
        book.member(request.member, request.expected_epoch)
            .unwrap()
            .status,
        MemberStatus::Active
    );
    assert_eq!(
        book.next_heartbeat_sequence(request.member, request.expected_epoch),
        Some(2)
    );
}

#[test]
fn a_new_owner_cannot_be_installed_until_every_domain_has_fenced() {
    let domains = vec![id(), id(), id()];
    let mut book = MembersBook::new(
        id(),
        SubscriptionKind::Stream,
        InitialPosition::EarliestAvailable,
        1,
        domains.clone(),
    )
    .unwrap();
    activate(&mut book, 0);
    let first = id();
    let client = id();
    let credential = id();
    join(&mut book, first, client, credential, 100);
    activate(&mut book, 100);
    let before = book.grants(100).unwrap();
    let (_, second) = join(&mut book, id(), client, credential, 200);
    let fencing = book.grants(200).unwrap();
    assert_eq!(fencing.len(), 1);
    assert!(fencing.first().unwrap().lanes.len() < before.first().unwrap().lanes.len());
    assert_eq!(
        book.member(second.member, second.expected_epoch)
            .unwrap()
            .status,
        MemberStatus::Joining
    );
    let plan = book.plan().clone();
    for domain in domains.iter().take(2) {
        book.observe_installed(plan.version, false, *domain, 200)
            .unwrap();
    }
    assert!(!book.plan().fenced);
    book.observe_installed(plan.version, false, *domains.last().unwrap(), 200)
        .unwrap();
    assert!(book.plan().fenced);
    assert_eq!(book.grants(200).unwrap().len(), 2);
    assert_eq!(
        book.observe_installed(plan.version, false, domains[0], 200),
        Err(Rejection::Superseded)
    );
    activate(&mut book, 200);
    assert_eq!(
        book.member(second.member, second.expected_epoch)
            .unwrap()
            .status,
        MemberStatus::Active
    );
    book.validate().unwrap();
    let recovered: MembersBook =
        postcard::from_bytes(&postcard::to_stdvec(&book).unwrap()).unwrap();
    assert_eq!(book, recovered);
}

#[test]
fn expired_or_retired_sessions_never_revive_from_a_late_join_or_heartbeat() {
    let domains = vec![id()];
    let mut book = MembersBook::new(
        id(),
        SubscriptionKind::Queue,
        InitialPosition::Latest,
        1,
        domains.clone(),
    )
    .unwrap();
    activate(&mut book, 0);
    let (operation, request) = join(&mut book, id(), id(), id(), 100);
    activate(&mut book, 100);
    let heartbeat = MemberHeartbeat {
        member: request.member,
        epoch: request.expected_epoch,
        client: request.client,
        credential: request.credential,
        sequence: 1,
        now_ms: 1_000,
        credential_expires_ms: 1_000_000,
        digest: [8; 32],
    };
    let heartbeat_operation = id();
    let expiry = book
        .heartbeat(heartbeat_operation, heartbeat.clone())
        .unwrap();
    let mut replay = heartbeat.clone();
    replay.now_ms = 2_000;
    assert_eq!(book.heartbeat(heartbeat_operation, replay).unwrap(), expiry);
    assert_eq!(book.join(operation, &request).unwrap().expires_ms, 15_100);
    book.refresh(1, domains.clone(), expiry).unwrap();
    activate(&mut book, expiry);
    let mut late = heartbeat;
    late.sequence = 2;
    late.now_ms = expiry;
    assert_eq!(book.heartbeat(id(), late), Err(Rejection::Superseded));
    assert_eq!(book.reap(1_000_000), 1);
    assert_eq!(book.join(operation, &request), Err(Rejection::Superseded));
    let (_, replacement) = join(
        &mut book,
        request.member,
        request.client,
        request.credential,
        500_000,
    );
    assert!(replacement.expected_epoch > request.expected_epoch);
    activate(&mut book, 500_000);
    book.validate().unwrap();
}

#[test]
fn queue_worksets_cover_every_domain_without_an_all_to_all_scan() {
    let domains: Vec<_> = (0..12).map(|_| id()).collect();
    let mut book = MembersBook::new(
        id(),
        SubscriptionKind::Queue,
        InitialPosition::EarliestAvailable,
        1,
        domains.clone(),
    )
    .unwrap();
    activate(&mut book, 0);
    for _ in 0..4 {
        join(&mut book, id(), id(), id(), 100);
        activate(&mut book, 100);
    }
    let worksets = book.queue_worksets(100).unwrap();
    assert_eq!(worksets.len(), 4);
    assert!(worksets.values().all(|domains| domains.len() <= 4));
    assert!(
        domains
            .iter()
            .all(|domain| worksets.values().any(|work| work.contains(domain)))
    );
}

#[test]
fn a_concurrent_join_conflict_proves_the_epoch_was_used_by_a_different_member() {
    let mut book = MembersBook::new(
        id(),
        SubscriptionKind::Queue,
        InitialPosition::Latest,
        1,
        vec![id()],
    )
    .unwrap();
    activate(&mut book, 0);
    let (_, first) = join(&mut book, id(), id(), id(), 1);
    activate(&mut book, 1);
    let mut second = first;
    second.member = id();
    assert_eq!(
        book.join(id(), &second),
        Err(Rejection::MemberEpochConflict { expected: 2 })
    );
    book.validate().unwrap();
}
