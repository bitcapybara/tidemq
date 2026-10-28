use super::*;
use std::{
    collections::BTreeSet,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
};
use tidemq::runtime::watch_test_hooks::{
    Diagnostics, InitialTopicGroups, ReadTrace, Snapshot, WatchPhase,
};
use tokio::{
    sync::watch,
    task::{JoinHandle, JoinSet},
};

const TOPICS: usize = 8;
const SHARDS_PER_TOPIC: usize = 125;
const ROUNDS: usize = 8;

struct PhaseTimer {
    name: &'static str,
    started: Instant,
    complete: bool,
}

impl PhaseTimer {
    fn start(name: &'static str) -> Self {
        eprintln!("Watch scale stage start: stage={name}");
        Self {
            name,
            started: Instant::now(),
            complete: false,
        }
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    fn finish(mut self) -> Duration {
        self.complete = true;
        self.elapsed()
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        eprintln!(
            "Watch scale stage ledger: stage={} elapsed_ms={} complete={}",
            self.name,
            self.elapsed().as_millis(),
            self.complete
        );
    }
}

struct Scope {
    member: Arc<Member>,
    routes: Vec<public::ShardRoute>,
}

struct Page {
    scope: usize,
    group: u64,
    targets: Vec<public::WatchTarget>,
}

struct Heartbeats {
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    accepted: Vec<Arc<AtomicU64>>,
}

impl Heartbeats {
    fn new() -> Self {
        Self {
            stop: watch::channel(false).0,
            tasks: Vec::new(),
            accepted: Vec::new(),
        }
    }

    fn add(&mut self, wire: Arc<WireClient>, member: Arc<Member>) {
        let accepted = Arc::new(AtomicU64::new(0));
        self.accepted.push(accepted.clone());
        let mut stop = self.stop.subscribe();
        self.tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut sequence = member.status.next_heartbeat_sequence;
            loop {
                tokio::select! { _=stop.changed()=>break, _=tick.tick()=>{} }
                let request = public::ConsumerControl {
                    action: public::ConsumerAction::HeartbeatConsumer.into(),
                    heartbeat_sequence: sequence,
                    ..member.request.clone()
                };
                let status = wire.member_control(&request).await;
                assert_eq!(status.member_epoch, member.request.member_epoch);
                assert!(
                    status.plan_active,
                    "An idle member lost its active assignment"
                );
                assert_eq!(status.assignment_version, member.status.assignment_version);
                assert!(status.next_heartbeat_sequence > sequence);
                sequence = status.next_heartbeat_sequence;
                accepted.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    fn counts(&self) -> Vec<u64> {
        assert!(
            self.tasks.iter().all(|task| !task.is_finished()),
            "A member heartbeat task exited before cleanup"
        );
        self.accepted
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect()
    }

    async fn close(mut self) {
        self.stop.send_replace(true);
        for task in self.tasks.drain(..) {
            tokio::time::timeout_at(deadline(), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

impl Drop for Heartbeats {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn layout(wire: &WireClient, topic: Id, count: usize) -> Vec<public::ShardRoute> {
    let mut after = Bytes::new();
    let mut revision = 0;
    let mut routes = BTreeMap::new();
    for _ in 0..8 {
        let reply = wire
            .original(
                FrameKind::GetTopicLayout,
                &public::GetTopicLayout {
                    topic_id: bytes(topic),
                    expected_revision: revision,
                    after_shard: after,
                },
            )
            .await;
        let page: public::TopicLayoutResult = decoded(&reply, FrameKind::TopicLayoutResult);
        assert_eq!(page.topic_id, bytes(topic));
        assert_eq!(page.total_shards as usize, count);
        assert!(page.history.is_empty());
        if revision != 0 {
            assert_eq!(page.revision, revision);
        }
        revision = page.revision;
        for route in page.active {
            assert!(routes.insert(route.shard_id.clone(), route).is_none());
        }
        after = page.next_after_shard;
        if after.is_empty() {
            assert_eq!(routes.len(), count);
            return routes.into_values().collect();
        }
    }
    panic!("The real initial-shard layout exceeded its finite pagination bound");
}

fn pages(scopes: &[Scope]) -> Vec<Page> {
    let mut pages = Vec::new();
    for (scope, entry) in scopes.iter().enumerate() {
        let mut groups = BTreeMap::<u64, Vec<public::WatchTarget>>::new();
        for route in &entry.routes {
            groups
                .entry(route.group_id)
                .or_default()
                .push(public::WatchTarget {
                    shard_id: route.shard_id.clone(),
                    birth_partition: route.birth_partition,
                    observed_token: Bytes::new(),
                });
        }
        for (group, targets) in groups {
            for batch in targets.chunks(tidemq_protocol::watch::MAX_TARGETS) {
                pages.push(Page {
                    scope,
                    group,
                    targets: batch.to_vec(),
                });
            }
        }
    }
    assert_eq!(
        pages.iter().map(|page| page.targets.len()).sum::<usize>(),
        scopes.iter().map(|scope| scope.routes.len()).sum::<usize>()
    );
    pages
}

async fn initialize_empty_reads(
    wire: &Arc<WireClient>,
    scopes: &[Scope],
) -> BTreeMap<Bytes, PartitionKey> {
    let mut running = JoinSet::new();
    let mut initialized = BTreeMap::new();
    for scope in scopes {
        for route in &scope.routes {
            if running.len() == 8 {
                let (shard, partition) = running.join_next().await.unwrap().unwrap();
                assert!(initialized.insert(shard, partition).is_none());
            }
            let member = scope.member.clone();
            let wire = wire.clone();
            let shard = route.shard_id.clone();
            let partition = PartitionKey {
                topic_id: member.partition.topic_id,
                partition: route.birth_partition,
            };
            running.spawn(async move {
                let read = member
                    .read_partition(&wire, partition.partition, 1, 1)
                    .await;
                assert!(
                    read.deliveries.is_empty(),
                    "An idle birth domain unexpectedly contained a message"
                );
                member.retire_partition(&wire, partition.partition, 1).await;
                (shard, partition)
            });
        }
    }
    while let Some(result) = running.join_next().await {
        let (shard, partition) = result.unwrap();
        assert!(initialized.insert(shard, partition).is_none());
    }
    assert_eq!(
        initialized.len(),
        scopes.iter().map(|scope| scope.routes.len()).sum::<usize>()
    );
    initialized
}

fn request(
    member: &Member,
    page: &Page,
    tokens: &BTreeMap<Bytes, Bytes>,
    wait: u32,
) -> public::GroupWatch {
    let mut request = member.watch(Bytes::new(), wait);
    request.targets = page
        .targets
        .iter()
        .map(|target| public::WatchTarget {
            observed_token: tokens.get(&target.shard_id).cloned().unwrap_or_default(),
            ..target.clone()
        })
        .collect();
    request
}

async fn observation_failure_ledger(observer: &Diagnostics, request: &public::GroupWatch) {
    let root = Id::from_slice(&request.root_operation_id).unwrap();
    let targets: Vec<_> = request
        .targets
        .iter()
        .map(|target| {
            serde_json::json!({
                "shard": Id::from_slice(&target.shard_id).unwrap(),
                "partition": target.birth_partition,
                "token_hash": blake3::hash(&target.observed_token).to_hex().to_string(),
            })
        })
        .collect();
    eprintln!(
        "Unexpected idle request ledger: {}",
        serde_json::json!({
            "root": root, "topic": Id::from_slice(&request.topic_id).unwrap(),
            "subscription": Id::from_slice(&request.subscription_id).unwrap(),
            "member": Id::from_slice(&request.member_id).unwrap(),
            "epoch": request.member_epoch, "assignment_version": request.assignment_version,
            "attempt": request.attempt, "wait_ms": request.wait_ms, "targets": targets,
        })
    );
    eprintln!(
        "Unexpected idle immediate state ledger: {}",
        serde_json::to_string(&observer.snapshot().await).unwrap()
    );
    let events: Vec<_> = observer
        .events()
        .into_iter()
        .filter(|event| event.root == root)
        .collect();
    eprintln!(
        "Unexpected idle branch ledger: {}",
        serde_json::to_string(&events).unwrap()
    );
}

enum ObservationReply {
    Complete,
    Retry(Duration),
    Invalid(String),
}

struct Observation {
    request: public::GroupWatch,
    digest: [u8; 32],
    until: Instant,
    request_ids: BTreeSet<Bytes>,
    current_request: Bytes,
    refusals: usize,
}

struct Observed {
    targets: BTreeMap<Bytes, Bytes>,
    attempts: usize,
    refusals: usize,
}

#[derive(Default, serde::Serialize)]
struct ObservationTotals {
    roots: usize,
    attempts: usize,
    refusals: usize,
    maximum_attempts: usize,
}

impl ObservationTotals {
    fn record(&mut self, observed: &Observed) {
        self.roots += 1;
        self.attempts += observed.attempts;
        self.refusals += observed.refusals;
        self.maximum_attempts = self.maximum_attempts.max(observed.attempts);
    }
}

impl Observation {
    fn new(request: public::GroupWatch, timeout: Duration) -> Self {
        let digest = tidemq_protocol::watch::digest(&request);
        Self {
            request,
            digest,
            until: Instant::now() + timeout,
            request_ids: BTreeSet::new(),
            current_request: Bytes::new(),
            refusals: 0,
        }
    }

    async fn submit(&mut self, wire: &WireClient) -> tidemq::transport::rpc::RpcResponse {
        assert_eq!(
            tidemq_protocol::watch::digest(&self.request),
            self.digest,
            "An observation retry changed its immutable branch scope"
        );
        assert!(
            self.request_ids.len() < 64,
            "An observation exhausted its finite attempt bound"
        );
        let (response, request_id) = wire.submit_until(&self.request, self.until).await;
        assert!(
            self.request_ids.insert(request_id.clone()),
            "A retry reused its request ID"
        );
        self.current_request = request_id;
        eprintln!(
            "Watch observation attempt ledger: root={} request={} attempt={} remaining_ms={} scope_hash={}",
            Id::from_slice(&self.request.root_operation_id).unwrap(),
            Id::from_slice(&self.current_request).unwrap(),
            self.request.attempt,
            self.until
                .saturating_duration_since(Instant::now())
                .as_millis(),
            blake3::hash(&self.digest).to_hex()
        );
        response
    }

    fn classify(
        &self,
        reply: &tidemq_protocol::common::Frame,
        expected: public::WatchStatus,
    ) -> ObservationReply {
        if reply.kind == i32::from(FrameKind::Error) {
            let error: tidemq_protocol::common::ErrorReply = decoded(reply, FrameKind::Error);
            eprintln!(
                "Watch explicit refusal ledger: root={} attempt={} error={error:?}",
                Id::from_slice(&self.request.root_operation_id).unwrap(),
                self.request.attempt
            );
            return if error.code == i32::from(ErrorCode::QuotaExceeded)
                && error.outcome == i32::from(tidemq_protocol::common::Outcome::Rejected)
                && error.retryable
            {
                ObservationReply::Retry(Duration::from_millis(
                    error.retry_after_ms.unwrap_or(50).max(1),
                ))
            } else {
                ObservationReply::Invalid(format!(
                    "The observation returned a nonrecoverable error: {error:?}"
                ))
            };
        }
        let result: public::GroupWatchResult = decoded(reply, FrameKind::GroupWatchResult);
        let expected_shards: BTreeSet<_> = self
            .request
            .targets
            .iter()
            .map(|target| &target.shard_id)
            .collect();
        let actual: BTreeSet<_> = result
            .targets
            .iter()
            .map(|target| &target.shard_id)
            .collect();
        if actual != expected_shards || result.targets.len() != expected_shards.len() {
            return ObservationReply::Invalid(
                "An observation omitted or duplicated a target".into(),
            );
        }
        let mut retry = false;
        for target in &result.targets {
            let original = self
                .request
                .targets
                .iter()
                .find(|original| original.shard_id == target.shard_id)
                .unwrap();
            if target.status == i32::from(expected)
                && target.error_code == i32::from(ErrorCode::Unspecified)
                && !target.retryable
                && !target.observed_token.is_empty()
                && target.observed_token.len() <= tidemq_protocol::watch::MAX_TOKEN_BYTES
                && (expected != public::WatchStatus::WatchUnchanged
                    || target.observed_token == original.observed_token)
            {
                continue;
            }
            if target.status == i32::from(public::WatchStatus::WatchRetry)
                && target.retryable
                && target.observed_token == original.observed_token
                && matches!(
                    ErrorCode::try_from(target.error_code),
                    Ok(ErrorCode::QuotaExceeded
                        | ErrorCode::Unavailable
                        | ErrorCode::DeadlineExceeded)
                )
            {
                retry = true;
            } else {
                return ObservationReply::Invalid(format!(
                    "An observation returned a nonrecoverable target: {target:?}"
                ));
            }
        }
        if retry {
            // WatchResult has no retry-after field; pace only the explicitly refused observation.
            ObservationReply::Retry(Duration::from_millis(50))
        } else {
            ObservationReply::Complete
        }
    }

    async fn finish(
        &mut self,
        wire: &WireClient,
        observer: &Diagnostics,
        mut reply: tidemq_protocol::common::Frame,
        expected: public::WatchStatus,
    ) -> Observed {
        loop {
            assert_eq!(reply.operation_id, self.request.root_operation_id);
            assert_eq!(reply.request_id, self.current_request);
            let delay = match self.classify(&reply, expected) {
                ObservationReply::Complete => {
                    return Observed {
                        targets: checked_results(observer, &self.request, &reply, expected).await,
                        attempts: self.request_ids.len(),
                        refusals: self.refusals,
                    };
                }
                ObservationReply::Retry(delay) => delay,
                ObservationReply::Invalid(reason) => {
                    observation_failure_ledger(observer, &self.request).await;
                    panic!("{reason}");
                }
            };
            self.refusals += 1;
            observation_failure_ledger(observer, &self.request).await;
            let retry_at = Instant::now() + delay;
            assert!(
                retry_at < self.until,
                "The original observation deadline cannot accommodate the explicit retry delay"
            );
            eprintln!(
                "Watch bounded retry ledger: root={} rejected_attempt={} refusals={} retry_after_ms={} remaining_ms={}",
                Id::from_slice(&self.request.root_operation_id).unwrap(),
                self.request.attempt,
                self.refusals,
                delay.as_millis(),
                self.until
                    .saturating_duration_since(Instant::now())
                    .as_millis()
            );
            // This delay paces a known refusal; only the subsequent authenticated reply proves progress.
            tokio::time::sleep_until(retry_at).await;
            self.request.attempt = self.request.attempt.checked_add(1).unwrap();
            let response = self.submit(wire).await;
            let received = tokio::time::timeout_at(self.until, response.receive())
                .await
                .expect("The original observation deadline expired during recovery")
                .unwrap();
            reply = received.frame().clone();
        }
    }
}

async fn observe(
    wire: &WireClient,
    observer: &Diagnostics,
    request: public::GroupWatch,
    expected: public::WatchStatus,
) -> Observed {
    let mut observation = Observation::new(request, Duration::from_millis(RPC_MS));
    let response = observation.submit(wire).await;
    let received = tokio::time::timeout_at(observation.until, response.receive())
        .await
        .unwrap()
        .unwrap();
    observation
        .finish(wire, observer, received.frame().clone(), expected)
        .await
}

async fn parked_gate_failure_ledger(
    observer: &Diagnostics,
    held: Vec<(Id, Instant, tidemq::transport::rpc::RpcResponse)>,
    parked: usize,
    expected: usize,
) {
    eprintln!(
        "Watch authority gate failure ledger: expected={expected} actual_parked={parked} resources={}",
        serde_json::to_string(&observer.snapshot().await).unwrap()
    );
    let roots: BTreeSet<_> = held.iter().map(|(root, _, _)| *root).collect();
    let events: Vec<_> = observer
        .events()
        .into_iter()
        .filter(|event| roots.contains(&event.root))
        .collect();
    eprintln!(
        "Watch authority gate branch ledger: {}",
        serde_json::to_string(&events).unwrap()
    );
    for (root, until, response) in held {
        let remaining = until.saturating_duration_since(Instant::now()).as_millis();
        let mut receive = Box::pin(response.receive());
        let ready = std::future::poll_fn(|context| {
            Poll::Ready(match receive.as_mut().poll(context) {
                Poll::Ready(result) => Some(result),
                Poll::Pending => None,
            })
        })
        .await;
        match ready {
            None => eprintln!(
                "Watch authority gate RPC ledger: root={root} original_remaining_ms={remaining} state=pending"
            ),
            Some(Err(error)) => eprintln!(
                "Watch authority gate RPC ledger: root={root} original_remaining_ms={remaining} transport_error={error:?}"
            ),
            Some(Ok(received)) => {
                let reply = received.frame();
                if reply.kind == i32::from(FrameKind::Error) {
                    let error: tidemq_protocol::common::ErrorReply =
                        decoded(reply, FrameKind::Error);
                    eprintln!(
                        "Watch authority gate RPC ledger: root={root} original_remaining_ms={remaining} state=complete error={error:?}"
                    );
                } else if reply.kind == i32::from(FrameKind::GroupWatchResult) {
                    let result: public::GroupWatchResult =
                        decoded(reply, FrameKind::GroupWatchResult);
                    let statuses: Vec<_> = result
                        .targets
                        .iter()
                        .map(|target| (target.status, target.error_code, target.retryable))
                        .collect();
                    eprintln!(
                        "Watch authority gate RPC ledger: root={root} original_remaining_ms={remaining} state=complete statuses={statuses:?}"
                    );
                } else {
                    eprintln!(
                        "Watch authority gate RPC ledger: root={root} original_remaining_ms={remaining} state=complete reply_kind={}",
                        reply.kind
                    );
                }
            }
        }
    }
}

pub(super) async fn verify_observation_recovery(fixture: &WatchFixture, token: Bytes) {
    use tidemq::runtime::watch_test_hooks::AuthorizeQuorumPause;
    let observer = fixture.runtime.running.get(&1).unwrap().watch_diagnostics();
    let before = quiescent(&observer).await;
    let cluster = fixture
        .runtime
        .configs
        .get(&1)
        .unwrap()
        .cluster_id
        .parse()
        .unwrap();
    let before_wait = BeforeWait::install(fixture.member.partition).unwrap();
    let mut held = Vec::new();
    let mut held_roots = BTreeSet::new();
    for _ in 0..8 {
        let request = fixture.member.watch(token.clone(), 1_000);
        let root = Id::from_slice(&request.root_operation_id).unwrap();
        held_roots.insert(root);
        let until = deadline();
        let (response, _) = fixture.wire.submit_until(&request, until).await;
        held.push((root, until, response));
    }
    tokio::time::timeout_at(deadline(), async {
        loop {
            let registered: BTreeSet<_> = observer
                .events()
                .iter()
                .filter(|event| {
                    held_roots.contains(&event.root) && event.phase == WatchPhase::Registered
                })
                .map(|event| event.root)
                .collect();
            if registered == held_roots {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("The eight real observations did not release their initial admission turns");
    before_wait.wait(deadline()).await.unwrap();
    let pause = AuthorizeQuorumPause::install(cluster, 1).unwrap();
    let parked_until = deadline();
    for expected in 1..=8 {
        // BeforeWait releases one waiter per notification; late arrivals observe its released flag.
        for _ in 0..8 {
            before_wait.resume();
        }
        if let Err(error) = pause.wait_parked(expected, parked_until).await {
            parked_gate_failure_ledger(&observer, held, pause.parked(), expected).await;
            panic!("The original authority pause deadline expired: {error}");
        }
    }
    drop(before_wait);
    let mut observation = Observation::new(
        fixture.member.watch(token.clone(), 0),
        Duration::from_millis(RPC_MS),
    );
    let root = Id::from_slice(&observation.request.root_operation_id).unwrap();
    let response = observation.submit(&fixture.wire).await;
    let received = tokio::time::timeout_at(observation.until, response.receive())
        .await
        .unwrap()
        .unwrap();
    let error: tidemq_protocol::common::ErrorReply = decoded(received.frame(), FrameKind::Error);
    assert_eq!(error.code, i32::from(ErrorCode::QuotaExceeded));
    assert_eq!(
        error.outcome,
        i32::from(tidemq_protocol::common::Outcome::Rejected)
    );
    assert!(error.retryable);
    assert_eq!(error.retry_after_ms, Some(50));
    assert_eq!(pause.parked(), 8);
    let saturated = observer.snapshot().await;
    eprintln!(
        "Known observation refusal resource ledger: {}",
        serde_json::to_string(&saturated).unwrap()
    );
    let configured_memory = &fixture.runtime.configs.get(&1).unwrap().memory;
    let data_capacity = tidemq::budget::MemoryBudget::new(configured_memory)
        .unwrap()
        .data_capacity();
    let watch_maximum = (data_capacity / 16).min(8 * 1024 * 1024);
    assert_eq!(saturated.authority_inbound, 8);
    assert_eq!(saturated.authority_outbound, 8);
    assert!(saturated.admission_queued <= 128 && saturated.rechecks_queued <= 128);
    assert!(saturated.charged_bytes <= watch_maximum);
    assert!(saturated.admission_rejected > before.admission_rejected);
    assert!(observer.events().iter().any(|event| event.root == root
        && matches!(
            event.phase, WatchPhase::Failed { stage: "entry_authorize", code, outcome, .. }
                if code == i32::from(ErrorCode::QuotaExceeded)
                    && outcome == i32::from(tidemq_protocol::common::Outcome::Rejected)
        )));
    pause.release();
    for (_, _, pending) in held {
        let result = receive(pending).await;
        assert_eq!(result.targets.len(), 1);
        let target = result.targets.first().unwrap();
        assert_eq!(
            target.status,
            i32::from(public::WatchStatus::WatchUnchanged)
        );
        assert_eq!(target.observed_token, token);
    }
    let result = observation
        .finish(
            &fixture.wire,
            &observer,
            received.frame().clone(),
            public::WatchStatus::WatchUnchanged,
        )
        .await;
    assert!(result.attempts >= 2 && result.refusals >= 1);
    assert_eq!(result.targets.get(&fixture.member.shard), Some(&token));
    assert!(
        Instant::now() < observation.until,
        "Recovery extended the original two-second deadline"
    );
    drop(pause);
    let after = quiescent(&observer).await;
    assert_eq!(after.charged_bytes, before.charged_bytes);
    assert_eq!(after.target_contexts, before.target_contexts);
    assert_eq!(after.authority_inbound, 0);
    assert_eq!(after.authority_outbound, 0);
    assert_eq!(after.admission_queued, 0);
    assert_eq!(after.rechecks_queued, 0);
    assert_eq!(
        [
            after.group_reads,
            after.group_retires,
            after.empty_reads,
            after.dirty
        ],
        [
            before.group_reads,
            before.group_retires,
            before.empty_reads,
            before.dirty
        ]
    );
    eprintln!(
        "Known observation recovery ledger: root={root} attempts={} refusals={} original_deadline_preserved=true",
        result.attempts, result.refusals
    );
}

async fn checked_results(
    observer: &Diagnostics,
    request: &public::GroupWatch,
    reply: &tidemq_protocol::common::Frame,
    status: public::WatchStatus,
) -> BTreeMap<Bytes, Bytes> {
    if reply.kind == i32::from(FrameKind::Error) {
        observation_failure_ledger(observer, request).await;
        let error: tidemq_protocol::common::ErrorReply = decoded(reply, FrameKind::Error);
        panic!(
            "Idle Watch returned an explicit error: root={} attempt={} wait_ms={} error={error:?}",
            Id::from_slice(&request.root_operation_id).unwrap(),
            request.attempt,
            request.wait_ms
        );
    }
    let result: public::GroupWatchResult = decoded(reply, FrameKind::GroupWatchResult);
    if result.targets.iter().any(|target| {
        target.status != i32::from(status)
            || target.error_code != i32::from(ErrorCode::Unspecified)
            || target.retryable
    }) {
        observation_failure_ledger(observer, request).await;
    }
    let expected: BTreeSet<_> = request
        .targets
        .iter()
        .map(|target| target.shard_id.clone())
        .collect();
    let mut actual = BTreeMap::new();
    for target in result.targets {
        assert_eq!(
            target.status,
            i32::from(status),
            "Unexpected idle observation for root={} wait_ms={}: {target:?}",
            Id::from_slice(&request.root_operation_id).unwrap(),
            request.wait_ms
        );
        assert_eq!(target.error_code, i32::from(ErrorCode::Unspecified));
        assert!(!target.retryable);
        assert!(!target.observed_token.is_empty());
        assert!(target.observed_token.len() <= tidemq_protocol::watch::MAX_TOKEN_BYTES);
        assert!(
            actual
                .insert(target.shard_id, target.observed_token)
                .is_none()
        );
    }
    assert_eq!(actual.keys().cloned().collect::<BTreeSet<_>>(), expected);
    actual
}

fn fail_before_pause(reply: &tidemq_protocol::common::Frame) -> ! {
    if reply.kind == i32::from(FrameKind::Error) {
        let error: tidemq_protocol::common::ErrorReply = decoded(reply, FrameKind::Error);
        panic!(
            "Watch completed before its registration pause: code={} outcome={} retryable={}",
            error.code, error.outcome, error.retryable
        );
    }
    let result: public::GroupWatchResult = decoded(reply, FrameKind::GroupWatchResult);
    let statuses: Vec<_> = result
        .targets
        .iter()
        .map(|target| {
            (
                Id::from_slice(&target.shard_id).unwrap(),
                target.status,
                target.error_code,
                target.retryable,
            )
        })
        .collect();
    panic!("Watch completed before its registration pause: {statuses:?}");
}

async fn quiescent(observer: &Diagnostics) -> Snapshot {
    tokio::time::timeout_at(deadline(), async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        loop {
            tick.tick().await;
            let snapshot = observer.snapshot().await;
            if snapshot.active_requests == 0
                && snapshot.cancellation_watermarks == 0
                && snapshot.cleanup_tasks == 0
            {
                return snapshot;
            }
        }
    })
    .await
    .expect("Watch registrations did not expire within their fixed deadlines")
}

fn idle_commands(
    snapshot: &Snapshot,
    idle: &BTreeMap<Bytes, PartitionKey>,
) -> BTreeMap<PartitionKey, u64> {
    let commands: BTreeMap<_, _> = snapshot
        .groups
        .iter()
        .flat_map(|group| group.partition_commands.iter().copied())
        .collect();
    idle.values()
        .map(|partition| {
            let count = *commands
                .get(partition)
                .expect("A real initialized partition is missing its applied-command ledger");
            assert!(
                count >= 2,
                "The partition ledger did not observe initialization Read and retirement"
            );
            (*partition, count)
        })
        .collect()
}

struct Hot {
    member: Arc<Member>,
    producer: tidemq_sdk::Producer,
    read_sequence: u64,
    published: u64,
    acknowledged: u64,
    empty_reads: u64,
    empty_retirements: u64,
    trace: Arc<ReadTrace>,
}

impl Hot {
    fn new(member: Arc<Member>, producer: tidemq_sdk::Producer, trace: Arc<ReadTrace>) -> Self {
        Self {
            member,
            producer,
            read_sequence: 1,
            published: 0,
            acknowledged: 0,
            empty_reads: 0,
            empty_retirements: 0,
            trace,
        }
    }

    async fn step(&mut self, wire: &WireClient) {
        tokio::time::timeout(Duration::from_secs(5), self.step_known(wire))
            .await
            .expect("A hot message did not complete its known read/ACK/retirement within the bounded step");
    }

    async fn step_known(&mut self, wire: &WireClient) {
        assert_eq!(
            self.published, self.acknowledged,
            "A prior known publish still requires its original hot delivery"
        );
        let offset = self.acknowledged;
        let body = format!("watch-hot-{}", self.published + 1);
        let message_bytes = tidemq_protocol::topic::delivery_bytes(
            b"watch-hot-fixed-key",
            body.as_bytes(),
            &BTreeMap::new(),
        )
        .unwrap();
        let receipt = self
            .producer
            .send(
                tidemq_sdk::Message::new(body.clone())
                    .key(Bytes::from_static(b"watch-hot-fixed-key")),
            )
            .await
            .unwrap();
        let message_id = receipt.message_id().to_string();
        assert_eq!(
            message_id,
            format!(
                "{}:{}:{}",
                Id::from_slice(&self.member.request.topic_id).unwrap(),
                Id::from_slice(&self.member.shard).unwrap(),
                offset
            ),
            "The known publish moved to another topic/shard or did not append the expected offset"
        );
        self.published += 1;
        let mut retries = tokio::time::interval(Duration::from_millis(100));
        retries.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        for attempt in 1..=50 {
            retries.tick().await;
            let sequence = self.read_sequence;
            let read = self.member.read(wire, sequence).await;
            assert!(
                !self.trace.overflowed(),
                "Hot ReadTrace lost a real apply event"
            );
            let event = self
                .trace
                .events()
                .last()
                .cloned()
                .expect("A hot GroupRead completed without its exact apply observation");
            assert_eq!(event.sequence, sequence);
            assert_eq!(
                event.member,
                Id::from_slice(&self.member.request.member_id).unwrap()
            );
            assert_eq!(
                event.subscription,
                Id::from_slice(&self.member.request.subscription_id).unwrap()
            );
            assert_eq!(event.epoch, self.member.request.member_epoch);
            assert_eq!(event.producer_next, self.published);
            assert!(event.cursor_next == offset || event.cursor_next == offset + 1);
            assert_eq!(event.deliveries, Some(read.deliveries.len()));
            eprintln!(
                "Hot exact-at-apply ReadTrace ledger: {}",
                serde_json::to_string(&event).unwrap()
            );
            eprintln!(
                "Hot delivery attempt ledger: message_id={message_id} read_sequence={sequence} attempt={attempt} assignment_version={} deliveries={} sealed_complete={}",
                read.assignment_version,
                read.deliveries.len(),
                read.sealed_complete
            );
            assert!(
                !read.sealed_complete,
                "The fixed hot shard unexpectedly sealed before its known publish was delivered"
            );
            if read.deliveries.is_empty() {
                let member = event
                    .member_state
                    .as_ref()
                    .expect("Known empty Read lost its current member");
                assert!(member.active && member.expires_ms > event.now_ms);
                assert!(member.in_flight_messages < member.max_messages);
                assert!(member.in_flight_bytes < member.max_bytes);
                assert!(
                    event.candidates > 0 || event.eligible_ready > 0,
                    "The known published message was not an eligible candidate"
                );
                assert!(
                    event.budget_messages == 0
                        || event.budget_results == 0
                        || event.budget_bytes < message_bytes,
                    "An empty hot Read had sufficient budget and requires a production investigation"
                );
                self.empty_reads += 1;
                self.member.retire(wire, sequence).await;
                self.empty_retirements += 1;
                self.read_sequence += 1;
                eprintln!(
                    "Hot known-empty retirement ledger: message_id={message_id} retired_read_sequence={sequence} next_read_sequence={} acknowledged={} publish_count={} result=known",
                    self.read_sequence, self.acknowledged, self.published
                );
                continue;
            }
            assert_eq!(read.deliveries.len(), 1);
            let delivery = read.deliveries.first().unwrap();
            assert_eq!(delivery.body, body.as_bytes());
            assert_eq!(delivery.key, b"watch-hot-fixed-key".as_slice());
            assert_eq!(delivery.proof.as_ref().unwrap().offset, offset);
            assert_eq!(delivery.proof.as_ref().unwrap().attempt, 1);
            let ack_sequence = self.acknowledged + 1;
            self.member.ack(wire, delivery, ack_sequence).await;
            self.acknowledged += 1;
            self.member.retire_ack(wire, ack_sequence).await;
            self.member.retire(wire, sequence).await;
            self.read_sequence += 1;
            return;
        }
        panic!("A fixed hot publish exhausted its finite known-empty read attempts without an ACK");
    }
}

async fn scale(shards_per_topic: usize) {
    let idle_shards = TOPICS * shards_per_topic;
    eprintln!(
        "Watch real scale topology: idle_shards={idle_shards} topics={TOPICS} shards_per_topic={shards_per_topic} data_groups=8 rounds={ROUNDS}"
    );
    let total = PhaseTimer::start("total");
    let setup = PhaseTimer::start("setup");
    let mut runtime = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    runtime.configs.get_mut(&1).unwrap().runtime.groups = (2..10)
        .map(|group_id| GroupConfig {
            group_id,
            voters: Vec::new(),
            standby: Vec::new(),
        })
        .collect();
    let cluster = runtime.configs.get(&1).unwrap().cluster_id.parse().unwrap();
    let names: Vec<_> = (0..TOPICS)
        .map(|index| format!("test/app/idle-{index}"))
        .collect();
    let mut placements = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let groups = (0..shards_per_topic)
            .map(|partition| 2 + ((partition + index) % 8) as u64)
            .collect();
        placements.push(InitialTopicGroups::install(cluster, name.clone(), groups).unwrap());
    }
    runtime.start_all().await;
    let observer = runtime.running.get(&1).unwrap().watch_diagnostics();
    let admin = runtime
        .client_with_timeouts(1, Duration::from_secs(120), Duration::from_secs(10))
        .await;
    let tenant = admin.admin().create_tenant("test").await.unwrap();
    admin.admin().create_namespace(tenant, "app").await.unwrap();
    let wire = Arc::new(
        WireClient::connect(&runtime, 1)
            .await
            .with_timeout(Duration::from_secs(120)),
    );
    let mut heartbeats = Heartbeats::new();
    let hot_topic = admin.admin().create_topic(TOPIC).await.unwrap();
    assert_eq!(hot_topic.shard_count(), 1);
    let hot_member = Arc::new(Member::join(&wire, hot_topic.id().into(), "hot").await);
    heartbeats.add(wire.clone(), hot_member.clone());
    let mut scopes = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let topic = admin.admin().create_topic(name).await.unwrap();
        assert_eq!(topic.shard_count(), shards_per_topic);
        let routes = layout(&wire, topic.id().into(), shards_per_topic).await;
        eprintln!(
            "Watch real topic layout ledger: topic={name} shards={}",
            routes.len()
        );
        for route in &routes {
            assert_eq!(
                route.group_id,
                2 + ((route.birth_partition as usize + index) % 8) as u64
            );
        }
        let member = Arc::new(Member::join(&wire, topic.id().into(), "idle").await);
        assert_eq!(
            member
                .status
                .work_shards
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            routes.iter().map(|route| route.shard_id.clone()).collect()
        );
        heartbeats.add(wire.clone(), member.clone());
        scopes.push(Scope { member, routes });
        eprintln!(
            "Watch scale setup: topics={} real_idle_shards={}",
            scopes.len(),
            scopes.len() * shards_per_topic
        );
    }
    wire.reopen_delivery().await;
    let idle = initialize_empty_reads(&wire, &scopes).await;
    let hot_trace = Arc::new(
        ReadTrace::install(
            cluster,
            hot_member.partition,
            Id::from_slice(&hot_member.shard).unwrap(),
        )
        .unwrap(),
    );
    assert_eq!(hot_trace.scope(), (cluster, hot_member.partition));
    assert!(hot_trace.events().is_empty());
    let mut hot = Hot::new(
        hot_member,
        admin.new_producer(TOPIC).build().await.unwrap(),
        hot_trace.clone(),
    );
    hot.step(&wire).await;
    let pages = pages(&scopes);
    let mut tokens = BTreeMap::new();
    let mut observation_totals = ObservationTotals::default();
    for page in &pages {
        let request = request(&scopes.get(page.scope).unwrap().member, page, &tokens, 0);
        let observed = observe(&wire, &observer, request, public::WatchStatus::WatchSync).await;
        observation_totals.record(&observed);
        for (shard, token) in observed.targets {
            assert!(tokens.insert(shard, token).is_none());
        }
    }
    assert_eq!(tokens.len(), idle_shards);
    let before = quiescent(&observer).await;
    assert_eq!(
        before
            .groups
            .iter()
            .map(|group| group.cached_targets)
            .sum::<usize>(),
        idle_shards
    );
    let writes_before = idle_commands(&before, &idle);
    let unscoped_before: Vec<_> = before
        .groups
        .iter()
        .map(|group| (group.group, group.unscoped_commands))
        .collect();
    let heartbeat_before = heartbeats.counts();
    let first_hot = hot.acknowledged;
    let first_hot_read = hot.read_sequence;
    let first_hot_empty = hot.empty_reads;
    let first_hot_publish = hot.published;
    let mut observations = BTreeMap::<Bytes, usize>::new();
    let setup_elapsed = setup.finish();
    let measurement = PhaseTimer::start("measurement");
    for round in 0..ROUNDS {
        let long_scope = round % TOPICS;
        let long: Vec<_> = pages
            .iter()
            .filter(|page| page.scope == long_scope)
            .collect();
        let paused_page = *long.first().unwrap();
        let pause = BeforeWait::install_group(cluster, paused_page.group).unwrap();
        let held_request = request(
            &scopes.get(paused_page.scope).unwrap().member,
            paused_page,
            &tokens,
            1_000,
        );
        let mut held_observation = Observation::new(held_request.clone(), Duration::from_secs(10));
        let response = held_observation.submit(&wire).await;
        let mut held = Box::pin(response.receive());
        tokio::select! {
            entered = pause.wait(deadline()) => entered.unwrap(),
            reply = &mut held => {
                observation_failure_ledger(&observer, &held_request).await;
                fail_before_pause(reply.unwrap().frame());
            },
        }
        let mut pending = vec![(held_observation, held)];
        for page in long.iter().skip(1) {
            let request = request(
                &scopes.get(page.scope).unwrap().member,
                page,
                &tokens,
                1_000,
            );
            let mut observation = Observation::new(request, Duration::from_millis(RPC_MS));
            let response = observation.submit(&wire).await;
            pending.push((observation, Box::pin(response.receive())));
        }
        assert!(observer.snapshot().await.active_requests > 0);
        let long_roots: BTreeSet<_> = pending
            .iter()
            .map(|(observation, _)| Id::from_slice(&observation.request.root_operation_id).unwrap())
            .collect();
        let held_root =
            Id::from_slice(&pending.first().unwrap().0.request.root_operation_id).unwrap();
        tokio::time::timeout(Duration::from_secs(5), hot.step(&wire))
            .await
            .expect("Hot ACK did not progress while a real ordinary Watch was paused");
        std::future::poll_fn(|context| {
            assert!(
                pending
                    .first_mut()
                    .unwrap()
                    .1
                    .as_mut()
                    .poll(context)
                    .is_pending(),
                "Hot ACK completed after the held Watch had already ended"
            );
            Poll::Ready(())
        })
        .await;
        assert!(
            !observer
                .events()
                .iter()
                .any(|event| event.root == held_root && event.phase == WatchPhase::Complete),
            "Hot ACK waited for the paused ordinary Watch to expire"
        );
        pause.resume();
        drop(pause);
        let mut served = BTreeSet::new();
        for (index, page) in pages
            .iter()
            .filter(|page| page.scope != long_scope)
            .enumerate()
        {
            let request = request(&scopes.get(page.scope).unwrap().member, page, &tokens, 0);
            let observed = observe(
                &wire,
                &observer,
                request,
                public::WatchStatus::WatchUnchanged,
            )
            .await;
            observation_totals.record(&observed);
            for (shard, token) in observed.targets {
                assert_eq!(tokens.get(&shard), Some(&token));
                assert!(served.insert(shard));
            }
            if index % 8 == 0 {
                hot.step(&wire).await;
            }
        }
        for (mut observation, pending) in pending {
            let response = tokio::time::timeout_at(observation.until, pending)
                .await
                .unwrap()
                .unwrap();
            let observed = observation
                .finish(
                    &wire,
                    &observer,
                    response.frame().clone(),
                    public::WatchStatus::WatchUnchanged,
                )
                .await;
            observation_totals.record(&observed);
            for (shard, token) in observed.targets {
                assert_eq!(tokens.get(&shard), Some(&token));
                assert!(served.insert(shard));
            }
        }
        // Independent one-second waits need not remain live at the same instant.
        let registered: BTreeSet<_> = observer
            .events()
            .iter()
            .filter(|event| {
                long_roots.contains(&event.root) && event.phase == WatchPhase::Registered
            })
            .map(|event| event.root)
            .collect();
        assert_eq!(registered, long_roots);
        assert_eq!(
            served.len(),
            idle_shards,
            "A fair observation round omitted a real idle shard"
        );
        for shard in served {
            *observations.entry(shard).or_default() += 1;
        }
        let snapshot = quiescent(&observer).await;
        assert_eq!(
            snapshot.dirty, before.dirty,
            "Hot ACK dirtied an unrelated idle target"
        );
        assert_eq!(
            snapshot.empty_reads,
            before.empty_reads + hot.empty_reads - first_hot_empty,
            "Empty reads outside the explicitly accounted hot partition appeared"
        );
        assert_eq!(hot.empty_reads, hot.empty_retirements);
        assert_eq!(
            idle_commands(&snapshot, &idle),
            writes_before,
            "Hot ACK caused data Raft commands on idle partitions"
        );
        assert_eq!(
            snapshot
                .groups
                .iter()
                .map(|group| (group.group, group.unscoped_commands))
                .collect::<Vec<_>>(),
            unscoped_before
        );
        assert_eq!(
            snapshot
                .groups
                .iter()
                .map(|group| group.cached_targets)
                .sum::<usize>(),
            idle_shards
        );
        eprintln!(
            "Watch scale round ledger: round={} observed_real_idle_shards={} hot_acks={} charged_bytes={} contexts={} measurement_elapsed_ms={}",
            round + 1,
            idle_shards,
            hot.acknowledged - first_hot,
            snapshot.charged_bytes,
            snapshot.target_contexts,
            measurement.elapsed().as_millis()
        );
    }
    let after = observer.snapshot().await;
    assert!(after.group_reads >= before.group_reads + hot.read_sequence - first_hot_read);
    assert!(after.group_retires >= before.group_retires + hot.read_sequence - first_hot_read);
    assert_eq!(hot.acknowledged - first_hot, 64);
    assert_eq!(hot.published - first_hot_publish, 64);
    assert_eq!(hot.published, hot.acknowledged);
    assert_eq!(hot.empty_reads, hot.empty_retirements);
    assert!(!hot_trace.overflowed());
    let hot_events = hot_trace.events();
    assert_eq!(
        hot_events
            .iter()
            .filter(|event| event.deliveries.is_some())
            .count() as u64,
        hot.read_sequence - 1
    );
    assert_eq!(observations.len(), idle_shards);
    assert!(observations.values().all(|rounds| *rounds == ROUNDS));
    let heartbeat_after = heartbeats.counts();
    assert!(
        heartbeat_after
            .iter()
            .zip(&heartbeat_before)
            .all(|(after, before)| after > before),
        "Watch activity starved ordinary member heartbeat progress"
    );
    let writes_after = idle_commands(&after, &idle);
    let ledger: Vec<_> = idle
        .iter()
        .map(|(shard, partition)| {
            serde_json::json!({
                "shard": Id::from_slice(shard).unwrap(), "partition": partition,
                "initialized_reads": 1, "retired_initial_reads": 1,
                "observed_rounds": observations.get(shard).unwrap(),
                "token_hash": blake3::hash(tokens.get(shard).unwrap()).to_hex().to_string(),
                "data_commands_before": writes_before.get(partition).unwrap(),
                "data_commands_after": writes_after.get(partition).unwrap(),
            })
        })
        .collect();
    let evidence = runtime.directory.join("watch-idle-scale-ledger.json");
    tokio::fs::write(
        &evidence,
        serde_json::to_vec_pretty(&serde_json::json!({
            "idle_shards": idle_shards, "hot_shards": 1, "data_groups": 8,
            "pages": pages.len(), "max_targets": tidemq_protocol::watch::MAX_TARGETS,
            "observation_recovery": observation_totals,
            "rounds": ROUNDS, "hot_acks": hot.acknowledged - first_hot,
            "hot_publishes": hot.published - first_hot_publish,
            "hot_reads": hot.read_sequence - first_hot_read,
            "hot_known_empty_reads": hot.empty_reads - first_hot_empty,
            "hot_empty_retirements": hot.empty_retirements - first_hot_empty,
            "hot_applied_reads": hot_events,
            "setup_elapsed_ms": setup_elapsed.as_millis(),
            "measurement_before_artifact_ms": measurement.elapsed().as_millis(),
            "heartbeat_before": heartbeat_before, "heartbeat_after": heartbeat_after,
            "before": before, "after": after, "shards": ledger,
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    eprintln!("Watch scale evidence ledger: {}", evidence.display());
    measurement.finish();
    let cleanup = PhaseTimer::start("cleanup");
    heartbeats.close().await;
    for scope in scopes {
        scope.member.leave(&wire).await;
    }
    hot.member.leave(&wire).await;
    hot.producer.close().await.unwrap();
    Arc::try_unwrap(wire)
        .ok()
        .expect("A Watch scale task retained the raw client")
        .close()
        .await;
    admin.close().await.unwrap();
    runtime.close().await;
    assert_released(&observer.snapshot().await);
    drop(placements);
    cleanup.finish();
    total.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_ack_and_control_progress_leave_one_thousand_real_idle_shards_clean_and_fair() {
    let _serial = SERIAL.lock().await;
    tokio::time::timeout(Duration::from_secs(900), scale(SHARDS_PER_TOPIC))
        .await
        .expect("The real 1000-shard Watch regression exceeded its total deadline");
}
