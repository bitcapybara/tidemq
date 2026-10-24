use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use serde::Serialize;
use tidemq_protocol::Id;
use tidemq_sdk::{
    AckReceipt, Client, Message, MessageId, Producer, QueueConsumer, RequestContext, SendReceipt,
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinSet,
    time::Instant,
};

use super::{
    Args, Mode, sample,
    stats::{Distribution, Histogram, Seen},
};

const MAGIC: &[u8; 8] = b"TIDEPF01";

#[derive(Serialize)]
pub struct PhaseReport {
    pub label: String,
    pub started_at_unix_ms: u128,
    pub topic_id: String,
    pub planned_messages: u64,
    pub committed_messages: u64,
    pub acknowledged_messages: u64,
    pub durable_acknowledged_messages: u64,
    pub logical_bytes: u64,
    pub committed_logical_bytes: u64,
    pub acknowledged_logical_bytes: u64,
    pub produce_elapsed_ns: u64,
    pub consume_ack_elapsed_ns: u64,
    pub produce_messages_per_second: f64,
    pub consume_ack_messages_per_second: f64,
    pub produce_logical_bytes_per_second: f64,
    pub consume_ack_logical_bytes_per_second: f64,
    pub produce_batch: Distribution,
    pub produce_scheduling_lateness: Distribution,
    pub receive_call: Distribution,
    pub ack_batch: Distribution,
    pub offered_to_durable_ack: Distribution,
    pub control_query: Distribution,
    pub control_errors: u64,
    pub produce_unknown: u64,
    pub produce_recovered_original_identity: u64,
    pub ack_unknown: u64,
    pub cold_cache_misses_before: Option<u64>,
    pub cold_cache_misses_after: Option<u64>,
    pub physical_cold_io_observed: bool,
    pub resources: Vec<sample::ResourceSample>,
    pub resource_summary: sample::ResourceSummary,
    pub client_metrics_before: serde_json::Value,
    pub client_metrics_after: serde_json::Value,
    pub client_transport_before: serde_json::Value,
    pub client_transport_after: serde_json::Value,
    pub server_metrics_before: String,
    pub server_metrics_after: String,
    pub terminal_error: Option<String>,
}

struct ProduceStats {
    latency: Histogram,
    scheduling_lateness: Histogram,
    messages: u64,
    unknown: u64,
    recovered: u64,
    error: Option<String>,
}

struct ConsumeStats {
    receive: Histogram,
    ack: Histogram,
    end_to_end: Histogram,
    acknowledged: u64,
    frontier: u64,
    unknown: u64,
    elapsed_ns: u64,
    error: Option<String>,
}

enum Background {
    Samples(Vec<sample::ResourceSample>),
    Control { latency: Histogram, errors: u64 },
}

struct Prepared {
    name: String,
    topic: String,
    producers: Vec<Producer>,
    consumer: Option<QueueConsumer>,
}

#[derive(Serialize)]
pub struct GroupReport {
    pub elapsed_ns: u64,
    pub partitions: Vec<PhaseReport>,
}

pub async fn group(
    client: &Client,
    args: &Args,
    label: &str,
    run: Id,
    count: u64,
) -> Result<GroupReport> {
    let mut prepared = Vec::new();
    prepared.try_reserve_exact(args.concurrent_partitions)?;
    // Serial setup keeps metadata admission outside the measured concurrent workload.
    for partition in 0..args.concurrent_partitions {
        let label = format!("{label}-{partition}");
        prepared.push((label.clone(), prepare(client, args, &label, run).await?));
    }
    let started = Instant::now();
    let mut running = JoinSet::new();
    for (partition, (label, prepared)) in prepared.into_iter().enumerate() {
        let client = client.clone();
        let args = args.clone();
        let offset = args
            .produce_interval_ms
            .checked_mul(u64::try_from(partition)?)
            .context("Partition offer offset overflow")?
            / u64::try_from(args.concurrent_partitions)?;
        running.spawn(
            async move { phase(&client, &args, &label, run, count, prepared, offset).await },
        );
    }
    let mut partitions = Vec::new();
    partitions.try_reserve_exact(args.concurrent_partitions)?;
    while let Some(result) = running.join_next().await {
        partitions.push(result??);
    }
    partitions.sort_by(|left, right| left.label.cmp(&right.label));
    Ok(GroupReport {
        elapsed_ns: nanos(started.elapsed())?,
        partitions,
    })
}

async fn prepare(client: &Client, args: &Args, label: &str, run: Id) -> Result<Prepared> {
    let name = format!("validation/default/perf-{run}-{label}");
    client.admin().create_topic(&name).await?;
    let producer = client.new_producer(&name).build().await?;
    let producers = vec![producer; args.workers];
    let consumer = join(client, &name).await?;
    let consumer = if matches!(args.mode, Mode::Live) {
        Some(consumer)
    } else {
        consumer.close().await?;
        None
    };
    Ok(Prepared {
        topic: name.clone(),
        name,
        producers,
        consumer,
    })
}

async fn phase(
    client: &Client,
    args: &Args,
    label: &str,
    run: Id,
    count: u64,
    prepared: Prepared,
    offer_offset_ms: u64,
) -> Result<PhaseReport> {
    let Prepared {
        name,
        topic,
        producers,
        consumer,
    } = prepared;
    let server_metrics_before = sample::metrics(args.management).await?;
    let cold_before = sample::cold_misses(&server_metrics_before)?;
    let client_metrics_before = client_metrics(client);
    let client_transport_before = serde_json::to_value(client.transport_metrics().snapshot())?;
    let started = Instant::now();
    let started_at_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let deadline = started
        .checked_add(Duration::from_secs(args.phase_timeout_secs))
        .context("Phase deadline overflow")?;
    let (stop, stopped) = watch::channel(false);
    let mut background = JoinSet::new();
    let control_histogram = Histogram::new()?;
    let sampler_args = args.clone();
    let sampler_stop = stopped.clone();
    background.spawn(async move {
        Background::Samples(samples(sampler_args, started, deadline, sampler_stop).await)
    });
    let control_client = client.clone();
    let control_name = name.clone();
    background.spawn(async move {
        control(
            control_client,
            control_name,
            deadline,
            stopped,
            control_histogram,
        )
        .await
    });
    let produced = produce_all(
        client,
        args,
        &producers,
        started,
        deadline,
        count,
        consumer,
        &topic,
        run,
        offer_offset_ms,
    )
    .await;
    let _ = stop.send(true);
    let mut resources = Vec::new();
    let mut control_latency = Histogram::new()?;
    let mut control_errors = 0;
    while let Some(result) = background.join_next().await {
        match result? {
            Background::Samples(samples) => resources = samples,
            Background::Control { latency, errors } => {
                control_latency = latency;
                control_errors = errors;
            }
        }
    }
    let (produce, consume, produce_elapsed_ns) = produced?;
    let server_metrics_after = sample::metrics(args.management).await?;
    let cold_after = sample::cold_misses(&server_metrics_after)?;
    let physical_cold = cold_before
        .zip(cold_after)
        .is_some_and(|(before, after)| after > before);
    let mut terminal_error = produce.error.or(consume.error);
    if args.require_cold && !physical_cold {
        terminal_error
            .get_or_insert("Replay had no verified physical cold range cache miss".into());
    }
    if control_errors > 0 {
        terminal_error.get_or_insert("Control query failed during the mixed load".into());
    }
    if resources.is_empty() || resources.iter().any(|sample| sample.error.is_some()) {
        terminal_error
            .get_or_insert("Process or runtime resource observations were unavailable".into());
    }
    if produce.messages != count || consume.acknowledged != count || consume.frontier != count {
        terminal_error.get_or_insert(
            "Committed messages, unique deliveries and durable ACK frontier did not match the plan"
                .into(),
        );
    }
    let cleanup_deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .context("Cleanup deadline overflow")?;
    for producer in producers {
        if Instant::now() >= cleanup_deadline {
            terminal_error.get_or_insert(
                "Producer close exceeded the shared five-second cleanup budget".into(),
            );
            break;
        }
        match tokio::time::timeout_at(cleanup_deadline, producer.close()).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                let error = anyhow::Error::new(error);
                terminal_error.get_or_insert(format!("Producer close failed: {error:#}"));
            }
            Err(error) => {
                terminal_error
                    .get_or_insert(format!("Producer close exceeded its deadline: {error:#}"));
            }
        }
    }
    let resource_summary = sample::summarize(&resources);
    let message_bytes = u64::try_from(args.message_bytes + 8)?;
    let committed_logical_bytes = produce
        .messages
        .checked_mul(message_bytes)
        .context("Committed byte count overflow")?;
    let acknowledged_logical_bytes = consume
        .acknowledged
        .checked_mul(message_bytes)
        .context("Acknowledged byte count overflow")?;
    Ok(PhaseReport {
        label: label.into(),
        started_at_unix_ms,
        topic_id: topic.to_string(),
        planned_messages: count,
        committed_messages: produce.messages,
        acknowledged_messages: consume.acknowledged,
        durable_acknowledged_messages: consume.frontier,
        logical_bytes: count
            .checked_mul(u64::try_from(args.message_bytes + 8)?)
            .context("Logical byte count overflow")?,
        committed_logical_bytes,
        acknowledged_logical_bytes,
        produce_elapsed_ns,
        consume_ack_elapsed_ns: consume.elapsed_ns,
        produce_messages_per_second: rate(produce.messages, produce_elapsed_ns),
        consume_ack_messages_per_second: rate(consume.acknowledged, consume.elapsed_ns),
        produce_logical_bytes_per_second: rate(committed_logical_bytes, produce_elapsed_ns),
        consume_ack_logical_bytes_per_second: rate(acknowledged_logical_bytes, consume.elapsed_ns),
        produce_batch: produce.latency.report(),
        produce_scheduling_lateness: produce.scheduling_lateness.report(),
        receive_call: consume.receive.report(),
        ack_batch: consume.ack.report(),
        offered_to_durable_ack: consume.end_to_end.report(),
        control_query: control_latency.report(),
        control_errors,
        produce_unknown: produce.unknown,
        produce_recovered_original_identity: produce.recovered,
        ack_unknown: consume.unknown,
        cold_cache_misses_before: cold_before,
        cold_cache_misses_after: cold_after,
        physical_cold_io_observed: physical_cold,
        resources,
        resource_summary,
        client_metrics_before,
        client_metrics_after: client_metrics(client),
        client_transport_before,
        client_transport_after: serde_json::to_value(client.transport_metrics().snapshot())?,
        server_metrics_before,
        server_metrics_after,
        terminal_error,
    })
}

fn client_metrics(client: &Client) -> serde_json::Value {
    let snapshot = client.metrics().snapshot();
    serde_json::json!({
        "requests":snapshot.requests,"retries":snapshot.retries,"unknown":snapshot.unknown,
        "deliveries":snapshot.deliveries,"acknowledged":snapshot.acknowledged,
        "request_duration_ns":snapshot.request_duration_ns,"admission_wait_ns":snapshot.admission_wait_ns,
        "admitted_bytes":snapshot.admitted_bytes,"pending_sends":snapshot.pending_sends,"active_deliveries":snapshot.active_deliveries,
    })
}

// Work setup is separate so every spawned background task is joined even after an error.
// Every argument is bounded phase state; keeping it explicit avoids hidden mutable benchmark globals.
#[allow(clippy::too_many_arguments)]
async fn produce_all(
    client: &Client,
    args: &Args,
    producers: &[Producer],
    started: Instant,
    deadline: Instant,
    count: u64,
    consumer: Option<QueueConsumer>,
    topic: &str,
    run: Id,
    offer_offset_ms: u64,
) -> Result<(ProduceStats, ConsumeStats, u64)> {
    let mut consuming = JoinSet::new();
    if let Some(consumer) = consumer {
        let args = args.clone();
        consuming
            .spawn(async move { consume(consumer, &args, started, deadline, count, run).await });
    }
    let next = Arc::new(AtomicU64::new(0));
    let offsets = Arc::new(Mutex::new(std::collections::HashSet::<MessageId>::new()));
    let mut workers = JoinSet::new();
    for (worker, producer) in producers.iter().enumerate() {
        let args = args.clone();
        let producer = producer.clone();
        let next = next.clone();
        let offsets = offsets.clone();
        workers.spawn(async move {
            produce(
                producer,
                &args,
                worker,
                next,
                offsets,
                started,
                deadline,
                count,
                run,
                offer_offset_ms,
            )
            .await
        });
    }
    let mut produced = ProduceStats {
        latency: Histogram::new()?,
        scheduling_lateness: Histogram::new()?,
        messages: 0,
        unknown: 0,
        recovered: 0,
        error: None,
    };
    while let Some(result) = workers.join_next().await {
        let worker = result??;
        produced.latency.merge(&worker.latency)?;
        produced
            .scheduling_lateness
            .merge(&worker.scheduling_lateness)?;
        produced.messages += worker.messages;
        produced.unknown += worker.unknown;
        produced.recovered += worker.recovered;
        if produced.error.is_none() {
            produced.error = worker.error;
        }
    }
    let elapsed = nanos(started.elapsed())?;
    if matches!(args.mode, Mode::Replay) {
        tokio::time::sleep_until(
            deadline.min(
                Instant::now()
                    .checked_add(Duration::from_millis(args.replay_settle_ms))
                    .context("Settle deadline overflow")?,
            ),
        )
        .await;
        let consumer = join(client, topic).await?;
        let args = args.clone();
        consuming
            .spawn(async move { consume(consumer, &args, started, deadline, count, run).await });
    }
    let consumed = consuming
        .join_next()
        .await
        .context("Missing consumer task")???;
    Ok((produced, consumed, elapsed))
}

async fn join(client: &Client, topic: &str) -> Result<QueueConsumer> {
    client
        .new_queue_consumer(topic, "load")
        .prefetch_messages(256)
        .prefetch_bytes(4 * 1024 * 1024)
        .build()
        .await
        .context("Join performance queue")
}

// Worker inputs belong to one fixed phase and are not retained between runs.
#[allow(clippy::too_many_arguments)]
async fn produce(
    producer: Producer,
    args: &Args,
    worker: usize,
    next: Arc<AtomicU64>,
    offsets: Arc<Mutex<std::collections::HashSet<MessageId>>>,
    started: Instant,
    deadline: Instant,
    count: u64,
    run: Id,
    offer_offset_ms: u64,
) -> Result<ProduceStats> {
    let mut stats = ProduceStats {
        latency: Histogram::new()?,
        scheduling_lateness: Histogram::new()?,
        messages: 0,
        unknown: 0,
        recovered: 0,
        error: None,
    };
    let result = async {
        loop {
            let batch = next.fetch_add(1, Ordering::Relaxed);
            let first = batch
                .checked_mul(u64::from(args.batch_messages))
                .context("Batch sequence overflow")?;
            if first >= count {
                break;
            }
            let size = (count - first).min(u64::from(args.batch_messages));
            let offered = offer_time(
                started,
                deadline,
                batch,
                args.produce_interval_ms,
                offer_offset_ms,
            )
            .await?;
            stats.scheduling_lateness.record(
                nanos(started.elapsed())?
                    .checked_sub(offered)
                    .context("Offer timestamp is in the future")?,
            )?;
            let messages = messages(args.message_bytes, worker, first, size, run, offered)?;
            let result = RequestContext::with_deadline(deadline)
                .scope(producer.send_batch(messages))
                .await;
            let receipts = recover(result, deadline, &mut stats).await?;
            stats.latency.record(
                nanos(started.elapsed())?
                    .checked_sub(offered)
                    .context("Produce completion predates its planned offer")?,
            )?;
            ensure!(receipts.len() as u64 == size, "Send receipt count mismatch");
            let mut offsets = offsets.lock().await;
            for receipt in receipts {
                ensure!(
                    offsets.insert(receipt.message_id()),
                    "Duplicate committed message identity"
                );
            }
            stats.messages += size;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    stats.error = result.err().map(|error| format!("{error:#}"));
    Ok(stats)
}

async fn offer_time(
    started: Instant,
    deadline: Instant,
    batch: u64,
    interval_ms: u64,
    offset_ms: u64,
) -> Result<u64> {
    if interval_ms == 0 {
        return nanos(started.elapsed());
    }
    let scheduled = Duration::from_millis(
        batch
            .checked_mul(interval_ms)
            .and_then(|delay| delay.checked_add(offset_ms))
            .context("Offer schedule overflow")?,
    );
    let offered = started
        .checked_add(scheduled)
        .context("Offer deadline overflow")?;
    ensure!(
        offered < deadline,
        "Offer exceeded the original phase deadline"
    );
    tokio::time::sleep_until(offered).await;
    ensure!(Instant::now() < deadline, "Offer phase deadline expired");
    nanos(scheduled)
}

async fn recover(
    result: tidemq_sdk::Result<tidemq_sdk::SendBatchResult>,
    deadline: Instant,
    stats: &mut ProduceStats,
) -> Result<Vec<SendReceipt>> {
    let outcomes = match result {
        Ok(batch) => batch.outcomes,
        Err(tidemq_sdk::Error::BatchSendInterrupted { pending, source }) => pending
            .into_iter()
            .map(|pending| {
                Err(tidemq_sdk::Error::SendUnresolved {
                    pending,
                    source: source.clone(),
                })
            })
            .collect(),
        Err(error) => return Err(error.into()),
    };
    let mut receipts = Vec::new();
    for outcome in outcomes {
        match outcome {
            Ok(receipt) => receipts.push(receipt),
            Err(tidemq_sdk::Error::SendUnresolved { mut pending, .. }) => {
                stats.unknown += 1;
                for attempt in 0..4 {
                    match RequestContext::with_deadline(deadline)
                        .scope(pending.retry())
                        .await
                    {
                        Ok(receipt) => {
                            receipts.push(receipt);
                            stats.recovered += 1;
                            break;
                        }
                        Err(tidemq_sdk::Error::SendUnresolved {
                            pending: original, ..
                        }) if attempt < 3 => pending = original,
                        Err(error) => {
                            return Err(anyhow::Error::new(error)).context("Recover original send");
                        }
                    }
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(receipts)
}

// The consumer validates the same immutable run context as the producer workers.
#[allow(clippy::too_many_arguments)]
async fn consume(
    consumer: QueueConsumer,
    args: &Args,
    started: Instant,
    deadline: Instant,
    count: u64,
    run: Id,
) -> Result<ConsumeStats> {
    let mut stats = ConsumeStats {
        receive: Histogram::new()?,
        ack: Histogram::new()?,
        end_to_end: Histogram::new()?,
        acknowledged: 0,
        frontier: 0,
        unknown: 0,
        elapsed_ns: 0,
        error: None,
    };
    let consuming_started = Instant::now();
    let result = consume_loop(&consumer, args, started, deadline, count, run, &mut stats).await;
    stats.elapsed_ns = nanos(consuming_started.elapsed())?;
    stats.error = result.err().map(|error| format!("{error:#}"));
    if stats.error.is_none() {
        ensure!(
            consumer.pending_acks().await?.is_empty(),
            "Original ACKs remain unresolved"
        );
    }
    consumer.close().await?;
    Ok(stats)
}

// Validation and measurements share this single bounded consumption loop.
#[allow(clippy::too_many_arguments)]
async fn consume_loop(
    consumer: &QueueConsumer,
    args: &Args,
    started: Instant,
    deadline: Instant,
    count: u64,
    run: Id,
    stats: &mut ConsumeStats,
) -> Result<()> {
    let mut identities = Seen::new(count)?;
    let mut message_ids = std::collections::HashSet::new();
    while stats.acknowledged < count {
        ensure!(
            Instant::now() < deadline,
            "Consumer phase deadline exceeded"
        );
        let begin = Instant::now();
        let delivery = RequestContext::with_deadline(deadline)
            .scope(consumer.receive())
            .await
            .context("Receive performance delivery")?;
        stats.receive.record(nanos(begin.elapsed())?)?;
        let mut batch = Vec::new();
        batch.try_reserve_exact(args.ack_messages)?;
        batch.push(delivery);
        while batch.len() < args.ack_messages {
            let Some(delivery) = consumer
                .try_receive()
                .context("Read buffered performance delivery")?
            else {
                break;
            };
            batch.push(delivery);
        }
        let mut offered = Vec::new();
        offered.try_reserve_exact(batch.len())?;
        for delivery in &batch {
            ensure!(
                delivery.payload().len() == args.message_bytes
                    && delivery.key().is_some_and(|key| key.len() == 8),
                "Delivery payload shape mismatch"
            );
            let (identity, sent) = decode(delivery.payload(), run)?;
            identities.insert(identity)?;
            ensure!(
                message_ids.insert(delivery.message_id()),
                "Duplicate received message identity"
            );
            offered.push(sent);
        }
        if args.consumer_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.consumer_delay_ms)).await;
        }
        let begin = Instant::now();
        for delivery in &batch {
            let mut result = RequestContext::with_deadline(deadline)
                .scope(consumer.ack(delivery))
                .await;
            for _ in 0..4 {
                match &result {
                    Ok(AckReceipt::Acknowledged) => break,
                    Err(tidemq_sdk::Error::Acknowledgement { pending, .. }) => {
                        stats.unknown += 1;
                        result = RequestContext::with_deadline(deadline)
                            .scope(pending.retry())
                            .await;
                    }
                    _ => break,
                }
            }
            ensure!(
                result? == AckReceipt::Acknowledged,
                "Queue ACK returned a redelivery receipt"
            );
        }
        stats.ack.record(nanos(begin.elapsed())?)?;
        let completed = nanos(started.elapsed())?;
        for sent in offered {
            stats.end_to_end.record(
                completed
                    .checked_sub(sent)
                    .context("Message timestamp is in the future")?,
            )?;
        }
        stats.acknowledged += u64::try_from(batch.len())?;
        stats.frontier = stats.acknowledged;
    }
    Ok(())
}

async fn samples(
    args: Args,
    started: Instant,
    deadline: Instant,
    mut stop: watch::Receiver<bool>,
) -> Vec<sample::ResourceSample> {
    let mut samples = Vec::new();
    if samples.try_reserve_exact(242).is_err() {
        return samples;
    }
    let period_ms = (args.phase_timeout_secs * 1_000).div_ceil(240).max(1_000);
    let mut ticker = tokio::time::interval(Duration::from_millis(period_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while Instant::now() < deadline {
        tokio::select! { _ = stop.changed() => break, _ = ticker.tick() => {} }
        samples.push(sample::resource(&args, started).await);
        if samples.len() >= 241 {
            break;
        }
    }
    samples.push(sample::resource(&args, started).await);
    samples
}

async fn control(
    client: Client,
    topic: String,
    deadline: Instant,
    mut stop: watch::Receiver<bool>,
    mut latency: Histogram,
) -> Background {
    let mut errors = 0;
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while Instant::now() < deadline {
        tokio::select! { _ = stop.changed() => break, _ = ticker.tick() => {} }
        let begin = Instant::now();
        let result = RequestContext::with_deadline(deadline)
            .scope(client.admin().topic_info(&topic))
            .await;
        match result {
            Ok(_) => {
                if nanos(begin.elapsed())
                    .and_then(|value| latency.record(value))
                    .is_err()
                {
                    errors += 1;
                    break;
                }
            }
            Err(_) => errors += 1,
        }
    }
    Background::Control { latency, errors }
}

fn messages(
    bytes: usize,
    worker: usize,
    first: u64,
    count: u64,
    run: Id,
    sent_ns: u64,
) -> Result<Vec<Message>> {
    let mut messages = Vec::new();
    messages.try_reserve_exact(usize::try_from(count)?)?;
    let key = Bytes::copy_from_slice(&u64::try_from(worker)?.to_be_bytes());
    for identity in first
        ..first
            .checked_add(count)
            .context("Message identity overflow")?
    {
        let body = payload(bytes, identity, run, sent_ns)?;
        messages.push(Message::new(body).key(key.clone()));
    }
    Ok(messages)
}

fn payload(bytes: usize, identity: u64, run: Id, sent_ns: u64) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    body.try_reserve_exact(bytes)?;
    body.resize(bytes, 0x5a);
    body.get_mut(..8)
        .context("Missing payload magic")?
        .copy_from_slice(MAGIC);
    body.get_mut(8..16)
        .context("Missing run nonce")?
        .copy_from_slice(run.as_bytes().get(..8).context("Invalid run nonce")?);
    body.get_mut(16..24)
        .context("Missing message identity")?
        .copy_from_slice(&identity.to_be_bytes());
    body.get_mut(24..32)
        .context("Missing offered timestamp")?
        .copy_from_slice(&sent_ns.to_be_bytes());
    Ok(body)
}

fn decode(body: &[u8], run: Id) -> Result<(u64, u64)> {
    ensure!(
        body.get(..8) == Some(MAGIC.as_slice()),
        "Payload magic mismatch"
    );
    ensure!(
        body.get(8..16) == run.as_bytes().get(..8),
        "Run identity mismatch"
    );
    let identity = u64::from_be_bytes(
        body.get(16..24)
            .context("Missing message identity")?
            .try_into()?,
    );
    let sent = u64::from_be_bytes(
        body.get(24..32)
            .context("Missing offered timestamp")?
            .try_into()?,
    );
    Ok((identity, sent))
}

fn nanos(duration: Duration) -> Result<u64> {
    Ok(u64::try_from(duration.as_nanos())?)
}
fn rate(messages: u64, elapsed_ns: u64) -> f64 {
    if elapsed_ns == 0 {
        0.0
    } else {
        messages as f64 * 1e9 / elapsed_ns as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn payload_round_trip_preserves_identity_and_rejects_foreign_or_truncated_data() {
        let run = Id::new_v7().unwrap();
        let body = payload(128, 20, run, 5_000).unwrap();
        assert_eq!(decode(&body, run).unwrap(), (20, 5_000));
        assert!(decode(&body, Id::new_v7().unwrap()).is_err());
        assert!(decode(b"TIDEPF01", run).is_err());
        assert_eq!(messages(128, 3, 20, 2, run, 5_000).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn delayed_producer_keeps_original_offer_times_and_rejects_expired_schedule() {
        let started = Instant::now()
            .checked_sub(Duration::from_millis(20))
            .unwrap();
        let deadline = Instant::now().checked_add(Duration::from_secs(1)).unwrap();
        assert_eq!(
            offer_time(started, deadline, 0, 4, 1).await.unwrap(),
            1_000_000
        );
        assert_eq!(
            offer_time(started, deadline, 2, 4, 1).await.unwrap(),
            9_000_000
        );
        assert!(nanos(started.elapsed()).unwrap() >= 20_000_000);
        assert!(offer_time(started, Instant::now(), 0, 4, 1).await.is_err());
        assert!(offer_time(started, deadline, u64::MAX, 4, 1).await.is_err());
    }
}
