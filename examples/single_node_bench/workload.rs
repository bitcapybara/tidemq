use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use tidemq_protocol::Id;
use tidemq_sdk::{AckReceipt, Client, Producer, QueueConsumer, ReceivedMessage};
use tokio::{task::JoinSet, time::Instant};

use super::{
    Args,
    ledger::{self, Ledger},
};

pub type SharedLedger = Arc<Mutex<Ledger>>;

pub fn lock(ledger: &SharedLedger) -> Result<MutexGuard<'_, Ledger>> {
    ledger
        .lock()
        .map_err(|_| anyhow::anyhow!("Benchmark ledger lock poisoned"))
}

pub fn elapsed(started: Instant) -> Result<u64> {
    Ok(u64::try_from(started.elapsed().as_nanos())?)
}

pub async fn traffic(
    args: &Args,
    client: &Client,
    producer: &Producer,
    topic: &str,
    ledger: &SharedLedger,
) -> Result<(u64, QueueConsumer)> {
    let started = Instant::now();
    let run = lock(ledger)?.run_id;
    let mut tasks = JoinSet::new();
    for worker in 0..args.concurrency {
        let (producer, ledger, count, size, concurrency, batch_size) = (
            producer.clone(),
            ledger.clone(),
            args.messages,
            args.message_bytes,
            args.concurrency,
            args.producer_batch_size,
        );
        tasks.spawn(async move {
            let mut sequences = (worker..count).step_by(concurrency);
            loop {
                let chunk: Vec<_> = sequences.by_ref().take(batch_size).collect();
                if chunk.is_empty() {
                    break;
                }
                let mut messages = Vec::with_capacity(chunk.len());
                for sequence in &chunk {
                    messages.push(ledger::message(run, *sequence, size)?);
                    lock(&ledger)?.start(*sequence, elapsed(started)?)?;
                }
                if batch_size == 1 {
                    let sequence = *chunk.first().ok_or_else(|| {
                        anyhow::anyhow!("Producer batch unexpectedly has no sequence")
                    })?;
                    let message = messages.pop().ok_or_else(|| {
                        anyhow::anyhow!("Producer batch unexpectedly has no message")
                    })?;
                    let receipt = producer
                        .send(message)
                        .await
                        .with_context(|| format!("Send failed at business sequence {sequence}"))?;
                    lock(&ledger)?.commit(
                        sequence,
                        receipt.message_id().to_string(),
                        elapsed(started)?,
                    )?;
                } else {
                    let result = producer
                        .send_batch(messages)
                        .await
                        .context("Publish batch failed")?;
                    ensure!(
                        result.outcomes.len() == chunk.len(),
                        "Publish batch omitted an original send outcome"
                    );
                    for (sequence, outcome) in chunk.into_iter().zip(result.outcomes) {
                        let receipt = outcome.with_context(|| {
                            format!("Send failed at business sequence {sequence}")
                        })?;
                        lock(&ledger)?.commit(
                            sequence,
                            receipt.message_id().to_string(),
                            elapsed(started)?,
                        )?;
                    }
                }
            }
            Ok(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result
            .context("Benchmark worker failed")
            .and_then(|result| result)
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            return Err(error);
        }
    }
    let producer_elapsed = {
        let ledger = lock(ledger)?;
        ledger
            .entries
            .iter()
            .filter_map(|entry| entry.committed_ns)
            .max()
            .context("Missing last durable publish receipt timestamp")?
    };
    super::write(args, "producer-finished.json", b"{}\n".to_vec()).await?;
    let consumer = client
        .new_queue_consumer(topic, "perf")
        .prefetch_messages(256)
        .prefetch_bytes(8 * 1024 * 1024)
        .build()
        .await?;
    if args.ack_batch_size == 1 {
        for worker in 0..args.concurrency {
            spawn_consumer(
                &mut tasks,
                consumer.clone(),
                ledger.clone(),
                run,
                started,
                worker,
                args,
            );
        }
    } else {
        spawn_cumulative_consumer(
            &mut tasks,
            consumer.clone(),
            ledger.clone(),
            run,
            started,
            args,
        );
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result
            .context("Benchmark consumer failed")
            .and_then(|result| result)
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            return Err(error);
        }
    }
    let ledger = lock(ledger)?;
    ledger.complete()?;
    Ok((producer_elapsed, consumer))
}

fn spawn_consumer(
    tasks: &mut JoinSet<Result<()>>,
    consumer: QueueConsumer,
    ledger: SharedLedger,
    run: Id,
    started: Instant,
    worker: usize,
    args: &Args,
) {
    let (count, size, concurrency) = (args.messages, args.message_bytes, args.concurrency);
    tasks.spawn(async move {
        for _ in (worker..count).step_by(concurrency) {
            let delivery = consumer.receive().await.context("Receive failed")?;
            let received = elapsed(started)?;
            let sequence = ledger::sequence(run, delivery.key(), delivery.payload(), size)?;
            lock(&ledger)?.receive(sequence, delivery.message_id().to_string(), received)?;
            let receipt = consumer
                .ack(&delivery)
                .await
                .with_context(|| format!("ACK failed at business sequence {sequence}"))?;
            ensure!(
                receipt == AckReceipt::Acknowledged,
                "Original delivery was not durably acknowledged"
            );
            lock(&ledger)?.ack(sequence, elapsed(started)?)?;
        }
        Ok(())
    });
}

fn spawn_cumulative_consumer(
    tasks: &mut JoinSet<Result<()>>,
    consumer: QueueConsumer,
    ledger: SharedLedger,
    run: Id,
    started: Instant,
    args: &Args,
) {
    let (count, size, batch_size) = (args.messages, args.message_bytes, args.ack_batch_size);
    tasks.spawn(async move {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..count {
            let delivery = consumer.receive().await.context("Receive failed")?;
            let received = elapsed(started)?;
            let sequence = ledger::sequence(run, delivery.key(), delivery.payload(), size)?;
            lock(&ledger)?.receive(sequence, delivery.message_id().to_string(), received)?;
            batch.push((sequence, delivery));
            if batch.len() == batch_size {
                acknowledge_cumulative_batch(&consumer, &ledger, started, &mut batch).await?;
            }
        }
        if !batch.is_empty() {
            acknowledge_cumulative_batch(&consumer, &ledger, started, &mut batch).await?;
        }
        Ok(())
    });
}

async fn acknowledge_cumulative_batch(
    consumer: &QueueConsumer,
    ledger: &SharedLedger,
    started: Instant,
    batch: &mut Vec<(usize, ReceivedMessage)>,
) -> Result<()> {
    let receipt = {
        let last = batch.last().context("Cumulative ACK batch is empty")?;
        consumer
            .ack_cumulative(&last.1)
            .await
            .context("Cumulative ACK failed")?
    };
    ensure!(
        receipt == AckReceipt::Acknowledged,
        "Cumulative delivery was not durably acknowledged"
    );
    let acknowledged = elapsed(started)?;
    for (sequence, _) in batch.drain(..) {
        lock(ledger)?.ack(sequence, acknowledged)?;
    }
    Ok(())
}

pub async fn idle(consumer: &QueueConsumer, seconds: u64) -> Result<u64> {
    let started = Instant::now();
    match tokio::time::timeout(Duration::from_secs(seconds), consumer.receive()).await {
        Err(_) => elapsed(started),
        Ok(Ok(delivery)) => anyhow::bail!("Unexpected idle delivery {}", delivery.message_id()),
        Ok(Err(error)) => Err(error).context("Idle consumer failed"),
    }
}

pub async fn check_tail(consumer: &QueueConsumer) -> Result<()> {
    // The bounded observation window catches queued duplicates after the expected ACK count.
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(delivery) = consumer.try_receive()? {
            anyhow::bail!(
                "Unexpected extra delivery after complete ledger: {}",
                delivery.message_id()
            );
        }
    }
    Ok(())
}
