use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use tidemq_protocol::Id;
use tidemq_sdk::{AckReceipt, Producer, QueueConsumer};
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
    producer: &Producer,
    consumer: &QueueConsumer,
    ledger: &SharedLedger,
) -> Result<u64> {
    let started = Instant::now();
    let run = lock(ledger)?.run_id;
    let mut tasks = JoinSet::new();
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
        let (producer, ledger, count, size, concurrency) = (
            producer.clone(),
            ledger.clone(),
            args.messages,
            args.message_bytes,
            args.concurrency,
        );
        tasks.spawn(async move {
            for sequence in (worker..count).step_by(concurrency) {
                let message = ledger::message(run, sequence, size)?;
                lock(&ledger)?.start(sequence, elapsed(started)?)?;
                // Any error invalidates the run. No new send identity is submitted as a retry.
                let receipt = producer
                    .send(message)
                    .await
                    .with_context(|| format!("Send failed at business sequence {sequence}"))?;
                lock(&ledger)?.commit(
                    sequence,
                    receipt.message_id().to_string(),
                    elapsed(started)?,
                )?;
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
    let ledger = lock(ledger)?;
    ledger.complete()?;
    ledger
        .entries
        .iter()
        .filter_map(|entry| entry.acknowledged_ns)
        .max()
        .context("Missing last durable ACK timestamp")
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
