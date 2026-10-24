use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tidemq_protocol::Id;
use tidemq_sdk::{AckReceipt, Client, Producer, QueueConsumer, ReceivedMessage};
use tokio::time::Instant;

use super::{
    Args, Report,
    output::{Output, request_deadline, unix_ms},
    recovery::{Batch, BatchEvidence},
};

struct Queue<'a> {
    client: &'a Client,
    topic: String,
    producer: Producer,
    consumer: Option<QueueConsumer>,
}

pub async fn run(
    args: &Args,
    output: &Output,
    client: &Client,
    run: Id,
    report: &mut Report,
    deadline: Instant,
) -> Result<()> {
    report.mark("creating_queue")?;
    let mut queue = Queue::create(client, run, report, deadline).await?;
    if args.replay_settle_ms == 0 {
        queue.join(report, deadline).await?;
    }
    report.mark("initial_production")?;
    queue.produce_until(args.messages, report, deadline).await?;
    if args.replay_settle_ms > 0 {
        report.mark("replay_settle")?;
        tokio::time::sleep(Duration::from_millis(args.replay_settle_ms)).await;
        queue.join(report, deadline).await?;
    }
    report.mark("initial_consumption")?;
    queue.consume_until(args.messages, report, deadline).await?;
    queue.check_complete(args.messages, report)?;
    report.ledger.complete(args.messages)?;
    report.mark("initial_complete")?;
    if args.stage_fault {
        queue.fault(args, output, report, deadline).await?;
    }
    report.ledger.complete(report.ledger.expected)?;
    ensure!(
        queue.producer.pending_sends().await?.is_empty(),
        "Final producer still has unresolved batches"
    );
    queue
        .consumer
        .as_ref()
        .context("Original consumer is missing")?
        .close()
        .await?;
    queue.producer.close().await?;
    report.mark("ledger_complete")
}

impl<'a> Queue<'a> {
    async fn create(
        client: &'a Client,
        run: Id,
        report: &mut Report,
        deadline: Instant,
    ) -> Result<Self> {
        let topic = format!("validation/default/deploy-{run}");
        let info = tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(client.admin().create_topic(&topic))
            .await?;
        report.topic_id = Some(info.id().to_string());
        let consumer = tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(client.new_queue_consumer(&topic, "validation").build())
            .await?;
        consumer.close().await?;
        let subscription = client
            .admin()
            .queue_subscription(&topic, "validation")
            .await?;
        report.subscription_id = Some(subscription.id().to_string());
        let producer = tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(client.new_producer(&topic).build())
            .await?;
        Ok(Self {
            client,
            topic,
            producer,
            consumer: None,
        })
    }

    async fn join(&mut self, report: &mut Report, deadline: Instant) -> Result<()> {
        ensure!(
            self.consumer.is_none(),
            "Replacing the original consumer is forbidden"
        );
        self.consumer = Some(
            tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
                .scope(
                    self.client
                        .new_queue_consumer(&self.topic, "validation")
                        .prefetch_messages(256)
                        .prefetch_bytes(4 * 1024 * 1024)
                        .build(),
                )
                .await?,
        );
        report.replay_started_after_publish = report.ledger.committed > 0;
        Ok(())
    }

    async fn begin_batch(
        &self,
        count: u64,
        report: &mut Report,
        deadline: Instant,
    ) -> Result<Batch> {
        let first = report.ledger.committed;
        let messages = report.ledger.messages(first, count)?;
        ensure!(report.batches.len() < 34, "Batch evidence bound exceeded");
        report
            .batches
            .push(BatchEvidence::new(first, messages.len()));
        let evidence = report
            .batches
            .last_mut()
            .context("Batch evidence is unavailable")?;
        Batch::begin(&self.producer, messages, evidence, deadline).await
    }

    async fn finish_batch(
        &self,
        batch: &mut Batch,
        report: &mut Report,
        deadline: Instant,
    ) -> Result<()> {
        let evidence = report
            .batches
            .last_mut()
            .context("Original batch evidence is unavailable")?;
        let receipts = batch.finish(evidence, deadline).await?;
        report.ledger.commit(
            evidence.first_sequence,
            &receipts
                .iter()
                .map(|receipt| receipt.message_id().to_string())
                .collect::<Vec<_>>(),
        )
    }

    async fn produce_until(
        &self,
        target: u64,
        report: &mut Report,
        deadline: Instant,
    ) -> Result<()> {
        while report.ledger.committed < target {
            let count = target
                .checked_sub(report.ledger.committed)
                .context("Production range underflow")?
                .min(8);
            let mut batch = self.begin_batch(count, report, deadline).await?;
            self.finish_batch(&mut batch, report, deadline).await?;
        }
        Ok(())
    }

    async fn consume_until(
        &self,
        target: u64,
        report: &mut Report,
        deadline: Instant,
    ) -> Result<()> {
        let consumer = self
            .consumer
            .as_ref()
            .context("The original consumer is unavailable")?;
        while report.ledger.acknowledged < target {
            let delivery = tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
                .scope(consumer.receive())
                .await?;
            let identity = report.ledger.observe(
                &delivery.message_id().to_string(),
                delivery.key().context("Delivery key is absent")?,
                delivery.payload(),
            )?;
            ensure!(
                ack(consumer, &delivery, report, deadline).await? == AckReceipt::Acknowledged,
                "ACK must durably acknowledge the original delivery"
            );
            report.ledger.ack(identity)?;
        }
        Ok(())
    }

    fn check_complete(&self, target: u64, report: &mut Report) -> Result<()> {
        report.ledger.complete(target)?;
        report.original_consumer_reused = self.consumer.is_some();
        Ok(())
    }

    async fn fault(
        &self,
        args: &Args,
        output: &Output,
        report: &mut Report,
        deadline: Instant,
    ) -> Result<()> {
        output.absent("fault-injected").await?;
        output.absent("recovered").await?;
        report.mark("waiting_fault")?;
        output.write("waiting-fault.json", &serde_json::json!({"schema_version":2,"unix_ms":unix_ms()?,
            "topic_id":report.topic_id,"subscription_id":report.subscription_id,
            "partition_id":0,"owner_node":null,"owner_lookup":"public_sdk_does_not_expose_owner",
            "business_acknowledged_before":report.ledger.frontier,"fault_batch_first_sequence":report.ledger.committed,
            "fault_batch_messages":8,"require_unavailable":args.require_unavailable})).await?;
        output.wait("fault-injected", deadline).await?;
        report.mark("fault_injected")?;
        report.mark("fault_production")?;
        let mut batch = self.begin_batch(8, report, deadline).await?;
        if args.require_unavailable {
            ensure!(
                batch.unresolved(),
                "Unavailable validation unexpectedly obtained a success receipt"
            );
        } else {
            self.finish_batch(&mut batch, report, deadline).await?;
            report.mark("fault_consumption")?;
            self.consume_until(
                args.messages
                    .checked_add(8)
                    .context("Fault range overflow")?,
                report,
                deadline,
            )
            .await?;
            self.check_complete(report.ledger.committed, report)?;
        }
        output.absent("recovered").await?;
        report.mark("fault_observed")?;
        output.write("fault-observed.json", &serde_json::json!({"schema_version":2,"unix_ms":unix_ms()?,
            "require_unavailable":args.require_unavailable,"business_acknowledged_before":report.ledger.frontier,
            "ledger":report.ledger,"batch":report.batches.last().context("Fault batch evidence is unavailable")?})).await?;
        output.wait("recovered", deadline).await?;
        report.mark("host_recovered")?;
        if args.require_unavailable {
            report.mark("original_pending_retry")?;
            self.finish_batch(&mut batch, report, deadline).await?;
            report.mark("recovered_consumption")?;
            self.consume_until(
                args.messages
                    .checked_add(8)
                    .context("Recovery range overflow")?,
                report,
                deadline,
            )
            .await?;
            self.check_complete(report.ledger.committed, report)?;
        }
        report.mark("final_production")?;
        self.produce_until(report.ledger.expected, report, deadline)
            .await?;
        report.mark("final_consumption")?;
        self.consume_until(report.ledger.expected, report, deadline)
            .await?;
        self.check_complete(report.ledger.expected, report)
    }
}

async fn ack(
    consumer: &QueueConsumer,
    delivery: &ReceivedMessage,
    report: &mut Report,
    deadline: Instant,
) -> Result<AckReceipt> {
    let result = tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
        .scope(consumer.ack(delivery))
        .await;
    let mut pending = match result {
        Ok(receipt) => return Ok(receipt),
        Err(tidemq_sdk::Error::Acknowledgement { pending, .. }) => pending,
        Err(error) => return Err(error.into()),
    };
    for attempt in 0..4 {
        report.ack_unknown_retries = report
            .ack_unknown_retries
            .checked_add(1)
            .context("ACK retry counter overflow")?;
        match tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(pending.retry())
            .await
        {
            Ok(receipt) => return Ok(receipt),
            Err(tidemq_sdk::Error::Acknowledgement {
                pending: original, ..
            }) if attempt < 3 => {
                pending = original;
                tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(100)))
                    .await;
            }
            Err(error) => {
                return Err(anyhow::Error::new(error)).context("Recover the original ACK");
            }
        }
    }
    anyhow::bail!("Original ACK retry budget exhausted")
}
