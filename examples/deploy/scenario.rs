use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tidemq_sdk::{Client, Consumer, ConsumerConfig, Delivery, Id, Producer, public};
use tokio::time::Instant;

use super::{
    Args, Report,
    output::{Output, request_deadline, unix_ms},
    recovery::{Batch, BatchEvidence},
};

struct Queue<'a> {
    client: &'a Client,
    topic: Id,
    subscription: Id,
    producer: Producer,
    consumer: Option<Consumer>,
    session: Option<Id>,
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
        queue.claim(report, 0, deadline).await?;
    }
    report.mark("initial_production")?;
    queue.produce_until(args.messages, report, deadline).await?;
    if args.replay_settle_ms > 0 {
        report.mark("replay_settle")?;
        tokio::time::sleep(Duration::from_millis(args.replay_settle_ms)).await;
        queue.claim(report, args.messages, deadline).await?;
    }
    report.mark("initial_consumption")?;
    queue.consume_until(args.messages, report, deadline).await?;
    queue
        .check_frontier(args.messages, report, deadline)
        .await?;
    report.ledger.complete(args.messages)?;
    report.mark("initial_complete")?;
    if args.stage_fault {
        queue.fault(args, output, report, deadline).await?;
    }
    report.ledger.complete(report.ledger.expected)?;
    ensure!(
        queue.producer.pending_batches().await?.is_empty(),
        "Final producer still has unresolved batches"
    );
    report.mark("ledger_complete")
}

impl<'a> Queue<'a> {
    async fn create(
        client: &'a Client,
        run: Id,
        report: &mut Report,
        deadline: Instant,
    ) -> Result<Self> {
        let topic = tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(client.admin().create_topic(&format!("deploy-{run}"), 1))
            .await?;
        let topic = Id::from_slice(&topic.topic_id)?;
        report.topic_id = Some(topic.to_string());
        let subscription = tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(client.admin().create_subscription(
                topic,
                "validation",
                0,
                public::DeliveryMode::Parallel,
            ))
            .await?;
        let subscription = Id::from_slice(&subscription.subscription_id)?;
        report.subscription_id = Some(subscription.to_string());
        let producer = open(client, topic, deadline).await?;
        Ok(Self {
            client,
            topic,
            subscription,
            producer,
            consumer: None,
            session: None,
        })
    }

    async fn claim(
        &mut self,
        report: &mut Report,
        expected_h: u64,
        deadline: Instant,
    ) -> Result<()> {
        ensure!(
            self.consumer.is_none(),
            "Replacing the original consumer is forbidden"
        );
        self.consumer = Some(
            tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
                .scope(Consumer::claim(
                    self.client.clone(),
                    self.topic,
                    0,
                    self.subscription,
                    ConsumerConfig::default(),
                ))
                .await?,
        );
        let assignment = self.assignment(deadline).await?;
        ensure!(
            assignment.acknowledged_before == 0 && assignment.cold_before == expected_h,
            "Initial claim does not match the original ACK frontier and replay H"
        );
        self.session = Some(Id::from_slice(&assignment.session_id)?);
        report.initial_cold_before = Some(assignment.cold_before);
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
        report.batches.push(BatchEvidence::new(
            self.topic,
            &self.producer,
            first,
            &messages,
        )?);
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
        let receipt = batch.finish(&self.producer, evidence, deadline).await?;
        report
            .ledger
            .commit(receipt.first_offset, receipt.message_count)
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
            let Some(delivery) =
                tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
                    .scope(consumer.recv())
                    .await?
            else {
                let next = Instant::now()
                    .checked_add(Duration::from_millis(50))
                    .context("Receive delay overflow")?;
                tokio::time::sleep_until(next.min(deadline)).await;
                continue;
            };
            report
                .ledger
                .observe(delivery.offset(), delivery.key(), delivery.body())?;
            let result = ack(consumer, &delivery, report, deadline).await?;
            ensure!(
                result.accepted.checked_add(result.already_acknowledged) == Some(1),
                "ACK response must cover exactly the original delivery token"
            );
            report
                .ledger
                .ack(delivery.offset(), result.acknowledged_before)?;
        }
        Ok(())
    }

    async fn assignment(&self, deadline: Instant) -> Result<public::AssignmentResult> {
        tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(
                self.client
                    .admin()
                    .subscription_status(self.topic, 0, self.subscription),
            )
            .await?
            .assignment
            .context("The original subscription is unassigned")
    }

    async fn check_frontier(
        &self,
        target: u64,
        report: &mut Report,
        deadline: Instant,
    ) -> Result<()> {
        let assignment = self.assignment(deadline).await?;
        let session = self
            .session
            .context("Original logical consumer session is unavailable")?;
        ensure!(
            Id::from_slice(&assignment.session_id)? == session,
            "Logical consumer session was replaced"
        );
        ensure!(
            assignment.acknowledged_before == target && report.ledger.frontier == target,
            "Authoritative durable ACK frontier does not match the complete ledger"
        );
        ensure!(
            report.authoritative_frontiers.len() < 4,
            "Frontier evidence bound exceeded"
        );
        report.authoritative_frontiers.push(target);
        report.same_consumer_session_verified = true;
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
        output.write("waiting-fault.json", &serde_json::json!({"schema_version":1,"unix_ms":unix_ms()?,
            "topic_id":report.topic_id,"subscription_id":report.subscription_id,
            "partition_id":0,"owner_node":null,"owner_lookup":"public_sdk_does_not_expose_owner",
            "ack_frontier":report.ledger.frontier,"fault_batch_first_sequence":report.ledger.committed,
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
            self.check_frontier(report.ledger.committed, report, deadline)
                .await?;
        }
        output.absent("recovered").await?;
        report.mark("fault_observed")?;
        output.write("fault-observed.json", &serde_json::json!({"schema_version":1,"unix_ms":unix_ms()?,
            "require_unavailable":args.require_unavailable,"ack_frontier":report.ledger.frontier,
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
            self.check_frontier(report.ledger.committed, report, deadline)
                .await?;
        }
        report.mark("final_production")?;
        self.produce_until(report.ledger.expected, report, deadline)
            .await?;
        report.mark("final_consumption")?;
        self.consume_until(report.ledger.expected, report, deadline)
            .await?;
        self.check_frontier(report.ledger.expected, report, deadline)
            .await
    }
}

async fn open(client: &Client, topic: Id, deadline: Instant) -> Result<Producer> {
    let identity = Id::new_v7()?;
    match tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
        .scope(Producer::open(client.clone(), topic, 0, identity))
        .await
    {
        Ok(producer) => Ok(producer),
        Err(tidemq_sdk::Error::RegistrationUnknown {
            producer_id,
            registration_epoch,
            ..
        }) => {
            ensure!(
                producer_id == identity,
                "Unknown registration changed the original producer identity"
            );
            Ok(
                tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
                    .scope(Producer::open_at(
                        client.clone(),
                        topic,
                        0,
                        identity,
                        registration_epoch,
                    ))
                    .await?,
            )
        }
        Err(error) => Err(anyhow::Error::new(error)).context("Open the original producer"),
    }
}

async fn ack(
    consumer: &Consumer,
    delivery: &Delivery,
    report: &mut Report,
    deadline: Instant,
) -> Result<public::AckResult> {
    for attempt in 0..5 {
        match tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(consumer.ack(delivery))
            .await
        {
            Ok(result) => return Ok(result),
            Err(error) if error.outcome_unknown() && attempt < 4 => {
                report.ack_unknown_retries = report
                    .ack_unknown_retries
                    .checked_add(1)
                    .context("ACK retry counter overflow")?;
                let next = Instant::now()
                    .checked_add(Duration::from_millis(100))
                    .context("ACK delay overflow")?;
                tokio::time::sleep_until(next.min(deadline)).await;
            }
            Err(error) => {
                return Err(anyhow::Error::new(error)).context("ACK the original delivery token");
            }
        }
    }
    anyhow::bail!("Original ACK retry budget exhausted")
}
