use std::{env, error::Error, time::Duration};
use tidemq_sdk::{Client, ClientConfig, Consumer, ConsumerConfig, DeliveryOrdering, Id};
use tokio::{task::JoinSet, time::Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = env::var("TIDEMQ_ENDPOINT").unwrap_or_else(|_| "localhost:7443".into());
    let roots = tokio::fs::read(env::var("TIDEMQ_CA_PEM")?).await?;
    let client = Client::connect(
        ClientConfig::new(endpoint),
        roots.into(),
        env::var("TIDEMQ_TOKEN")?,
    )
    .await?;
    let ordering = match env::var("TIDEMQ_ORDERING").as_deref() {
        Ok("key") => DeliveryOrdering::KeySerial,
        _ => DeliveryOrdering::Parallel,
    };
    let topic: Id = env::var("TIDEMQ_TOPIC_ID")?.parse()?;
    let subscription: Id = env::var("TIDEMQ_SUBSCRIPTION_ID")?.parse()?;
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig {
            ordering,
            ..ConsumerConfig::default()
        },
        client.deadline()?,
    )
    .await?;
    let stop = client.deadline()?;
    let mut handlers = JoinSet::new();
    while Instant::now() < stop {
        if handlers.len() < 8
            && let Some(delivery) = consumer.recv(stop).await?
        {
            let consumer = consumer.clone();
            handlers.spawn(async move {
                // Replace this action with the application's idempotent business operation.
                println!(
                    "Handled offset {}, body_bytes={}",
                    delivery.offset(),
                    delivery.body().len()
                );
                consumer.ack(&delivery, stop).await.map(|_| ())
            });
        }
        if handlers.len() >= 8 {
            if let Some(result) = handlers.join_next().await {
                result??;
            }
        } else {
            while let Some(result) = handlers.try_join_next() {
                result??;
            }
            tokio::time::sleep_until(
                stop.min(
                    Instant::now()
                        .checked_add(Duration::from_millis(10))
                        .ok_or("deadline overflow")?,
                ),
            )
            .await;
        }
    }
    while let Some(result) = handlers.join_next().await {
        result??;
    }
    client.close(client.deadline()?).await;
    Ok(())
}
