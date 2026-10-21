use std::{env, error::Error};
use tidemq_sdk::{Bytes, Client, ClientConfig, Id, Producer, public::Message};

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
    let topic: Id = env::var("TIDEMQ_TOPIC_ID")?.parse()?;
    let producer_id = Id::new_v7()?;
    let producer = Producer::open(client.clone(), topic, 0, producer_id).await?;
    let result = producer
        .produce(vec![Message {
            key: Bytes::from_static(b"example-key"),
            body: Bytes::from_static(b"example-message"),
        }])
        .await;
    match result {
        Ok(receipt) => println!("Committed offset {}", receipt.first_offset),
        Err(error) if error.outcome_unknown() => {
            for batch in producer.pending_batches().await? {
                let receipt = producer.retry(&batch).await?;
                println!(
                    "Recovered offset {}, duplicate={}",
                    receipt.first_offset, receipt.duplicate
                );
            }
        }
        Err(error) => return Err(error.into()),
    }
    producer.retire().await?;
    client.close().await;
    Ok(())
}
