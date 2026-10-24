use std::{env, error::Error};
use tidemq_sdk::{Client, Credentials, Message, TlsTrust};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::builder(
        env::var("TIDEMQ_ENDPOINT").unwrap_or_else(|_| "localhost:7443".into()),
        TlsTrust::ca_file(env::var("TIDEMQ_CA_PEM")?),
        Credentials::token_env("TIDEMQ_TOKEN"),
    )
    .build()
    .await?;
    let producer = client
        .new_producer(env::var("TIDEMQ_TOPIC")?)
        .build()
        .await?;
    match producer
        .send(Message::new("example-message").key("example-key"))
        .await
    {
        Ok(receipt) => println!("Committed message {}", receipt.message_id()),
        Err(error) if error.outcome_unknown() => {
            for pending in producer.pending_sends().await? {
                let receipt = pending.retry().await?;
                println!("Recovered message {}", receipt.message_id());
            }
        }
        Err(error) => return Err(error.into()),
    }
    producer.close().await?;
    client.close().await?;
    Ok(())
}
