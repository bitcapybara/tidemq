use std::{env, error::Error, time::Duration};
use tidemq_sdk::{Client, Credentials, RequestContext, TlsTrust};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::builder(
        env::var("TIDEMQ_ENDPOINT").unwrap_or_else(|_| "localhost:7443".into()),
        TlsTrust::ca_file(env::var("TIDEMQ_CA_PEM")?),
        Credentials::token_env("TIDEMQ_TOKEN"),
    )
    .build()
    .await?;
    let consumer = client
        .new_queue_consumer(env::var("TIDEMQ_TOPIC")?, env::var("TIDEMQ_SUBSCRIPTION")?)
        .build()
        .await?;
    let count: usize = env::var("TIDEMQ_MESSAGE_COUNT")
        .unwrap_or_else(|_| "10".into())
        .parse()?;
    let context = RequestContext::with_timeout(Duration::from_secs(30))?;
    let mut handlers = JoinSet::new();
    for _ in 0..count {
        if handlers.len() >= 8
            && let Some(result) = handlers.join_next().await
        {
            result??;
        }
        let message = context.scope(consumer.receive()).await?;
        let consumer = consumer.clone();
        let context = context.clone();
        handlers.spawn(async move {
            // Replace this action with the application's idempotent business operation.
            println!(
                "Handled message {}, payload_bytes={}",
                message.message_id(),
                message.payload().len()
            );
            context.scope(consumer.ack(&message)).await.map(|_| ())
        });
    }
    while let Some(result) = handlers.join_next().await {
        result??;
    }
    consumer.close().await?;
    client.close().await?;
    Ok(())
}
