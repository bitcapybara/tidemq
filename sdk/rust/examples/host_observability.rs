use std::{error::Error, sync::Arc};
use tidemq_sdk::Metrics;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // The executable owns subscriber installation and can also choose its allocator.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .try_init()?;
    let metrics = Arc::new(Metrics::default());
    #[cfg(feature = "metrics")]
    {
        let mut registry = prometheus_client::registry::Registry::default();
        metrics.register(registry.sub_registry_with_prefix("my_queue_client"));
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &registry)?;
        println!("{text}");
    }
    tracing::info!(
        requests = metrics.snapshot().requests,
        "Host owns observability setup"
    );
    Ok(())
}
