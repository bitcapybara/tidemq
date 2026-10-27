use std::sync::Arc;

use serde_json::{Value, json};
use snafu::Snafu;
use tokio::sync::Semaphore;

use super::{ComponentProbeConfig, MAX_BODY_BYTES, PROBE_TIMEOUT, links, observed_at_ms};

#[derive(Debug, Snafu)]
pub(super) enum ProbeError {
    #[snafu(display("The configured dependency could not be reached within the probe budget"))]
    Unreachable,
    #[snafu(display(
        "The configured dependency redirected its health request; redirects are disabled"
    ))]
    Redirect,
    #[snafu(display("The configured dependency rejected the read-only probe"))]
    Access,
    #[snafu(display("The configured health endpoint returned an unsuccessful HTTP status"))]
    Status,
    #[snafu(display("The configured dependency response exceeded the body budget"))]
    BodyLimit,
    #[snafu(display("The configured dependency response body could not be read"))]
    Body,
    #[snafu(display("The shared dependency probe capacity is busy; retry later"))]
    Capacity,
}

pub(super) async fn component(
    client: &reqwest::Client,
    permits: &Arc<Semaphore>,
    config: &ComponentProbeConfig,
) -> Value {
    let result = request(client, permits, &config.probe_url).await;
    let (health, detail, partial) = match result {
        Ok(_) => ("healthy", "The configured health endpoint returned HTTP success; this does not prove full service health.".to_owned(), false),
        Err(error) => {
            let health = if matches!(error, ProbeError::Status | ProbeError::Unreachable) { "unavailable" } else { "unknown" };
            (health, error.to_string(), !matches!(error, ProbeError::Status))
        }
    };
    json!({
        "id": config.id, "name": config.name, "health": health, "detail": detail,
        "instances": null, "registered": null, "ready": null, "links": links(config),
        "observedAtMs": observed_at_ms(), "source": format!("configured_health_endpoint:{}", config.id), "partial": partial,
    })
}

pub(super) async fn request(
    client: &reqwest::Client,
    permits: &Arc<Semaphore>,
    url: &str,
) -> Result<Vec<u8>, ProbeError> {
    let _permit = tokio::time::timeout(PROBE_TIMEOUT, permits.clone().acquire_owned())
        .await
        .map_err(|_| ProbeError::Capacity)?
        .map_err(|_| ProbeError::Capacity)?;
    let mut response = client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|_| ProbeError::Unreachable)?;
    if response.status().is_redirection() {
        return Err(ProbeError::Redirect);
    }
    if matches!(response.status().as_u16(), 401 | 403) {
        return Err(ProbeError::Access);
    }
    if !response.status().is_success() {
        return Err(ProbeError::Status);
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BODY_BYTES as u64)
    {
        return Err(ProbeError::BodyLimit);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| ProbeError::Body)? {
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > MAX_BODY_BYTES)
        {
            return Err(ProbeError::BodyLimit);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
