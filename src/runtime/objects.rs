use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant as MonoInstant;

use aws_sdk_s3::config::Credentials;
use bytes::Bytes;
use serde::Deserialize;
use tidemq_protocol::{Id, common::TrafficClass};

use crate::broker::DataApplication;
use crate::budget::MemoryBudget;
use crate::config::Config;
use crate::lifecycle::remote::{LifecycleRpc, RpcFuture};
use crate::lifecycle::{Coordinator, DataReplica, MetadataReplica, ReplicaCheckpoint};
use crate::metadata::{DeploymentMode, MetadataState};
use crate::object_store::{S3Config, S3Store};
use crate::ownership::remote::Authority;
use crate::ownership::{ControlLease, FenceRecord};

use super::node::Replica;
use super::wire::{PeerRpc, Request};
use super::{Error, Result};

struct RuntimeLifecycleRpc {
    rpc: PeerRpc,
    authority: Arc<Authority>,
}

impl LifecycleRpc for RuntimeLifecycleRpc {
    fn request(
        &self,
        target: u64,
        group: u64,
        class: TrafficClass,
        kind: u32,
        body: Bytes,
        deadline: MonoInstant,
    ) -> RpcFuture<'_, Bytes> {
        Box::pin(async move {
            let deadline = deadline.into();
            let operation = super::node::random_id().map_err(lifecycle_error)?;
            let request = Request {
                kind,
                body,
                ..Request::default()
            };
            self.rpc
                .submit(target, group, class, operation, request, deadline, group)
                .await
                .map_err(lifecycle_error)?
                .receive()
                .await
                .map_err(lifecycle_error)
        })
    }
    fn control_lease(
        &self,
        fence: FenceRecord,
        deadline: MonoInstant,
    ) -> RpcFuture<'_, ControlLease> {
        Box::pin(async move {
            self.authority
                .active_lease(&fence, deadline.into())
                .await
                .map_err(lifecycle_error)
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectCredentials {
    access_key: String,
    secret_key: String,
}

// Runtime assembly supplies the existing shared identities, transport, budget and metrics.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn coordinators(
    config: &Config,
    cluster: Id,
    metadata: Option<&Replica<MetadataState>>,
    data: &BTreeMap<u64, Replica<DataApplication>>,
    rpc: PeerRpc,
    authority: Arc<Authority>,
    budget: MemoryBudget,
    registry: &mut prometheus_client::registry::Registry,
) -> Result<BTreeMap<u64, Coordinator>> {
    if data.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Some((store, metrics)) = lifecycle_store(config, cluster, &budget, registry).await? else {
        return Ok(BTreeMap::new());
    };
    let (metadata, rpc) = if config.role == crate::config::Role::Combined {
        let metadata =
            metadata.ok_or_else(|| Error::invalid("Local lifecycle metadata group is absent"))?;
        if !metadata.group.is_local() || data.values().any(|replica| !replica.group.is_local()) {
            return Err(Error::invalid(
                "Local lifecycle requires local metadata and data groups",
            ));
        }
        (MetadataReplica::local(metadata.group.clone()), None)
    } else {
        let rpc: Arc<dyn LifecycleRpc> = Arc::new(RuntimeLifecycleRpc { rpc, authority });
        let metadata = MetadataReplica::remote(
            rpc.clone(),
            cluster,
            config.runtime.metadata_group,
            config.runtime.metadata_voters.clone(),
            if config.replication_factor == 1 {
                DeploymentMode::SeparateRf1
            } else {
                DeploymentMode::SeparateRf3
            },
        )
        .map_err(|source| Error::operation("create remote lifecycle metadata authority", source))?;
        (metadata, Some(rpc))
    };
    attach_coordinators(config, cluster, data, metadata, rpc, store, budget, metrics)
}

async fn lifecycle_store(
    config: &Config,
    cluster: Id,
    budget: &MemoryBudget,
    registry: &mut prometheus_client::registry::Registry,
) -> Result<Option<(S3Store, Arc<crate::lifecycle::LifecycleMetrics>)>> {
    let Some(objects) = &config.runtime.objects else {
        return Ok(None);
    };
    let bytes = super::read_secret(&objects.credentials_file, 16_384).await?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| Error::invalid("Object credentials file is not UTF-8"))?;
    let credentials: ObjectCredentials =
        toml::from_str(text).map_err(|_| Error::invalid("Object credentials file is malformed"))?;
    if !credentials
        .access_key
        .bytes()
        .all(|byte| byte.is_ascii_graphic())
        || !credentials
            .secret_key
            .bytes()
            .all(|byte| byte.is_ascii_graphic())
        || credentials.access_key.is_empty()
        || credentials.access_key.len() > 256
        || credentials.secret_key.is_empty()
        || credentials.secret_key.len() > 256
    {
        return Err(Error::invalid(
            "Object credentials are empty or exceed their finite bounds",
        ));
    }
    let store = S3Store::new(
        S3Config {
            endpoint: objects.endpoint.clone(),
            region: objects.region.clone(),
            bucket: objects.bucket.clone(),
            prefix: objects.prefix.clone(),
            cluster_id: cluster,
            credentials: Credentials::new(
                credentials.access_key,
                credentials.secret_key,
                None,
                None,
                "TideMqRuntime",
            ),
            allow_http_loopback: objects.allow_http_loopback,
        },
        crate::object_store::Limits::default(),
        budget.clone(),
    )
    .map_err(|source| Error::operation("create bounded S3 client", source))?;
    registry
        .sub_registry_with_prefix("tidemq_s3")
        .register_collector(Box::new(store.metrics()));
    let metrics = Arc::new(crate::lifecycle::LifecycleMetrics::default());
    metrics.register(registry);
    Ok(Some((store, metrics)))
}

// Both execution kinds use the same lifecycle worker and resource limits.
#[allow(clippy::too_many_arguments)]
fn attach_coordinators(
    config: &Config,
    cluster: Id,
    data: &BTreeMap<u64, Replica<DataApplication>>,
    metadata: MetadataReplica,
    rpc: Option<Arc<dyn LifecycleRpc>>,
    store: S3Store,
    budget: MemoryBudget,
    metrics: Arc<crate::lifecycle::LifecycleMetrics>,
) -> Result<BTreeMap<u64, Coordinator>> {
    let mut coordinators = BTreeMap::new();
    for (group, replica) in data {
        let local = DataReplica::local(replica.group.clone());
        let checkpoints = if let Some(rpc) = &rpc {
            let configured = config.runtime.group(*group)?;
            configured
                .voters
                .iter()
                .chain(&configured.standby)
                .map(|node| ReplicaCheckpoint::remote(cluster, *group, *node, rpc.clone()))
                .collect::<crate::lifecycle::Result<_>>()
                .map_err(|source| {
                    Error::operation("create trusted replica checkpoint candidates", source)
                })?
        } else {
            vec![ReplicaCheckpoint::local(local.clone())]
        };
        let coordinator = Coordinator::new(
            metadata.clone(),
            local.clone(),
            vec![local],
            store.clone(),
            budget.clone(),
        )
        .map_err(|source| Error::operation("create object lifecycle worker", source))?
        .with_checkpoint_candidates(checkpoints)
        .map_err(|source| Error::operation("bind lifecycle checkpoint voters", source))?
        .with_metrics(metrics.clone())
        .map_err(|source| Error::operation("bind shared lifecycle metrics", source))?;
        coordinators.insert(*group, coordinator);
    }
    Ok(coordinators)
}

fn lifecycle_error(error: Error) -> crate::lifecycle::Error {
    crate::lifecycle::Error::Consensus {
        reason: error.to_string(),
    }
}
