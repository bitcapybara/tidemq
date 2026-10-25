use std::{io, net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Args as ClapArgs, Subcommand};
use serde_json::{Value, json};
use tidemq::{Error, Result};
use tidemq_sdk::{
    Admin, AdminOperationId, AuthorityTransferInfo, Client, ClusterNodeInfo, ClusterRole,
    Credentials, DataGroupInfo, NodeRegistration, ShardId, TlsTrust,
};
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(ClapArgs)]
pub(super) struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    ca: PathBuf,
    #[arg(long)]
    token_file: PathBuf,
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u16).range(1..=300))]
    timeout_seconds: u16,
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    Nodes,
    Groups,
    RegisterNode {
        #[arg(long)]
        node_id: u64,
        #[arg(long, value_parser = ["combined", "metadata", "storage", "broker", "gateway"])]
        role: String,
        #[arg(long)]
        zone: String,
        #[arg(long)]
        internal_endpoint: SocketAddr,
        #[arg(long)]
        certificate_name: String,
        #[arg(long)]
        certificate_der: PathBuf,
    },
    CreateGroup {
        #[arg(long)]
        group_id: u64,
        #[arg(long, value_delimiter = ',', required = true)]
        voters: Vec<u64>,
    },
    SetSchedulable {
        #[arg(long)]
        node_id: u64,
        #[arg(long, action = clap::ArgAction::Set, required = true)]
        enabled: bool,
    },
    ReplaceDataReplica {
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        removed: u64,
        #[arg(long)]
        added: u64,
    },
    ReplaceMetadataReplica {
        #[arg(long)]
        removed: u64,
        #[arg(long)]
        added: u64,
    },
    DrainNode {
        #[arg(long)]
        node_id: u64,
    },
    DrainStatus {
        #[arg(long)]
        node_id: u64,
    },
    RetireNode {
        #[arg(long)]
        node_id: u64,
    },
    Shards {
        topic: String,
    },
    SplitTopic {
        topic: String,
        #[arg(long)]
        at: u64,
    },
    MergeTopic {
        topic: String,
        #[arg(long)]
        boundary: u64,
    },
    TransferShard {
        shard: ShardId,
        #[arg(long)]
        target_group: u64,
    },
    TransferStatus {
        operation: AdminOperationId,
    },
}

pub(super) async fn run(args: Args) -> Result<()> {
    let client = Client::builder(
        args.endpoint,
        TlsTrust::ca_file(args.ca),
        Credentials::token_file(args.token_file),
    )
    .request_timeout(Duration::from_secs(u64::from(args.timeout_seconds)))
    .build()
    .await
    .map_err(sdk_error)?;
    let admin = client.admin();
    let result = execute(&admin, args.action).await;
    let pending = admin.pending_operations();
    let close = client.close().await;
    let pending = pending.map_err(sdk_error)?;
    let operations: Vec<_> = pending
        .iter()
        .map(|pending| pending.operation_id().to_string())
        .collect();
    let report = match &close {
        Ok(report) => report,
        Err(error) => error.report(),
    };
    let cleanup = json!({"local_cleanup_complete":report.local_cleanup_complete(),
        "unresolved_administrations":report.unresolved_administrations(),
        "unclaimed_results":report.unclaimed_results()});
    match result {
        Ok(value) => {
            super::print_json(&json!({"status":"completed","result":value,"close":cleanup}))?
        }
        Err(source) => {
            super::print_json(
                &json!({"status":if source.outcome_unknown() { "unresolved" } else { "rejected" },
                "operations":operations,"close":cleanup}),
            )?;
            return Err(sdk_error(source));
        }
    }
    close.map(|_| ()).map_err(|source| Error::Io {
        operation: "close administration client",
        path: PathBuf::from("cluster"),
        source: io::Error::other(source),
    })
}

async fn execute(admin: &Admin, action: Action) -> tidemq_sdk::Result<Value> {
    match action {
        Action::Nodes => Ok(Value::Array(admin.cluster_nodes().await?.into_iter().map(node).collect())),
        Action::Groups => Ok(Value::Array(admin.data_groups().await?.into_iter().map(group).collect())),
        Action::RegisterNode { node_id, role, zone, internal_endpoint, certificate_name, certificate_der } => {
            let der = certificate(&certificate_der).await?;
            Ok(node(admin.register_node(NodeRegistration { node_id, role: cluster_role(&role)?, zone,
                internal_endpoint, certificate_name, certificate_pin: NodeRegistration::certificate_pin(&der) }).await?))
        }
        Action::CreateGroup { group_id, voters } => Ok(group(admin.create_data_group(group_id, voters).await?)),
        Action::SetSchedulable { node_id, enabled } => Ok(node(admin.set_node_schedulable(node_id, enabled).await?)),
        Action::DrainNode { node_id } => Ok(node(admin.begin_node_drain(node_id).await?)),
        Action::RetireNode { node_id } => Ok(node(admin.retire_node(node_id).await?)),
        Action::DrainStatus { node_id } => {
            let status = admin.node_drain_status(node_id).await?;
            Ok(json!({"node_id":status.node_id,"owned_shards":status.owned_shards,"storage_groups":status.storage_groups,
                "pending_membership_changes":status.pending_membership_changes,"metadata_voter":status.metadata_voter,
                "retired":status.retired,"durable_state_drained":status.durable_state_drained()}))
        }
        Action::ReplaceDataReplica { group_id, removed, added } => Ok(group(admin.replace_data_replica(group_id, removed, added).await?)),
        Action::ReplaceMetadataReplica { removed, added } => {
            let membership = admin.replace_metadata_replica(removed, added).await?;
            Ok(json!({"previous_voters":membership.previous_voters,"voters":membership.voters,"membership_index":membership.membership_index}))
        }
        Action::Shards { topic } => Ok(Value::Array(admin.topic_shards(&topic).await?.into_iter().map(|shard|
            json!({"id":shard.id.to_string(),"topic":shard.topic.to_string(),"authority_group":shard.authority_group,"hash_start":shard.hash_start,
                "hash_end":shard.hash_end,"sealed":shard.sealed})).collect())),
        Action::SplitTopic { topic, at } => {
            let topic = admin.split_topic_at(&topic, at).await?;
            Ok(json!({"id":topic.id().to_string(),"name":topic.name(),"shard_count":topic.shard_count()}))
        }
        Action::MergeTopic { topic, boundary } => {
            let topic = admin.merge_topic_at(&topic, boundary).await?;
            Ok(json!({"id":topic.id().to_string(),"name":topic.name(),"shard_count":topic.shard_count()}))
        }
        Action::TransferShard { shard, target_group } => Ok(transfer(admin.transfer_shard(shard, target_group).await?)),
        Action::TransferStatus { operation } => Ok(admin.authority_transfer_status(operation).await?.map(transfer).unwrap_or(Value::Null)),
    }
}

fn node(node: ClusterNodeInfo) -> Value {
    let registration = node.registration;
    let pin: String = registration
        .certificate_pin
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    json!({"node_id":registration.node_id,"role":format!("{:?}", registration.role).to_ascii_lowercase(),
        "zone":registration.zone,"internal_endpoint":registration.internal_endpoint.to_string(),
        "certificate_name":registration.certificate_name,"certificate_pin":pin,"schedulable":node.schedulable,
        "retired":node.retired,"draining":node.draining,"generation":node.generation})
}

fn group(group: DataGroupInfo) -> Value {
    json!({"group_id":group.group_id,"generation":group.generation,"voters":group.voters})
}

fn transfer(info: AuthorityTransferInfo) -> Value {
    json!({"operation":info.operation.to_string(),"shard":info.shard.to_string(),"topic":info.topic.to_string(),
        "source_group":info.source_group,"target_group":info.target_group,"stage":format!("{:?}", info.stage)})
}

fn cluster_role(role: &str) -> tidemq_sdk::Result<ClusterRole> {
    Ok(match role {
        "combined" => ClusterRole::Combined,
        "metadata" => ClusterRole::Metadata,
        "storage" => ClusterRole::Storage,
        "broker" => ClusterRole::Broker,
        "gateway" => ClusterRole::Gateway,
        _ => {
            return Err(tidemq_sdk::Error::Configuration {
                reason: "node role is invalid",
            });
        }
    })
}

async fn certificate(path: &std::path::Path) -> tidemq_sdk::Result<Vec<u8>> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| tidemq_sdk::Error::Material { source })?;
    let mut der = Vec::new();
    bounded_certificate(file, &mut der).await?;
    Ok(der)
}

async fn bounded_certificate(
    file: impl AsyncRead + Unpin,
    der: &mut Vec<u8>,
) -> tidemq_sdk::Result<()> {
    file.take(65_537)
        .read_to_end(der)
        .await
        .map_err(|source| tidemq_sdk::Error::Material { source })?;
    if der.is_empty() || der.len() > 65_536 {
        return Err(tidemq_sdk::Error::Configuration {
            reason: "node leaf certificate DER must contain 1..=65536 bytes",
        });
    }
    Ok(())
}

fn sdk_error(source: tidemq_sdk::Error) -> Error {
    Error::Io {
        operation: "run cluster administration",
        path: PathBuf::from("cluster"),
        source: io::Error::other(source),
    }
}

#[cfg(test)]
#[path = "../../tests/cli_support/admin.rs"]
mod tests;
