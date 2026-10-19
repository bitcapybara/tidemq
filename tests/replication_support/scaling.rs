#![allow(dead_code)] // Shared real-QUIC scaling fixtures serve separate test binaries.
use crate::quic::{Cluster, GROUP};
use std::collections::BTreeSet;
use tidemq::metadata::{self, DeploymentMode, MetadataLimits, MetadataState, NodeRole, NodeSpec};
use tidemq::replication::rpc::GroupRole;
use tidemq_protocol::Id;

pub async fn metadata(cluster: Id) -> Cluster<MetadataState> {
    let meta =
        Cluster::<MetadataState>::with_identity(&[1, 2, 3], cluster, 77, GroupRole::Metadata).await;
    meta.initialize().await;
    let raft = &meta.node(1).raft;
    let write = |command: metadata::Command| async move {
        assert!(raft.client_write(command).await.unwrap().data.is_applied());
    };
    write(metadata::Command::bootstrap(
        Id::random().unwrap(),
        cluster,
        77,
        DeploymentMode::ThreeReplica,
        MetadataLimits::default(),
    ))
    .await;
    for id in 1..=4 {
        let revision = meta.node(1).state.application().await.revision();
        let spec = NodeSpec {
            node_id: id,
            zone: format!("zone-{}", if id == 4 { 3 } else { id }),
            endpoint: format!("127.0.0.1:{}", 7400 + id).parse().unwrap(),
            roles: if id < 4 {
                BTreeSet::from([NodeRole::Metadata, NodeRole::Storage, NodeRole::Broker])
            } else {
                BTreeSet::from([NodeRole::Storage, NodeRole::Broker])
            },
            schedulable: true,
        };
        write(metadata::Command::put_node(
            Id::random().unwrap(),
            revision,
            spec,
            0,
        ))
        .await;
    }
    let revision = meta.node(1).state.application().await.revision();
    write(metadata::Command::create_group(
        Id::random().unwrap(),
        revision,
        GROUP,
        BTreeSet::from([1, 2, 3]),
    ))
    .await;
    meta
}
