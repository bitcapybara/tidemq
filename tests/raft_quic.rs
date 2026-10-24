#[path = "replication_support/quic.rs"]
mod quic;
#[path = "replication_support/mod.rs"]
mod support;

use bytes::Bytes;
use openraft::BasicNode;
use quic::{Cluster, GROUP, WAIT};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::time::Duration;
use support::{Command, Response};
use tidemq::transport::{Authorization, NodeGrant};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{FrameKind, TrafficClass},
    framing::new_frame,
    internal::RaftRequest,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_quic_rf3_rejects_isolated_writes_and_recovers_from_disk() {
    if std::env::var_os("TIDEMQ_TEST_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("tidemq=debug,openraft=warn")
            .with_test_writer()
            .try_init();
    }
    let mut cluster = Cluster::new(&[1, 2, 3]).await;
    cluster.initialize().await;
    let first = cluster
        .node(1)
        .raft
        .client_write(Command::Append {
            epoch: 0,
            request: 1,
            value: 11,
        })
        .await
        .unwrap();
    assert_eq!(first.data, Response::Appended { offset: 0 });
    cluster.applied(&[1, 2, 3], first.log_id.index).await;
    cluster.isolated.write().await.insert(1);
    let unknown = tokio::time::timeout(
        Duration::from_millis(300),
        cluster.node(1).raft.client_write(Command::Append {
            epoch: 0,
            request: 2,
            value: 22,
        }),
    )
    .await;
    assert!(!matches!(unknown, Ok(Ok(_))));
    assert_eq!(cluster.node(1).state.application().await.next_offset, 1);
    let leader = tokio::time::timeout(WAIT, async {
        loop {
            let left = cluster.node(2).raft.metrics().borrow().current_leader;
            let right = cluster.node(3).raft.metrics().borrow().current_leader;
            if left == right && matches!(left, Some(2 | 3)) {
                return left.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let second = cluster
        .node(leader)
        .raft
        .client_write(Command::Append {
            epoch: 0,
            request: 3,
            value: 33,
        })
        .await
        .unwrap();
    assert_eq!(second.data, Response::Appended { offset: 1 });
    cluster.applied(&[2, 3], second.log_id.index).await;
    cluster.stop(1).await;
    cluster.isolated.write().await.remove(&1);
    cluster.start(1).await;
    cluster.applied(&[1], second.log_id.index).await;
    assert_eq!(
        cluster.node(1).state.application().await,
        cluster.node(2).state.application().await
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_quic_bulk_snapshot_and_lost_replies_preserve_original_offsets() {
    let cluster = Cluster::new(&[1, 2, 3, 4]).await;
    cluster.initialize().await;
    cluster.lose_append_reply.store(true, Ordering::Release);
    let command = Command::Append {
        epoch: 0,
        request: 7,
        value: 77,
    };
    let unknown = tokio::time::timeout(
        Duration::from_millis(250),
        cluster.node(1).raft.client_write(command.clone()),
    )
    .await;
    assert!(!matches!(unknown, Ok(Ok(_))));
    cluster.lose_append_reply.store(false, Ordering::Release);
    let retry = tokio::time::timeout(WAIT, cluster.node(1).raft.client_write(command))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.data, Response::Appended { offset: 0 });
    cluster.applied(&[1, 2, 3], retry.log_id.index).await;
    cluster.node(1).raft.trigger().snapshot().await.unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(WAIT))
        .snapshot(retry.log_id, "Snapshot is durable before QUIC transfer")
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .trigger()
        .purge_log(retry.log_id.index)
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(WAIT))
        .purged(Some(retry.log_id), "Only snapshotted logs are purged")
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .add_learner(4, BasicNode::new("configured-node-4"), true)
        .await
        .unwrap();
    let changed = cluster
        .node(1)
        .raft
        .change_membership(BTreeSet::from([1, 2, 4]), false)
        .await
        .unwrap();
    cluster.applied(&[1, 2, 4], changed.log_id.index).await;
    assert_eq!(
        cluster.node(4).state.application().await,
        cluster.node(1).state.application().await
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_raft_rpc_rejects_forged_sender_group_and_nonempty_control_body() {
    let cluster = Cluster::new(&[1, 2, 3]).await;
    let service = cluster
        .services
        .get(&2)
        .unwrap()
        .current
        .read()
        .await
        .clone()
        .unwrap();
    let id = Id::new_v7().unwrap();
    let peer = Authorization::Internal(
        (NodeGrant {
            certificate_pin: [1; 32],
            certificate_name: "node-1.local".into(),
            cluster_id: cluster.cluster,
            node_id: 1,
            role: "storage".into(),
            allowed_groups: vec![GROUP],
            allowed_classes: vec![TrafficClass::Control],
        })
        .into(),
    );
    let make = |group, sender, entries| {
        let rpc = openraft::raft::AppendEntriesRequest::<support::Config> {
            vote: openraft::Vote::new_committed(5, sender),
            prev_log_id: None,
            entries,
            leader_commit: None,
        };
        let mut encoded = Vec::new();
        postcard::to_io(&rpc, &mut encoded).unwrap();
        new_frame(
            FrameKind::RaftAppend,
            TrafficClass::Control,
            id,
            id,
            &RaftRequest {
                group_id: group,
                rpc_id: Bytes::copy_from_slice(id.as_bytes()),
                encoded_rpc: Bytes::from(encoded),
            },
            1000,
            &ProtocolLimits::default(),
        )
        .unwrap()
    };
    assert!(
        service
            .handle(&peer, &make(GROUP + 1, 1, vec![]))
            .await
            .is_err()
    );
    assert!(
        service
            .handle(&peer, &make(GROUP, 3, vec![]))
            .await
            .is_err()
    );
    assert!(
        service
            .handle(
                &peer,
                &make(GROUP, 1, vec![support::entry(5, 0, Command::Fence(1))])
            )
            .await
            .is_err()
    );
    assert!(
        service
            .handle(&Authorization::Server, &make(GROUP, 1, vec![]))
            .await
            .is_err()
    );
    assert_eq!(cluster.node(2).state.application().await.next_offset, 0);
    cluster.shutdown().await;
}
