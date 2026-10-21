use std::mem::size_of;

use bytes::Bytes;
use tidemq_protocol::{Id, common::TrafficClass};

use crate::budget::MemoryBudget;
use crate::config::MemoryConfig;
use crate::metadata::PartitionKey;
use crate::ownership::ControlLease;
use crate::producer::{AppendBatch, MessageData, RegisterProducer};

use super::super::{Broker, BrokerLimits, Error, ProducerMetrics};
use super::support::{Cluster, context, lease};

fn broker(cluster: &Cluster, memory: &MemoryBudget, limits: BrokerLimits) -> Broker {
    let node = cluster.node(1);
    Broker::new(
        1,
        node.raft.clone(),
        node.state.clone(),
        cluster.config.clone(),
        memory.clone(),
        limits,
        ProducerMetrics::default(),
    )
    .unwrap()
}

fn available(memory: &MemoryBudget) -> [usize; 3] {
    [
        memory.available_bytes(),
        memory.available_data_bytes(),
        memory.available_normal_bytes(),
    ]
}

async fn seed(
    cluster: &Cluster,
    broker: &Broker,
    partition_number: u32,
    count: usize,
) -> (PartitionKey, ControlLease) {
    let partition = PartitionKey {
        topic_id: Id::random().unwrap(),
        partition: partition_number,
    };
    let grant = cluster.grant(partition, 1);
    cluster.fence_and_recover(&grant).await;
    let authorization = lease(&grant);
    let session = broker
        .register(
            &authorization,
            context(),
            partition,
            RegisterProducer {
                producer_id: Id::random().unwrap(),
                expected_registration_epoch: 1,
            },
        )
        .await
        .unwrap();
    let messages = vec![
        MessageData {
            key: Bytes::new(),
            body: Bytes::from_static(b"x"),
        };
        count
    ];
    let content_digest = tidemq_protocol::producer::canonical_produce_digest_parts(
        partition.topic_id,
        partition.partition,
        session.producer_id,
        session.producer_epoch,
        0,
        messages
            .iter()
            .map(|message| (message.key.as_ref(), message.body.as_ref())),
    )
    .unwrap();
    broker
        .produce(
            &authorization,
            context(),
            AppendBatch {
                topic_id: partition.topic_id,
                partition_id: partition.partition,
                producer_id: session.producer_id,
                producer_epoch: session.producer_epoch,
                first_sequence: 0,
                messages,
                content_digest,
            },
        )
        .await
        .unwrap();
    (partition, authorization)
}

#[tokio::test]
async fn hot_read_admission_covers_allocated_message_metadata_until_drop() {
    let cluster = Cluster::new(&[1]).await;
    let memory = MemoryBudget::new(&MemoryConfig::default()).unwrap();
    let broker = broker(&cluster, &memory, BrokerLimits::default());
    let (partition, authorization) = seed(&cluster, &broker, 0, 1_024).await;

    for max_messages in [1, 2, 3, 4, 1_024] {
        let before = available(&memory);
        let page = broker
            .hot_read(&authorization, context(), partition, 0, max_messages, 1)
            .await
            .unwrap();
        let charged = before[0] - memory.available_bytes();
        let metadata = page.page().messages.capacity() * size_of::<MessageData>();
        eprintln!(
            "Hot read metadata: max_messages={max_messages}, messages={}, capacity={}, element_bytes={}, metadata_bytes={metadata}, charged_bytes={charged}",
            page.page().messages.len(),
            page.page().messages.capacity(),
            size_of::<MessageData>()
        );
        assert_eq!(page.page().messages.len(), 1);
        assert!(
            charged >= metadata,
            "Hot read allocated {metadata} metadata bytes but only charged {charged} bytes"
        );
        drop(page);
        assert_eq!(available(&memory), before);
    }
    drop(broker);
    cluster.shutdown().await;
}

#[tokio::test]
async fn four_default_partition_pages_fit_and_releasing_a_page_restores_admission() {
    let cluster = Cluster::new(&[1]).await;
    let memory = MemoryBudget::new(&MemoryConfig::default()).unwrap();
    let broker = broker(&cluster, &memory, BrokerLimits::default());
    let mut partitions = Vec::new();
    for partition in 0..4 {
        partitions.push(seed(&cluster, &broker, partition, 256).await);
    }
    let before = available(&memory);
    let mut pages = Vec::new();
    for (partition, authorization) in &partitions {
        let page = broker
            .hot_read(authorization, context(), *partition, 0, 256, 1_040_384)
            .await
            .unwrap();
        assert_eq!(page.page().messages.len(), 256);
        let metadata = page.page().messages.capacity() * size_of::<MessageData>();
        assert!(metadata <= 1_040_896);
        pages.push(page);
    }
    assert_eq!(before[0] - memory.available_bytes(), 4_163_584);
    let (partition, authorization) = partitions.first().unwrap();
    let occupied = available(&memory);
    assert!(matches!(
        broker
            .hot_read(authorization, context(), *partition, 0, 256, 1_040_384)
            .await,
        Err(Error::Capacity)
    ));
    assert_eq!(available(&memory), occupied);
    drop(pages.pop());
    let replacement = broker
        .hot_read(authorization, context(), *partition, 0, 256, 1_040_384)
        .await
        .unwrap();
    assert_eq!(available(&memory), occupied);
    eprintln!(
        "Four default partition pages: charged_bytes={}, fifth_read=Capacity, release_and_reacquire=success",
        before[0] - memory.available_bytes()
    );
    drop(replacement);
    drop(pages);
    assert_eq!(available(&memory), before);
    drop(broker);
    cluster.shutdown().await;
}

#[tokio::test]
async fn overflowing_read_bounds_are_rejected_without_charging_memory() {
    let cluster = Cluster::new(&[1]).await;
    let memory = MemoryBudget::new(&MemoryConfig::default()).unwrap();
    let broker = broker(&cluster, &memory, BrokerLimits::default());
    let (partition, authorization) = seed(&cluster, &broker, 0, 1).await;
    let before = available(&memory);
    let too_many_metadata_bytes = u32::MAX as usize / size_of::<MessageData>() + 1;
    for (messages, bytes) in [(usize::MAX, 1), (too_many_metadata_bytes, 1), (1, u32::MAX)] {
        assert!(matches!(
            broker
                .hot_read(&authorization, context(), partition, 0, messages, bytes)
                .await,
            Err(Error::InvalidRequest)
        ));
        assert_eq!(available(&memory), before);
    }
    let page = broker
        .hot_read(&authorization, context(), partition, 0, 1, 1)
        .await
        .unwrap();
    drop(page);
    assert_eq!(available(&memory), before);
    drop(broker);
    cluster.shutdown().await;
}

#[tokio::test]
async fn failed_lane_and_memory_admission_release_all_read_permits() {
    let cluster = Cluster::new(&[1]).await;
    let memory = MemoryBudget::new(&MemoryConfig::default()).unwrap();
    let normal = broker(&cluster, &memory, BrokerLimits::default());
    let (partition, authorization) = seed(&cluster, &normal, 0, 1_024).await;
    let limited = broker(
        &cluster,
        &memory,
        BrokerLimits {
            read_operations: 1,
            read_bytes: 1_024,
            ..BrokerLimits::default()
        },
    );
    let before = available(&memory);
    assert!(matches!(
        limited
            .hot_read(&authorization, context(), partition, 0, 1_024, 1)
            .await,
        Err(Error::Capacity)
    ));
    assert_eq!(available(&memory), before);
    let page = limited
        .hot_read(&authorization, context(), partition, 0, 1, 1)
        .await
        .unwrap();
    drop(page);
    assert_eq!(available(&memory), before);
    let memory_limited = broker(
        &cluster,
        &memory,
        BrokerLimits {
            read_operations: 1,
            ..BrokerLimits::default()
        },
    );
    let blocked_bytes = u32::try_from(memory.available_data_bytes() - 1_024).unwrap();
    let blocker = memory
        .try_reserve(TrafficClass::Delivery, blocked_bytes)
        .unwrap();
    let blocked = available(&memory);
    assert!(matches!(
        memory_limited
            .hot_read(&authorization, context(), partition, 0, 1_024, 1)
            .await,
        Err(Error::Memory { .. })
    ));
    assert_eq!(available(&memory), blocked);
    drop(blocker);
    let page = memory_limited
        .hot_read(&authorization, context(), partition, 0, 1_024, 1)
        .await
        .unwrap();
    drop(page);
    assert_eq!(available(&memory), before);
    drop(limited);
    drop(memory_limited);
    drop(normal);
    cluster.shutdown().await;
}
