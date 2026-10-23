use tidemq::{
    broker::{
        DataApplication, DataCommand, DataGroupConfig, DataRejection, DataResponse,
        ProducerCommand, WriterFence,
    },
    metadata::PartitionKey,
    ownership::{FenceCommand, Grant},
    producer::{AppendBatch, MessageData, ProducerLimits, RegisterProducer},
    replication::Application,
};
use tidemq_protocol::{Id, producer::canonical_produce_digest_parts};

fn id() -> Id {
    Id::new_v7().unwrap()
}

#[test]
fn seal_prevents_late_writes_but_replays_committed_results_after_restart() {
    let cluster = id();
    let partition = PartitionKey {
        topic_id: id(),
        partition: 0,
    };
    let mut data = DataApplication::default();
    assert_eq!(
        data.apply(DataCommand::Initialize(DataGroupConfig {
            cluster_id: cluster,
            group_id: 2,
            max_partitions: 8,
            max_hot_bytes: 32 * 1024 * 1024,
            max_hot_messages: 65536,
            producer: ProducerLimits::default(),
            subscription: Default::default(),
            retention: Default::default(),
        })),
        DataResponse::Initialized
    );
    let writer = WriterFence {
        node_id: 1,
        owner_epoch: 1,
    };
    let grant = Grant {
        cluster_id: cluster,
        partition,
        group_id: 2,
        operation_id: tidemq::metadata::grant_operation_id(cluster, partition, 1, 1, 0).unwrap(),
        epoch: 1,
        owner_node: 1,
        expected_route_generation: 0,
        metadata_revision: 1,
    };
    assert!(matches!(
        data.apply(DataCommand::InstallFence(FenceCommand { grant })),
        DataResponse::Fenced(_)
    ));
    assert!(matches!(
        data.apply(DataCommand::Recover { partition, writer }),
        DataResponse::Recovered(_)
    ));
    let producer = id();
    let DataResponse::Registered(session) = data.apply(DataCommand::Producer {
        partition,
        writer,
        now_ms: 1000,
        command: ProducerCommand::Register(RegisterProducer {
            producer_id: producer,
            expected_registration_epoch: 1,
        }),
    }) else {
        panic!("Expected a registered producer");
    };
    let messages = vec![MessageData {
        key: "key".into(),
        body: "body".into(),
    }];
    let content_digest = canonical_produce_digest_parts(
        partition.topic_id,
        partition.partition,
        producer,
        session.producer_epoch,
        0,
        messages
            .iter()
            .map(|message| (message.key.as_ref(), message.body.as_ref())),
    )
    .unwrap();
    let request = AppendBatch {
        topic_id: partition.topic_id,
        partition_id: partition.partition,
        producer_id: producer,
        producer_epoch: session.producer_epoch,
        first_sequence: 0,
        messages,
        content_digest,
    };
    assert!(matches!(
        data.apply(DataCommand::Producer {
            partition,
            writer,
            now_ms: 1001,
            command: ProducerCommand::Append(request.clone())
        }),
        DataResponse::Produced(_)
    ));
    let plan = id();
    let sealed = DataCommand::SealShard {
        partition,
        writer,
        plan,
    };
    assert_eq!(
        data.apply(sealed.clone()),
        DataResponse::ShardSealed {
            plan,
            committed_end: 1
        }
    );
    assert_eq!(
        data.apply(sealed),
        DataResponse::ShardSealed {
            plan,
            committed_end: 1
        }
    );
    data.validate().unwrap();
    let mut data: DataApplication =
        postcard::from_bytes(&postcard::to_stdvec(&data).unwrap()).unwrap();
    data.validate().unwrap();
    assert_eq!(data.shard_seal(partition), Some((plan, 1)));
    let duplicate = data.apply(DataCommand::Producer {
        partition,
        writer,
        now_ms: 1002,
        command: ProducerCommand::Append(request.clone()),
    });
    assert!(
        matches!(duplicate, DataResponse::Produced(result) if result.duplicate && result.first_offset == 0)
    );
    let mut next = request;
    next.first_sequence = 1;
    next.content_digest = canonical_produce_digest_parts(
        partition.topic_id,
        partition.partition,
        producer,
        session.producer_epoch,
        1,
        next.messages
            .iter()
            .map(|message| (message.key.as_ref(), message.body.as_ref())),
    )
    .unwrap();
    assert_eq!(
        data.apply(DataCommand::Producer {
            partition,
            writer,
            now_ms: 1003,
            command: ProducerCommand::Append(next)
        }),
        DataResponse::Rejected(DataRejection::ShardSealed)
    );
    assert_eq!(data.producer(partition).unwrap().committed_end(), 1);
    assert_eq!(
        data.apply(DataCommand::SealShard {
            partition,
            writer,
            plan: id()
        }),
        DataResponse::Rejected(DataRejection::ShardSealed)
    );
}
