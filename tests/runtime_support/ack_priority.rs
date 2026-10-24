use super::*;
use crate::runtime::wire;
use crate::transport::rpc::{RpcClient, RpcResponse};
use tidemq_protocol::framing::{encode_payload, new_frame};

async fn occupy_normal_requests(rpc: &RpcClient) -> Vec<RpcResponse> {
    let mut held = Vec::new();
    for _ in 0..256 {
        let operation = Id::new_v7().unwrap();
        let request = wire::Request::control(wire::LEADER, &()).unwrap();
        let payload = tidemq_protocol::internal::InternalCommand {
            group_id: 2,
            operation_id: Bytes::copy_from_slice(operation.as_bytes()),
            encoded_command: encode_payload(
                &request,
                ProtocolLimits::default().max_control_frame_bytes,
            )
            .unwrap(),
        };
        let frame = new_frame(
            FrameKind::InternalCommand,
            TrafficClass::Control,
            Id::new_v7().unwrap(),
            operation,
            &payload,
            5000,
            &ProtocolLimits::default(),
        )
        .unwrap();
        match rpc.submit(frame, deadline()).await {
            Ok(response) => held.push(response),
            Err(crate::transport::rpc::Error::Admission { .. }) => break,
            Err(error) => panic!("Normal control reservation failed: {error}"),
        }
    }
    assert!(!held.is_empty() && held.len() < 256);
    held
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_ack_keeps_authority_queries_and_confirmation_live_under_control_pressure() {
    let mut fixture = Fixture::new(
        &[
            (1, Role::Metadata),
            (2, Role::Storage),
            (3, Role::Broker),
            (4, Role::Gateway),
        ],
        &[1],
        &[2],
    )
    .await;
    fixture.start_all().await;
    let client = fixture.client(4).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = "test/app/ack-priority";
    client.admin().create_topic(topic).await.unwrap();
    let queue = client
        .new_queue_consumer(topic, "jobs")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(topic).build().await.unwrap();
    let sent = producer.send("priority").await.unwrap();
    let message = queue.receive().await.unwrap();
    assert_eq!(message.message_id(), sent.message_id());
    let gateway = &fixture.running.get(&4).unwrap().service;
    let broker = &fixture.running.get(&3).unwrap().service;
    let first = gateway
        .rpc
        .pool
        .connection(3, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let second = broker
        .rpc
        .pool
        .connection(2, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let held_first = occupy_normal_requests(&first).await;
    let held_second = occupy_normal_requests(&second).await;
    for service in [gateway, broker] {
        service.routes.write().await.clear();
        service.leaders.write().await.clear();
    }
    tokio::time::timeout(Duration::from_secs(5), queue.ack(&message))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.lane_count(), 4);
    assert_eq!(second.lane_count(), 4);
    drop((held_first, held_second));
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
