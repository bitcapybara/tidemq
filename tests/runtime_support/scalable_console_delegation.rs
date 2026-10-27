use super::*;
use tidemq::transport::{
    Client as NativeClient, ClientCredentials, ClientTls, NodeGrant, PeerTarget, TlsIdentity,
    TransportConfig,
    rpc::{RpcClient, RpcConfig},
};
use tidemq_protocol::{
    ProtocolLimits,
    common::{ErrorCode, ErrorReply, Frame, FrameKind, TrafficClass},
    framing::{decode_frame, decode_payload, encode_payload, new_frame},
    internal::{InternalCommand, InternalResult},
    topic::{SendIdentity, message_digest},
};

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Delegation {
    #[prost(uint32, tag = "1")]
    kind: u32,
    #[prost(bytes = "bytes", tag = "2")]
    body: Bytes,
    #[prost(string, tag = "3")]
    principal: String,
    #[prost(string, tag = "4")]
    scope: String,
    #[prost(uint32, tag = "5")]
    hops: u32,
    #[prost(bytes = "bytes", tag = "6")]
    client_session: Bytes,
    #[prost(bytes = "bytes", tag = "7")]
    tenant: Bytes,
}

async fn metadata_rpc(fixture: &Fixture) -> (NativeClient, RpcClient) {
    let source = fixture.configs.get(&11).unwrap();
    let target = source.runtime.peer(31).unwrap();
    let cluster = source.cluster_id.parse().unwrap();
    let grant = NodeGrant {
        certificate_pin: NodeGrant::certificate_pin(
            &tokio::fs::read(&target.certificate_der).await.unwrap(),
        ),
        certificate_name: target.server_name.clone(),
        cluster_id: cluster,
        node_id: 31,
        role: "broker".into(),
        allowed_groups: vec![1, 2],
        allowed_classes: vec![TrafficClass::Produce],
    };
    let identity = TlsIdentity::new(
        tokio::fs::read(&source.tls.certificate)
            .await
            .unwrap()
            .into(),
        tokio::fs::read(&source.tls.private_key)
            .await
            .unwrap()
            .into(),
    )
    .unwrap();
    let client = NativeClient::bind(
        "127.0.0.1:0".parse().unwrap(),
        ClientTls {
            trust_roots: fixture.roots.clone(),
            identity: Some(identity),
            expected_nodes: vec![grant],
        },
        TransportConfig::default(),
        tidemq::budget::MemoryBudget::new(&tidemq::config::MemoryConfig::default()).unwrap(),
    )
    .unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    let session = client
        .connect(
            PeerTarget {
                address: target.internal_address,
                server_name: target.server_name.clone(),
                local_role: "metadata".into(),
                remote_role: "broker".into(),
                cluster_id: Some(cluster),
                node_id: Some(31),
            },
            &ClientCredentials::Internal {
                session_id: Id::new_v7().unwrap(),
                cluster_id: cluster,
                node_id: 11,
                role: "metadata".into(),
            },
            TrafficClass::Produce,
            until,
        )
        .await
        .unwrap();
    let rpc = RpcClient::from_session_with_lanes(session, RpcConfig::default(), 1, until)
        .await
        .unwrap();
    (client, rpc)
}

fn request(operation: Id, credential: Id, tenant: Id, publish: &public::Publish) -> Delegation {
    let limits = ProtocolLimits::default();
    let frame = new_frame(
        FrameKind::Publish,
        TrafficClass::Produce,
        Id::new_v7().unwrap(),
        operation,
        publish,
        10_000,
        &limits,
    )
    .unwrap();
    Delegation {
        kind: 3,
        body: encode_payload(&frame, limits.max_frame_bytes).unwrap(),
        principal: format!("credential:{credential}"),
        scope: "*".into(),
        hops: 1,
        client_session: Bytes::copy_from_slice(operation.as_bytes()),
        tenant: Bytes::copy_from_slice(tenant.as_bytes()),
    }
}

async fn call(rpc: &RpcClient, operation: Id, request: &Delegation) -> Frame {
    let limits = ProtocolLimits::default();
    let frame = new_frame(
        FrameKind::InternalCommand,
        TrafficClass::Produce,
        Id::new_v7().unwrap(),
        operation,
        &InternalCommand {
            group_id: 2,
            operation_id: Bytes::copy_from_slice(operation.as_bytes()),
            encoded_command: encode_payload(request, limits.max_frame_bytes).unwrap(),
        },
        10_000,
        &limits,
    )
    .unwrap();
    rpc.call(frame, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap()
        .frame()
        .clone()
}

fn forbidden(response: Frame, name: &str) {
    assert_eq!(
        response.kind,
        i32::from(FrameKind::Error),
        "A changed {name} was accepted"
    );
    let error: ErrorReply = decode_payload(response.payload, &ProtocolLimits::default()).unwrap();
    assert_eq!(
        error.code,
        i32::from(ErrorCode::Forbidden),
        "A changed {name} was not refused at the authorization boundary"
    );
}

pub(super) async fn assert_bindings(
    fixture: &Fixture,
    root: &Client,
    operation: Id,
    token: &AccessToken,
    tenant: tidemq_sdk::TenantId,
    principal: PrincipalId,
) -> Delegation {
    let topic: Id = root
        .admin()
        .topic_info("acme/app/recovery")
        .await
        .unwrap()
        .id()
        .into();
    let Reply::Topic(detail) = root.console_read(Query::Topic { topic }).await.unwrap() else {
        panic!("Expected the original shard identity");
    };
    let shard = detail.shard_details.first().unwrap().id;
    let Reply::Producers(producers) = root
        .console_read(Query::Producers {
            topic,
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected the Console publisher identity");
    };
    let registration = producers
        .items
        .iter()
        .find(|producer| producer.id == operation)
        .unwrap()
        .registration
        .0;
    let message = public::PublishMessage {
        key: Some(Bytes::from_static(b"recovery-key")),
        payload: Bytes::from_static(b"original-body"),
        properties: BTreeMap::from([("source".into(), Bytes::from_static(b"lost-http-receipt"))]),
    };
    let identity = SendIdentity {
        topic,
        producer: operation,
        registration,
        sequence: 1,
    };
    let mut publish = public::Publish {
        topic_id: Bytes::copy_from_slice(topic.as_bytes()),
        shard_id: Bytes::copy_from_slice(shard.as_bytes()),
        partition_id: 0,
        producer_id: Bytes::copy_from_slice(operation.as_bytes()),
        registration,
        sequence: 1,
        retired_before: 1,
        digest: Bytes::copy_from_slice(
            &message_digest(
                identity,
                message.key.as_deref(),
                &message.payload,
                &message.properties,
            )
            .unwrap(),
        ),
        message: Some(message),
    };
    let base = request(operation, token.id().into(), tenant.into(), &publish);
    let (client, rpc) = metadata_rpc(fixture).await;
    let limits = ProtocolLimits::default();
    let valid = call(&rpc, operation, &base).await;
    assert_eq!(valid.kind, i32::from(FrameKind::InternalResult));
    let result: InternalResult = decode_payload(valid.payload, &limits).unwrap();
    let inner = decode_frame(result.encoded_result, TrafficClass::Produce, &limits).unwrap();
    let receipt: public::PublishResult = decode_payload(inner.payload, &limits).unwrap();
    assert_eq!(receipt.offset, Some(1));
    let other = root
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    let mut client_changed = base.clone();
    client_changed.client_session = Bytes::copy_from_slice(Id::new_v7().unwrap().as_bytes());
    let mut credential_changed = base.clone();
    credential_changed.principal = format!("credential:{}", other.id());
    let mut tenant_changed = base.clone();
    tenant_changed.tenant = Bytes::copy_from_slice(Id::new_v7().unwrap().as_bytes());
    let mut hop_changed = base.clone();
    hop_changed.hops = 2;
    let mut scope_changed = base.clone();
    scope_changed.scope = "other".into();
    let message = publish.message.as_mut().unwrap();
    message.payload = Bytes::from_static(b"forged-body");
    publish.digest = Bytes::copy_from_slice(
        &message_digest(
            identity,
            message.key.as_deref(),
            &message.payload,
            &message.properties,
        )
        .unwrap(),
    );
    let digest_changed = request(operation, token.id().into(), tenant.into(), &publish);
    for (name, outer_operation, delegated) in [
        ("client", operation, client_changed),
        ("credential", operation, credential_changed),
        ("tenant", operation, tenant_changed),
        ("hop", operation, hop_changed),
        ("scope", operation, scope_changed),
        ("digest", operation, digest_changed),
        ("operation", Id::new_v7().unwrap(), base.clone()),
    ] {
        forbidden(call(&rpc, outer_operation, &delegated).await, name);
    }
    rpc.shutdown(Instant::now() + Duration::from_secs(5)).await;
    client
        .close(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    eprintln!("Metadata Console delegation refused seven forged bindings");
    base
}

pub(super) async fn assert_revoked(fixture: &Fixture, operation: Id, request: &Delegation) {
    let (client, rpc) = metadata_rpc(fixture).await;
    forbidden(call(&rpc, operation, request).await, "revoked credential");
    rpc.shutdown(Instant::now() + Duration::from_secs(5)).await;
    client
        .close(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
