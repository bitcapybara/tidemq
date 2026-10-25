use super::*;
use tidemq::transport::{CredentialGrant, TokenAuthenticator};
use tidemq_protocol::common::{ErrorCode, ErrorReply};

const SECOND_TOKEN: &str = "second-credential-in-the-same-tenant-with-32-characters";
const OTHER_TOKEN: &str = "other-tenant-credential-with-at-least-32-characters";

struct Tenants;
impl TokenAuthenticator for Tenants {
    fn authenticate<'a>(
        &'a self,
        token: &'a str,
        _: Instant,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = tidemq::transport::Result<CredentialGrant>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let (credential, tenant) = match token {
                TOKEN => (1, 1),
                SECOND_TOKEN => (2, 1),
                OTHER_TOKEN => (3, 2),
                _ => {
                    return Err(tidemq::transport::Error::Authentication {
                        reason: "unknown test credential",
                    });
                }
            };
            Ok(CredentialGrant {
                credential: Id::from_slice(&[credential; 16]).unwrap(),
                tenant: Some(Id::from_slice(&[tenant; 16]).unwrap()),
                allowed_kinds: vec![FrameKind::Publish, FrameKind::GroupAck, FrameKind::Diagnose],
                lease: None,
            })
        })
    }
}

async fn rpc(fixture: &Fixture, token: &str, class: TrafficClass) -> RpcClient {
    let session = fixture
        .client
        .connect(
            fixture.target.clone(),
            &ClientCredentials::External {
                session_id: Id::new_v7().unwrap(),
                access_token: token.into(),
            },
            class,
            deadline(),
        )
        .await
        .unwrap();
    RpcClient::from_session(session, RpcConfig::default())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_tenant_cannot_fill_data_handlers_by_opening_another_credential_and_ack_stays_live() {
    let fixture = Fixture::start_with_authorization(
        DispatchConfig {
            data_handlers: 8,
            ..Default::default()
        },
        TransportConfig::default(),
        ServerAuthorization::Dynamic(Arc::new(Tenants)),
    )
    .await;
    let first = rpc(&fixture, TOKEN, TrafficClass::Produce).await;
    let same_tenant = rpc(&fixture, SECOND_TOKEN, TrafficClass::Produce).await;
    let other_tenant = rpc(&fixture, OTHER_TOKEN, TrafficClass::Control).await;
    let ack = rpc(&fixture, TOKEN, TrafficClass::Control).await;
    let publish = |sequence| {
        request(
            FrameKind::Publish,
            TrafficClass::Produce,
            &Publish {
                sequence,
                ..Default::default()
            },
        )
    };
    let one = first.submit(publish(1), deadline()).await.unwrap();
    let two = first.submit(publish(2), deadline()).await.unwrap();
    tokio::time::timeout_at(deadline(), async {
        while fixture.service.submitted.lock().unwrap().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let refusal = same_tenant.call(publish(3), deadline()).await.unwrap();
    assert_eq!(refusal.frame().kind, i32::from(FrameKind::Error));
    let error: ErrorReply =
        decode_payload(refusal.frame().payload.clone(), &ProtocolLimits::default()).unwrap();
    assert_eq!(error.code, i32::from(ErrorCode::QuotaExceeded));
    assert_eq!(
        fixture.service.submitted.lock().unwrap().as_slice(),
        &[1, 2]
    );
    let live = other_tenant
        .call(
            request(
                FrameKind::Diagnose,
                TrafficClass::Control,
                &LogicalTopic {
                    name: "other-tenant".into(),
                },
            ),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(live.frame().kind, i32::from(FrameKind::DiagnosticResult));
    let confirmed = ack
        .call(
            request(
                FrameKind::GroupAck,
                TrafficClass::Control,
                &GroupAck::default(),
            ),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(confirmed.frame().kind, i32::from(FrameKind::GroupAckResult));
    fixture.service.replies.add_permits(2);
    assert_eq!(
        one.receive().await.unwrap().frame().kind,
        i32::from(FrameKind::PublishResult)
    );
    assert_eq!(
        two.receive().await.unwrap().frame().kind,
        i32::from(FrameKind::PublishResult)
    );
    let reused = same_tenant.submit(publish(4), deadline()).await.unwrap();
    fixture.service.replies.add_permits(1);
    assert_eq!(
        reused.receive().await.unwrap().frame().kind,
        i32::from(FrameKind::PublishResult)
    );
    for rpc in [first, same_tenant, other_tenant, ack] {
        rpc.shutdown(deadline()).await;
    }
    fixture.client.close(deadline()).await.unwrap();
    fixture.shutdown.send(true).unwrap();
    fixture.task.await.unwrap().unwrap();
}
