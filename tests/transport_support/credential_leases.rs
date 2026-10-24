use super::*;

#[tokio::test]
async fn an_expired_connection_lease_rejects_frames_and_wakes_an_idle_connection() {
    let (validity, receiver) =
        tokio::sync::watch::channel(Instant::now() + std::time::Duration::from_secs(3600));
    let authorization = Authorization::Credential {
        credential: Id::new_v7().unwrap(),
        session_id: Id::new_v7().unwrap(),
        allowed_kinds: vec![FrameKind::GetTopic],
        lease: Some(receiver),
    };
    let frame = Frame {
        kind: FrameKind::GetTopic.into(),
        ..Default::default()
    };
    assert!(
        authorization
            .validate_frame(&frame, &ProtocolLimits::default())
            .is_ok()
    );
    let waiting = authorization.clone();
    let task = tokio::spawn(async move { credential_expired(&waiting).await });
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    validity.send(Instant::now()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        authorization.validate_frame(&frame, &ProtocolLimits::default()),
        Err(Error::Authentication { .. })
    ));
}
