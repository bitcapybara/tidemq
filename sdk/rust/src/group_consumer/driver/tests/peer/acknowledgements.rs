use super::*;
use tidemq_protocol::common::{ErrorCode, ErrorReply, Outcome};

async fn delivery(core: &Arc<Core>) -> ReceivedMessage {
    let (id, read, permit) = scheduling::next_read(core, false).unwrap().unwrap();
    let result = poll_read(core, id, read.clone(), permit).await.unwrap();
    scheduling::finish_read(core, id, &read, Ok(result)).unwrap();
    core.try_receive().unwrap().unwrap()
}

fn refusal(code: ErrorCode, known: bool) -> ErrorReply {
    ErrorReply {
        code: code.into(),
        outcome: Outcome::Rejected.into(),
        retryable: true,
        original_outcome_known: known,
        message: "The original ACK received a server refusal".into(),
        ..Default::default()
    }
}

fn acknowledgement_frames(peer: &Peer) -> Vec<Frame> {
    peer.oracle
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter(|frame| frame.kind == i32::from(FrameKind::GroupAck))
        .cloned()
        .collect()
}

async fn close(peer: &Peer, core: &Arc<Core>) {
    core.stop.cancel();
    finish_joined(core).await;
    peer.client.close().await.unwrap();
}

#[tokio::test]
async fn a_retryable_ack_refusal_keeps_the_original_batch_pending_until_recovery() {
    for code in [
        ErrorCode::Fenced,
        ErrorCode::Unavailable,
        ErrorCode::QuotaExceeded,
    ] {
        let peer = peer().await;
        let core = core(&peer.client, 1, 256, 8 * 1024 * 1024);
        native_registered(&core).await;
        let message = delivery(&core).await;
        *peer.oracle.ack_error.lock().unwrap() = Some(refusal(code, false));
        let error = ack::start(&core, &message, None, false).await.unwrap_err();
        let Error::Acknowledgement { pending, .. } = error else {
            panic!("An ACK refusal did not retain its original recovery handle");
        };
        let before = pending.progress().unwrap();
        let original = acknowledgement_frames(&peer).first().unwrap().clone();
        let retired_before = peer
            .oracle
            .frames
            .lock()
            .unwrap()
            .iter()
            .any(|frame| frame.kind == i32::from(FrameKind::GroupRetireAck));
        *peer.oracle.ack_error.lock().unwrap() = None;
        let recovered = pending.retry().await;
        let replay = pending.retry().await;
        let after = pending.progress().unwrap();
        let requests = acknowledgement_frames(&peer);
        let pending_count = core.state().unwrap().pending.len();
        drop(message);
        close(&peer, &core).await;
        assert_eq!(before.pending, 1, "A transient refusal settled the batch");
        assert_eq!(before.rejected, 0);
        assert!(
            !retired_before,
            "A transient refusal retired the original ACK"
        );
        assert_eq!(recovered.unwrap(), ack::AckReceipt::Acknowledged);
        assert_eq!(replay.unwrap(), ack::AckReceipt::Acknowledged);
        assert_eq!(after.acknowledged, 1);
        assert_eq!((after.pending, after.rejected, pending_count), (0, 0, 0));
        assert!(requests.iter().all(|frame| {
            frame.operation_id == original.operation_id && frame.payload == original.payload
        }));
        assert_eq!(
            requests.len(),
            usize::try_from(peer.client.inner.config.max_reconnect_attempts).unwrap() + 1
        );
    }
}

#[tokio::test]
async fn known_ack_decisions_and_permission_denials_are_not_reopened_for_recovery() {
    for (code, known) in [
        (ErrorCode::Fenced, true),
        (ErrorCode::QuotaExceeded, true),
        (ErrorCode::Forbidden, false),
    ] {
        let peer = peer().await;
        let core = core(&peer.client, 1, 256, 8 * 1024 * 1024);
        native_registered(&core).await;
        let message = delivery(&core).await;
        *peer.oracle.ack_error.lock().unwrap() = Some(refusal(code, known));
        let error = ack::start(&core, &message, None, false).await.unwrap_err();
        let Error::Acknowledgement { pending, .. } = error else {
            panic!("A terminal ACK refusal did not expose its decision");
        };
        let before = pending.progress().unwrap();
        let request_count = acknowledgement_frames(&peer).len();
        *peer.oracle.ack_error.lock().unwrap() = None;
        let replay = pending.retry().await;
        let after = acknowledgement_frames(&peer).len();
        drop(message);
        close(&peer, &core).await;
        assert_eq!((before.rejected, before.pending), (1, 0));
        assert!(replay.is_err(), "A terminal ACK decision was resubmitted");
        assert_eq!(after, request_count);
    }
}

#[tokio::test]
async fn a_stale_retirement_error_keeps_the_known_ack_pending_without_resubmitting_it() {
    for code in [ErrorCode::StaleDelivery, ErrorCode::StaleGeneration] {
        let peer = peer().await;
        let core = core(&peer.client, 1, 256, 8 * 1024 * 1024);
        native_registered(&core).await;
        let consumer = QueueConsumer {
            owner: Arc::new(Owner {
                core: core.clone(),
                _client: peer.client.clone(),
            }),
        };
        let message = delivery(&core).await;
        let identity = message.message_id();
        *peer.oracle.retire_ack_error.lock().unwrap() = Some(ErrorReply {
            retryable: false,
            message: "Retirement was refused after the original ACK succeeded".into(),
            ..refusal(code, false)
        });
        let error = consumer.ack(&message).await.unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::StaleDelivery);
        assert!(error.outcome_unknown());
        let old_guard_discards = error.kind() == crate::ErrorKind::StaleDelivery;
        let corrected_guard_discards = old_guard_discards && !error.outcome_unknown();
        let pending = consumer.pending_acks().await.unwrap();
        assert_eq!(pending.len(), 1);
        let original = pending.first().unwrap();
        let before = original.progress().unwrap();
        assert_eq!(
            (before.acknowledged, before.pending, before.rejected),
            (1, 0, 0)
        );
        assert!(core.state().unwrap().ledger.is_empty());
        assert!(consumer.try_receive().unwrap().is_none());
        let requests_before = acknowledgement_frames(&peer);
        assert_eq!(requests_before.len(), 1);
        let recovered = original.retry().await.unwrap();
        let replayed = original.retry().await.unwrap();
        let after = original.progress().unwrap();
        assert!(consumer.pending_acks().await.unwrap().is_empty());
        let requests_after = acknowledgement_frames(&peer);
        let retirements: Vec<_> = peer
            .oracle
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame.kind == i32::from(FrameKind::GroupRetireAck))
            .cloned()
            .collect();
        drop(message);
        close(&peer, &core).await;
        assert!(old_guard_discards && !corrected_guard_discards);
        assert_eq!(recovered, ack::AckReceipt::Acknowledged);
        assert_eq!(replayed, ack::AckReceipt::Acknowledged);
        assert_eq!(
            (after.acknowledged, after.pending, after.rejected),
            (1, 0, 0)
        );
        assert_eq!(
            requests_after.len(),
            1,
            "Retirement recovery resubmitted a known ACK"
        );
        assert_eq!(retirements.len(), 2);
        let first = retirements.first().unwrap();
        let second = retirements.last().unwrap();
        assert_eq!(first.operation_id, second.operation_id);
        assert_eq!(first.payload, second.payload);
        assert_ne!(first.request_id, second.request_id);
        eprintln!(
            "ACK retirement classification ledger: code={code:?} identity={identity:?} kind=StaleDelivery unknown=true acknowledged=1 pending=0 old_guard_discards={old_guard_discards} corrected_guard_discards={corrected_guard_discards} retry=known group_ack=1 retire_ack=2"
        );
    }
}
