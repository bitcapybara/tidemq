//! Exhaustive finite specification checks, not service integration tests.

mod models;

use std::collections::{BTreeSet, VecDeque};

use models::publication::{Command, Data, Metadata, Publication};
use models::subscription::Subscription;
use models::{Fault, Outcome};
use tidemq_protocol::state::DataBoundaries;

#[test]
fn publication_explores_cancel_authorize_publish_snapshot_and_lost_reply_interleavings() {
    let initial = Publication::default();
    let mut visited = BTreeSet::from([initial.clone()]);
    let mut pending = VecDeque::from([initial]);
    let mut transitions = 0;
    while let Some(state) = pending.pop_front() {
        for command in Command::ALL {
            for fault in Fault::ALL {
                let mut next = state.crash_and_restore();
                let outcome = next.apply(command, fault);
                next.assert_safe();
                if fault == Fault::NoQuorum {
                    assert_eq!(next, state, "no quorum cannot change durable state");
                    assert_ne!(outcome, Outcome::Durable);
                }
                if state.data == Data::Aborted {
                    assert_eq!(next.data, Data::Aborted, "aborted evidence is irreversible");
                }
                assert_eq!(next.crash_and_restore(), next);
                transitions += 1;
                if visited.insert(next.clone()) {
                    pending.push_back(next);
                }
            }
        }
    }
    assert!(visited.iter().any(|s| s.data == Data::Completed));
    assert!(visited.iter().any(|s| !s.object_exists));
    assert!(
        visited
            .iter()
            .any(|s| s.metadata == Metadata::Aborted && s.snapshot_pin)
    );
    assert!(
        transitions >= 300,
        "all fault modes and enabled commands are explored"
    );
    println!(
        "publication model: {} states, {transitions} transitions",
        visited.len()
    );
}

#[test]
fn cancellation_is_not_abort_and_old_snapshot_protects_until_release() {
    let mut state = Publication::default();
    for command in [
        Command::Register,
        Command::CaptureSnapshot,
        Command::RequestCancel,
    ] {
        assert_eq!(state.apply(command, Fault::Healthy), Outcome::Durable);
    }
    assert_eq!(
        state.apply(Command::Collect, Fault::Healthy),
        Outcome::Rejected
    );
    assert_eq!(
        state.apply(Command::Authorize, Fault::ReplyLost),
        Outcome::Unknown
    );
    assert_eq!(state.data, Data::Authorized);
    assert_eq!(
        state.apply(Command::Publish, Fault::Healthy),
        Outcome::Rejected
    );
    assert_eq!(
        state.apply(Command::Abort, Fault::ReplyLost),
        Outcome::Unknown
    );
    state = state.crash_and_restore();
    assert_eq!(
        state.apply(Command::Authorize, Fault::Healthy),
        Outcome::Rejected
    );
    assert_eq!(
        state.apply(Command::FinishCancel, Fault::Healthy),
        Outcome::Durable
    );
    assert_eq!(
        state.apply(Command::Collect, Fault::Healthy),
        Outcome::Rejected
    );
    state.apply(Command::ReleaseSnapshot, Fault::Healthy);
    assert_eq!(
        state.apply(Command::Collect, Fault::Healthy),
        Outcome::Durable
    );
    state.assert_safe();
}

#[test]
fn durable_ack_rejects_old_attempt_assignment_and_seek_generation_in_every_reachable_state() {
    let initial = Subscription::default();
    let mut visited = BTreeSet::from([initial.clone()]);
    let mut pending = VecDeque::from([initial]);
    let mut rejected_stale = 0;
    while let Some(state) = pending.pop_front() {
        let mut successors = Vec::new();
        if state.delayed.is_none() && state.token().is_some() {
            let mut remembered = state.clone();
            remembered.delayed = state.token();
            successors.push(remembered);
        }
        for fault in Fault::ALL {
            for action in [
                Subscription::deliver,
                Subscription::seek,
                Subscription::reassign,
            ] {
                let mut next = state.clone();
                let outcome = action(&mut next, fault);
                assert_fault_boundary(&state, &next, fault, outcome);
                successors.push(next);
            }
            for token in [state.token(), state.delayed].into_iter().flatten() {
                let mut next = state.clone();
                let outcome = next.acknowledge(token, fault);
                assert_fault_boundary(&state, &next, fault, outcome);
                if state.token() != Some(token) {
                    assert_ne!(outcome, Outcome::Durable, "a stale token cannot commit ACK");
                    assert_eq!(next, state, "a stale token cannot mutate the new delivery");
                    rejected_stale += 1;
                }
                successors.push(next);
            }
        }
        for next in successors {
            if visited.insert(next.clone()) {
                pending.push_back(next);
            }
        }
    }
    assert!(visited.iter().any(|s| s.acknowledged));
    assert!(rejected_stale > 100);
    println!(
        "subscription model: {} states, {rejected_stale} stale attempts",
        visited.len()
    );
}

fn assert_fault_boundary(
    before: &Subscription,
    after: &Subscription,
    fault: Fault,
    outcome: Outcome,
) {
    if fault == Fault::NoQuorum {
        assert_eq!(after, before);
        assert_ne!(outcome, Outcome::Durable);
    }
    if fault == Fault::ReplyLost {
        assert_ne!(outcome, Outcome::Durable);
    }
}

#[test]
fn real_boundaries_preserve_four_distinct_completion_points_and_independent_retention() {
    for committed in 0..=4 {
        for published in 0..=4 {
            for reclaimed in 0..=4 {
                for retained in 0..=4 {
                    let actual = DataBoundaries {
                        committed_end: committed,
                        object_published_end: published,
                        local_reclaim_end: reclaimed,
                        retained_floor: retained,
                    };
                    assert_eq!(
                        actual.validate().is_ok(),
                        reclaimed <= published && published <= committed && retained <= committed,
                        "exclusive data boundaries must follow their own authorities"
                    );
                }
            }
        }
    }
    let produce_only = DataBoundaries {
        committed_end: 4,
        ..DataBoundaries::default()
    };
    assert!(produce_only.validate().is_ok());
    // Consumption completion is separate from publication and local reclamation.
    let mut ack = Subscription::default();
    ack.deliver(Fault::Healthy);
    assert_eq!(
        ack.acknowledge(ack.token().unwrap(), Fault::Healthy),
        Outcome::Durable
    );
    assert_eq!(produce_only.object_published_end, 0);
    assert_eq!(produce_only.local_reclaim_end, 0);
}
