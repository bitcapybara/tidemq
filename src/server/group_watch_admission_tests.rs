use super::*;
use crate::{
    budget::{MemoryBudget, MemoryLimits},
    server::group_watch::Budget,
};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::Duration,
};

type Request = Pin<Box<dyn Future<Output = Option<Turn>> + Send>>;

fn setup() -> (Budget, Queue, MemoryBudget) {
    let memory = MemoryBudget::new(MemoryLimits {
        total_bytes: 128 * 1024 * 1024,
        control_reserved_bytes: 16 * 1024 * 1024,
    })
    .unwrap();
    let budget = Budget::new(memory.clone());
    (budget, Queue::new(1, Gauge::default()), memory)
}

fn request(queue: &Queue, budget: &Budget, tenant: Tenant, member: Id) -> Request {
    let queue = queue.clone();
    let charge = budget.reserve(tenant, member, 0, 1_024, false).unwrap();
    Box::pin(async move {
        queue
            .enter(
                tenant,
                member,
                charge,
                Instant::now() + Duration::from_secs(2),
            )
            .await
    })
}

async fn arm(request: &mut Request) {
    poll_fn(|context| {
        assert!(
            request.as_mut().poll(context).is_pending(),
            "The request must remain queued behind the current work batch"
        );
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn watch_round_robin_rotates_tenants_and_members_instead_of_hot_request_count() {
    let (budget, queue, memory) = setup();
    let before = memory.available_bytes();
    let tenant_a = Some(Id::new_v7().unwrap());
    let tenant_b = Some(Id::new_v7().unwrap());
    let member_a = Id::new_v7().unwrap();
    let member_b = Id::new_v7().unwrap();
    let blocker = request(&queue, &budget, None, member_a).await.unwrap();
    let mut a1 = request(&queue, &budget, tenant_a, member_a);
    let mut a2 = request(&queue, &budget, tenant_a, member_a);
    let mut a3 = request(&queue, &budget, tenant_a, member_b);
    let mut b1 = request(&queue, &budget, tenant_b, member_a);
    let mut b2 = request(&queue, &budget, tenant_b, member_a);
    for pending in [&mut a1, &mut a2, &mut a3, &mut b1, &mut b2] {
        arm(pending).await;
    }
    assert_eq!(queue.0.state.lock().unwrap().queued(), 5);
    assert_eq!(before - memory.available_bytes(), 6 * 1_024);
    drop(blocker);
    let turn = a1.await.unwrap();
    for pending in [&mut a2, &mut a3, &mut b1, &mut b2] {
        arm(pending).await;
    }
    drop(turn);
    let turn = b1.await.unwrap();
    arm(&mut a3).await;
    drop(turn);
    let turn = a3.await.unwrap();
    arm(&mut b2).await;
    drop(turn);
    let turn = b2.await.unwrap();
    arm(&mut a2).await;
    drop(turn);
    drop(a2.await.unwrap());
    assert!(queue.0.state.lock().unwrap().entries.is_empty());
    assert_eq!(budget.diagnostic(), (0, 0, 0));
    assert_eq!(memory.available_bytes(), before);
}

#[tokio::test]
async fn watch_queue_cancellation_deadline_and_close_release_charges_and_do_not_grant_stale_turns()
{
    let (budget, queue, memory) = setup();
    let before = memory.available_bytes();
    let member = Id::new_v7().unwrap();
    let blocker = request(&queue, &budget, None, member).await.unwrap();
    let mut canceled = request(&queue, &budget, None, member);
    arm(&mut canceled).await;
    drop(canceled);
    assert_eq!(queue.0.state.lock().unwrap().queued(), 0);
    let expired = budget.reserve(None, member, 0, 1_024, false).unwrap();
    assert!(
        queue
            .enter(None, member, expired, Instant::now())
            .await
            .is_none()
    );
    assert_eq!(before - memory.available_bytes(), 1_024);
    let mut pending = request(&queue, &budget, None, member);
    arm(&mut pending).await;
    queue.close();
    assert!(pending.await.is_none());
    assert!(request(&queue, &budget, None, member).await.is_none());
    drop(blocker);
    let state = queue.0.state.lock().unwrap();
    assert!(state.entries.is_empty() && state.active.is_empty() && state.queues.is_empty());
    assert_eq!(state.tenants.capacity(), 0);
    assert_eq!(budget.diagnostic(), (0, 0, 0));
    assert_eq!(memory.available_bytes(), before);
}

#[tokio::test]
async fn watch_queue_member_tenant_and_node_limits_reject_without_unaccounted_waiters() {
    let (budget, queue, memory) = setup();
    let before = memory.available_bytes();
    let blocker = request(&queue, &budget, None, Id::new_v7().unwrap())
        .await
        .unwrap();
    let tenant = Some(Id::new_v7().unwrap());
    let member = Id::new_v7().unwrap();
    let mut pending = Vec::new();
    for _ in 0..8 {
        let mut next = request(&queue, &budget, tenant, member);
        arm(&mut next).await;
        pending.push(next);
    }
    assert!(request(&queue, &budget, tenant, member).await.is_none());
    drop(pending);
    let mut pending = Vec::new();
    for _ in 0..4 {
        let member = Id::new_v7().unwrap();
        for _ in 0..8 {
            let mut next = request(&queue, &budget, tenant, member);
            arm(&mut next).await;
            pending.push(next);
        }
    }
    assert!(
        request(&queue, &budget, tenant, Id::new_v7().unwrap())
            .await
            .is_none()
    );
    drop(pending);
    let mut pending = Vec::new();
    for count in [32, 32, 32, 31] {
        let tenant = Some(Id::new_v7().unwrap());
        let mut member = Id::new_v7().unwrap();
        for index in 0..count {
            if index % 8 == 0 {
                member = Id::new_v7().unwrap();
            }
            let mut next = request(&queue, &budget, tenant, member);
            arm(&mut next).await;
            pending.push(next);
        }
    }
    assert_eq!(queue.0.state.lock().unwrap().entries.len(), 128);
    assert!(
        request(
            &queue,
            &budget,
            Some(Id::new_v7().unwrap()),
            Id::new_v7().unwrap()
        )
        .await
        .is_none()
    );
    queue.close();
    drop((pending, blocker));
    assert_eq!(budget.diagnostic(), (0, 0, 0));
    assert_eq!(memory.available_bytes(), before);
}

#[test]
fn watch_authority_is_bounded_in_both_directions_without_borrowing_core_or_ack_memory() {
    let (budget, _, memory) = setup();
    let core = memory.available_core_bytes();
    let ack = memory.available_ack_bytes();
    let before = memory.available_bytes();
    let member = Id::new_v7().unwrap();
    let mut guards = Vec::new();
    for inbound in [false, true] {
        for _ in 0..8 {
            guards.push(budget.reserve_authority(None, member, inbound).unwrap());
        }
        assert!(budget.reserve_authority(None, member, inbound).is_none());
    }
    assert_eq!(memory.available_core_bytes(), core);
    assert_eq!(memory.available_ack_bytes(), ack);
    assert_eq!(before - memory.available_bytes(), 16 * 512);
    drop(guards);
    assert_eq!(budget.diagnostic(), (0, 0, 0));
    assert_eq!(memory.available_bytes(), before);
    budget.close();
    assert!(budget.reserve_authority(None, member, false).is_none());
}
