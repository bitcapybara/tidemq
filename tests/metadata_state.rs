use std::collections::BTreeSet;

use tidemq::metadata::{
    Command, DeploymentMode, MetadataLimits, MetadataState, NodeRole, NodeSpec, Outcome,
    PartitionKey, RecordStatus, Rejection, Response, SubscriptionConfig, TopicDeletionCommand,
};
use tidemq_protocol::Id;

fn id(value: u64) -> Id {
    format!("00000000-0000-4000-8000-{value:012x}")
        .parse()
        .unwrap()
}

struct Fixture {
    state: MetadataState,
    next_operation: u64,
}

impl Fixture {
    fn new(mode: DeploymentMode, limits: MetadataLimits) -> Self {
        let mut state = MetadataState::default();
        assert!(
            state
                .apply(Command::bootstrap(id(1), id(2), 1, mode, limits))
                .is_applied()
        );
        Self {
            state,
            next_operation: 100,
        }
    }

    fn operation(&mut self) -> Id {
        self.next_operation += 1;
        id(self.next_operation)
    }

    fn node(&mut self, node_id: u64, zone: &str, roles: BTreeSet<NodeRole>) {
        let operation = self.operation();
        let spec = NodeSpec {
            node_id,
            zone: zone.into(),
            endpoint: format!("127.0.0.1:{}", 20_000 + node_id).parse().unwrap(),
            roles,
            schedulable: true,
        };
        assert!(
            self.state
                .apply(Command::put_node(operation, self.state.revision(), spec, 0))
                .is_applied()
        );
    }

    fn group(&mut self, group_id: u64, voters: BTreeSet<u64>) -> Response {
        let operation = self.operation();
        self.state.apply(Command::create_group(
            operation,
            self.state.revision(),
            group_id,
            voters,
        ))
    }

    fn topic(&mut self, topic_id: Id, name: &str, groups: Vec<u64>) -> Response {
        let operation = self.operation();
        self.state.apply(Command::create_topic(
            operation,
            self.state.revision(),
            topic_id,
            name.into(),
            groups,
        ))
    }

    fn local() -> Self {
        let mut fixture = Self::new(DeploymentMode::LocalRf1, MetadataLimits::default());
        fixture.node(
            1,
            "local",
            BTreeSet::from([NodeRole::Broker, NodeRole::Storage]),
        );
        assert!(fixture.group(2, BTreeSet::from([1])).is_applied());
        fixture
    }
}

fn rejected(response: Response, reason: Rejection) {
    assert!(matches!(response, Response::Rejected { reason: actual, .. } if actual == reason));
}

#[test]
fn operation_retries_return_original_decision_and_changed_content_conflicts() {
    let mut fixture = Fixture::local();
    let operation = fixture.operation();
    let expected_revision = fixture.state.revision();
    let command = Command::create_topic(
        operation,
        expected_revision,
        id(3),
        "orders".into(),
        vec![2, 2],
    );
    let response = fixture.state.apply(command.clone());
    assert!(response.is_applied());
    assert!(matches!(&response, Response::Applied { revision, .. }
        if revision.checked_sub(1) == Some(expected_revision)));
    assert_eq!(fixture.state.revision(), expected_revision + 1);
    assert_eq!(fixture.state.apply(command), response);
    rejected(
        fixture.state.apply(Command::create_topic(
            operation,
            fixture.state.revision(),
            id(4),
            "changed".into(),
            vec![2],
        )),
        Rejection::OperationConflict,
    );
    let old_revision = fixture.state.revision() - 1;
    let operation = fixture.operation();
    let conflict = Command::retire_topic(operation, old_revision, id(3), 1);
    let original = fixture.state.apply(conflict.clone());
    rejected(original.clone(), Rejection::RevisionConflict);
    assert_eq!(fixture.state.apply(conflict), original);
    fixture.state.validate_snapshot().unwrap();
}

#[test]
fn retiring_and_recreating_a_name_never_reuses_topic_or_subscription_identity() {
    let mut fixture = Fixture::local();
    assert!(fixture.topic(id(3), "orders", vec![2]).is_applied());
    let operation = fixture.operation();
    assert!(
        fixture
            .state
            .apply(Command::put_subscription(
                operation,
                fixture.state.revision(),
                id(4),
                id(3),
                "worker".into(),
                0,
                SubscriptionConfig::default()
            ))
            .is_applied()
    );
    let operation = fixture.operation();
    assert!(
        fixture
            .state
            .apply(Command::retire_subscription(
                operation,
                fixture.state.revision(),
                id(4),
                1
            ))
            .is_applied()
    );
    let operation = fixture.operation();
    rejected(
        fixture.state.apply(Command::put_subscription(
            operation,
            fixture.state.revision(),
            id(4),
            id(3),
            "worker".into(),
            2,
            SubscriptionConfig::default(),
        )),
        Rejection::IdentityRetired,
    );
    let operation = fixture.operation();
    assert!(
        fixture
            .state
            .apply(Command::topic_deletion(
                operation,
                fixture.state.revision(),
                TopicDeletionCommand::Begin {
                    topic: id(3),
                    generation: 1
                }
            ))
            .is_applied()
    );
    let domains = fixture.state.topic_deletion(id(3)).unwrap().domains.clone();
    for domain in domains {
        let step = fixture.operation();
        assert!(
            fixture
                .state
                .apply(Command::topic_deletion(
                    step,
                    fixture.state.revision(),
                    TopicDeletionCommand::ObserveSeal {
                        topic: id(3),
                        deletion: operation,
                        domain,
                        end: 0
                    }
                ))
                .is_applied()
        );
    }
    let step = fixture.operation();
    assert!(
        fixture
            .state
            .apply(Command::topic_deletion(
                step,
                fixture.state.revision(),
                TopicDeletionCommand::Finish {
                    topic: id(3),
                    deletion: operation
                }
            ))
            .is_applied()
    );
    assert_eq!(
        fixture.state.topic(id(3)).unwrap().status,
        RecordStatus::Retired
    );
    assert!(fixture.state.topic_by_name("orders").is_none());
    rejected(
        fixture.topic(id(3), "orders", vec![2]),
        Rejection::IdentityRetired,
    );
    assert!(fixture.topic(id(5), "orders", vec![2]).is_applied());
    assert_eq!(
        fixture.state.topic_by_name("orders").unwrap().topic_id,
        id(5)
    );
    assert_eq!(
        fixture.state.partition_group(PartitionKey {
            topic_id: id(3),
            partition: 0
        }),
        Some(2)
    );
    let bytes = serde_json::to_vec(&fixture.state).unwrap();
    let restored: MetadataState = serde_json::from_slice(&bytes).unwrap();
    restored.validate_snapshot().unwrap();
    assert_eq!(restored, fixture.state);
}

#[test]
fn rf3_requires_three_storage_nodes_in_distinct_zones_and_never_falls_back() {
    let mut fixture = Fixture::new(DeploymentMode::ThreeReplica, MetadataLimits::default());
    fixture.node(1, "zone-a", BTreeSet::from([NodeRole::Storage]));
    fixture.node(2, "zone-a", BTreeSet::from([NodeRole::Storage]));
    fixture.node(3, "zone-b", BTreeSet::from([NodeRole::Storage]));
    fixture.node(4, "zone-c", BTreeSet::from([NodeRole::Broker]));
    rejected(
        fixture.group(2, BTreeSet::from([1])),
        Rejection::PlacementViolation,
    );
    rejected(
        fixture.group(2, BTreeSet::from([1, 2, 3])),
        Rejection::PlacementViolation,
    );
    rejected(
        fixture.group(2, BTreeSet::from([1, 3, 4])),
        Rejection::PlacementViolation,
    );
    let operation = fixture.operation();
    let mut node = fixture.state.node(4).unwrap().spec.clone();
    node.roles.insert(NodeRole::Storage);
    assert!(
        fixture
            .state
            .apply(Command::put_node(
                operation,
                fixture.state.revision(),
                node,
                1
            ))
            .is_applied()
    );
    assert!(fixture.group(2, BTreeSet::from([1, 3, 4])).is_applied());
    let operation = fixture.operation();
    let mut node = fixture.state.node(3).unwrap().spec.clone();
    node.zone = "zone-a".into();
    rejected(
        fixture.state.apply(Command::put_node(
            operation,
            fixture.state.revision(),
            node,
            1,
        )),
        Rejection::NodeInUse,
    );
    rejected(
        fixture.group(1, BTreeSet::from([1, 3, 4])),
        Rejection::InvalidGroup,
    );
    fixture.state.validate_snapshot().unwrap();
}

#[test]
fn state_budgets_keep_tombstones_and_diagnostics_are_bounded_and_read_only() {
    let limits = MetadataLimits {
        topics: 2,
        partitions: 2,
        partitions_per_topic: 1,
        operations: 16,
        ..MetadataLimits::default()
    };
    let mut fixture = Fixture::new(DeploymentMode::LocalRf1, limits);
    fixture.node(
        1,
        "local",
        BTreeSet::from([NodeRole::Broker, NodeRole::Storage]),
    );
    assert!(fixture.group(2, BTreeSet::from([1])).is_applied());
    assert!(fixture.topic(id(3), "one", vec![2]).is_applied());
    assert!(fixture.topic(id(4), "two", vec![2]).is_applied());
    rejected(fixture.topic(id(5), "three", vec![2]), Rejection::Capacity);
    let before = fixture.state.clone();
    let page = fixture.state.topics_page(None, 1, 512).unwrap();
    assert_eq!(page.topics.len(), 1);
    assert_eq!(page.next_after, Some(id(3)));
    let page = fixture.state.topics_page(page.next_after, 1, 512).unwrap();
    assert_eq!(page.topics.first().unwrap().topic_id, id(4));
    assert!(page.next_after.is_none());
    assert!(serde_json::to_vec(&page).unwrap().len() <= 512);
    assert!(fixture.state.topics_page(None, 65, 512).is_err());
    assert!(
        fixture
            .state
            .diagnose_ownership(PartitionKey {
                topic_id: id(3),
                partition: 0
            })
            .is_some()
    );
    assert_eq!(before, fixture.state);
    while fixture.state.summary().remaining_operations > 0 {
        let operation = fixture.operation();
        rejected(
            fixture.state.apply(Command::retire_topic(
                operation,
                fixture.state.revision(),
                id(3),
                1,
            )),
            Rejection::RecoveryRequired,
        );
    }
    let before = fixture.state.clone();
    let operation = fixture.operation();
    rejected(
        fixture.state.apply(Command::retire_topic(
            operation,
            fixture.state.revision(),
            id(3),
            1,
        )),
        Rejection::Capacity,
    );
    assert_eq!(before, fixture.state);
    fixture.state.validate_snapshot().unwrap();
}

#[test]
fn ownership_grants_are_monotonic_conditional_and_do_not_expose_a_writable_route() {
    let mut fixture = Fixture::local();
    assert!(fixture.topic(id(3), "orders", vec![2]).is_applied());
    let partition = PartitionKey {
        topic_id: id(3),
        partition: 0,
    };
    let grant_command =
        Command::grant_ownership(fixture.state.revision(), id(2), partition, 1, 0, 0).unwrap();
    let response = fixture.state.apply(grant_command.clone());
    let Response::Applied {
        outcome: Outcome::OwnershipGranted(first),
        ..
    } = response.clone()
    else {
        panic!("expected ownership grant");
    };
    assert_eq!(first.epoch, 1);
    assert_eq!(fixture.state.apply(grant_command.clone()), response);
    assert!(fixture.state.route(partition).is_none());
    rejected(
        fixture.state.apply(
            Command::grant_ownership(fixture.state.revision(), id(2), partition, 1, 9, 0).unwrap(),
        ),
        Rejection::GenerationConflict,
    );
    assert!(
        fixture
            .state
            .apply(
                Command::grant_ownership(fixture.state.revision(), id(2), partition, 1, 1, 0)
                    .unwrap()
            )
            .is_applied()
    );
    assert_eq!(fixture.state.grant(partition).unwrap().epoch, 2);
    rejected(fixture.state.apply(grant_command), Rejection::Superseded);
    assert_eq!(
        fixture.state.lookup_operation(first.operation_id),
        tidemq::metadata::OperationResult::UnknownOrRetired
    );
    assert_eq!(fixture.state.grant(partition).unwrap().epoch, 2);
    fixture.state.validate_snapshot().unwrap();
}
