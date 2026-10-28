use std::time::Duration;

use tidemq_sdk::Error;

#[test]
fn sdk_sources_are_preserved_for_anyhow_chain_rendering() {
    let protocol = tidemq_protocol::ProtocolLimits {
        max_message_bytes: 0,
        ..Default::default()
    };
    let error = Error::from(protocol.validate().unwrap_err());
    assert_eq!(error.to_string(), "Invalid protocol data");
    let report = format!("{:#}", anyhow::Error::new(error));
    assert!(report.contains("max_message_bytes must be greater than zero"));
    assert_eq!(report.matches("max_message_bytes").count(), 1);

    let transport = tidemq::transport::TransportConfig {
        handshake_timeout: Duration::ZERO,
        ..Default::default()
    };
    let error = Error::from(transport.validate().unwrap_err());
    assert_eq!(error.to_string(), "QUIC transport failed");
    let report = format!("{:#}", anyhow::Error::new(error));
    assert!(report.contains("handshake_timeout must be greater than zero"));
    assert_eq!(report.matches("handshake_timeout").count(), 1);
}

#[test]
fn transport_memory_errors_retain_the_invalid_reservation() {
    let error = Error::Memory {
        source: tidemq::transport::budget::MemoryBudget::new(
            tidemq::transport::budget::MemoryLimits {
                total_bytes: 1024,
                control_reserved_bytes: 1025,
            },
        )
        .err()
        .unwrap(),
    };
    assert_eq!(error.to_string(), "Invalid client transport memory budget");
    let report = format!("{:#}", anyhow::Error::new(error));
    assert!(report.contains("control_reserved_bytes must not exceed total_bytes"));
    assert_eq!(report.matches("control_reserved_bytes").count(), 1);
}

#[test]
fn resolution_context_and_leaf_error_are_reported_once() {
    let error = tidemq_sdk::Error::Resolve {
        endpoint: "missing.invalid:7443".into(),
        source: std::io::Error::new(std::io::ErrorKind::NotFound, "DNS record does not exist"),
    };
    assert!(!error.to_string().contains("DNS record"));
    let report = format!("{:#}", anyhow::Error::new(error));
    assert!(report.contains("missing.invalid:7443"));
    assert_eq!(report.matches("DNS record does not exist").count(), 1);
}

#[test]
fn server_configuration_reports_identity_memory_and_logging_fields() {
    type Case = (&'static str, fn(&mut tidemq::config::Config));
    let cases: &[Case] = &[
        ("node_id", |c| c.node_id = 0),
        ("data_dir", |c| c.data_dir.clear()),
        ("cluster_id", |c| c.cluster_id = "invalid".into()),
        ("replication_factor", |c| c.replication_factor = 2),
        ("management_listen", |c| {
            c.management_listen = "0.0.0.0:9419".parse().unwrap()
        }),
        ("console.listen", |c| {
            c.console.as_mut().unwrap().listen = c.management_listen
        }),
        ("control_reserved_bytes", |c| {
            c.memory.control_reserved_bytes = 1
        }),
        ("control_reserved_bytes", |c| {
            c.memory.control_reserved_bytes = c.memory.total_bytes + 1
        }),
        ("total_bytes", |c| {
            c.memory.total_bytes = c.memory.control_reserved_bytes + 1
        }),
        ("max_connections", |c| c.memory.max_connections = 0),
        ("max_streams_per_connection", |c| {
            c.memory.max_streams_per_connection = 1
        }),
        ("max_pending_requests", |c| {
            c.memory.max_pending_requests = 0
        }),
        ("queue_events", |c| c.logging.queue_events = 0),
        ("max_event_bytes", |c| c.logging.max_event_bytes = 1),
        ("filter", |c| c.logging.filter = "x".repeat(1_025)),
    ];
    for &(field, mutate) in cases {
        let mut config = tidemq::config::Config::default();
        mutate(&mut config);
        let report = format!("{:#}", anyhow::Error::new(config.validate().unwrap_err()));
        assert!(
            report.contains(field),
            "Expected {field}, received {report}"
        );
    }
}

fn assert_distinct_diagnostics(values: &[impl std::fmt::Display]) {
    let messages: std::collections::BTreeSet<_> = values.iter().map(ToString::to_string).collect();
    assert_eq!(
        messages.len(),
        values.len(),
        "Distinct failures need distinct diagnostics"
    );
    assert!(
        messages
            .iter()
            .all(|message| !message.is_empty() && message.is_ascii())
    );
}

#[test]
fn metadata_rejections_have_distinct_user_facing_reasons() {
    use tidemq::metadata::Rejection as R;
    assert_distinct_diagnostics(&[
        R::Uninitialized,
        R::AlreadyInitialized,
        R::InvalidLimit,
        R::InvalidName,
        R::InvalidNode,
        R::InvalidGroup,
        R::InvalidSubscription,
        R::InvalidEvidence,
        R::InvalidCommand,
        R::Capacity,
        R::RevisionConflict,
        R::GenerationConflict,
        R::OperationConflict,
        R::IdentityRetired,
        R::NameInUse,
        R::NotFound,
        R::NodeInUse,
        R::NotSchedulable,
        R::PlacementViolation,
        R::Superseded,
        R::RecoveryRequired,
        R::CounterExhausted,
        R::Lifecycle(tidemq::lifecycle::Rejection::Conflict),
    ]);
    assert!(
        R::NameInUse
            .to_string()
            .contains("name is already assigned")
    );
}

#[test]
fn lifecycle_and_fence_rejections_explain_the_blocker() {
    use tidemq::lifecycle::Rejection as L;
    assert_distinct_diagnostics(&[
        L::Identity,
        L::Capacity,
        L::Conflict,
        L::NotFound,
        L::InvalidRange,
        L::StaleGeneration,
        L::CancelRequested,
        L::Aborted,
        L::AlreadyPublished,
        L::NotPublished,
        L::References,
        L::Pinned,
        L::RecoveryDependency,
        L::CheckpointRequired,
        L::Membership,
        L::InvalidSnapshot,
    ]);
    use tidemq::ownership::FenceRejection as F;
    assert_distinct_diagnostics(&[
        F::InvalidIdentity,
        F::Capacity,
        F::StaleEpoch,
        F::EpochConflict,
        F::CounterExhausted,
        F::RecoveryRequired,
        F::InvalidRecovery,
    ]);
    assert!(L::Pinned.to_string().contains("read pins"));
    assert!(
        F::RecoveryRequired
            .to_string()
            .contains("before writes are admitted")
    );
}

#[test]
fn scaling_blockers_distinguish_capacity_from_missing_evidence() {
    use tidemq::scaling::BlockReason as R;
    assert_distinct_diagnostics(&[
        R::NoStandby,
        R::Topology,
        R::MissingObservation,
        R::ExpiredObservation,
        R::ReachabilityPending,
        R::FreeCapacity,
        R::Cpu,
        R::ByteRate,
        R::Connections,
        R::AckState,
        R::Cooldown,
        R::Balanced,
        R::Authority,
        R::Admission,
        R::Unknown,
    ]);
    assert!(R::ExpiredObservation.to_string().contains("expired"));
    assert!(R::FreeCapacity.to_string().contains("insufficient free"));
}
