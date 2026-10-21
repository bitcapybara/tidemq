use super::*;
use std::error::Error as _;

type InvalidConfig = (&'static str, fn(&mut ClientConfig));

#[test]
fn configuration_errors_identify_the_invalid_field() {
    let cases: &[InvalidConfig] = &[
        ("endpoint", |c| c.endpoint.clear()),
        ("endpoint", |c| c.endpoint = "x".repeat(513)),
        ("endpoint", |c| c.endpoint = "local host:7443".into()),
        ("endpoint", |c| c.endpoint = "user@localhost:7443".into()),
        ("endpoint", |c| c.endpoint = "localhost\0:7443".into()),
        ("endpoint", |c| c.endpoint = "localhost".into()),
        ("endpoint host", |c| c.endpoint = ":7443".into()),
        ("endpoint port", |c| c.endpoint = "localhost:0".into()),
        ("endpoint port", |c| {
            c.endpoint = "localhost:not-a-port".into()
        }),
        ("request_timeout_ms", |c| c.request_timeout_ms = 0),
        ("request_timeout_ms", |c| c.request_timeout_ms = 300_001),
        ("max_pending_requests", |c| c.max_pending_requests = 0),
        ("max_pending_requests", |c| c.max_pending_requests = 65_537),
        ("max_pending_bytes", |c| c.max_pending_bytes = 1),
        ("max_pending_bytes", |c| {
            c.max_pending_bytes = u64::from(u32::MAX) + 1
        }),
        ("max_reconnect_attempts", |c| c.max_reconnect_attempts = 0),
        ("max_reconnect_attempts", |c| c.max_reconnect_attempts = 17),
        ("request_lanes", |c| c.request_lanes = 0),
        ("request_lanes", |c| c.request_lanes = 65),
        ("request_lanes", |c| {
            c.request_lanes = c.transport.max_streams + 1
        }),
        ("retry_backoff", |c| c.retry_backoff = Duration::ZERO),
        ("retry_backoff", |c| {
            c.retry_backoff = Duration::from_secs(6)
        }),
        ("liveness_timeout", |c| {
            c.liveness_timeout = Duration::from_millis(99)
        }),
        ("liveness_timeout", |c| {
            c.liveness_timeout = Duration::from_secs(11)
        }),
    ];
    for &(field, mutate) in cases {
        let mut config = ClientConfig::new("localhost:7443");
        mutate(&mut config);
        let Error::Configuration { reason } = config.validate().unwrap_err() else {
            panic!("Expected a configuration error for {field}");
        };
        assert!(
            reason.starts_with(field),
            "Expected {field}, received {reason}"
        );
    }
}

#[test]
fn nested_validation_keeps_protocol_transport_and_memory_reasons() {
    let mut config = ClientConfig::new("localhost:7443");
    config.protocol.max_message_bytes = 0;
    assert!(
        config
            .validate()
            .unwrap_err()
            .source()
            .unwrap()
            .to_string()
            .contains("max_message_bytes")
    );
    config.protocol = ProtocolLimits::default();
    config.transport.handshake_timeout = Duration::ZERO;
    assert!(
        config
            .validate()
            .unwrap_err()
            .source()
            .unwrap()
            .to_string()
            .contains("handshake_timeout")
    );
    config.transport = TransportConfig::default();
    config.transport_control_reserved_bytes = config.transport_memory_bytes + 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .source()
            .unwrap()
            .to_string()
            .contains("control_reserved_bytes")
    );
    config.transport_control_reserved_bytes = 3;
    assert!(
        config
            .validate()
            .unwrap_err()
            .source()
            .unwrap()
            .to_string()
            .contains("at least 4 bytes")
    );
    ClientConfig::new("localhost:7443").validate().unwrap();
}

#[test]
fn retry_jitter_uses_v7_entropy_and_retains_the_backoff_cap() {
    let first: Id = "018bcfe5-6800-7000-8000-000000000001".parse().unwrap();
    let second: Id = "018bcfe5-6800-7000-8000-00000000000f".parse().unwrap();
    let base = Duration::from_millis(25);
    assert_eq!(backoff_delay(base, first, 0), Duration::from_millis(26));
    assert_eq!(backoff_delay(base, second, 0), Duration::from_millis(40));
    assert_eq!(backoff_delay(base, second, 1), Duration::from_millis(65));
    assert_eq!(
        backoff_delay(base, second, u32::MAX),
        Duration::from_millis(1015)
    );
}
