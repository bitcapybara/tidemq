use std::io::{BufRead, Read};
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use tidemq::budget::MemoryBudget;
use tidemq::config::{LoggingConfig, MemoryConfig};
use tidemq::observability::logging::initialize;
use tidemq::observability::metrics::{Metrics, Operation, Outcome, Stage};
use tidemq_protocol::common::{FrameKind, TrafficClass};
use tidemq_protocol::framing::{decode_frame, encode_payload, new_frame};
use tidemq_protocol::public::CreateTopic;
use tidemq_protocol::{Id, ProtocolLimits};

#[tokio::test]
async fn cancelled_cross_task_buffers_return_capacity_and_preserve_control() {
    let config = MemoryConfig {
        total_bytes: 100,
        control_reserved_bytes: 20,
        ..MemoryConfig::default()
    };
    let budget = MemoryBudget::new(&config).unwrap();
    for _ in 0..32 {
        let child_budget = budget.clone();
        let (ready, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _permit = child_budget
                .try_reserve(TrafficClass::Delivery, 80)
                .unwrap();
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        receiver.await.unwrap();
        assert!(budget.try_reserve(TrafficClass::Bulk, 1).is_err());
        let control = budget.try_reserve(TrafficClass::Control, 5).unwrap();
        let core = budget.try_reserve_core(10).unwrap();
        let handshake = budget.try_reserve_handshake(5).unwrap();
        assert_eq!(budget.available_bytes(), 0);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(control);
        drop(core);
        drop(handshake);
        assert_eq!(budget.available_bytes(), 100);
    }
}

#[test]
fn untrusted_trace_metadata_does_not_change_request_identity_payload_or_authority() {
    let limits = ProtocolLimits::default();
    let id: Id = "00000000-0000-4000-8000-000000000001".parse().unwrap();
    let frame = new_frame(
        FrameKind::CreateTopic,
        TrafficClass::Control,
        id,
        id,
        &CreateTopic {
            name: "events".into(),
            partitions: 1,
        },
        1_000,
        &limits,
    )
    .unwrap();
    for context in [
        "invalid".into(),
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01".into(),
        "x".repeat(1_000),
    ] {
        let mut untrusted = frame.clone();
        untrusted.traceparent = context;
        let encoded = encode_payload(&untrusted, limits.max_control_frame_bytes).unwrap();
        let decoded = decode_frame(encoded, TrafficClass::Control, &limits).unwrap();
        assert!(decoded.traceparent.is_empty());
        assert_eq!(decoded.payload, frame.payload);
        assert_eq!(decoded.request_id, frame.request_id);
        assert_eq!(decoded.operation_id, frame.operation_id);
        assert_eq!(decoded.remaining_timeout_ms, frame.remaining_timeout_ms);
    }
    let encoded = encode_payload(&frame, limits.max_control_frame_bytes).unwrap();
    assert!(decode_frame(encoded, TrafficClass::Bulk, &limits).is_err());
}

#[test]
fn malformed_frames_have_bounded_failure_without_panicking() {
    let limits = ProtocolLimits::default();
    let mut seed = 0x5eed_u64;
    for length in 0..=256 {
        let bytes = (0..length)
            .map(|_| {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                (seed >> 32) as u8
            })
            .collect::<Vec<_>>();
        assert!(decode_frame(Bytes::from(bytes), TrafficClass::Control, &limits).is_err());
    }
    assert!(
        decode_frame(
            Bytes::from(vec![0; limits.max_control_frame_bytes as usize + 1]),
            TrafficClass::Control,
            &limits
        )
        .is_err()
    );
}

#[test]
fn invalid_duration_samples_do_not_add_samples_or_metric_series() {
    let metrics = Metrics::default();
    let empty = metrics.encode().unwrap();
    let handle = metrics.operation(Operation::Produce).unwrap();
    for sample in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
        handle.observe(Stage::Sync, sample);
    }
    assert_eq!(metrics.encode().unwrap(), empty);
    for _ in 0..1_000 {
        handle.result(Outcome::Unknown);
        handle.observe(Stage::Sync, 0.002);
    }
    let populated = metrics.encode().unwrap();
    assert_eq!(empty.lines().count(), populated.lines().count());
    let count = populated
        .lines()
        .find(|line| {
            line.starts_with("tidemq_stage_duration_seconds_count{")
                && line.contains("operation=\"Produce\"")
                && line.contains("stage=\"Sync\"")
        })
        .unwrap();
    assert!(count.ends_with(" 1000"));
    assert!(!populated.contains("request_id"));
}

#[test]
fn logging_child() {
    let Ok(mode) = std::env::var("TIDEMQ_LOG_TEST_MODE") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(log_scenario(&mode));
    if mode == "broken_sink" {
        // The harness cannot print its result after this test deliberately closes stdout.
        std::process::exit(0);
    }
}

async fn log_scenario(mode: &str) {
    let config = LoggingConfig {
        filter: if mode == "filter" {
            "error".into()
        } else {
            "info".into()
        },
        queue_events: 8,
        max_event_bytes: 512,
        ..LoggingConfig::default()
    };
    let logging = initialize(&config).unwrap();
    match mode {
        "filter" => {
            assert!(
                logging
                    .control
                    .temporary_filter("info", Duration::ZERO)
                    .is_err()
            );
            assert!(
                logging
                    .control
                    .temporary_filter("[", Duration::from_secs(1))
                    .is_err()
            );
            logging
                .control
                .temporary_filter("info", Duration::from_millis(100))
                .unwrap();
            logging
                .control
                .temporary_filter("debug", Duration::from_millis(600))
                .unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
            tidemq_sdk::ClientConfig::new("localhost:7443")
                .validate()
                .unwrap();
            tracing::debug!(event_code = "newer_filter_survived");
            tokio::time::sleep(Duration::from_millis(500)).await;
            tracing::info!(event_code = "expired_filter_must_not_emit");
            tracing::error!(event_code = "baseline_restored");
        }
        "oversize" => {
            tracing::info!(event_code = "oversize", detail = %"x".repeat(2_048));
            tracing::info!(event_code = "following_valid_event");
        }
        "saturate" => {
            for sequence in 0..20_000 {
                tracing::info!(event_code = "bounded_queue_sample", sequence);
            }
        }
        "broken_sink" => {
            eprintln!("logging_child_ready");
            tokio::time::sleep(Duration::from_millis(100)).await;
            tracing::info!(event_code = "broken_sink_sample");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        _ => panic!("unknown logging test mode"),
    }
    tokio::time::sleep(Duration::from_millis(25)).await;
    eprintln!(
        "{}",
        serde_json::json!({
            "queue_drops": logging.health.dropped_events(),
            "oversized_events": logging.health.oversized_events.load(Ordering::Relaxed),
            "sink_errors": logging.health.sink_errors.load(Ordering::Relaxed)
        })
    );
    drop(logging);
}

fn child(mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "logging_child", "--nocapture"])
        .env("TIDEMQ_LOG_TEST_MODE", mode)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn health(output: &std::process::Output) -> serde_json::Value {
    assert!(output.status.success(), "logging child failed: {output:?}");
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .find_map(|line| serde_json::from_str(line).ok())
        .expect("child reports independent health counters")
}

#[test]
fn temporary_filters_expire_without_old_timer_overriding_new_filter_or_sdk_host_state() {
    let output = child("filter").output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("newer_filter_survived"));
    assert!(stdout.contains("baseline_restored"));
    assert!(!stdout.contains("expired_filter_must_not_emit"));
    assert_eq!(health(&output)["sink_errors"], 0);
}

#[test]
fn oversized_logs_are_dropped_whole_and_queue_saturation_is_visible() {
    let output = child("oversize").output().unwrap();
    assert_eq!(health(&output)["oversized_events"], 1);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let events: Vec<serde_json::Value> = stdout
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["fields"]["event_code"], "following_valid_event");

    let task = child("saturate").spawn().unwrap();
    std::thread::sleep(Duration::from_millis(150));
    let output = task.wait_with_output().unwrap();
    assert!(health(&output)["queue_drops"].as_u64().unwrap() > 0);
    assert_eq!(health(&output)["sink_errors"], 0);
}

#[test]
fn closed_log_sink_is_counted_independently_of_queue_drops() {
    let mut task = child("broken_sink").spawn().unwrap();
    let mut errors = std::io::BufReader::new(task.stderr.take().unwrap());
    let mut ready = String::new();
    errors.read_line(&mut ready).unwrap();
    assert_eq!(ready.trim(), "logging_child_ready");
    drop(task.stdout.take());
    let mut stderr = Vec::new();
    errors.read_to_end(&mut stderr).unwrap();
    let output = std::process::Output {
        status: task.wait().unwrap(),
        stdout: Vec::new(),
        stderr,
    };
    assert!(health(&output)["sink_errors"].as_u64().unwrap() > 0);
    assert_eq!(health(&output)["queue_drops"], 0);
}
