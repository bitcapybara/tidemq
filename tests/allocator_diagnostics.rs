use std::process::Command;

// The embedding test executable owns allocation independently of the service binary.
#[global_allocator]
static HOST_ALLOCATOR: std::alloc::System = std::alloc::System;

fn service(arguments: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_tidemq"))
        .args(arguments)
        .output()
        .expect("service binary starts")
}

fn expected_backend() -> &'static str {
    if cfg!(feature = "allocator-mimalloc") {
        "mimalloc"
    } else if cfg!(feature = "allocator-jemalloc") {
        "jemalloc"
    } else {
        "system"
    }
}

#[test]
fn real_binary_reports_allocator_identity_and_bounded_probe_without_claiming_heap_profile() {
    let _builder = tidemq_sdk::Client::builder(
        "localhost:7443",
        tidemq_sdk::TlsTrust::ca_pem("deferred CA material"),
        tidemq_sdk::Credentials::token("x".repeat(32)),
    );
    let output = service(&["allocator-probe", "--block-bytes", "4096", "--blocks", "4"]);
    assert!(
        output.status.success(),
        "allocator probe failed: {output:?}"
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["requested_live_bytes"], 16_384);
    for stage in ["before", "held", "after_free"] {
        let snapshot = &value[stage];
        assert_eq!(snapshot["backend"], expected_backend());
        assert_eq!(snapshot["selection_scope"], "service_binary");
        if expected_backend() == "system" {
            assert!(snapshot["stats"].is_null());
            assert!(snapshot["native_version"].is_null());
        } else {
            assert!(snapshot["stats"].is_object());
            assert!(snapshot["native_version"].is_string());
            assert!(snapshot["note"].as_str().unwrap().contains("profile"));
        }
    }
}

#[test]
fn invalid_probe_budgets_fail_before_allocating_and_do_not_print_partial_snapshots() {
    for arguments in [
        ["allocator-probe", "--block-bytes", "0", "--blocks", "1"],
        ["allocator-probe", "--block-bytes", "1", "--blocks", "65"],
        [
            "allocator-probe",
            "--block-bytes",
            "33554433",
            "--blocks",
            "1",
        ],
    ] {
        let output = service(&arguments);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("allocation probe"));
    }
}
