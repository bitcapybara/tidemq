use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires cargo build --example deploy_validation"]
async fn deployment_driver_checks_stable_message_ids_and_original_pending_handles() {
    let fixture = dynamic_fixture().await;
    let admin = fixture.client(1).await;
    let tenant = admin.admin().create_tenant("validation").await.unwrap();
    admin
        .admin()
        .create_namespace(tenant, "default")
        .await
        .unwrap();
    admin.close().await.unwrap();
    let backend = fixture.configs.get(&1).unwrap().listen;
    let executable = std::env::var_os("TIDEMQ_TEST_DEPLOY_VALIDATION")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("examples")
                .join(format!("deploy_validation{}", std::env::consts::EXE_SUFFIX))
        });
    assert!(
        executable.is_file(),
        "Build the deployment validation example first"
    );
    for case in ["live", "replay", "unavailable"] {
        let proxy = udp_proxy::Proxy::start([backend, backend]).await;
        let output = fixture.directory.join(format!("deployment-{case}"));
        let log = fixture.directory.join(format!("deployment-{case}.log"));
        let file = std::fs::File::create(&log).unwrap();
        let mut command = tokio::process::Command::new(&executable);
        command
            .arg("--endpoint")
            .arg(proxy.address.to_string())
            .arg("--ca")
            .arg(fixture.directory.join("ca.pem"))
            .arg("--token-file")
            .arg(fixture.directory.join("bootstrap.token"))
            .arg("--output-dir")
            .arg(&output)
            .arg("--messages")
            .arg("8")
            .arg("--message-bytes")
            .arg("1024")
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .kill_on_drop(true);
        if case == "replay" {
            command.arg("--replay-settle-ms").arg("1");
        }
        if case == "unavailable" {
            command.arg("--stage-fault").arg("--require-unavailable");
        }
        let mut child = command.spawn().unwrap();
        let until = Instant::now() + Duration::from_secs(60);
        if case == "unavailable" {
            let waiting = stage(&output.join("waiting-fault.json"), &mut child, until, &log).await;
            assert_eq!(waiting["business_acknowledged_before"], 8);
            proxy.set_mtu(1199);
            tokio::fs::write(output.join("fault-injected"), [])
                .await
                .unwrap();
            let observed =
                stage(&output.join("fault-observed.json"), &mut child, until, &log).await;
            assert_eq!(observed["ledger"]["committed"], 8);
            assert_eq!(observed["batch"]["receipt_seen"], false);
            assert!(observed["batch"]["original_pending"].as_u64().unwrap() > 0);
            proxy.set_mtu(1200);
            tokio::fs::write(output.join("recovered"), [])
                .await
                .unwrap();
        }
        let status = tokio::time::timeout_at(until, child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(
            status.success(),
            "Deployment case {case} failed; inspect {}",
            log.display()
        );
        let report: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(output.join("report.json")).await.unwrap())
                .unwrap();
        let expected = if case == "unavailable" { 24 } else { 8 };
        assert_eq!(report["schema_version"], 2);
        assert_eq!(report["status"], "passed");
        assert_eq!(report["client_close_completed"], true);
        assert_eq!(report["original_consumer_reused"], true);
        assert_eq!(report["replay_started_after_publish"], case == "replay");
        for count in [
            "expected",
            "committed",
            "received",
            "acknowledged",
            "frontier",
        ] {
            assert_eq!(report["ledger"][count], expected);
        }
        let mut identities = BTreeSet::new();
        for batch in report["batches"].as_array().unwrap() {
            assert_eq!(batch["receipt_seen"], true);
            for id in batch["message_ids"].as_array().unwrap() {
                assert!(identities.insert(id.as_str().unwrap()));
            }
        }
        assert_eq!(identities.len(), expected as usize);
        proxy.close().await;
    }
    fixture.close().await;
}

async fn stage(
    path: &std::path::Path,
    child: &mut tokio::process::Child,
    until: Instant,
    log: &std::path::Path,
) -> serde_json::Value {
    loop {
        if tokio::fs::try_exists(path).await.unwrap() {
            let bytes = tokio::fs::read(path).await.unwrap();
            assert!(bytes.len() <= 65_536);
            return serde_json::from_slice(&bytes).unwrap();
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "Deployment driver exited; inspect {}",
            log.display()
        );
        assert!(
            Instant::now() < until,
            "Deployment marker timed out: {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
