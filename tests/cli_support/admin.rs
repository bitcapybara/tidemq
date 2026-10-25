use super::*;
use clap::Parser;

#[test]
fn administrative_cli_requires_explicit_identity_material_and_a_scheduling_decision() {
    assert!(crate::Cli::try_parse_from(["tidemq", "admin", "nodes"]).is_err());
    let prefix = [
        "tidemq",
        "admin",
        "--endpoint",
        "127.0.0.1:7443",
        "--ca",
        "missing-ca.pem",
        "--token-file",
        "missing.token",
    ];
    let missing: Vec<_> = prefix
        .into_iter()
        .chain(["set-schedulable", "--node-id", "2"])
        .collect();
    assert!(crate::Cli::try_parse_from(missing).is_err());
    let valid: Vec<_> = prefix
        .into_iter()
        .chain(["set-schedulable", "--node-id", "2", "--enabled", "false"])
        .collect();
    let parsed = crate::Cli::try_parse_from(valid).unwrap();
    let Some(crate::Command::Admin(args)) = parsed.command else {
        panic!("Expected administration arguments");
    };
    assert!(matches!(
        args.action,
        Action::SetSchedulable {
            node_id: 2,
            enabled: false
        }
    ));
    let invalid: Vec<_> = prefix
        .into_iter()
        .chain(["transfer-shard", "not-a-shard", "--target-group", "3"])
        .collect();
    assert!(crate::Cli::try_parse_from(invalid).is_err());
}

#[tokio::test]
async fn certificate_input_never_reads_beyond_its_finite_der_limit() {
    let mut bytes = Vec::new();
    assert!(bounded_certificate(&[][..], &mut bytes).await.is_err());
    let source = vec![0; 128 * 1024];
    assert!(
        bounded_certificate(source.as_slice(), &mut bytes)
            .await
            .is_err()
    );
    assert_eq!(bytes.len(), 65_537);
    bytes.clear();
    bounded_certificate(&[1, 2, 3][..], &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, [1, 2, 3]);
}
