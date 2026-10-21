use super::*;

fn config() -> S3Config {
    S3Config {
        endpoint: "https://s3.example.invalid".into(),
        region: "test-region".into(),
        bucket: "diagnostic-bucket".into(),
        prefix: "tests/diagnostics".into(),
        cluster_id: Id::new_v7().unwrap(),
        credentials: Credentials::new("test-access", "test-secret", None, None, "DiagnosticsTest"),
        allow_http_loopback: false,
    }
}

#[test]
fn s3_configuration_reports_the_failed_field_without_echoing_credentials() {
    type Case = (&'static str, fn(&mut S3Config));
    let cases: &[Case] = &[
        ("endpoint", |c| c.endpoint = "x".repeat(2_049)),
        ("endpoint", |c| c.endpoint = "not-a-url".into()),
        ("endpoint", |c| {
            c.endpoint = " https://s3.example.invalid".into()
        }),
        ("endpoint", |c| {
            c.endpoint = "https://secret-user:secret-password@s3.example.invalid".into()
        }),
        ("endpoint", |c| c.endpoint.push_str("?secret-query=value")),
        ("endpoint", |c| c.endpoint.push_str("#fragment")),
        ("endpoint", |c| {
            c.endpoint = "ftp://s3.example.invalid".into()
        }),
        ("allow_http_loopback", |c| {
            c.endpoint = "http://127.0.0.1:9000".into()
        }),
        ("HTTP endpoint", |c| {
            c.allow_http_loopback = true;
            c.endpoint = "http://192.0.2.1:9000".into();
        }),
        ("bucket", |c| c.bucket = "UPPERCASE".into()),
        ("prefix", |c| c.prefix.clear()),
        ("prefix", |c| c.prefix = "bad!".into()),
        ("prefix", |c| c.prefix = "/leading-slash".into()),
        ("region", |c| c.region.clear()),
    ];
    for &(field, mutate) in cases {
        let mut config = config();
        mutate(&mut config);
        let Error::Configuration { reason } =
            validate_config(&config, Limits::default()).unwrap_err()
        else {
            panic!("Expected an S3 configuration error for {field}");
        };
        assert!(
            reason.starts_with(field),
            "Expected {field}, received {reason}"
        );
        assert!(!reason.contains("secret-"));
    }
    validate_config(&config(), Limits::default()).unwrap();
    let mut local = config();
    local.endpoint = "http://127.0.0.1:9000".into();
    local.allow_http_loopback = true;
    validate_config(&local, Limits::default()).unwrap();
}

#[test]
fn s3_limits_report_the_specific_budget() {
    type Case = (&'static str, fn(&mut Limits));
    let cases: &[Case] = &[
        ("max_in_flight", |c| c.max_in_flight = 1),
        ("max_object_bytes", |c| c.max_object_bytes = 0),
        ("max_range_bytes", |c| c.max_range_bytes = 0),
        ("max_range_bytes", |c| {
            c.max_range_bytes = c.max_object_bytes + 1
        }),
        ("bytes_per_second", |c| c.bytes_per_second = 0),
        ("request_timeout", |c| c.request_timeout = Duration::ZERO),
        ("part_bytes", |c| c.part_bytes = 1),
    ];
    for &(field, mutate) in cases {
        let mut limits = Limits::default();
        mutate(&mut limits);
        let Error::Configuration { reason } = validate_limits(limits).unwrap_err() else {
            panic!("Expected an S3 limit error for {field}");
        };
        assert!(
            reason.starts_with(field),
            "Expected {field}, received {reason}"
        );
    }
}
