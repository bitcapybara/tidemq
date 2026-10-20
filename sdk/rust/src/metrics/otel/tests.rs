use std::{collections::BTreeMap, sync::Arc};

use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData},
};

use crate::metrics::Metrics;

fn exported(exporter: &InMemoryMetricExporter) -> BTreeMap<String, f64> {
    let reports = exporter.get_finished_metrics().unwrap();
    let report = reports.last().expect("a flushed metric report");
    report
        .scope_metrics()
        .flat_map(|scope| scope.metrics())
        .map(|metric| {
            let value = match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    sum.data_points().map(|point| point.value() as f64).sum()
                }
                AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                    sum.data_points().map(|point| point.value() as f64).sum()
                }
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                    .data_points()
                    .map(|point| point.count() as f64)
                    .sum(),
                data => panic!("Unexpected SDK metric data: {data:?}"),
            };
            (metric.name().to_owned(), value)
        })
        .collect()
}

#[test]
fn shared_meter_preserves_counters_and_releases_all_client_charges() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let meter = provider.meter("tidemq-sdk-test");
    let clients: Vec<_> = (0..2)
        .map(|_| {
            Arc::new(Metrics {
                telemetry: super::Instruments::new(&meter),
                ..Metrics::default()
            })
        })
        .collect();
    let mut charges = Vec::new();
    for metrics in &clients {
        metrics.record_request();
        metrics.record_retry();
        metrics.record_unknown();
        metrics.record_deliveries(3);
        metrics.record_acknowledged(2);
        drop(metrics.timer());
        drop(metrics.admission_timer());
        charges.push(metrics.charge(64, false));
        charges.push(metrics.charge(16, true));
        assert_eq!(metrics.snapshot().requests, 1);
    }
    provider.force_flush().unwrap();
    let active = exported(&exporter);
    assert_eq!(active.get("tidemq.sdk.admitted.bytes"), Some(&160.0));
    assert_eq!(active.get("tidemq.sdk.retained.batches"), Some(&2.0));
    assert_eq!(active.get("tidemq.sdk.active.deliveries"), Some(&2.0));
    drop(charges);
    drop(clients);
    provider.force_flush().unwrap();
    let finished = exported(&exporter);
    for (name, expected) in [
        ("tidemq.sdk.requests", 2.0),
        ("tidemq.sdk.retries", 2.0),
        ("tidemq.sdk.unknown", 2.0),
        ("tidemq.sdk.deliveries", 6.0),
        ("tidemq.sdk.acknowledged", 4.0),
        ("tidemq.sdk.operation.duration", 2.0),
        ("tidemq.sdk.admission.wait", 2.0),
        ("tidemq.sdk.admitted.bytes", 0.0),
        ("tidemq.sdk.retained.batches", 0.0),
        ("tidemq.sdk.active.deliveries", 0.0),
    ] {
        assert_eq!(finished.get(name), Some(&expected), "{name}");
    }
    assert_eq!(finished.len(), 10);
    provider.shutdown().unwrap();
}
