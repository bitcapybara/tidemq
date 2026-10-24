use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

#[derive(Debug)]
pub(super) struct Instruments {
    pub requests: Counter<u64>,
    pub retries: Counter<u64>,
    pub unknown: Counter<u64>,
    pub deliveries: Counter<u64>,
    pub acknowledged: Counter<u64>,
    pub operation_duration: Histogram<f64>,
    pub admission_wait: Histogram<f64>,
    admitted_bytes: UpDownCounter<i64>,
    retained_batches: UpDownCounter<i64>,
    active_deliveries: UpDownCounter<i64>,
}

impl Instruments {
    pub fn new(meter: &Meter) -> Self {
        let counter = |name: &'static str, description: &'static str| {
            meter
                .u64_counter(name)
                .with_description(description)
                .build()
        };
        let gauge = |name: &'static str, description: &'static str, unit: &'static str| {
            meter
                .i64_up_down_counter(name)
                .with_description(description)
                .with_unit(unit)
                .build()
        };
        Self {
            requests: counter("tidemq.sdk.requests", "SDK RPC attempts."),
            retries: counter("tidemq.sdk.retries", "SDK transport retries."),
            unknown: counter(
                "tidemq.sdk.unknown",
                "SDK operations with an unknown result.",
            ),
            deliveries: counter(
                "tidemq.sdk.deliveries",
                "Deliveries admitted to SDK queues.",
            ),
            acknowledged: counter("tidemq.sdk.acknowledged", "Tokens durably acknowledged."),
            operation_duration: histogram(
                meter,
                "tidemq.sdk.operation.duration",
                "SDK operation duration including retries and local queueing; excludes user handler time.",
            ),
            admission_wait: histogram(
                meter,
                "tidemq.sdk.admission.wait",
                "Time waiting for SDK admission.",
            ),
            admitted_bytes: gauge(
                "tidemq.sdk.admitted.bytes",
                "Charged bytes held by pending batches and deliveries.",
                "By",
            ),
            retained_batches: gauge(
                "tidemq.sdk.retained.batches",
                "Retained SDK batch objects.",
                "{batch}",
            ),
            active_deliveries: gauge(
                "tidemq.sdk.active.deliveries",
                "Retained SDK delivery objects.",
                "{delivery}",
            ),
        }
    }

    pub fn charge(&self, bytes: u32, delivery: bool, acquire: bool) {
        let direction = if acquire { 1 } else { -1 };
        self.admitted_bytes.add(direction * i64::from(bytes), &[]);
        let objects = if delivery {
            &self.active_deliveries
        } else {
            &self.retained_batches
        };
        objects.add(direction, &[]);
    }
    pub fn release_bytes(&self, bytes: u32) {
        self.admitted_bytes.add(-i64::from(bytes), &[]);
    }
}

fn histogram(meter: &Meter, name: &'static str, description: &'static str) -> Histogram<f64> {
    meter
        .f64_histogram(name)
        .with_description(description)
        .with_unit("s")
        .with_boundaries(vec![
            0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 300.0,
        ])
        .build()
}

#[cfg(test)]
mod tests;
