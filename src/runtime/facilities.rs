use std::sync::Arc;

use crate::broker::ProducerMetrics;
use crate::config::Config;
use crate::observability::metrics::Metrics;
use crate::ownership::OwnershipMetrics;
use crate::server::DispatchMetrics;

use super::{Error, Result};

pub(super) struct Facilities {
    pub metrics: Metrics,
    pub dispatch: Arc<DispatchMetrics>,
    pub runtime: super::telemetry::RuntimeMetrics,
    pub producer: ProducerMetrics,
    pub ownership: OwnershipMetrics,
    pub quota: crate::quota::Quota,
    pub repair: crate::scaling::RepairBudget,
}

impl Facilities {
    pub fn new(config: &Config) -> Result<Self> {
        let mut metrics = Metrics::default();
        let dispatch = Arc::new(DispatchMetrics::default());
        dispatch.register(metrics.registry_mut());
        let runtime = super::telemetry::RuntimeMetrics::default();
        metrics
            .registry_mut()
            .sub_registry_with_prefix("tidemq_runtime")
            .register_collector(Box::new(runtime.clone()));
        let producer = ProducerMetrics::default();
        metrics
            .registry_mut()
            .sub_registry_with_prefix("tidemq_producer")
            .register_collector(Box::new(producer.clone()));
        let ownership = OwnershipMetrics::register(metrics.registry_mut());
        let quota_metrics = crate::quota::Metrics::default();
        quota_metrics.register(metrics.registry_mut());
        let quota = crate::quota::Quota::new(config.runtime.capacity, quota_metrics)
            .map_err(|source| Error::operation("initialize capacity admission", source))?;
        let repair = crate::scaling::RepairBudget::new(crate::scaling::RepairLimits::default())
            .map_err(|source| Error::operation("initialize node repair budget", source))?;
        Ok(Self {
            metrics,
            dispatch,
            runtime,
            producer,
            ownership,
            quota,
            repair,
        })
    }
}
