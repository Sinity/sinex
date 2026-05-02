use super::output::{ServiceRunStatus, ServiceStatus};
use crate::infra::probe::{NatsProbe, PostgresProbe};
use crate::infra::stack::StackConfig;
use crate::runtime_metrics::{IngestdStatus, RuntimeMetrics};
use crate::runtime_target::{checkout_status_snapshot, signal, warning};
use color_eyre::eyre::{Result, WrapErr};
use sinex_primitives::{
    RuntimeStatusSignalStatus, RuntimeStatusSnapshot, RuntimeTargetDescriptor, RuntimeTargetKind,
};
use std::any::Any;

pub(super) fn service_status_from_active_job(
    service_name: &str,
    job: &crate::jobs::Job,
) -> ServiceStatus {
    ServiceStatus {
        name: service_name.to_string(),
        status: ServiceRunStatus::Running,
        probe: "background_job",
        pid: job.pid,
        message: None,
    }
}

pub(super) fn active_job_for_service<'a>(
    service_name: &str,
    active_jobs: &'a [crate::jobs::Job],
) -> Option<&'a crate::jobs::Job> {
    active_jobs.iter().find(|job| {
        job.is_alive()
            && std::path::Path::new(&job.command)
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|binary| binary == service_name)
    })
}

pub(super) fn gateway_service_status_from_readiness(
    readiness: crate::commands::doctor::DeploymentReadinessItem,
    pid: Option<u32>,
    force_probe: bool,
) -> ServiceStatus {
    let (status, message) = match readiness.status.as_str() {
        "pass" => (ServiceRunStatus::Running, None),
        "fail" => {
            if force_probe {
                (
                    ServiceRunStatus::Unknown,
                    Some(format!(
                        "gateway process is alive but readiness probe failed: {}",
                        readiness.description
                    )),
                )
            } else {
                (ServiceRunStatus::Stopped, Some(readiness.description))
            }
        }
        "skip" => {
            if force_probe {
                (
                    ServiceRunStatus::Unknown,
                    Some(format!(
                        "gateway process is alive but readiness probe skipped unexpectedly: {}",
                        readiness.description
                    )),
                )
            } else {
                (ServiceRunStatus::Skipped, Some(readiness.description))
            }
        }
        other => (
            ServiceRunStatus::Unknown,
            Some(format!(
                "gateway readiness probe returned unexpected status `{other}`: {}",
                readiness.description
            )),
        ),
    };

    ServiceStatus {
        name: "sinex-gateway".to_string(),
        status,
        probe: "gateway_ready_http",
        pid,
        message,
    }
}

pub(super) async fn probe_gateway_service_status(
    gateway_url: Option<&str>,
    force_probe: bool,
    pid: Option<u32>,
) -> ServiceStatus {
    let readiness = crate::commands::doctor::check_gateway_ready(gateway_url, None).await;
    gateway_service_status_from_readiness(readiness, pid, force_probe)
}

pub(super) async fn collect_core_service_statuses(
    gateway_url: Option<&str>,
    runtime_metrics: Option<&RuntimeMetrics>,
    active_jobs: &[crate::jobs::Job],
) -> Vec<ServiceStatus> {
    let ingestd = active_job_for_service("sinex-ingestd", active_jobs).map_or_else(
        || ingestd_service_status_from_runtime_metrics(runtime_metrics),
        |job| service_status_from_active_job("sinex-ingestd", job),
    );
    let gateway_process = active_job_for_service("sinex-gateway", active_jobs).map_or_else(
        || ServiceStatus {
            name: "sinex-gateway".to_string(),
            status: ServiceRunStatus::Stopped,
            probe: "checkout_local",
            pid: None,
            message: Some("no active checkout-local gateway job is tracked".to_string()),
        },
        |job| service_status_from_active_job("sinex-gateway", job),
    );
    let gateway_force_probe = matches!(gateway_process.status, ServiceRunStatus::Running);

    vec![
        probe_gateway_service_status(gateway_url, gateway_force_probe, gateway_process.pid).await,
        ingestd,
    ]
}

pub(super) fn resolve_runtime_metrics_database_url(
    database_url: Option<&str>,
) -> Result<Option<String>> {
    if let Some(url) = database_url {
        return Ok(Some(url.to_string()));
    }

    let stack_config = StackConfig::for_current_checkout()
        .wrap_err("failed to load checkout stack config for runtime metrics")?;
    Ok(Some(stack_config.database_url()))
}

pub(super) fn ingestd_service_status_from_runtime_metrics(
    runtime_metrics: Option<&RuntimeMetrics>,
) -> ServiceStatus {
    let (status, message) = match runtime_metrics {
        Some(metrics) => match metrics.ingestd_status {
            IngestdStatus::Healthy => (ServiceRunStatus::Running, None),
            IngestdStatus::Down => (
                ServiceRunStatus::Stopped,
                Some(
                    "no checkout-local ingestd heartbeat found in the local runtime database"
                        .to_string(),
                ),
            ),
            IngestdStatus::Stale => (
                ServiceRunStatus::Unknown,
                Some(
                    "checkout-local ingestd heartbeat is stale in the local runtime database"
                        .to_string(),
                ),
            ),
            IngestdStatus::Unknown => (
                ServiceRunStatus::Unknown,
                metrics
                    .query_error
                    .clone()
                    .or_else(|| Some("checkout-local ingestd status is unavailable".to_string())),
            ),
        },
        None => (
            ServiceRunStatus::Unknown,
            Some("checkout-local runtime database target is unavailable".to_string()),
        ),
    };

    ServiceStatus {
        name: "sinex-ingestd".to_string(),
        status,
        probe: "runtime_metrics",
        pid: None,
        message,
    }
}

fn collect_runtime_metrics(runtime_db_url: Result<Option<String>>) -> Option<RuntimeMetrics> {
    match runtime_db_url {
        Ok(Some(url)) => match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => Some(rt.block_on(crate::runtime_metrics::query_runtime_metrics(&url))),
            Err(error) => Some(RuntimeMetrics::query_failure(format!(
                "failed to build runtime probe executor: {error}"
            ))),
        },
        Ok(None) => None,
        Err(error) => Some(RuntimeMetrics::query_failure(error.to_string())),
    }
}

pub(super) fn collect_runtime_metrics_if_postgres_ready(
    pg_probe: &PostgresProbe,
    runtime_db_url: Result<Option<String>>,
    target_kind: &RuntimeTargetKind,
) -> Option<RuntimeMetrics> {
    // Only gate on the local dev-stack probe when the runtime target IS that
    // local stack.  For deployed or VM targets the runtime database is a
    // separate system; skipping it because the local dev Postgres is not
    // running would silently suppress valid runtime telemetry.
    if *target_kind == RuntimeTargetKind::DevCheckout && !pg_probe.ready() {
        return Some(RuntimeMetrics::unavailable());
    }
    collect_runtime_metrics(runtime_db_url)
}

pub(super) fn describe_thread_panic(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else {
        "non-string panic payload".to_string()
    }
}

#[cfg(test)]
pub(super) fn recover_runtime_metrics_thread(
    result: std::thread::Result<Option<RuntimeMetrics>>,
) -> Option<RuntimeMetrics> {
    match result {
        Ok(metrics) => metrics,
        Err(payload) => Some(RuntimeMetrics::query_failure(format!(
            "runtime metrics collection thread panicked: {}",
            describe_thread_panic(&*payload)
        ))),
    }
}

fn service_signal_status(status: ServiceRunStatus) -> RuntimeStatusSignalStatus {
    match status {
        ServiceRunStatus::Running => RuntimeStatusSignalStatus::Healthy,
        ServiceRunStatus::Stopped => RuntimeStatusSignalStatus::Unhealthy,
        ServiceRunStatus::Skipped => RuntimeStatusSignalStatus::Skipped,
        ServiceRunStatus::Unknown => RuntimeStatusSignalStatus::Unknown,
    }
}

pub(super) fn build_runtime_status_snapshot(
    target: &RuntimeTargetDescriptor,
    pg_probe: &PostgresProbe,
    nats_probe: &NatsProbe,
    services: &[ServiceStatus],
    runtime_metrics: Option<&RuntimeMetrics>,
    warnings: &[String],
) -> RuntimeStatusSnapshot {
    let mut signals = vec![
        signal(
            "postgres",
            if pg_probe.ready() {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unhealthy
            },
            "checkout-local postgres probe",
            pg_probe.message.clone(),
        ),
        signal(
            "nats",
            if nats_probe.ready() {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unhealthy
            },
            "checkout-local nats probe",
            nats_probe.message.clone(),
        ),
    ];

    for service in services {
        signals.push(signal(
            service.name.clone(),
            service_signal_status(service.status),
            service.probe,
            service.message.clone(),
        ));
    }

    if let Some(metrics) = runtime_metrics {
        signals.push(signal(
            "ingestd_heartbeat",
            match metrics.ingestd_status {
                IngestdStatus::Healthy => RuntimeStatusSignalStatus::Healthy,
                IngestdStatus::Stale => RuntimeStatusSignalStatus::Stale,
                IngestdStatus::Down => RuntimeStatusSignalStatus::Unhealthy,
                IngestdStatus::Unknown => RuntimeStatusSignalStatus::Unknown,
            },
            "checkout-local runtime database telemetry",
            metrics
                .last_heartbeat_age_secs
                .map(|age| format!("heartbeat {age}s ago"))
                .or_else(|| metrics.query_error.clone()),
        ));

        signals.push(signal(
            "consumer_lag",
            if metrics.consumer_lag_is_stale() {
                RuntimeStatusSignalStatus::Stale
            } else if metrics.fresh_consumer_lag_pending().is_some() {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unknown
            },
            "checkout-local runtime database telemetry",
            metrics
                .fresh_consumer_lag_pending()
                .map(|pending| format!("{pending:.0} pending"))
                .or_else(|| metrics.consumer_lag_stale_note()),
        ));

        signals.push(signal(
            "batch_latency",
            if metrics.batch_latency_is_stale() {
                RuntimeStatusSignalStatus::Stale
            } else if metrics.fresh_batch_latency_ms().is_some() {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unknown
            },
            "checkout-local runtime database telemetry",
            metrics
                .fresh_batch_latency_ms()
                .map(|latency| format!("{latency:.0}ms"))
                .or_else(|| metrics.batch_latency_stale_note()),
        ));
    }

    let attributed_warnings = warnings
        .iter()
        .map(|message| warning("xtask status", message.clone()))
        .collect();

    checkout_status_snapshot(target.clone(), signals, attributed_warnings)
}

pub(super) fn runtime_query_error_message(metrics: &RuntimeMetrics) -> Option<String> {
    metrics
        .query_error
        .as_ref()
        .map(|error| format!("Runtime metrics query failed: {error}"))
}
