use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use crate::parse::parse_duration;
use clap::Args;
use color_eyre::Result;
use console::style;
use futures::StreamExt;
use serde_json::json;
use sinex_primitives::domain::HealthStatus;
use sinex_primitives::privacy::{load_private_mode_state, resolve_private_mode_state_dir};
use sinex_primitives::query::{
    EventQuery, EventQueryResult, PayloadFilter, SortDirection, SubscriptionFilter, TimeRange,
};
use sinex_primitives::rpc::ingestors::EmitStallThresholds;
use sinex_primitives::rpc::sources::{
    SourceReadiness, SourceReadinessStatus, SourcesReadinessListRequest,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{EventCardListView, EventCardView, ViewEnvelope};
use sinex_primitives::{
    RuntimeStatusSignal, RuntimeStatusSignalStatus, RuntimeStatusWarning, RuntimeTargetDescriptor,
    RuntimeTargetKind,
};
use std::path::Path;

use crate::client::{GatewayClient, gateway::SseClientMessage};

/// Quick system status check
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Check system status
    sinexctl status

    # Pipe to jq for scripting
    sinexctl status -f json | jq '.nodes.active'
")]
pub struct StatusCommand;

impl StatusCommand {
    pub async fn execute(
        &self,
        client: &GatewayClient,
        runtime_target: Option<&RuntimeTargetDescriptor>,
        format: OutputFormat,
    ) -> Result<()> {
        use sinex_primitives::RuntimeStatusSnapshot;

        let target = runtime_target
            .cloned()
            .unwrap_or_else(|| RuntimeTargetDescriptor {
                name: "unknown".to_string(),
                kind: RuntimeTargetKind::Unknown,
                ..Default::default()
            });

        let mut signals = Vec::new();
        let mut warnings = Vec::new();

        collect_gateway_and_health_signals(client, &target, &mut signals, &mut warnings).await;
        collect_node_and_dlq_signals(client, &mut signals, &mut warnings).await;
        let stalled_units =
            collect_source_and_stall_signals(client, &mut signals, &mut warnings).await;

        let snapshot = RuntimeStatusSnapshot {
            target,
            signals,
            warnings,
        };

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", serde_json::to_string_pretty(&snapshot)?);
            }
            OutputFormat::Yaml => {
                println!("{}", serde_yml::to_string(&snapshot)?);
            }
            OutputFormat::Table => {
                render_status_table(&snapshot, &stalled_units);
            }
        }

        Ok(())
    }
}

async fn collect_gateway_and_health_signals(
    client: &GatewayClient,
    target: &RuntimeTargetDescriptor,
    signals: &mut Vec<RuntimeStatusSignal>,
    warnings: &mut Vec<RuntimeStatusWarning>,
) {
    let gateway_signal = match client.version().await {
        Ok(version) => RuntimeStatusSignal {
            name: "gateway".to_string(),
            status: RuntimeStatusSignalStatus::Healthy,
            source: "gateway version probe".to_string(),
            message: Some(format!("v{version}")),
        },
        Err(e) => {
            warnings.push(RuntimeStatusWarning {
                source: "gateway".to_string(),
                message: format!("unreachable: {e}"),
            });
            RuntimeStatusSignal {
                name: "gateway".to_string(),
                status: RuntimeStatusSignalStatus::Unhealthy,
                source: "gateway version probe".to_string(),
                message: Some(e.to_string()),
            }
        }
    };
    signals.push(gateway_signal);

    match private_mode_signal(target.state.state_dir.as_deref()) {
        Ok(signal) => signals.push(signal),
        Err(warning) => {
            warnings.push(warning.clone());
            warnings.push(private_mode_unavailable_privacy_warning());
            signals.push(RuntimeStatusSignal {
                name: "private-mode".to_string(),
                status: RuntimeStatusSignalStatus::Unknown,
                source: "runtime private-mode state file".to_string(),
                message: Some(warning.message),
            });
            signals.push(private_mode_unavailable_privacy_signal());
        }
    }

    collect_health_probe_signals(client, signals, warnings).await;
}

async fn collect_health_probe_signals(
    client: &GatewayClient,
    signals: &mut Vec<RuntimeStatusSignal>,
    warnings: &mut Vec<RuntimeStatusWarning>,
) {
    match client.health().await {
        Ok(health) => {
            let db_status = if health.components.database.connected {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unhealthy
            };
            let db_msg = component_latency_message(
                health.components.database.latency_ms,
                health.components.database.detail.as_deref(),
            );
            signals.push(RuntimeStatusSignal {
                name: "db".to_string(),
                status: db_status,
                source: "system.health database probe".to_string(),
                message: db_msg,
            });

            let nats_status = if health.components.nats.connected {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unhealthy
            };
            let nats_msg = component_latency_message(
                health.components.nats.latency_ms,
                health.components.nats.detail.as_deref(),
            );
            signals.push(RuntimeStatusSignal {
                name: "nats".to_string(),
                status: nats_status,
                source: "system.health NATS active probe".to_string(),
                message: nats_msg,
            });

            let sse = &health.components.sse_confirmation;
            let sse_status = match sse.status {
                HealthStatus::Healthy => RuntimeStatusSignalStatus::Healthy,
                HealthStatus::Degraded => RuntimeStatusSignalStatus::Degraded,
                HealthStatus::Unhealthy | HealthStatus::Unknown => {
                    RuntimeStatusSignalStatus::Unhealthy
                }
            };
            signals.push(RuntimeStatusSignal {
                name: "confirmation-path".to_string(),
                status: sse_status,
                source: "system.health SSE confirmation probe".to_string(),
                message: sse.detail.clone(),
            });
        }
        Err(e) => {
            warnings.push(RuntimeStatusWarning {
                source: "system.health".to_string(),
                message: format!("unavailable: {e}"),
            });
            signals.push(RuntimeStatusSignal {
                name: "db".to_string(),
                status: RuntimeStatusSignalStatus::Unknown,
                source: "system.health database probe".to_string(),
                message: Some("health probe failed".to_string()),
            });
            signals.push(RuntimeStatusSignal {
                name: "nats".to_string(),
                status: RuntimeStatusSignalStatus::Unknown,
                source: "system.health NATS active probe".to_string(),
                message: Some("health probe failed".to_string()),
            });
        }
    }
}

fn component_latency_message(latency_ms: Option<f64>, detail: Option<&str>) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(latency) = latency_ms {
        parts.push(format!("{latency:.0}ms"));
    }
    if let Some(d) = detail {
        parts.push(d.to_string());
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

async fn collect_node_and_dlq_signals(
    client: &GatewayClient,
    signals: &mut Vec<RuntimeStatusSignal>,
    warnings: &mut Vec<RuntimeStatusWarning>,
) {
    match client.list_nodes(None).await {
        Ok(nodes) => {
            let total = nodes.len();
            let now = Timestamp::now();
            let healthy = nodes
                .iter()
                .filter(|n| {
                    n.last_heartbeat
                        .is_some_and(|hb| (now - hb).whole_seconds() < 60)
                })
                .count();
            let status = if healthy == total {
                RuntimeStatusSignalStatus::Healthy
            } else if healthy > 0 {
                RuntimeStatusSignalStatus::Degraded
            } else {
                RuntimeStatusSignalStatus::Unhealthy
            };
            signals.push(RuntimeStatusSignal {
                name: "nodes".to_string(),
                status,
                source: "gateway nodes probe".to_string(),
                message: Some(format!("{healthy}/{total} healthy")),
            });
        }
        Err(e) => {
            warnings.push(RuntimeStatusWarning {
                source: "nodes".to_string(),
                message: format!("error: {e}"),
            });
            signals.push(RuntimeStatusSignal {
                name: "nodes".to_string(),
                status: RuntimeStatusSignalStatus::Unknown,
                source: "gateway nodes probe".to_string(),
                message: Some(e.to_string()),
            });
        }
    }

    match client.dlq_list().await {
        Ok(stats) => {
            let status = if stats.total_messages == 0 {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Degraded
            };
            signals.push(RuntimeStatusSignal {
                name: "dlq".to_string(),
                status,
                source: "gateway dlq probe".to_string(),
                message: Some(format!("{} messages", stats.total_messages)),
            });
            if let Some(signal) = privacy_dlq_signal(stats.total_messages) {
                signals.push(signal);
            }
            if let Some(warning) = privacy_dlq_warning(stats.total_messages) {
                warnings.push(warning);
            }
        }
        Err(e) => {
            warnings.push(RuntimeStatusWarning {
                source: "dlq".to_string(),
                message: format!("error: {e}"),
            });
            warnings.push(RuntimeStatusWarning {
                source: "privacy.dlq".to_string(),
                message: format!("DLQ privacy posture unknown: {e}"),
            });
            signals.push(RuntimeStatusSignal {
                name: "dlq".to_string(),
                status: RuntimeStatusSignalStatus::Unknown,
                source: "gateway dlq probe".to_string(),
                message: Some(e.to_string()),
            });
            signals.push(RuntimeStatusSignal {
                name: "privacy-dlq".to_string(),
                status: RuntimeStatusSignalStatus::Unknown,
                source: "gateway dlq privacy probe".to_string(),
                message: Some("DLQ backlog could not be inspected".to_string()),
            });
        }
    }
}

/// Collect source readiness and emit-stall signals.
/// Returns the list of stalled units for table rendering.
async fn collect_source_and_stall_signals(
    client: &GatewayClient,
    signals: &mut Vec<RuntimeStatusSignal>,
    warnings: &mut Vec<RuntimeStatusWarning>,
) -> Vec<(
    sinex_primitives::rpc::ingestors::IngestorStatus,
    sinex_primitives::rpc::ingestors::EmitStallVerdict,
)> {
    match client
        .sources_readiness_list(SourcesReadinessListRequest::default())
        .await
    {
        Ok(response) => {
            let summary = summarize_source_readiness(&response.sources);
            signals.push(source_readiness_signal(&summary));
            if let Some(warning) = source_readiness_warning(&summary) {
                warnings.push(warning);
            }
        }
        Err(e) => {
            warnings.push(RuntimeStatusWarning {
                source: "sources.readiness".to_string(),
                message: format!("capture-gap readiness unavailable: {e}"),
            });
            signals.push(RuntimeStatusSignal {
                name: "source-readiness".to_string(),
                status: RuntimeStatusSignalStatus::Unknown,
                source: "sources.readiness capture-gap probe".to_string(),
                message: Some("capture-gap readiness could not be inspected".to_string()),
            });
        }
    }

    // Emit-rate stall detection for source units (issue #992).
    //
    // Heartbeats prove liveness, not productivity. Surface units that are
    // alive and past the uptime gate but have not emitted in `quiet_secs`.
    let thresholds = EmitStallThresholds::from_env_or_default();
    let window_secs = thresholds.quiet_secs.max(60);
    let stalled_units = match client.ingestors_status(window_secs, window_secs).await {
        Ok(resp) => {
            let now = resp.generated_at;
            resp.ingestors
                .into_iter()
                .filter_map(|ing| {
                    let verdict = ing.classify_emit_stall(thresholds, now);
                    verdict.is_degraded().then_some((ing, verdict))
                })
                .collect::<Vec<_>>()
        }
        Err(e) => {
            warnings.push(RuntimeStatusWarning {
                source: "ingestors.status".to_string(),
                message: format!("emit-rate stall check unavailable: {e}"),
            });
            Vec::new()
        }
    };

    if !stalled_units.is_empty() {
        signals.push(RuntimeStatusSignal {
            name: "emit-rate".to_string(),
            status: RuntimeStatusSignalStatus::Degraded,
            source: "ingestors.status emit-stall classifier".to_string(),
            message: Some(format!(
                "{} stalled source unit(s) (quiet ≥ {}s, uptime ≥ {}s)",
                stalled_units.len(),
                thresholds.quiet_secs,
                thresholds.uptime_gate_secs,
            )),
        });
    }

    stalled_units
}

fn render_status_table(
    snapshot: &sinex_primitives::RuntimeStatusSnapshot,
    stalled_units: &[(
        sinex_primitives::rpc::ingestors::IngestorStatus,
        sinex_primitives::rpc::ingestors::EmitStallVerdict,
    )],
) {
    println!("{}", style("System Status").bold().cyan());
    println!("{}", style("═".repeat(50)).dim());

    println!(
        "Target:  {} {}",
        style("●").cyan(),
        style(format!(
            "{} ({})",
            snapshot.target.name,
            runtime_target_kind_label(&snapshot.target.kind)
        ))
        .cyan()
    );
    if let Some(source) = &snapshot.target.source {
        println!("         {}", style(format!("source: {source}")).dim());
    }
    if let Some(path) = &snapshot.target.source_path {
        println!(
            "         {}",
            style(format!("descriptor: {}", path.display())).dim()
        );
    }

    for signal in &snapshot.signals {
        let color = match signal.status {
            RuntimeStatusSignalStatus::Healthy => style("●").green(),
            RuntimeStatusSignalStatus::Degraded => style("●").yellow(),
            RuntimeStatusSignalStatus::Unhealthy => style("●").red(),
            RuntimeStatusSignalStatus::Unknown => style("●").dim(),
            RuntimeStatusSignalStatus::Skipped => style("●").dim(),
            RuntimeStatusSignalStatus::Stale => style("●").yellow(),
        };

        let name = format!("{:width$}", signal.name, width = 8);
        let message = signal.message.as_deref().unwrap_or("");
        println!("{name}: {color} {message}");
    }

    for warning in &snapshot.warnings {
        println!("Warning [{}]: {}", warning.source, warning.message);
    }

    if !stalled_units.is_empty() {
        println!();
        println!("{}", style("Stalled source units").bold().yellow());
        println!("{}", style("─".repeat(50)).dim());
        for (ing, verdict) in stalled_units {
            let last = ing
                .last_output_at
                .map_or_else(|| "never".to_string(), |t| t.to_string());
            let uptime = ing.started_at.map_or_else(
                || "?".to_string(),
                |s| format!("{}s", (Timestamp::now() - s).whole_seconds()),
            );
            println!(
                "  {} {}  ({}, uptime {}, last_output {})",
                style("●").yellow(),
                ing.node_name,
                verdict.label(),
                uptime,
                last,
            );
        }
    }
}

fn private_mode_signal(
    state_dir: Option<&Path>,
) -> std::result::Result<RuntimeStatusSignal, RuntimeStatusWarning> {
    let state_dir = resolve_private_mode_state_dir(state_dir.map(Path::to_path_buf));
    let state = load_private_mode_state(&state_dir).map_err(|e| RuntimeStatusWarning {
        source: "private-mode".to_string(),
        message: format!("state unavailable at {}: {e}", state_dir.display()),
    })?;
    let scope = if state.affected_source_classes.is_empty() {
        "all".to_string()
    } else {
        state.affected_source_classes.join(",")
    };
    let status = if state.enabled {
        RuntimeStatusSignalStatus::Degraded
    } else {
        RuntimeStatusSignalStatus::Healthy
    };
    let message = if state.enabled {
        format!("enabled (scope: {scope}, actor: {})", state.actor)
    } else {
        "disabled".to_string()
    };

    Ok(RuntimeStatusSignal {
        name: "private-mode".to_string(),
        status,
        source: "runtime private-mode state file".to_string(),
        message: Some(message),
    })
}

fn privacy_dlq_signal(total_messages: u64) -> Option<RuntimeStatusSignal> {
    (total_messages > 0).then(|| RuntimeStatusSignal {
        name: "privacy-dlq".to_string(),
        status: RuntimeStatusSignalStatus::Degraded,
        source: "gateway dlq privacy probe".to_string(),
        message: Some(format!(
            "{total_messages} raw DLQ message(s) require sanitized inspection"
        )),
    })
}

fn privacy_dlq_warning(total_messages: u64) -> Option<RuntimeStatusWarning> {
    (total_messages > 0).then(|| RuntimeStatusWarning {
        source: "privacy.dlq".to_string(),
        message: "raw DLQ backlog present; inspect via redacted dlq.peek previews only".to_string(),
    })
}

fn private_mode_unavailable_privacy_signal() -> RuntimeStatusSignal {
    RuntimeStatusSignal {
        name: "privacy-private-mode".to_string(),
        status: RuntimeStatusSignalStatus::Degraded,
        source: "runtime private-mode fail-closed policy".to_string(),
        message: Some(
            "state unavailable; high-sensitivity live capture should fail closed".to_string(),
        ),
    }
}

fn private_mode_unavailable_privacy_warning() -> RuntimeStatusWarning {
    RuntimeStatusWarning {
        source: "privacy.private-mode".to_string(),
        message: "private-mode state unavailable; high-sensitivity live capture is suppressed or degraded until state is readable".to_string(),
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SourceReadinessSummary {
    pub total: usize,
    pub available: usize,
    pub disabled: usize,
    pub partial: usize,
    pub stale: usize,
    pub error: usize,
    pub missing: usize,
    pub blocked: usize,
    pub unknown: usize,
}

impl SourceReadinessSummary {
    fn degraded_count(self) -> usize {
        self.partial + self.stale + self.error + self.missing + self.blocked + self.unknown
    }

    fn blocking_count(self) -> usize {
        self.error + self.missing + self.blocked
    }

    fn status(self) -> RuntimeStatusSignalStatus {
        if self.total == 0 {
            RuntimeStatusSignalStatus::Unknown
        } else if self.blocking_count() > 0 {
            RuntimeStatusSignalStatus::Unhealthy
        } else if self.degraded_count() > 0 {
            RuntimeStatusSignalStatus::Degraded
        } else {
            RuntimeStatusSignalStatus::Healthy
        }
    }
}

#[must_use]
pub fn summarize_source_readiness(sources: &[SourceReadiness]) -> SourceReadinessSummary {
    let mut summary = SourceReadinessSummary {
        total: sources.len(),
        ..SourceReadinessSummary::default()
    };

    for source in sources {
        match source.status {
            SourceReadinessStatus::Available => summary.available += 1,
            SourceReadinessStatus::Partial => summary.partial += 1,
            SourceReadinessStatus::Stale => summary.stale += 1,
            SourceReadinessStatus::Error => summary.error += 1,
            SourceReadinessStatus::Missing => summary.missing += 1,
            SourceReadinessStatus::Blocked => summary.blocked += 1,
            SourceReadinessStatus::Disabled => summary.disabled += 1,
            SourceReadinessStatus::Unknown => summary.unknown += 1,
        }
    }

    summary
}

fn source_readiness_signal(summary: &SourceReadinessSummary) -> RuntimeStatusSignal {
    let message = if summary.total == 0 {
        "no source readiness records".to_string()
    } else if summary.degraded_count() == 0 {
        format!(
            "{} source(s) available, {} disabled",
            summary.available, summary.disabled
        )
    } else {
        format!(
            "{} degraded of {} source(s): partial={}, stale={}, error={}, missing={}, blocked={}, unknown={}",
            summary.degraded_count(),
            summary.total,
            summary.partial,
            summary.stale,
            summary.error,
            summary.missing,
            summary.blocked,
            summary.unknown
        )
    };

    RuntimeStatusSignal {
        name: "source-readiness".to_string(),
        status: summary.status(),
        source: "sources.readiness capture-gap probe".to_string(),
        message: Some(message),
    }
}

fn source_readiness_warning(summary: &SourceReadinessSummary) -> Option<RuntimeStatusWarning> {
    (summary.degraded_count() > 0).then(|| RuntimeStatusWarning {
        source: "sources.readiness".to_string(),
        message: format!(
            "capture readiness has {} degraded source(s); inspect sources readiness for caveats",
            summary.degraded_count()
        ),
    })
}

fn runtime_target_kind_label(kind: &RuntimeTargetKind) -> &'static str {
    match kind {
        RuntimeTargetKind::Unknown => "unknown",
        RuntimeTargetKind::DevCheckout => "dev_checkout",
        RuntimeTargetKind::DeployedHost => "deployed_host",
        RuntimeTargetKind::Vm => "vm",
        RuntimeTargetKind::Test => "test",
    }
}

#[cfg(test)]
mod status_tests {
    use super::*;
    use sinex_primitives::privacy::{
        PRIVATE_MODE_STATE_RELATIVE_PATH, RuntimePrivateModeState, save_private_mode_state,
    };
    use sinex_primitives::rpc::sources::SourceReadinessCost;
    use xtask::sandbox::prelude::sinex_test;

    fn readiness(status: SourceReadinessStatus) -> SourceReadiness {
        SourceReadiness {
            binding_id: None,
            source_family: "test".to_string(),
            source_unit_id: None,
            parser_id: None,
            source_identifier: format!("test.{status:?}"),
            status,
            cost: SourceReadinessCost::LocalFast,
            freshness_seconds: None,
            material_count: 1,
            parsed_event_count: Some(1),
            last_success_at: None,
            caveats: Vec::new(),
            evidence: serde_json::Value::Null,
        }
    }

    #[sinex_test]
    async fn private_mode_status_signal_defaults_disabled() -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let signal = private_mode_signal(Some(dir.path())).map_err(|warning| {
            color_eyre::eyre::eyre!("unexpected private-mode warning: {}", warning.message)
        })?;

        assert_eq!(signal.name, "private-mode");
        assert_eq!(signal.status, RuntimeStatusSignalStatus::Healthy);
        assert_eq!(signal.message.as_deref(), Some("disabled"));
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_status_signal_reports_enabled_scope() -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let state = RuntimePrivateModeState::enabled_by(
            "operator",
            vec!["desktop".to_string(), "weechat".to_string()],
            Timestamp::UNIX_EPOCH,
        );
        save_private_mode_state(dir.path(), &state)?;

        let signal = private_mode_signal(Some(dir.path())).map_err(|warning| {
            color_eyre::eyre::eyre!("unexpected private-mode warning: {}", warning.message)
        })?;

        assert_eq!(signal.status, RuntimeStatusSignalStatus::Degraded);
        assert_eq!(
            signal.message.as_deref(),
            Some("enabled (scope: desktop,weechat, actor: operator)")
        );
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_unavailable_status_reports_fail_closed_privacy_caveat()
    -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let state_path = dir.path().join(PRIVATE_MODE_STATE_RELATIVE_PATH);
        std::fs::create_dir_all(state_path.parent().ok_or_else(|| {
            color_eyre::eyre::eyre!("private-mode state path should have a parent")
        })?)?;
        std::fs::write(&state_path, b"{not-valid-json")?;

        let warning = private_mode_signal(Some(dir.path()))
            .expect_err("malformed private-mode state should be unavailable");
        let privacy_signal = private_mode_unavailable_privacy_signal();
        let privacy_warning = private_mode_unavailable_privacy_warning();

        assert_eq!(warning.source, "private-mode");
        assert_eq!(privacy_signal.name, "privacy-private-mode");
        assert_eq!(privacy_signal.status, RuntimeStatusSignalStatus::Degraded);
        assert!(
            privacy_signal
                .message
                .as_deref()
                .is_some_and(|message| message.contains("fail closed"))
        );
        assert_eq!(privacy_warning.source, "privacy.private-mode");
        assert!(privacy_warning.message.contains("high-sensitivity"));
        assert!(!privacy_warning.message.contains("payload"));
        assert!(!privacy_warning.message.contains("sample"));
        Ok(())
    }

    #[sinex_test]
    async fn privacy_dlq_status_is_quiet_when_backlog_empty() -> xtask::sandbox::TestResult<()> {
        assert!(privacy_dlq_signal(0).is_none());
        assert!(privacy_dlq_warning(0).is_none());
        Ok(())
    }

    #[sinex_test]
    async fn privacy_dlq_status_reports_sanitized_backlog() -> xtask::sandbox::TestResult<()> {
        let signal = privacy_dlq_signal(3)
            .ok_or_else(|| color_eyre::eyre::eyre!("privacy DLQ signal expected"))?;
        let warning = privacy_dlq_warning(3)
            .ok_or_else(|| color_eyre::eyre::eyre!("privacy DLQ warning expected"))?;

        assert_eq!(signal.name, "privacy-dlq");
        assert_eq!(signal.status, RuntimeStatusSignalStatus::Degraded);
        assert_eq!(
            signal.message.as_deref(),
            Some("3 raw DLQ message(s) require sanitized inspection")
        );
        assert_eq!(warning.source, "privacy.dlq");
        assert!(!warning.message.contains("payload"));
        assert!(!warning.message.contains("sample"));
        Ok(())
    }

    #[sinex_test]
    async fn source_readiness_status_reports_capture_gap_counts() -> xtask::sandbox::TestResult<()>
    {
        let summary = summarize_source_readiness(&[
            readiness(SourceReadinessStatus::Available),
            readiness(SourceReadinessStatus::Disabled),
            readiness(SourceReadinessStatus::Partial),
            readiness(SourceReadinessStatus::Stale),
            readiness(SourceReadinessStatus::Error),
            readiness(SourceReadinessStatus::Missing),
            readiness(SourceReadinessStatus::Blocked),
            readiness(SourceReadinessStatus::Unknown),
        ]);
        let signal = source_readiness_signal(&summary);
        let warning = source_readiness_warning(&summary)
            .ok_or_else(|| color_eyre::eyre::eyre!("source readiness warning expected"))?;

        assert_eq!(summary.degraded_count(), 6);
        assert_eq!(summary.blocking_count(), 3);
        assert_eq!(signal.name, "source-readiness");
        assert_eq!(signal.status, RuntimeStatusSignalStatus::Unhealthy);
        let message = signal.message.as_deref().ok_or_else(|| {
            color_eyre::eyre::eyre!("source readiness signal should explain counts")
        })?;
        assert!(message.contains("partial=1"));
        assert!(message.contains("stale=1"));
        assert!(message.contains("error=1"));
        assert!(message.contains("missing=1"));
        assert!(message.contains("blocked=1"));
        assert!(message.contains("unknown=1"));
        assert_eq!(warning.source, "sources.readiness");
        assert!(warning.message.contains("capture readiness"));
        Ok(())
    }

    #[sinex_test]
    async fn source_readiness_status_is_healthy_when_available_or_disabled()
    -> xtask::sandbox::TestResult<()> {
        let summary = summarize_source_readiness(&[
            readiness(SourceReadinessStatus::Available),
            readiness(SourceReadinessStatus::Disabled),
        ]);
        let signal = source_readiness_signal(&summary);

        assert_eq!(signal.status, RuntimeStatusSignalStatus::Healthy);
        assert!(source_readiness_warning(&summary).is_none());
        assert_eq!(
            signal.message.as_deref(),
            Some("1 source(s) available, 1 disabled")
        );
        Ok(())
    }
}

/// Show recent events
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Last 20 events
    sinexctl recent

    # Last 50 events
    sinexctl recent -n 50

    # Last 100 events from terminal
    sinexctl recent -n 100 --source shell.atuin
")]
pub struct RecentCommand {
    /// Number of events to show
    #[arg(short = 'n', long, default_value = "20")]
    limit: i32,

    /// Time window (default: last hour)
    #[arg(long, short = 's', default_value = "1h")]
    since: String,

    /// Filter by source
    #[arg(long)]
    source: Option<String>,
}

impl RecentCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let since = parse_duration(&self.since)?;
        let query = EventQuery {
            sources: self
                .source
                .clone()
                .map(|s| vec![s.into()])
                .unwrap_or_default(),
            event_types: vec![],
            time_range: TimeRange::new(Some(Timestamp::now() - since), None).ok(),
            payload: None,
            limit: i64::from(self.limit),
            direction: SortDirection::Desc,
            ..Default::default()
        };

        let events = match client.query_events(query).await? {
            EventQueryResult::Events { events, .. } => events,
            _ => vec![],
        };
        let event_cards = EventCardListView::from_query_events(&events);
        let envelope = ViewEnvelope::new("sinexctl.recent", event_cards).with_query_echo(json!({
            "since": self.since,
            "limit": self.limit,
            "source": self.source,
        }));

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&envelope)?);
                return Ok(());
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&envelope)?);
                return Ok(());
            }
            OutputFormat::Table => {}
        }

        if events.is_empty() {
            println!("No events found in the last {}", self.since);
            return Ok(());
        }

        println!(
            "{} events (last {})",
            style(events.len()).bold(),
            self.since
        );
        println!("{}", style("─".repeat(80)).dim());

        for card in &envelope.payload.cards {
            println!("{}", format_event_card_line(card));
        }

        Ok(())
    }
}

fn format_event_card_line(card: &EventCardView) -> String {
    let timestamp = card.timestamp.original.map_or_else(
        || "unknown".to_string(),
        |ts| {
            ts.format(time::macros::format_description!(
                "[hour]:[minute]:[second]"
            ))
            .unwrap_or_else(|_| "invalid".to_string())
        },
    );
    let source = style(card.source.raw.as_str()).cyan();
    let event_type = style(card.event_type.as_str()).yellow();
    let summary = truncate_chars(&card.summary, 60);

    format!(
        "{} [{}] {} - {}",
        style(timestamp).dim(),
        source,
        event_type,
        summary
    )
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let end = input
        .char_indices()
        .nth(keep)
        .map_or(input.len(), |(index, _)| index);
    format!("{}...", &input[..end])
}

/// Show recent errors only
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Recent errors
    sinexctl errors

    # Last 100 errors
    sinexctl errors -n 100
")]
pub struct ErrorsCommand {
    /// Number of errors to show
    #[arg(short = 'n', long, default_value = "50")]
    limit: i32,

    /// Time window
    #[arg(long, short = 's', default_value = "24h")]
    since: String,
}

impl ErrorsCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let since = parse_duration(&self.since)?;

        // Search for error-related events
        let query = EventQuery {
            sources: vec![],
            event_types: vec![],
            time_range: TimeRange::new(Some(Timestamp::now() - since), None).ok(),
            payload: Some(PayloadFilter::TextSearch {
                text: "error OR failed OR exception OR panic".to_string(),
            }),
            limit: i64::from(self.limit),
            direction: SortDirection::Desc,
            ..Default::default()
        };

        let events = match client.query_events(query).await? {
            EventQueryResult::Events { events, .. } => events,
            _ => vec![],
        };

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                let payload = json!({
                    "since": self.since,
                    "count": events.len(),
                    "events": events,
                });
                println!("{}", format_json(&payload)?);
                return Ok(());
            }
            OutputFormat::Yaml => {
                let payload = json!({
                    "since": self.since,
                    "count": events.len(),
                    "events": events,
                });
                println!("{}", format_yaml(&payload)?);
                return Ok(());
            }
            OutputFormat::Table => {}
        }

        if events.is_empty() {
            println!(
                "{} No errors found in the last {}",
                style("✓").green(),
                self.since
            );
            return Ok(());
        }

        println!(
            "{} {} errors (last {})",
            style("⚠").yellow(),
            style(events.len()).bold(),
            self.since
        );
        println!("{}", style("─".repeat(80)).dim());

        for result_event in &events {
            let timestamp = result_event.event.ts_orig.map_or_else(
                || "unknown".to_string(),
                |ts| {
                    ts.format(time::macros::format_description!(
                        "[year]-[month]-[day] [hour]:[minute]:[second]"
                    ))
                    .unwrap_or_else(|_| "invalid".to_string())
                },
            );
            let source = style(result_event.event.source.as_str()).cyan();
            let event_type = style(result_event.event.event_type.as_str()).red();
            let snippet = result_event.snippet.as_deref().unwrap_or("");
            let snippet_display = if snippet.len() > 60 {
                format!("{}...", &snippet[..57])
            } else {
                snippet.to_string()
            };

            println!(
                "{} [{}] {} - {}",
                style(timestamp).dim(),
                source,
                event_type,
                snippet_display
            );
        }

        Ok(())
    }
}

/// Watch events in real-time
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Watch all events
    sinexctl watch

    # Watch events from terminal ingestor
    sinexctl watch --source shell.atuin

    # Watch process execution events
    sinexctl watch --event-type process.started
")]
pub struct WatchCommand {
    /// Filter by source
    #[arg(long)]
    source: Option<String>,

    /// Filter by event type
    #[arg(long)]
    event_type: Option<String>,
}

impl WatchCommand {
    /// `--format json` emits one newline-delimited JSON object per stream
    /// message (`{"kind":"event"|"gap"|"error",...}`). `--format yaml` emits
    /// each message as a YAML document separated by `---`.
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let filter = SubscriptionFilter {
            sources: self
                .source
                .clone()
                .map(|s| vec![s.into()])
                .unwrap_or_default(),
            event_types: self
                .event_type
                .clone()
                .map(|t| vec![t.into()])
                .unwrap_or_default(),
            ..Default::default()
        };

        let table_mode = matches!(format, OutputFormat::Table);

        if table_mode {
            println!(
                "{}",
                style("Connecting to event stream... (Ctrl+C to stop)").dim()
            );
        }

        let mut stream = client.subscribe_events(filter).await?;

        if table_mode {
            println!("{}", style("─".repeat(80)).dim());
        }

        while let Some(result) = stream.next().await {
            match result {
                Ok(SseClientMessage::Event { event }) => match format {
                    OutputFormat::Json | OutputFormat::Dot => {
                        let line = json!({ "kind": "event", "event": event });
                        println!("{}", serde_json::to_string(&line)?);
                    }
                    OutputFormat::Yaml => {
                        let doc = json!({ "kind": "event", "event": event });
                        println!("---");
                        print!("{}", format_yaml(&doc)?);
                    }
                    OutputFormat::Table => {
                        let timestamp = event.ts_orig.map_or_else(
                            || "unknown".to_string(),
                            |ts| {
                                ts.format(time::macros::format_description!(
                                    "[hour]:[minute]:[second]"
                                ))
                                .unwrap_or_else(|_| "invalid".to_string())
                            },
                        );
                        let source = style(event.source.as_str()).cyan();
                        let event_type = style(event.event_type.as_str()).yellow();

                        let summary = event
                            .payload
                            .as_object()
                            .and_then(|obj| {
                                obj.get("path")
                                    .or(obj.get("command_string"))
                                    .or(obj.get("window_title"))
                                    .or(obj.get("command"))
                                    .or(obj.get("title"))
                                    .and_then(|v| v.as_str())
                            })
                            .unwrap_or("");
                        let summary_display = if summary.len() > 60 {
                            format!("{}...", &summary[..57])
                        } else {
                            summary.to_string()
                        };

                        println!(
                            "{} [{}] {} {}",
                            style(timestamp).dim(),
                            source,
                            event_type,
                            summary_display
                        );
                    }
                },
                Ok(SseClientMessage::Gap { dropped, .. }) => match format {
                    OutputFormat::Json | OutputFormat::Dot => {
                        let line = json!({ "kind": "gap", "dropped": dropped });
                        println!("{}", serde_json::to_string(&line)?);
                    }
                    OutputFormat::Yaml => {
                        let doc = json!({ "kind": "gap", "dropped": dropped });
                        println!("---");
                        print!("{}", format_yaml(&doc)?);
                    }
                    OutputFormat::Table => {
                        eprintln!(
                            "{}",
                            style(format!("⚠ {dropped} events dropped (slow consumer)")).yellow()
                        );
                    }
                },
                Ok(SseClientMessage::Heartbeat) => {
                    // Silent keepalive in all formats.
                }
                Ok(SseClientMessage::Error { code, message }) => {
                    match format {
                        OutputFormat::Json | OutputFormat::Dot => {
                            let line = json!({ "kind": "error", "code": code, "message": message });
                            println!("{}", serde_json::to_string(&line)?);
                        }
                        OutputFormat::Yaml => {
                            let doc = json!({ "kind": "error", "code": code, "message": message });
                            println!("---");
                            print!("{}", format_yaml(&doc)?);
                        }
                        OutputFormat::Table => {
                            eprintln!(
                                "{}",
                                style(format!("Stream error [{code}]: {message}")).red()
                            );
                        }
                    }
                    break;
                }
                Err(e) => {
                    match format {
                        OutputFormat::Json | OutputFormat::Dot => {
                            let line = json!({ "kind": "error", "message": e.to_string() });
                            println!("{}", serde_json::to_string(&line)?);
                        }
                        OutputFormat::Yaml => {
                            let doc = json!({ "kind": "error", "message": e.to_string() });
                            println!("---");
                            print!("{}", format_yaml(&doc)?);
                        }
                        OutputFormat::Table => {
                            eprintln!("{}", style(format!("Stream error: {e}")).red());
                        }
                    }
                    break;
                }
            }
        }

        if table_mode {
            println!("{}", style("Event stream ended.").dim());
        }
        Ok(())
    }
}
