use super::*;
use sinex_primitives::rpc::sources::{
    SourceCoverageEntry, SourcesCoverageRequest, SourcesRemediationPlanRequest,
    SourcesRemediationPlanResponse,
};
use sinex_primitives::runtime_pressure::RuntimePressureLevel;
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState,
    OpsCatchupConsumerSignalView, OpsCatchupDlqSignalView,
    OpsCatchupMaterialRemediationCandidateView, OpsCatchupMaterialRemediationView,
    OpsCatchupReadinessView, OpsCatchupRuntimeSignalView, OpsCatchupSourceMaterialSignalView,
    OpsCatchupStreamSignalView, ViewEnvelope,
};
use tabled::{builder::Builder, settings::Style};

const CATCHUP_REMEDIATION_TOP_LIMIT: i64 = 5;

/// Read-only catch-up/readiness view over cheap runtime surfaces.
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    sinexctl ops catchup status
    sinexctl ops catchup status --format json
")]
pub enum CatchupCommands {
    /// Show whether the runtime is caught up enough for live demos/dogfooding.
    Status {
        /// Runtime heartbeat staleness threshold in seconds.
        #[arg(long, default_value_t = 300)]
        stale_after_secs: u64,
        /// Include recent stream pressure telemetry when available.
        #[arg(long, default_value_t = true)]
        include_streams: bool,
        /// Include shadow consumer backlog when available.
        #[arg(long, default_value_t = true)]
        include_consumers: bool,
    },
}

impl CatchupCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Status {
                stale_after_secs,
                include_streams,
                include_consumers,
            } => {
                let view =
                    build_catchup_readiness(client, *stale_after_secs, *include_streams, *include_consumers)
                        .await?;
                let envelope = ViewEnvelope::new("sinexctl.ops.catchup.status", view.clone())
                    .with_query_echo(serde_json::json!({
                        "stale_after_secs": stale_after_secs,
                        "include_streams": include_streams,
                        "include_consumers": include_consumers,
                    }));

                if let Some(output) = render_envelope(&envelope, &[view.clone()], format)? {
                    print_machine_output(&output);
                    return Ok(());
                }

                println!("{}", format_catchup_table(&view));
            }
        }
        Ok(())
    }
}

async fn build_catchup_readiness(
    client: &GatewayClient,
    stale_after_secs: u64,
    include_streams: bool,
    include_consumers: bool,
) -> Result<OpsCatchupReadinessView> {
    let dlq = client.dlq_list().await?;
    let coverage = client.sources_coverage(SourcesCoverageRequest {}).await?;
    let runtime = client.runtime_health(stale_after_secs).await?;

    let dlq_signal = OpsCatchupDlqSignalView {
        total_messages: dlq.total_messages,
        total_bytes: dlq.total_bytes,
        pressure_level: dlq.pressure_level,
        pending_sequence_span: dlq.pending_sequence_span,
        recommended_action: dlq.recommended_action,
        action_reason: dlq.action_reason,
    };
    let source_signal = source_material_signal(&coverage.sources);
    let mut material_remediation_caveat = None;
    let material_remediation = match client
        .sources_remediation_plan(SourcesRemediationPlanRequest {
            source_identifier: None,
            limit: Some(CATCHUP_REMEDIATION_TOP_LIMIT),
            offset: Some(0),
            sort: Some("event-count".to_string()),
            include_empty: false,
        })
        .await
    {
        Ok(response) => Some(material_remediation_signal(&response)),
        Err(error) => {
            material_remediation_caveat = Some(format!(
                "source-material remediation details unavailable via sources.remediation_plan: {error}"
            ));
            None
        }
    };
    let runtime_signal = OpsCatchupRuntimeSignalView {
        active_count: runtime.active_count,
        inactive_count: runtime.inactive_count,
        unique_modules: runtime.unique_modules,
        active_run_count: runtime.active_run_count,
        oldest_heartbeat: runtime.oldest_heartbeat.map(|ts| ts.to_string()),
        pressure_level: runtime_pressure(runtime.inactive_count),
    };

    let mut pressure = RuntimePressureLevel::Nominal
        .strongest(dlq_signal.pressure_level)
        .strongest(source_signal.pressure_level)
        .strongest(runtime_signal.pressure_level);
    let mut view = OpsCatchupReadinessView::new(
        pressure,
        catchup_summary(
            &dlq_signal,
            &source_signal,
            material_remediation.as_ref(),
            &runtime_signal,
        ),
        dlq_signal,
        source_signal,
        runtime_signal,
    );
    view.material_remediation = material_remediation;
    if let Some(caveat) = material_remediation_caveat {
        view.caveats.push(caveat);
    }

    if include_streams {
        match client
            .telemetry_stream_stats(None, None, Some(20))
            .await
        {
            Ok(buckets) => {
                view.streams = buckets
                    .into_iter()
                    .filter_map(|bucket| {
                        let stream_name = bucket.stream_name?;
                        let pressure_level = bucket
                            .max_pressure_level
                            .map(stream_pressure)
                            .unwrap_or(RuntimePressureLevel::Unknown);
                        Some(OpsCatchupStreamSignalView {
                            stream_name,
                            max_messages: bucket.max_messages,
                            max_fill_pct: bucket.max_fill_pct,
                            pressure_level,
                            limiting_dimension: bucket
                                .limiting_dimension
                                .map(|dimension| dimension.as_str().to_string()),
                        })
                    })
                    .collect();
                for stream in &view.streams {
                    pressure = pressure.strongest(stream.pressure_level);
                }
            }
            Err(error) => view.caveats.push(format!(
                "stream pressure telemetry unavailable via telemetry.stream_stats: {error}"
            )),
        }
    }

    if include_consumers {
        match client.shadow_list(None).await {
            Ok(response) => {
                view.shadow_consumers = response
                    .consumers
                    .into_iter()
                    .map(|consumer| OpsCatchupConsumerSignalView {
                        pressure_level: consumer_pressure(consumer.num_pending),
                        consumer_name: consumer.consumer_name,
                        stream_name: consumer.stream_name,
                        subject_filter: consumer.subject_filter,
                        pending_messages: consumer.num_pending,
                    })
                    .collect();
                for consumer in &view.shadow_consumers {
                    pressure = pressure.strongest(consumer.pressure_level);
                }
            }
            Err(error) => view
                .caveats
                .push(format!("shadow consumer backlog unavailable via shadow.list: {error}")),
        }
    }

    view.pressure_level = pressure;
    view.verdict = match pressure {
        RuntimePressureLevel::Critical => "blocked",
        RuntimePressureLevel::Warning => "degraded",
        RuntimePressureLevel::Nominal => "caught_up",
        RuntimePressureLevel::Unknown => "unknown",
    }
    .to_string();
    view.actions = catchup_actions(&view);
    Ok(view)
}

pub(super) fn material_remediation_signal(
    response: &SourcesRemediationPlanResponse,
) -> OpsCatchupMaterialRemediationView {
    OpsCatchupMaterialRemediationView {
        total_candidates: response.summary.total_candidates,
        total_admitted_events: response.summary.total_admitted_events,
        by_status: response.summary.by_status.clone(),
        by_decision: response.summary.by_decision.clone(),
        by_severity: response.summary.by_severity.clone(),
        by_reason: response.summary.by_reason.clone(),
        top_candidates: response
            .items
            .iter()
            .map(|candidate| OpsCatchupMaterialRemediationCandidateView {
                material_id: candidate.material.id.clone(),
                source_identifier: candidate.material.source_identifier.clone(),
                status: candidate.material.status.to_string(),
                event_count: candidate.material.event_count.unwrap_or_default(),
                failure_reason: candidate.failure_reason.clone(),
                recovery_reason: candidate.recovery_reason.clone(),
                decision: candidate.decision.clone(),
                severity: candidate.severity.clone(),
                suggested_action: candidate.suggested_action.clone(),
            })
            .collect(),
    }
}

fn source_material_signal(sources: &[SourceCoverageEntry]) -> OpsCatchupSourceMaterialSignalView {
    let mut signal = OpsCatchupSourceMaterialSignalView {
        source_count: sources.len(),
        material_count: 0,
        event_count: 0,
        completed_material_count: 0,
        failed_material_count: 0,
        recovered_partial_material_count: 0,
        sensing_material_count: 0,
        cancelled_material_count: 0,
        total_bytes: 0,
        pressure_level: RuntimePressureLevel::Nominal,
    };

    for source in sources {
        signal.material_count += source.material_count.unwrap_or(0);
        signal.event_count += source.event_count.unwrap_or(0);
        signal.completed_material_count += source.completed_material_count.unwrap_or(0);
        signal.failed_material_count += source.failed_material_count.unwrap_or(0);
        signal.recovered_partial_material_count +=
            source.recovered_partial_material_count.unwrap_or(0);
        signal.sensing_material_count += source.sensing_material_count.unwrap_or(0);
        signal.cancelled_material_count += source.cancelled_material_count.unwrap_or(0);
        signal.total_bytes += source.total_bytes.unwrap_or(0);
    }

    signal.pressure_level =
        source_material_pressure(signal.failed_material_count, signal.recovered_partial_material_count);
    signal
}

fn source_material_pressure(failed: i64, recovered_partial: i64) -> RuntimePressureLevel {
    let remediation = failed.saturating_add(recovered_partial);
    if remediation >= 100 {
        RuntimePressureLevel::Critical
    } else if remediation > 0 {
        RuntimePressureLevel::Warning
    } else {
        RuntimePressureLevel::Nominal
    }
}

fn runtime_pressure(inactive_count: i64) -> RuntimePressureLevel {
    if inactive_count > 0 {
        RuntimePressureLevel::Warning
    } else {
        RuntimePressureLevel::Nominal
    }
}

fn stream_pressure(
    pressure: sinex_primitives::events::payloads::metrics::StreamPressureLevel,
) -> RuntimePressureLevel {
    match pressure {
        sinex_primitives::events::payloads::metrics::StreamPressureLevel::Critical => {
            RuntimePressureLevel::Critical
        }
        sinex_primitives::events::payloads::metrics::StreamPressureLevel::Warning => {
            RuntimePressureLevel::Warning
        }
        sinex_primitives::events::payloads::metrics::StreamPressureLevel::Nominal => {
            RuntimePressureLevel::Nominal
        }
    }
}

fn consumer_pressure(pending_messages: u64) -> RuntimePressureLevel {
    if pending_messages >= 100_000 {
        RuntimePressureLevel::Critical
    } else if pending_messages > 0 {
        RuntimePressureLevel::Warning
    } else {
        RuntimePressureLevel::Nominal
    }
}

fn catchup_summary(
    dlq: &OpsCatchupDlqSignalView,
    sources: &OpsCatchupSourceMaterialSignalView,
    remediation: Option<&OpsCatchupMaterialRemediationView>,
    runtime: &OpsCatchupRuntimeSignalView,
) -> String {
    let remediation_fragment = remediation.map_or(String::new(), |remediation| {
        format!(
            " remediation_candidates={} remediation_events={}",
            remediation.total_candidates, remediation.total_admitted_events
        )
    });
    format!(
        "dlq={} materials={} failed={} partial={}{} runtime_active={} inactive={}",
        dlq.total_messages,
        sources.material_count,
        sources.failed_material_count,
        sources.recovered_partial_material_count,
        remediation_fragment,
        runtime.active_count,
        runtime.inactive_count
    )
}

fn catchup_actions(view: &OpsCatchupReadinessView) -> Vec<ActionAvailability> {
    let mut actions = vec![
        ActionAvailability::read(
            "catchup.refresh",
            "Refresh",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint("sinexctl ops catchup status")
        .with_rpc_method("dlq.list"),
    ];

    if view.dlq.total_messages > 0 {
        actions.push(
            ActionAvailability::read(
                "catchup.inspect_dlq",
                "Inspect DLQ",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint("sinexctl ops dlq triage --tail 20")
            .with_rpc_method("dlq.peek"),
        );
    }
    if view.source_materials.failed_material_count
        + view.source_materials.recovered_partial_material_count
        > 0
    {
        actions.push(
            ActionAvailability::read(
                "catchup.inspect_capture_debt",
                "Inspect Capture Debt",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint("sinexctl ops debt list --include-capture")
            .with_rpc_method("sources.remediation_plan"),
        );
    }
    if view
        .shadow_consumers
        .iter()
        .any(|consumer| consumer.pending_messages > 0)
    {
        actions.push(
            ActionAvailability::read(
                "catchup.inspect_shadow_consumers",
                "Inspect Consumers",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint("sinexctl mcp call sinex.shadow_consumers")
            .with_rpc_method("shadow.list"),
        );
    }
    actions
}

fn format_catchup_table(view: &OpsCatchupReadinessView) -> String {
    let mut output = String::new();
    output.push_str("Catch-up Readiness:\n");
    output.push_str(&format!("  Verdict:  {}\n", view.verdict));
    output.push_str(&format!("  Pressure: {}\n", view.pressure_level));
    output.push_str(&format!("  Summary:  {}\n\n", view.summary));

    let mut builder = Builder::new();
    builder.push_record(["SIGNAL", "PRESSURE", "DETAIL"]);
    builder.push_record([
        "DLQ".to_string(),
        view.dlq.pressure_level.to_string(),
        format!(
            "{} msg, {} bytes, span {}",
            view.dlq.total_messages, view.dlq.total_bytes, view.dlq.pending_sequence_span
        ),
    ]);
    builder.push_record([
        "Source materials".to_string(),
        view.source_materials.pressure_level.to_string(),
        format!(
            "{} materials, {} events, failed {}, partial {}",
            view.source_materials.material_count,
            view.source_materials.event_count,
            view.source_materials.failed_material_count,
            view.source_materials.recovered_partial_material_count
        ),
    ]);
    if let Some(remediation) = &view.material_remediation {
        let top = remediation
            .top_candidates
            .first()
            .map(|candidate| {
                format!(
                    "; top {} events {} {}",
                    candidate.event_count, candidate.source_identifier, candidate.decision
                )
            })
            .unwrap_or_default();
        builder.push_record([
            "Remediation".to_string(),
            view.source_materials.pressure_level.to_string(),
            format!(
                "{} candidates, {} admitted events{}",
                remediation.total_candidates, remediation.total_admitted_events, top
            ),
        ]);
    }
    builder.push_record([
        "Runtime".to_string(),
        view.runtime.pressure_level.to_string(),
        format!(
            "{} active, {} inactive, {} unique modules",
            view.runtime.active_count, view.runtime.inactive_count, view.runtime.unique_modules
        ),
    ]);
    for stream in &view.streams {
        builder.push_record([
            format!("Stream {}", stream.stream_name),
            stream.pressure_level.to_string(),
            format!(
                "max_messages={}, max_fill_pct={}",
                display_opt_i64(stream.max_messages),
                display_opt_f64(stream.max_fill_pct)
            ),
        ]);
    }
    for consumer in &view.shadow_consumers {
        builder.push_record([
            format!("Consumer {}", consumer.consumer_name),
            consumer.pressure_level.to_string(),
            format!(
                "{} pending on {} ({})",
                consumer.pending_messages, consumer.stream_name, consumer.subject_filter
            ),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    output.push_str(&table.to_string());

    if !view.caveats.is_empty() {
        output.push_str("\n\nCaveats:\n");
        for caveat in &view.caveats {
            output.push_str(&format!("  - {caveat}\n"));
        }
    }
    if !view.actions.is_empty() {
        output.push_str("\nActions:\n");
        for action in &view.actions {
            if let Some(command) = &action.command_hint {
                output.push_str(&format!("  - {}: {}\n", action.label, command));
            }
        }
    }
    output
}

fn display_opt_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn display_opt_f64(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.1}"))
}

fn print_machine_output(output: &str) {
    print!("{output}");
    if !output.is_empty() && !output.ends_with('\n') {
        println!();
    }
}
