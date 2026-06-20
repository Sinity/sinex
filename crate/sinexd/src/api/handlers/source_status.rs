//! Operator-facing source status handler.

use crate::api::service_container::{ConfirmationBufferHealth, ServiceContainer};
use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::rpc::source_status::{
    SourceStatus, SourcesStatusRequest, SourcesStatusResponse, SourcesStatusViewRequest,
};
use sinex_primitives::source_contracts::{
    AccessScope, BudgetPressureAction, ResourceBudgetSpec, ResourceProfile, SourceContract,
    SourceRuntimeBinding, WorkClass, all_source_contracts, source_runtime_bindings,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, CaveatView, CoverageGapView, SinexObjectKind,
    SinexObjectRef, SourceCoverageContinuity, SourceCoverageListView, SourceCoverageReadiness,
    SourceCoverageView, SourcePrivacyPosture, SourceResourceBudgetView, ViewEnvelope,
};
use sqlx::FromRow;
use sqlx::PgPool;
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;
use time::OffsetDateTime;

type Result<T> = std::result::Result<T, SinexError>;

#[derive(Debug, FromRow)]
struct SourceEventAggregateRow {
    source: String,
    event_type: String,
    event_count: i64,
    last_event_at: Option<OffsetDateTime>,
}

#[derive(Debug, FromRow)]
struct SourceMaterialAggregateRow {
    source_identifier: String,
    material_count: i64,
    last_material_at: Option<OffsetDateTime>,
}

/// List registered sources with run, health, and recent-emission stats.
///
/// Mirrors `handle_automata_status` (`automata.status`) for the source-side
/// surface; filtered to `manifest_type = 'source'`.
pub async fn handle_sources_status(
    pool: &PgPool,
    request: SourcesStatusRequest,
) -> Result<SourcesStatusResponse> {
    let stale_after = Duration::from_secs(request.stale_after_secs);
    let recent_window = Duration::from_secs(request.recent_window_secs);
    let sources = pool
        .state()
        .list_sources_status(stale_after, recent_window)
        .await
        .map_err(|e| SinexError::database("Failed to list sources status").with_std_error(&e))?
        .into_iter()
        .map(|row| SourceStatus {
            module_name: row.module_name,
            version: row.version,
            description: row.description,
            manifest_status: row.manifest_status.unwrap_or_default(),
            live: row.live,
            service_name: row.service_name,
            instance_id: row.instance_id,
            module_run_id: row.module_run_id,
            host: row.host,
            run_status: row.run_status,
            started_at: row.started_at,
            last_heartbeat_at: row.last_heartbeat_at,
            current_health: row.current_health,
            health_changed_at: row.health_changed_at,
            health_reason: row.health_reason,
            recent_output_count: row.recent_output_count,
            last_output_at: row.last_output_at,
        })
        .collect();

    let response = SourcesStatusResponse {
        generated_at: Timestamp::now(),
        stale_after_secs: request.stale_after_secs,
        recent_window_secs: request.recent_window_secs,
        sources,
    };

    Ok(response)
}

pub async fn handle_sources_status_view(
    services: &ServiceContainer,
    _request: SourcesStatusViewRequest,
) -> Result<ViewEnvelope<SourceCoverageListView>> {
    let pool = services.pool();
    let confirmation_buffer = services.probe_confirmation_buffer_pressure().await;
    let event_rows = sqlx::query_as!(
        SourceEventAggregateRow,
        r#"
        SELECT
            source,
            event_type,
            COUNT(*)::bigint as "event_count!",
            MAX(ts_orig) as "last_event_at: _"
        FROM core.events
        GROUP BY source, event_type
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to compute source event coverage").with_std_error(&error)
    })?;

    let material_rows = sqlx::query_as!(
        SourceMaterialAggregateRow,
        r#"
        SELECT
            source_identifier,
            COUNT(*)::bigint as "material_count!",
            MAX(COALESCE(end_time, start_time, staged_at)) as "last_material_at: _"
        FROM raw.source_material_registry
        GROUP BY source_identifier
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to compute source material coverage").with_std_error(&error)
    })?;

    let mut event_aggregates = HashMap::<(String, String), SourceEventAggregateRow>::new();
    for row in event_rows {
        event_aggregates.insert((row.source.clone(), row.event_type.clone()), row);
    }
    let material_aggregates: HashMap<String, SourceMaterialAggregateRow> = material_rows
        .into_iter()
        .map(|row| (row.source_identifier.clone(), row))
        .collect();

    let bindings: Vec<&'static SourceRuntimeBinding> = source_runtime_bindings().collect();
    let mut contracts: Vec<&'static SourceContract> = all_source_contracts().collect();
    contracts.sort_by_key(|contract| contract.id);

    let mut views = Vec::with_capacity(contracts.len());
    for contract in contracts {
        let source_bindings: Vec<_> = bindings
            .iter()
            .copied()
            .filter(|binding| binding.source_id == contract.id)
            .collect();
        views.push(source_coverage_view(
            contract,
            &source_bindings,
            &event_aggregates,
            &material_aggregates,
            &confirmation_buffer,
        ));
    }

    Ok(ViewEnvelope::new(
        "sinexd.sources.status.view",
        SourceCoverageListView::new(views),
    ))
}

fn source_coverage_view(
    contract: &SourceContract,
    bindings: &[&SourceRuntimeBinding],
    event_aggregates: &HashMap<(String, String), SourceEventAggregateRow>,
    material_aggregates: &HashMap<String, SourceMaterialAggregateRow>,
    confirmation_buffer: &ConfirmationBufferHealth,
) -> SourceCoverageView {
    let mut event_count = 0i64;
    let mut last_event_at = None;
    let event_types: Vec<String> = contract
        .event_types
        .iter()
        .map(|(source, event_type)| {
            if let Some(row) = event_aggregates.get(&(source.to_string(), event_type.to_string())) {
                event_count += row.event_count;
                last_event_at = max_timestamp(last_event_at, row.last_event_at);
            }
            format!("{source}/{event_type}")
        })
        .collect();

    let material = material_aggregates.get(contract.id);
    let material_count = material.map_or(0, |row| row.material_count);
    let last_material_at = material.and_then(|row| row.last_material_at.map(Timestamp::from));
    let last_event_at = last_event_at.map(Timestamp::from);
    let live_binding_count = bindings.iter().filter(|binding| !binding.proposed).count();
    let proposed_binding_count = bindings.len().saturating_sub(live_binding_count);
    let has_live_binding = live_binding_count > 0;
    let has_material = material_count > 0;
    let has_events = event_count > 0;

    let mut gaps = Vec::new();
    let mut caveats = Vec::new();
    if bindings.is_empty() {
        gaps.push(CoverageGapView {
            kind: "missing_binding".to_string(),
            message: "source contract has no runtime binding".to_string(),
        });
    }
    if !has_material {
        gaps.push(CoverageGapView {
            kind: "missing_material".to_string(),
            message: "no source material is directly registered under this source id".to_string(),
        });
        caveats.push(CaveatView {
            id: "source.material.match.v0_exact_id".to_string(),
            message: "v0 material coverage only counts source_material_registry rows whose source_identifier exactly equals the source id".to_string(),
            ref_: None,
        });
    }
    if !has_events {
        gaps.push(CoverageGapView {
            kind: "missing_events".to_string(),
            message: "no live events match the contract's declared source/event_type pairs"
                .to_string(),
        });
    }
    if matches!(contract.access_scope, AccessScope::RuntimeBridge { .. })
        && has_live_binding
        && !has_material
        && !has_events
    {
        caveats.push(runtime_bridge_unobserved_caveat(contract));
    }
    let pressure = source_confirmation_pressure(contract, confirmation_buffer);
    if let Some(pressure) = &pressure {
        caveats.push(CaveatView {
            id: "source.pressure.confirmation_buffer.retained_payload".to_string(),
            message: format!(
                "confirmation buffer retains approximately {} byte(s) across {} declared event kind(s) for this source",
                pressure.total_bytes,
                pressure.event_kind_count
            ),
            ref_: None,
        });
    }

    let readiness = if !has_live_binding && proposed_binding_count > 0 {
        SourceCoverageReadiness::Proposed
    } else if !has_live_binding {
        SourceCoverageReadiness::MissingBinding
    } else if !has_material {
        SourceCoverageReadiness::MissingMaterial
    } else if !has_events {
        SourceCoverageReadiness::MissingEvents
    } else {
        SourceCoverageReadiness::Ready
    };
    let continuity = match (has_material, has_events) {
        (true, true) => SourceCoverageContinuity::Active,
        (true, false) => SourceCoverageContinuity::MaterialOnly,
        (false, true) => SourceCoverageContinuity::EventOnly,
        (false, false) if bindings.is_empty() => SourceCoverageContinuity::Unknown,
        (false, false) => SourceCoverageContinuity::Gapped,
    };
    let selected_binding = bindings
        .iter()
        .copied()
        .find(|binding| !binding.proposed)
        .or_else(|| bindings.first().copied());
    let privacy_context = selected_binding.map_or("none".to_string(), |binding| {
        format!("{:?}", binding.privacy_context).to_ascii_lowercase()
    });
    let resource_budget = selected_binding.map(source_resource_budget_view);

    SourceCoverageView {
        source_id: contract.id.to_string(),
        namespace: contract.namespace.to_string(),
        event_types,
        readiness,
        continuity,
        last_material_at,
        last_event_at,
        material_count,
        event_count,
        binding_count: bindings.len(),
        live_binding_count,
        proposed_binding_count,
        gaps,
        caveats,
        privacy: SourcePrivacyPosture {
            tier: format!("{:?}", contract.privacy_tier).to_ascii_lowercase(),
            context: privacy_context,
            proposed: live_binding_count == 0 && proposed_binding_count > 0,
        },
        resource_budget,
        actions: source_actions(contract.id, bindings, pressure.is_some()),
    }
}

fn runtime_bridge_unobserved_caveat(contract: &SourceContract) -> CaveatView {
    let surface = match contract.access_scope {
        AccessScope::RuntimeBridge { surface } => surface,
        _ => "runtime_bridge",
    };
    CaveatView {
        id: "source.runtime_bridge.unobserved".to_string(),
        message: format!(
            "runtime bridge `{surface}` is declared, but no material or admitted events have been observed for this source"
        ),
        ref_: Some(SinexObjectRef::new(
            SinexObjectKind::SourceDriver,
            contract.id.to_string(),
        )),
    }
}

#[derive(Debug, Clone, Copy)]
struct SourceConfirmationPressure {
    total_bytes: usize,
    event_kind_count: usize,
}

fn source_confirmation_pressure(
    contract: &SourceContract,
    confirmation_buffer: &ConfirmationBufferHealth,
) -> Option<SourceConfirmationPressure> {
    let mut total_bytes = 0usize;
    let mut event_kind_count = 0usize;
    for (source, event_type) in contract.event_types {
        let key = format!("{source}:{event_type}");
        let Some(bytes) = confirmation_buffer
            .approximate_payload_bytes_by_kind
            .get(&key)
            .copied()
        else {
            continue;
        };
        if bytes == 0 {
            continue;
        }
        total_bytes = total_bytes.saturating_add(bytes);
        event_kind_count += 1;
    }

    (total_bytes > 0).then_some(SourceConfirmationPressure {
        total_bytes,
        event_kind_count,
    })
}

fn source_resource_budget_view(binding: &SourceRuntimeBinding) -> SourceResourceBudgetView {
    let budget = binding.resource_budget();
    SourceResourceBudgetView {
        resource_profile: resource_profile_name(binding.resource_profile).to_string(),
        work_class: work_class_name(&budget).to_string(),
        steady_memory_mib: budget.steady_memory_mib,
        burst_memory_mib: budget.burst_memory_mib,
        cpu_weight: budget.cpu_weight,
        max_input_bytes_per_sec: budget.max_input_bytes_per_sec,
        max_input_events_per_sec: budget.max_input_events_per_sec,
        max_pending_material_bytes: budget.max_pending_material_bytes,
        max_pending_candidates: budget.max_pending_candidates,
        max_unacked_transport_messages: budget.max_unacked_transport_messages,
        batch_size: budget.batch_size,
        flush_interval_ms: budget.flush_interval_ms,
        checkpoint_interval_ms: budget.checkpoint_interval_ms,
        pressure_actions: budget
            .pressure_actions
            .iter()
            .map(|action| pressure_action_name(*action).to_string())
            .collect(),
    }
}

fn resource_profile_name(profile: ResourceProfile) -> &'static str {
    match profile {
        ResourceProfile::BoundedFile => "bounded_file",
        ResourceProfile::BoundedStream => "bounded_stream",
        ResourceProfile::LiveWatcher => "live_watcher",
        ResourceProfile::DirectoryScan => "directory_scan",
        ResourceProfile::Oneshot => "oneshot",
        ResourceProfile::EventStreamConsumer => "event_stream_consumer",
        ResourceProfile::EmbeddedEmitter => "embedded_emitter",
    }
}

fn work_class_name(budget: &ResourceBudgetSpec) -> &'static str {
    match budget.work_class {
        WorkClass::Interactive => "interactive",
        WorkClass::AdmissionHot => "admission_hot",
        WorkClass::CaptureLive => "capture_live",
        WorkClass::ProjectionHot => "projection_hot",
        WorkClass::ProjectionCold => "projection_cold",
        WorkClass::BulkImport => "bulk_import",
        WorkClass::Maintenance => "maintenance",
    }
}

fn pressure_action_name(action: BudgetPressureAction) -> &'static str {
    match action {
        BudgetPressureAction::Throttle => "throttle",
        BudgetPressureAction::Defer => "defer",
        BudgetPressureAction::Pause => "pause",
        BudgetPressureAction::Drain => "drain",
        BudgetPressureAction::Inspect => "inspect",
        BudgetPressureAction::Retry => "retry",
    }
}

fn max_timestamp(
    current: Option<OffsetDateTime>,
    candidate: Option<OffsetDateTime>,
) -> Option<OffsetDateTime> {
    match (current, candidate) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (None, Some(b)) => Some(b),
        (Some(a), None) => Some(a),
        (None, None) => None,
    }
}

fn source_actions(
    source_id: &str,
    bindings: &[&SourceRuntimeBinding],
    has_pressure: bool,
) -> Vec<ActionAvailability> {
    let mut actions = vec![
        ActionAvailability::read(
            "sources.readiness",
            "Readiness",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint(format!("sinexctl sources readiness {source_id}"))
        .with_rpc_method("sources.readiness.get"),
        ActionAvailability::read(
            "sources.coverage",
            "Coverage",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint("sinexctl sources coverage")
        .with_rpc_method("sources.coverage"),
    ];
    if has_pressure {
        actions.push(
            ActionAvailability::read(
                "runtime.health.inspect",
                "Inspect runtime pressure",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint("sinexctl runtime health")
            .with_rpc_method("system.health"),
        );
    }
    let mut seen = actions
        .iter()
        .map(|action| action.id.clone())
        .collect::<BTreeSet<_>>();
    for binding in bindings {
        for capability in binding.capabilities {
            let Some(operation) = capability.strip_prefix("operation:") else {
                continue;
            };
            if seen.insert(operation.to_string()) {
                actions.push(operation_capability_action(operation, source_id));
            }
        }
    }
    actions
}

fn operation_capability_action(operation: &str, source_id: &str) -> ActionAvailability {
    let (label, command_hint) = match operation.rsplit('.').next().unwrap_or(operation) {
        "check" => ("Check Bridge", Some("sinexctl sources status")),
        "inspect" => (
            "Inspect Bridge",
            Some("sinexctl sources status --format json"),
        ),
        "drain" => ("Drain Bridge", None),
        "flush" => ("Flush Bridge", None),
        "reconnect" => ("Reconnect Bridge", None),
        "pause" => ("Pause Bridge", None),
        "resume" => ("Resume Bridge", None),
        _ => ("Package Operation", None),
    };
    let mut action = ActionAvailability::read(
        operation,
        label,
        if command_hint.is_some() {
            ActionAvailabilityState::Enabled
        } else {
            ActionAvailabilityState::Unavailable
        },
    )
    .with_reason(format!(
        "package declares `{operation}` for source `{source_id}`"
    ));
    if let Some(command_hint) = command_hint {
        action = action.with_command_hint(command_hint);
    }
    action
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use sinex_primitives::domain::HealthStatus;
    use sinex_primitives::privacy::ProcessingContext;
    use sinex_primitives::source_contracts::{
        AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
        RetentionPolicy, RunnerPack, RuntimeShape, SourceBuildImpact, SubjectRef,
    };
    use std::collections::BTreeMap;
    use xtask::sandbox::sinex_test;

    static CONTRACT: SourceContract = SourceContract {
        id: "fixture.source",
        namespace: "fixture",
        event_types: &[("fixture", "fixture.event")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_scope: AccessScope::StagedExport,
    };

    static BINDING: SourceRuntimeBinding = SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:fixture.source"),
        "fixture.source",
        "fixture",
    )
    .implementation("sinexd")
    .adapter("StaticFileAdapter")
    .output_event_type("fixture.event")
    .privacy_context(ProcessingContext::Command)
    .resource_profile(ResourceProfile::BoundedFile)
    .source_id("fixture.source")
    .runner_pack(RunnerPack::SinexdSource)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .build_impact(SourceBuildImpact::ZERO)
    .build();

    #[sinex_test]
    async fn source_coverage_view_marks_ready_when_catalog_material_and_events_exist()
    -> xtask::TestResult<()> {
        let now = OffsetDateTime::now_utc();
        let mut events = HashMap::new();
        events.insert(
            ("fixture".to_string(), "fixture.event".to_string()),
            SourceEventAggregateRow {
                source: "fixture".to_string(),
                event_type: "fixture.event".to_string(),
                event_count: 3,
                last_event_at: Some(now),
            },
        );
        let mut materials = HashMap::new();
        materials.insert(
            "fixture.source".to_string(),
            SourceMaterialAggregateRow {
                source_identifier: "fixture.source".to_string(),
                material_count: 2,
                last_material_at: Some(now),
            },
        );

        let view = source_coverage_view(
            &CONTRACT,
            &[&BINDING],
            &events,
            &materials,
            &healthy_confirmation_buffer(),
        );

        assert_eq!(view.readiness, SourceCoverageReadiness::Ready);
        assert_eq!(view.continuity, SourceCoverageContinuity::Active);
        assert_eq!(view.event_count, 3);
        assert_eq!(view.material_count, 2);
        assert!(view.gaps.is_empty());
        assert_eq!(view.privacy.tier, "sensitive");
        assert_eq!(view.privacy.context, "command");
        let budget = view
            .resource_budget
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("resource budget expected"))?;
        assert_eq!(budget.resource_profile, "bounded_file");
        assert_eq!(budget.work_class, "bulk_import");
        assert!(budget.pressure_actions.contains(&"inspect".to_string()));
        assert!(
            view.actions
                .iter()
                .any(|action| action.id == "sources.readiness")
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_coverage_view_surfaces_missing_material_caveat() -> xtask::TestResult<()> {
        let events = HashMap::new();
        let materials = HashMap::new();

        let view = source_coverage_view(
            &CONTRACT,
            &[&BINDING],
            &events,
            &materials,
            &healthy_confirmation_buffer(),
        );

        assert_eq!(view.readiness, SourceCoverageReadiness::MissingMaterial);
        assert_eq!(view.continuity, SourceCoverageContinuity::Gapped);
        assert!(view.gaps.iter().any(|gap| gap.kind == "missing_material"));
        assert!(
            view.caveats
                .iter()
                .any(|caveat| caveat.id == "source.material.match.v0_exact_id")
        );
        Ok(())
    }

    #[sinex_test]
    async fn runtime_bridge_coverage_surfaces_unobserved_bridge_and_declared_actions()
    -> xtask::TestResult<()> {
        static BRIDGE_CAPABILITIES: &[&str] = &[
            "coverage:source-coverage",
            "operation:terminal.activity.check",
            "operation:terminal.activity.reconnect",
            "operation:terminal.activity.inspect",
        ];
        static BRIDGE_CONTRACT: SourceContract = SourceContract {
            id: "terminal.kitty-osc-live",
            namespace: "terminal",
            event_types: &[("shell.kitty", "command.executed")],
            privacy_tier: PrivacyTier::Sensitive,
            horizons: &[Horizon::Continuous],
            retention: RetentionPolicy::Forever,
            occurrence_identity: OccurrenceIdentity::Anchor,
            access_scope: AccessScope::RuntimeBridge {
                surface: "kitty_osc",
            },
        };
        let bridge_binding = SourceRuntimeBinding::builder(
            SubjectRef::from_static("source:terminal.kitty-osc-live"),
            "terminal.kitty-osc-live",
            "terminal",
        )
        .implementation("live-capture")
        .adapter("UnixSocketStreamAdapter")
        .output_event_type("command.executed")
        .privacy_context(ProcessingContext::Command)
        .resource_profile(ResourceProfile::LiveWatcher)
        .capabilities(BRIDGE_CAPABILITIES)
        .source_id("terminal.kitty-osc-live")
        .runner_pack(RunnerPack::Live)
        .checkpoint_family(CheckpointFamily::LiveObservation)
        .runtime_shape(RuntimeShape::Continuous)
        .build_impact(SourceBuildImpact::ZERO)
        .build();

        let view = source_coverage_view(
            &BRIDGE_CONTRACT,
            &[&bridge_binding],
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
        );

        let caveat = view
            .caveats
            .iter()
            .find(|caveat| caveat.id == "source.runtime_bridge.unobserved")
            .ok_or_else(|| color_eyre::eyre::eyre!("bridge caveat expected"))?;
        assert!(
            caveat.message.contains("kitty_osc"),
            "runtime bridge caveat should name the unobserved bridge surface"
        );
        assert_eq!(
            caveat
                .ref_
                .as_ref()
                .map(|ref_| (&ref_.kind, ref_.id.as_str())),
            Some((&SinexObjectKind::SourceDriver, "terminal.kitty-osc-live"))
        );

        let check = view
            .actions
            .iter()
            .find(|action| action.id == "terminal.activity.check")
            .ok_or_else(|| color_eyre::eyre::eyre!("check action expected"))?;
        assert_eq!(check.state, ActionAvailabilityState::Enabled);
        assert_eq!(
            check.command_hint.as_deref(),
            Some("sinexctl sources status")
        );

        let reconnect = view
            .actions
            .iter()
            .find(|action| action.id == "terminal.activity.reconnect")
            .ok_or_else(|| color_eyre::eyre::eyre!("reconnect action expected"))?;
        assert_eq!(reconnect.state, ActionAvailabilityState::Unavailable);
        assert!(
            reconnect
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("terminal.kitty-osc-live")),
            "declared operation refs should stay source-addressable even before actuator wiring"
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_coverage_view_surfaces_attributed_confirmation_pressure()
    -> xtask::TestResult<()> {
        let mut confirmation_buffer = healthy_confirmation_buffer();
        confirmation_buffer.status = HealthStatus::Degraded;
        confirmation_buffer.approximate_payload_bytes = 1536;
        confirmation_buffer.approximate_payload_bytes_by_kind = BTreeMap::from([
            ("fixture:fixture.event".to_string(), 1024),
            ("other.source:other.event".to_string(), 512),
        ]);

        let view = source_coverage_view(
            &CONTRACT,
            &[&BINDING],
            &HashMap::new(),
            &HashMap::new(),
            &confirmation_buffer,
        );

        let caveat = view
            .caveats
            .iter()
            .find(|caveat| caveat.id == "source.pressure.confirmation_buffer.retained_payload")
            .ok_or_else(|| color_eyre::eyre::eyre!("pressure caveat expected"))?;
        assert!(
            caveat.message.contains("1024 byte(s)"),
            "source-local caveat should report only bytes attributed to the source contract"
        );
        assert!(
            view.actions
                .iter()
                .any(|action| action.id == "runtime.health.inspect"),
            "source-local pressure should expose the runtime health inspection action"
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_coverage_view_does_not_localize_unattributed_pressure() -> xtask::TestResult<()>
    {
        let mut confirmation_buffer = healthy_confirmation_buffer();
        confirmation_buffer.status = HealthStatus::Degraded;
        confirmation_buffer.approximate_payload_bytes = 512;
        confirmation_buffer.approximate_payload_bytes_by_kind =
            BTreeMap::from([("other.source:other.event".to_string(), 512)]);

        let view = source_coverage_view(
            &CONTRACT,
            &[&BINDING],
            &HashMap::new(),
            &HashMap::new(),
            &confirmation_buffer,
        );

        assert!(
            !view
                .caveats
                .iter()
                .any(|caveat| caveat.id == "source.pressure.confirmation_buffer.retained_payload"),
            "unattributed/global pressure must stay in runtime health instead of becoming source-local"
        );
        assert!(
            !view
                .actions
                .iter()
                .any(|action| action.id == "runtime.health.inspect"),
            "global pressure without source ownership should not create source-local actions"
        );
        Ok(())
    }

    fn healthy_confirmation_buffer() -> ConfirmationBufferHealth {
        ConfirmationBufferHealth {
            status: HealthStatus::Healthy,
            connected: true,
            memory_owner: crate::api::service_container::ConfirmationBufferMemoryOwner::None,
            pressure_level: "nominal".to_string(),
            runtime_action: "admit".to_string(),
            observed_buffers: 0,
            pending_count: 0,
            timed_out_retained_count: 0,
            rejected_count: 0,
            late_confirmation_count: 0,
            retained_payload_bytes: 0,
            approximate_payload_bytes: 0,
            active_payload_bytes: 0,
            timed_out_retained_payload_bytes: 0,
            approximate_payload_bytes_by_kind: BTreeMap::new(),
            detail: "confirmation buffers nominal".to_string(),
        }
    }
}
