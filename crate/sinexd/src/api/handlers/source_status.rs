//! Operator-facing source status handler.

use crate::api::service_container::{ConfirmationBufferHealth, ServiceContainer};
use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::rpc::{
    methods,
    source_status::{
        EmitStallThresholds, EmitStallVerdict, SourceStatus, SourcesStatusRequest,
        SourcesStatusResponse, SourcesStatusViewRequest,
    },
};
use sinex_primitives::source_contracts::{
    AccessScope, BudgetPressureAction, ResourceBudgetSpec, ResourceProfile, SourceCapabilityKind,
    SourceContract, SourceRuntimeBinding, WorkClass, all_source_contracts, source_runtime_bindings,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, ActionSideEffect, CaveatView, CoverageGapView,
    SinexObjectKind, SinexObjectRef, SourceCoverageContinuity, SourceCoverageListView,
    SourceCoverageReadiness, SourceCoverageView, SourcePrivacyPosture, SourceResourceBudgetView,
    ViewEnvelope,
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
    let now = Timestamp::now();
    let status_defaults = SourcesStatusRequest::default();
    let source_status_rows = pool
        .state()
        .list_sources_status(
            Duration::from_secs(status_defaults.stale_after_secs),
            Duration::from_secs(status_defaults.recent_window_secs),
        )
        .await
        .map_err(|error| {
            SinexError::database("Failed to compute source runtime status").with_std_error(&error)
        })?;
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
    let runtime_observations = source_runtime_observations(source_status_rows);

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
            &runtime_observations,
            now,
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
    runtime_observations: &HashMap<String, SourceStatus>,
    now: Timestamp,
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
        if let Some(observation) = runtime_observation_for_source(contract.id, runtime_observations)
        {
            if !observation.live {
                gaps.push(CoverageGapView {
                    kind: "runtime_bridge_disconnected".to_string(),
                    message: runtime_bridge_status_message(contract, observation),
                });
            }
            let verdict = observation.classify_emit_stall(EmitStallThresholds::default(), now);
            if matches!(verdict, EmitStallVerdict::Stalled) {
                gaps.push(CoverageGapView {
                    kind: "runtime_bridge_stalled".to_string(),
                    message: runtime_bridge_stall_message(contract, observation),
                });
            }
            caveats.extend(runtime_bridge_observation_caveats(
                contract,
                observation,
                verdict,
            ));
        } else {
            caveats.push(runtime_bridge_unobserved_caveat(contract));
        }
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

fn source_runtime_observations(
    rows: Vec<sinex_db::repositories::state::SourcesStatusRow>,
) -> HashMap<String, SourceStatus> {
    rows.into_iter()
        .map(|row| {
            let status = SourceStatus {
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
            };
            (status.module_name.to_string(), status)
        })
        .collect()
}

fn runtime_observation_for_source<'a>(
    source_id: &str,
    observations: &'a HashMap<String, SourceStatus>,
) -> Option<&'a SourceStatus> {
    source_runtime_module(source_id)
        .and_then(|module| observations.get(module))
        .or_else(|| observations.get(source_id))
}

fn runtime_bridge_observation_caveats(
    contract: &SourceContract,
    observation: &SourceStatus,
    verdict: EmitStallVerdict,
) -> Vec<CaveatView> {
    let mut caveats = vec![CaveatView {
        id: "source.runtime_bridge.observed".to_string(),
        message: runtime_bridge_status_message(contract, observation),
        ref_: runtime_bridge_ref(contract),
    }];

    if observation.current_health.is_some() || observation.health_reason.is_some() {
        caveats.push(CaveatView {
            id: "source.runtime_bridge.health".to_string(),
            message: runtime_bridge_health_message(contract, observation),
            ref_: runtime_bridge_ref(contract),
        });
    }

    if matches!(verdict, EmitStallVerdict::Stalled) {
        caveats.push(CaveatView {
            id: "source.runtime_bridge.stalled".to_string(),
            message: runtime_bridge_stall_message(contract, observation),
            ref_: runtime_bridge_ref(contract),
        });
    }

    if !observation.live {
        caveats.push(CaveatView {
            id: "source.runtime_bridge.disconnected".to_string(),
            message: runtime_bridge_status_message(contract, observation),
            ref_: runtime_bridge_ref(contract),
        });
    }

    caveats
}

fn runtime_bridge_ref(contract: &SourceContract) -> Option<SinexObjectRef> {
    Some(SinexObjectRef::new(
        SinexObjectKind::SourceDriver,
        contract.id.to_string(),
    ))
}

fn runtime_bridge_surface(contract: &SourceContract) -> &'static str {
    match contract.access_scope {
        AccessScope::RuntimeBridge { surface } => surface,
        _ => "runtime_bridge",
    }
}

fn runtime_bridge_status_message(contract: &SourceContract, observation: &SourceStatus) -> String {
    let surface = runtime_bridge_surface(contract);
    let connection = if observation.live {
        "connected"
    } else {
        "disconnected"
    };
    format!(
        "runtime bridge `{surface}` is {connection} through module `{}`; last heartbeat {}; last output {}; recent output count {}",
        observation.module_name,
        optional_timestamp(observation.last_heartbeat_at),
        optional_timestamp(observation.last_output_at),
        observation.recent_output_count
    )
}

fn runtime_bridge_health_message(contract: &SourceContract, observation: &SourceStatus) -> String {
    let surface = runtime_bridge_surface(contract);
    let health = observation
        .current_health
        .map_or_else(|| "unknown".to_string(), |status| status.to_string());
    let reason = observation
        .health_reason
        .as_deref()
        .unwrap_or("no health reason recorded");
    format!(
        "runtime bridge `{surface}` health is {health}; {reason}; health changed {}",
        optional_timestamp(observation.health_changed_at)
    )
}

fn runtime_bridge_stall_message(contract: &SourceContract, observation: &SourceStatus) -> String {
    let surface = runtime_bridge_surface(contract);
    format!(
        "runtime bridge `{surface}` is heartbeating but has no recent source output; last output {}; recent output count {}",
        optional_timestamp(observation.last_output_at),
        observation.recent_output_count
    )
}

fn optional_timestamp(timestamp: Option<Timestamp>) -> String {
    timestamp.map_or_else(|| "unknown".to_string(), |timestamp| timestamp.to_string())
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
        .with_rpc_method(methods::SOURCES_READINESS_GET),
        ActionAvailability::read(
            "sources.coverage",
            "Coverage",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint("sinexctl sources coverage")
        .with_rpc_method(methods::SOURCES_COVERAGE),
    ];
    if has_pressure {
        actions.push(
            ActionAvailability::read(
                "runtime.health.inspect",
                "Inspect runtime pressure",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint("sinexctl runtime health")
            .with_rpc_method(methods::SYSTEM_HEALTH),
        );
    }
    let mut seen = actions
        .iter()
        .map(|action| action.id.clone())
        .collect::<BTreeSet<_>>();
    for binding in bindings {
        for capability in binding
            .capability_refs()
            .filter(|capability| capability.is_kind(SourceCapabilityKind::Operation))
        {
            let operation = capability.target;
            if seen.insert(operation.to_string()) {
                actions.push(operation_capability_action(operation, source_id));
            }
        }
    }
    actions
}

fn operation_capability_action(operation: &str, source_id: &str) -> ActionAvailability {
    let module = source_runtime_module(source_id);
    let (label, command_hint, rpc_method, side_effect) = match operation
        .rsplit('.')
        .next()
        .unwrap_or(operation)
    {
        "check" => (
            "Check Bridge",
            Some(format!("sinexctl sources status {source_id} --format json")),
            Some(methods::SOURCES_STATUS_VIEW),
            ActionSideEffect::Read,
        ),
        "inspect" => (
            "Inspect Bridge",
            module
                .map(|module| format!("sinexctl runtime status {module}"))
                .or_else(|| Some(format!("sinexctl sources status {source_id} --format json"))),
            module.map(|_| methods::COORDINATION_INSTANCE_HEALTH),
            ActionSideEffect::Read,
        ),
        "drain" => (
            "Drain Bridge",
            module
                .map(|module| format!("sinexctl runtime drain {module} --reason source-coverage")),
            module.map(|_| methods::RUNTIME_DRAIN),
            ActionSideEffect::Admin,
        ),
        "flush" => ("Flush Bridge", None, None, ActionSideEffect::Admin),
        "reconnect" => (
            "Reconnect Bridge",
            module.map(|module| format!("sinexctl runtime resume {module}")),
            module.map(|_| methods::RUNTIME_RESUME),
            ActionSideEffect::Admin,
        ),
        "pause" => (
            package_operation_label(operation, "Pause Package Mode").unwrap_or("Pause Bridge"),
            package_operation_command_hint(operation, source_id).or_else(|| {
                module.map(|module| format!("sinexctl runtime drain {module} --reason source-paused"))
            }),
            package_operation_rpc_method(operation, source_id)
                .or_else(|| module.map(|_| methods::RUNTIME_DRAIN)),
            ActionSideEffect::Admin,
        ),
        "resume" => (
            package_operation_label(operation, "Resume Package Mode").unwrap_or("Resume Bridge"),
            package_operation_command_hint(operation, source_id)
                .or_else(|| module.map(|module| format!("sinexctl runtime resume {module}"))),
            package_operation_rpc_method(operation, source_id)
                .or_else(|| module.map(|_| methods::RUNTIME_RESUME)),
            ActionSideEffect::Admin,
        ),
        "authorize" => (
            "Authorize Mailbox",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Admin,
        ),
        "sync" => (
            "Sync Mailbox",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Write,
        ),
        "import-transcript" => (
            "Import Transcript",
            Some("sinexctl sources stage <path> --format json".to_string()),
            Some(methods::SOURCES_STAGE),
            ActionSideEffect::Write,
        ),
        "import-ocr" => (
            "Import OCR",
            Some("sinexctl sources stage <path> --format json".to_string()),
            Some(methods::SOURCES_STAGE),
            ActionSideEffect::Write,
        ),
        "import-bundle" => (
            "Import Audio Bundle",
            Some(
                "sinexctl sources stage <path> --binding source:media.audio-transcript.audio-bundle-staged --format json"
                    .to_string(),
            ),
            Some(methods::SOURCES_STAGE),
            ActionSideEffect::Write,
        ),
        "import-screenshots" => (
            "Import Screenshot Bundle",
            Some(
                "sinexctl sources stage <path> --binding source:media.screen-ocr.screenshot-ocr-staged --format json"
                    .to_string(),
            ),
            Some(methods::SOURCES_STAGE),
            ActionSideEffect::Write,
        ),
        "export" => (
            "Export Source Events",
            Some(format!(
                "sinexctl privacy export --source {source_id} --output <file>"
            )),
            None,
            ActionSideEffect::Read,
        ),
        "replay" => (
            "Replay Source",
            Some(format!("sinexctl ops replay plan --source {source_id}")),
            Some(methods::REPLAY_CREATE_OPERATION),
            ActionSideEffect::Write,
        ),
        "delete-material" => (
            "Delete Material",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Destructive,
        ),
        "run-model" => (
            "Run Local Model",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Admin,
        ),
        "run-ocr" => (
            "Run OCR",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Admin,
        ),
        "retry" => (
            "Retry Package Operation",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Write,
        ),
        "rebuild-artifact" => (
            "Rebuild Artifact",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Write,
        ),
        "enable-session" => (
            "Enable Capture Session",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Admin,
        ),
        "disable-session" => (
            "Disable Capture Session",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Admin,
        ),
        "capture-region" => (
            "Capture Region",
            package_operation_command_hint(operation, source_id),
            package_operation_rpc_method(operation, source_id),
            ActionSideEffect::Admin,
        ),
        _ => ("Package Operation", None, None, ActionSideEffect::Read),
    };
    let state = if command_hint.is_some() {
        ActionAvailabilityState::Enabled
    } else {
        ActionAvailabilityState::Unavailable
    };
    let reason = if command_hint.is_some() {
        format!("package declares `{operation}` for source `{source_id}`")
    } else {
        format!(
            "package declares `{operation}` for source `{source_id}`, but no operator actuator command is wired yet"
        )
    };
    let mut action = ActionAvailability {
        id: operation.to_string(),
        label: label.to_string(),
        state,
        reason: Some(reason),
        command_hint: None,
        rpc_method: None,
        side_effect,
        requires_confirmation: matches!(
            side_effect,
            ActionSideEffect::Admin | ActionSideEffect::Destructive
        ),
        dry_run_available: false,
        audit_output_ref: None,
    };
    if let Some(command_hint) = command_hint {
        action = action.with_command_hint(command_hint);
    }
    if let Some(rpc_method) = rpc_method {
        action = action.with_rpc_method(rpc_method);
    }
    action
}

fn package_operation_command_hint(operation: &str, source_id: &str) -> Option<String> {
    let mode_id = package_operation_mode_hint(operation)?;

    Some(format!(
        "sinexctl ops start {operation} --scope '{{\"source_id\":\"{source_id}\",\"mode_id\":\"{mode_id}\"}}' --format json"
    ))
}

fn package_operation_mode_hint(operation: &str) -> Option<&'static str> {
    let mode_id = match operation {
        "media.audio-transcript.run-model"
        | "media.audio-transcript.retry"
        | "media.audio-transcript.rebuild-artifact" => {
            "source:media.audio-transcript.local-model-batch"
        }
        "media.audio-transcript.enable-session"
        | "media.audio-transcript.disable-session"
        | "media.audio-transcript.pause"
        | "media.audio-transcript.resume" => "source:media.audio-transcript.live-session",
        "media.audio-transcript.delete-material" => {
            "source:media.audio-transcript.audio-bundle-staged"
        }
        "media.screen-ocr.run-ocr"
        | "media.screen-ocr.retry"
        | "media.screen-ocr.rebuild-artifact" => "source:media.screen-ocr.local-model-batch",
        "media.screen-ocr.capture-region" => "source:media.screen-ocr.on-demand-region",
        "media.screen-ocr.enable-session"
        | "media.screen-ocr.disable-session"
        | "media.screen-ocr.pause"
        | "media.screen-ocr.resume" => "source:media.screen-ocr.live-session",
        "media.screen-ocr.delete-material" => "source:media.screen-ocr.screenshot-ocr-staged",
        "email.mailbox.authorize" | "email.mailbox.pause" | "email.mailbox.resume" => {
            "<provider-mode-id>"
        }
        "email.mailbox.sync" => "<email-mode-id>",
        _ => return None,
    };
    Some(mode_id)
}

fn package_operation_rpc_method(operation: &str, source_id: &str) -> Option<&'static str> {
    package_operation_command_hint(operation, source_id).map(|_| methods::OPS_START)
}

fn package_operation_label<'a>(operation: &str, label: &'a str) -> Option<&'a str> {
    package_operation_mode_hint(operation).map(|_| label)
}

fn source_runtime_module(source_id: &str) -> Option<&'static str> {
    match source_id {
        "terminal.kitty-osc-live" => Some("terminal-source"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use sinex_primitives::domain::{HealthStatus, ModuleName};
    use sinex_primitives::privacy::ProcessingContext;
    use sinex_primitives::rpc::method_catalog;
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

    fn assert_action_rpc_methods_are_cataloged(
        source_id: &str,
        actions: &[ActionAvailability],
    ) -> xtask::TestResult<()> {
        let catalog = method_catalog()
            .into_iter()
            .map(|method| method.name)
            .collect::<BTreeSet<_>>();
        let missing = actions
            .iter()
            .filter_map(|action| {
                action
                    .rpc_method
                    .as_deref()
                    .filter(|method| !catalog.contains(method))
                    .map(|method| format!("{} -> {}", action.id, method))
            })
            .collect::<Vec<_>>();
        color_eyre::eyre::ensure!(
            missing.is_empty(),
            "source {source_id} actions reference unknown RPC methods: {missing:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_actions_ignore_non_operation_capability_refs() -> xtask::TestResult<()> {
        static CAPABILITIES: &[&str] = &[
            "coverage:source-coverage",
            "operation:",
            "operation:fixture.source.check",
            "package:fixture.source",
        ];
        let binding = SourceRuntimeBinding::builder(
            SubjectRef::from_static("source:fixture.source"),
            "fixture.source",
            "fixture",
        )
        .implementation("sinexd")
        .adapter("StaticFileAdapter")
        .output_event_type("fixture.event")
        .privacy_context(ProcessingContext::Command)
        .resource_profile(ResourceProfile::BoundedFile)
        .capabilities(CAPABILITIES)
        .source_id("fixture.source")
        .runner_pack(RunnerPack::SinexdSource)
        .checkpoint_family(CheckpointFamily::AppendStream)
        .runtime_shape(RuntimeShape::OnDemand)
        .build_impact(SourceBuildImpact::ZERO)
        .build();

        let actions = source_actions("fixture.source", &[&binding], false);
        assert!(
            actions
                .iter()
                .any(|action| action.id == "fixture.source.check")
        );
        assert!(!actions.iter().any(|action| action.id.is_empty()));
        assert!(
            actions
                .iter()
                .all(|action| !action.id.starts_with("package:"))
        );
        assert_action_rpc_methods_are_cataloged("fixture.source", &actions)?;
        Ok(())
    }

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
            &HashMap::new(),
            Timestamp::now(),
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
        assert_action_rpc_methods_are_cataloged(CONTRACT.id, &view.actions)?;
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
            &HashMap::new(),
            Timestamp::now(),
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
            "operation:terminal.activity.pause",
            "operation:terminal.activity.resume",
            "operation:terminal.activity.drain",
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
            &HashMap::new(),
            Timestamp::now(),
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
            Some("sinexctl sources status terminal.kitty-osc-live --format json")
        );

        let pause = view
            .actions
            .iter()
            .find(|action| action.id == "terminal.activity.pause")
            .ok_or_else(|| color_eyre::eyre::eyre!("pause action expected"))?;
        assert_eq!(pause.state, ActionAvailabilityState::Enabled);
        assert_eq!(pause.side_effect, ActionSideEffect::Admin);
        assert!(pause.requires_confirmation);
        assert_eq!(pause.rpc_method.as_deref(), Some("runtime.drain"));
        assert_eq!(
            pause.command_hint.as_deref(),
            Some("sinexctl runtime drain terminal-source --reason source-paused")
        );

        let resume = view
            .actions
            .iter()
            .find(|action| action.id == "terminal.activity.resume")
            .ok_or_else(|| color_eyre::eyre::eyre!("resume action expected"))?;
        assert_eq!(resume.state, ActionAvailabilityState::Enabled);
        assert_eq!(resume.side_effect, ActionSideEffect::Admin);
        assert!(resume.requires_confirmation);
        assert_eq!(resume.rpc_method.as_deref(), Some("runtime.resume"));
        assert_eq!(
            resume.command_hint.as_deref(),
            Some("sinexctl runtime resume terminal-source")
        );

        let drain = view
            .actions
            .iter()
            .find(|action| action.id == "terminal.activity.drain")
            .ok_or_else(|| color_eyre::eyre::eyre!("drain action expected"))?;
        assert_eq!(drain.state, ActionAvailabilityState::Enabled);
        assert_eq!(drain.side_effect, ActionSideEffect::Admin);
        assert!(drain.requires_confirmation);
        assert_eq!(drain.rpc_method.as_deref(), Some("runtime.drain"));
        assert_eq!(
            drain.command_hint.as_deref(),
            Some("sinexctl runtime drain terminal-source --reason source-coverage")
        );

        let reconnect = view
            .actions
            .iter()
            .find(|action| action.id == "terminal.activity.reconnect")
            .ok_or_else(|| color_eyre::eyre::eyre!("reconnect action expected"))?;
        assert_eq!(reconnect.state, ActionAvailabilityState::Enabled);
        assert_eq!(reconnect.side_effect, ActionSideEffect::Admin);
        assert!(reconnect.requires_confirmation);
        assert_eq!(reconnect.rpc_method.as_deref(), Some("runtime.resume"));
        assert_eq!(
            reconnect.command_hint.as_deref(),
            Some("sinexctl runtime resume terminal-source")
        );
        assert_action_rpc_methods_are_cataloged(BRIDGE_CONTRACT.id, &view.actions)?;
        Ok(())
    }

    #[sinex_test]
    async fn media_package_operations_surface_operator_actions() -> xtask::TestResult<()> {
        let contract = all_source_contracts()
            .find(|contract| contract.id == "media.audio-transcript")
            .ok_or_else(|| color_eyre::eyre::eyre!("media audio contract expected"))?;
        let bindings = source_runtime_bindings()
            .filter(|binding| binding.source_id == "media.audio-transcript")
            .collect::<Vec<_>>();

        let view = source_coverage_view(
            contract,
            &bindings,
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
            &HashMap::new(),
            Timestamp::now(),
        );

        let import_transcript = view
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.import-transcript")
            .ok_or_else(|| color_eyre::eyre::eyre!("transcript import action expected"))?;
        assert_eq!(
            import_transcript.command_hint.as_deref(),
            Some("sinexctl sources stage <path> --format json")
        );
        assert_eq!(
            import_transcript.rpc_method.as_deref(),
            Some("sources.stage")
        );
        assert_eq!(import_transcript.side_effect, ActionSideEffect::Write);
        assert_eq!(import_transcript.state, ActionAvailabilityState::Enabled);

        let import_bundle = view
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.import-bundle")
            .ok_or_else(|| color_eyre::eyre::eyre!("audio bundle import action expected"))?;
        assert_eq!(
            import_bundle.command_hint.as_deref(),
            Some(
                "sinexctl sources stage <path> --binding source:media.audio-transcript.audio-bundle-staged --format json"
            )
        );
        assert_eq!(import_bundle.rpc_method.as_deref(), Some("sources.stage"));
        assert_eq!(import_bundle.side_effect, ActionSideEffect::Write);
        assert_eq!(import_bundle.state, ActionAvailabilityState::Enabled);

        let replay = view
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.replay")
            .ok_or_else(|| color_eyre::eyre::eyre!("replay action expected"))?;
        assert_eq!(
            replay.command_hint.as_deref(),
            Some("sinexctl ops replay plan --source media.audio-transcript")
        );
        assert_eq!(
            replay.rpc_method.as_deref(),
            Some(methods::REPLAY_CREATE_OPERATION)
        );
        assert_eq!(replay.side_effect, ActionSideEffect::Write);

        let export = view
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.export")
            .ok_or_else(|| color_eyre::eyre::eyre!("export action expected"))?;
        assert_eq!(
            export.command_hint.as_deref(),
            Some("sinexctl privacy export --source media.audio-transcript --output <file>")
        );

        let run_model = view
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.run-model")
            .ok_or_else(|| color_eyre::eyre::eyre!("model action expected"))?;
        assert_eq!(run_model.state, ActionAvailabilityState::Enabled);
        assert_eq!(run_model.side_effect, ActionSideEffect::Admin);
        assert_eq!(run_model.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(
            run_model.command_hint.as_deref(),
            Some(
                "sinexctl ops start media.audio-transcript.run-model --scope '{\"source_id\":\"media.audio-transcript\",\"mode_id\":\"source:media.audio-transcript.local-model-batch\"}' --format json"
            )
        );

        let pause = view
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.pause")
            .ok_or_else(|| color_eyre::eyre::eyre!("media pause action expected"))?;
        assert_eq!(pause.state, ActionAvailabilityState::Enabled);
        assert_eq!(pause.side_effect, ActionSideEffect::Admin);
        assert_eq!(pause.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(
            pause.command_hint.as_deref(),
            Some(
                "sinexctl ops start media.audio-transcript.pause --scope '{\"source_id\":\"media.audio-transcript\",\"mode_id\":\"source:media.audio-transcript.live-session\"}' --format json"
            )
        );

        let delete = view
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.delete-material")
            .ok_or_else(|| color_eyre::eyre::eyre!("delete material action expected"))?;
        assert_eq!(delete.state, ActionAvailabilityState::Enabled);
        assert_eq!(delete.side_effect, ActionSideEffect::Destructive);
        assert!(delete.requires_confirmation);
        assert_eq!(delete.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(
            delete.command_hint.as_deref(),
            Some(
                "sinexctl ops start media.audio-transcript.delete-material --scope '{\"source_id\":\"media.audio-transcript\",\"mode_id\":\"source:media.audio-transcript.audio-bundle-staged\"}' --format json"
            )
        );

        let screen_contract = all_source_contracts()
            .find(|contract| contract.id == "media.screen-ocr")
            .ok_or_else(|| color_eyre::eyre::eyre!("media screen contract expected"))?;
        let screen_bindings = source_runtime_bindings()
            .filter(|binding| binding.source_id == "media.screen-ocr")
            .collect::<Vec<_>>();
        let screen_view = source_coverage_view(
            screen_contract,
            &screen_bindings,
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
            &HashMap::new(),
            Timestamp::now(),
        );

        let import_screenshots = screen_view
            .actions
            .iter()
            .find(|action| action.id == "media.screen-ocr.import-screenshots")
            .ok_or_else(|| color_eyre::eyre::eyre!("screenshot import action expected"))?;
        assert_eq!(
            import_screenshots.command_hint.as_deref(),
            Some(
                "sinexctl sources stage <path> --binding source:media.screen-ocr.screenshot-ocr-staged --format json"
            )
        );
        assert_eq!(
            import_screenshots.rpc_method.as_deref(),
            Some("sources.stage")
        );
        assert_eq!(import_screenshots.side_effect, ActionSideEffect::Write);
        assert_eq!(import_screenshots.state, ActionAvailabilityState::Enabled);

        let run_ocr = screen_view
            .actions
            .iter()
            .find(|action| action.id == "media.screen-ocr.run-ocr")
            .ok_or_else(|| color_eyre::eyre::eyre!("run OCR action expected"))?;
        assert_eq!(run_ocr.state, ActionAvailabilityState::Enabled);
        assert_eq!(run_ocr.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(
            run_ocr.command_hint.as_deref(),
            Some(
                "sinexctl ops start media.screen-ocr.run-ocr --scope '{\"source_id\":\"media.screen-ocr\",\"mode_id\":\"source:media.screen-ocr.local-model-batch\"}' --format json"
            )
        );

        let capture_region = screen_view
            .actions
            .iter()
            .find(|action| action.id == "media.screen-ocr.capture-region")
            .ok_or_else(|| color_eyre::eyre::eyre!("capture-region action expected"))?;
        assert_eq!(capture_region.state, ActionAvailabilityState::Enabled);
        assert_eq!(capture_region.side_effect, ActionSideEffect::Admin);
        assert_eq!(capture_region.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(
            capture_region.command_hint.as_deref(),
            Some(
                "sinexctl ops start media.screen-ocr.capture-region --scope '{\"source_id\":\"media.screen-ocr\",\"mode_id\":\"source:media.screen-ocr.on-demand-region\"}' --format json"
            )
        );
        assert_action_rpc_methods_are_cataloged("media.audio-transcript", &view.actions)?;
        assert_action_rpc_methods_are_cataloged("media.screen-ocr", &screen_view.actions)?;

        Ok(())
    }

    #[sinex_test]
    async fn email_package_operations_surface_operator_actions() -> xtask::TestResult<()> {
        let contract = all_source_contracts()
            .find(|contract| contract.id == "email.mailbox")
            .ok_or_else(|| color_eyre::eyre::eyre!("email mailbox contract expected"))?;
        let bindings = source_runtime_bindings()
            .filter(|binding| binding.source_id == "email.mailbox")
            .collect::<Vec<_>>();

        let view = source_coverage_view(
            contract,
            &bindings,
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
            &HashMap::new(),
            Timestamp::now(),
        );

        let authorize = view
            .actions
            .iter()
            .find(|action| action.id == "email.mailbox.authorize")
            .ok_or_else(|| color_eyre::eyre::eyre!("email authorize action expected"))?;
        assert_eq!(authorize.state, ActionAvailabilityState::Enabled);
        assert_eq!(authorize.side_effect, ActionSideEffect::Admin);
        assert_eq!(authorize.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(
            authorize.command_hint.as_deref(),
            Some(
                "sinexctl ops start email.mailbox.authorize --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"<provider-mode-id>\"}' --format json"
            )
        );

        let sync = view
            .actions
            .iter()
            .find(|action| action.id == "email.mailbox.sync")
            .ok_or_else(|| color_eyre::eyre::eyre!("email sync action expected"))?;
        assert_eq!(sync.state, ActionAvailabilityState::Enabled);
        assert_eq!(sync.side_effect, ActionSideEffect::Write);
        assert_eq!(sync.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(
            sync.command_hint.as_deref(),
            Some(
                "sinexctl ops start email.mailbox.sync --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"<email-mode-id>\"}' --format json"
            )
        );

        let pause = view
            .actions
            .iter()
            .find(|action| action.id == "email.mailbox.pause")
            .ok_or_else(|| color_eyre::eyre::eyre!("email pause action expected"))?;
        assert_eq!(pause.state, ActionAvailabilityState::Enabled);
        assert_eq!(pause.side_effect, ActionSideEffect::Admin);
        assert_eq!(pause.rpc_method.as_deref(), Some("ops.start"));

        let check = view
            .actions
            .iter()
            .find(|action| action.id == "email.mailbox.check")
            .ok_or_else(|| color_eyre::eyre::eyre!("email check action expected"))?;
        assert_eq!(check.state, ActionAvailabilityState::Enabled);
        assert_eq!(
            check.command_hint.as_deref(),
            Some("sinexctl sources status email.mailbox --format json")
        );
        assert_eq!(check.side_effect, ActionSideEffect::Read);

        assert_action_rpc_methods_are_cataloged("email.mailbox", &view.actions)?;
        Ok(())
    }

    #[sinex_test]
    async fn runtime_bridge_coverage_uses_runtime_observation_for_last_seen()
    -> xtask::TestResult<()> {
        let now = Timestamp::now();
        let bridge_binding = terminal_bridge_binding();
        let mut observations = HashMap::new();
        observations.insert(
            "terminal-source".to_string(),
            terminal_bridge_status(now).with_recent_output(now - time::Duration::seconds(30), 7),
        );

        let view = source_coverage_view(
            &terminal_bridge_contract(),
            &[&bridge_binding],
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
            &observations,
            now,
        );

        assert!(
            !view
                .caveats
                .iter()
                .any(|caveat| caveat.id == "source.runtime_bridge.unobserved"),
            "observed runtime state should replace the static unobserved bridge caveat"
        );
        let observed = view
            .caveats
            .iter()
            .find(|caveat| caveat.id == "source.runtime_bridge.observed")
            .ok_or_else(|| color_eyre::eyre::eyre!("observed runtime caveat expected"))?;
        assert!(observed.message.contains("connected"));
        assert!(observed.message.contains("terminal-source"));
        assert!(observed.message.contains("last heartbeat"));
        assert!(observed.message.contains("last output"));
        assert!(observed.message.contains("recent output count 7"));
        assert!(
            view.gaps
                .iter()
                .all(|gap| gap.kind != "runtime_bridge_disconnected")
        );
        Ok(())
    }

    #[sinex_test]
    async fn runtime_bridge_coverage_surfaces_disconnected_runtime_observation()
    -> xtask::TestResult<()> {
        let now = Timestamp::now();
        let bridge_binding = terminal_bridge_binding();
        let mut status = terminal_bridge_status(now);
        status.live = false;
        status.current_health = Some(HealthStatus::Unhealthy);
        status.health_reason = Some("runtime module disconnected".to_string());
        let mut observations = HashMap::new();
        observations.insert("terminal-source".to_string(), status);

        let view = source_coverage_view(
            &terminal_bridge_contract(),
            &[&bridge_binding],
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
            &observations,
            now,
        );

        assert!(
            view.gaps
                .iter()
                .any(|gap| gap.kind == "runtime_bridge_disconnected"),
            "disconnected runtime observations should become source coverage gaps"
        );
        let caveat = view
            .caveats
            .iter()
            .find(|caveat| caveat.id == "source.runtime_bridge.disconnected")
            .ok_or_else(|| color_eyre::eyre::eyre!("disconnected caveat expected"))?;
        assert!(caveat.message.contains("disconnected"));
        assert!(caveat.message.contains("last heartbeat"));
        Ok(())
    }

    #[sinex_test]
    async fn runtime_bridge_coverage_surfaces_malformed_frame_health_reason()
    -> xtask::TestResult<()> {
        let now = Timestamp::now();
        let bridge_binding = terminal_bridge_binding();
        let mut status = terminal_bridge_status(now);
        status.current_health = Some(HealthStatus::Degraded);
        status.health_reason = Some("malformed Kitty OSC frame rejected".to_string());
        let mut observations = HashMap::new();
        observations.insert("terminal-source".to_string(), status);

        let view = source_coverage_view(
            &terminal_bridge_contract(),
            &[&bridge_binding],
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
            &observations,
            now,
        );

        let caveat = view
            .caveats
            .iter()
            .find(|caveat| caveat.id == "source.runtime_bridge.health")
            .ok_or_else(|| color_eyre::eyre::eyre!("runtime health caveat expected"))?;
        assert!(caveat.message.contains("degraded"));
        assert!(
            caveat
                .message
                .contains("malformed Kitty OSC frame rejected")
        );
        Ok(())
    }

    #[sinex_test]
    async fn runtime_bridge_coverage_surfaces_heartbeat_without_output_as_stalled()
    -> xtask::TestResult<()> {
        let now = Timestamp::now();
        let bridge_binding = terminal_bridge_binding();
        let mut observations = HashMap::new();
        observations.insert("terminal-source".to_string(), terminal_bridge_status(now));

        let view = source_coverage_view(
            &terminal_bridge_contract(),
            &[&bridge_binding],
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
            &observations,
            now,
        );

        assert!(
            view.gaps
                .iter()
                .any(|gap| gap.kind == "runtime_bridge_stalled"),
            "heartbeating bridge with no recent output should become coverage debt"
        );
        let caveat = view
            .caveats
            .iter()
            .find(|caveat| caveat.id == "source.runtime_bridge.stalled")
            .ok_or_else(|| color_eyre::eyre::eyre!("stalled caveat expected"))?;
        assert!(caveat.message.contains("heartbeating"));
        assert!(caveat.message.contains("no recent source output"));
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
            &HashMap::new(),
            Timestamp::now(),
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
            &HashMap::new(),
            Timestamp::now(),
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
            runtime_action: sinex_primitives::RuntimePressureAction::Admit,
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

    fn terminal_bridge_contract() -> SourceContract {
        SourceContract {
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
        }
    }

    fn terminal_bridge_binding() -> SourceRuntimeBinding {
        static BRIDGE_CAPABILITIES: &[&str] = &[
            "coverage:source-coverage",
            "operation:terminal.activity.check",
            "operation:terminal.activity.reconnect",
            "operation:terminal.activity.pause",
            "operation:terminal.activity.resume",
            "operation:terminal.activity.drain",
            "operation:terminal.activity.inspect",
        ];
        SourceRuntimeBinding::builder(
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
        .build()
    }

    trait SourceStatusTestExt {
        fn with_recent_output(self, last_output_at: Timestamp, recent_output_count: i64) -> Self;
    }

    impl SourceStatusTestExt for SourceStatus {
        fn with_recent_output(
            mut self,
            last_output_at: Timestamp,
            recent_output_count: i64,
        ) -> Self {
            self.last_output_at = Some(last_output_at);
            self.recent_output_count = recent_output_count;
            self
        }
    }

    fn terminal_bridge_status(now: Timestamp) -> SourceStatus {
        SourceStatus {
            module_name: ModuleName::new("terminal-source"),
            version: "1.0.0".to_string(),
            description: Some("Kitty OSC live terminal bridge".to_string()),
            manifest_status: "running".to_string(),
            live: true,
            service_name: Some("sinexd".to_string()),
            instance_id: Some("fixture-instance".to_string()),
            module_run_id: None,
            host: Some("fixture-host".to_string()),
            run_status: Some("running".to_string()),
            started_at: Some(now - time::Duration::seconds(3600)),
            last_heartbeat_at: Some(now - time::Duration::seconds(5)),
            current_health: Some(HealthStatus::Healthy),
            health_changed_at: Some(now - time::Duration::seconds(5)),
            health_reason: Some("bridge connected".to_string()),
            recent_output_count: 0,
            last_output_at: None,
        }
    }
}
