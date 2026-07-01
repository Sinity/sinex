//! Operator-facing source status handler.

use crate::api::service_container::{ConfirmationBufferHealth, ServiceContainer};
use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_db::repositories::{EmailMailboxProjectionSummary, EmailProviderStateRecord};
use sinex_primitives::SinexError;
use sinex_primitives::rpc::{
    methods,
    source_status::{
        EmitStallThresholds, EmitStallVerdict, SourceStatus, SourcesStatusRequest,
        SourcesStatusResponse, SourcesStatusViewRequest,
    },
};
use sinex_primitives::source_contracts::{
    AccessScope, BudgetPressureAction, CheckpointFamily, DeliverySemantics,
    MaterialLifecyclePolicy, OrderingSemantics, ResourceBudgetSpec, ResourceProfile, RunnerPack,
    SourceCapabilityKind, SourceContract, SourceRuntimeBinding, TransportKind, WorkClass,
    all_source_contracts, source_runtime_bindings,
};
use sinex_primitives::sources::source_identity_matches_family;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, ActionSideEffect, CaveatView, CoverageGapView,
    SinexObjectKind, SinexObjectRef, SourceCoverageContinuity, SourceCoverageListView,
    SourceCoverageReadiness, SourceCoverageView, SourceModeStatusView, SourcePrivacyPosture,
    SourceResourceBudgetView, ViewEnvelope,
};
use sqlx::FromRow;
use sqlx::PgPool;
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

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

#[derive(Debug)]
struct EmailProviderOperationState {
    operation_id: Uuid,
    result_status: String,
    provider_runtime: Option<Value>,
    provider_failure: Option<Value>,
    failure_class: Option<String>,
    required_action: Option<String>,
    retry_after_secs: Option<i32>,
    reconnect_state: Option<String>,
}

#[derive(Debug, Clone)]
struct EmailMailboxProjectionState {
    message_count: i64,
    thread_count: i64,
    body_bytes: i64,
    attachment_count: i64,
    attachment_observed_count: i64,
    last_observed_at: Timestamp,
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
    request: SourcesStatusViewRequest,
) -> Result<ViewEnvelope<SourceCoverageListView>> {
    let pool = services.pool();
    let confirmation_buffer = services.probe_confirmation_buffer_pressure().await;
    let now = Timestamp::now();
    let status_defaults = SourcesStatusRequest::default();
    let bindings: Vec<&'static SourceRuntimeBinding> = source_runtime_bindings().collect();
    let mut contracts = matching_source_contracts(&request);
    contracts.sort_by_key(|contract| contract.id);
    let source_status_module_names = source_status_module_names(&contracts);
    let source_status_rows = if contracts.is_empty() && source_status_view_has_filter(&request) {
        Vec::new()
    } else {
        pool.state()
            .list_sources_status_for_modules(
                Duration::from_secs(status_defaults.stale_after_secs),
                Duration::from_secs(status_defaults.recent_window_secs),
                &source_status_module_names,
            )
            .await
            .map_err(|error| {
                SinexError::database("Failed to compute source runtime status")
                    .with_std_error(&error)
            })?
    };
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

    let event_rows = load_source_event_aggregates(pool, &contracts, request.exact_counts).await?;

    let mut event_aggregates = HashMap::<(String, String), SourceEventAggregateRow>::new();
    for row in event_rows {
        event_aggregates.insert((row.source.clone(), row.event_type.clone()), row);
    }
    let material_aggregates: HashMap<String, SourceMaterialAggregateRow> = material_rows
        .into_iter()
        .map(|row| (row.source_identifier.clone(), row))
        .collect();
    let runtime_observations = source_runtime_observations(source_status_rows);
    let email_provider_states = latest_email_provider_operation_states(pool).await?;
    let email_projection_states = latest_email_mailbox_projection_states(pool).await?;
    let session_states = latest_source_session_states(pool).await?;

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
            &email_provider_states,
            &email_projection_states,
            &session_states,
            now,
        ));
    }

    let mut envelope = ViewEnvelope::new(
        "sinexd.sources.status.view",
        SourceCoverageListView::new(views),
    );
    if source_status_view_has_filter(&request) || !request.exact_counts {
        envelope.query_echo = Some(serde_json::json!({
            "source": request.source.as_deref().filter(|value| !value.is_empty()),
            "family": request.family.as_deref().filter(|value| !value.is_empty()),
            "exact_counts": request.exact_counts,
        }));
    }
    if !request.exact_counts {
        envelope.caveats.push(CaveatView {
            id: "source.status.event_counts.presence_probe".to_string(),
            message: "filtered source status uses bounded event-presence probes; event_count is the number of declared event kinds with at least one live event, not the lifetime row count".to_string(),
            ref_: None,
        });
    }
    Ok(envelope)
}

fn source_status_view_has_filter(request: &SourcesStatusViewRequest) -> bool {
    request
        .source
        .as_deref()
        .is_some_and(|value| !value.is_empty())
        || request
            .family
            .as_deref()
            .is_some_and(|value| !value.is_empty())
}

fn matching_source_contracts(request: &SourcesStatusViewRequest) -> Vec<&'static SourceContract> {
    let source_filter = request.source.as_deref().filter(|value| !value.is_empty());
    let family_filter = request.family.as_deref().filter(|value| !value.is_empty());
    all_source_contracts()
        .filter(|contract| {
            source_filter.is_none_or(|filter| contract.id.contains(filter))
                && family_filter.is_none_or(|family| {
                    source_identity_matches_family(contract.id, contract.namespace, family)
                })
        })
        .collect()
}

fn source_status_module_names(contracts: &[&SourceContract]) -> Vec<String> {
    let mut names = BTreeSet::new();
    for contract in contracts {
        names.insert(contract.id.to_string());
        if let Some(module_name) = source_runtime_module(contract.id) {
            names.insert(module_name.to_string());
        }
    }
    names.into_iter().collect()
}

async fn load_source_event_aggregates(
    pool: &PgPool,
    contracts: &[&SourceContract],
    exact_counts: bool,
) -> Result<Vec<SourceEventAggregateRow>> {
    let (sources, event_types) = source_event_pairs(contracts);
    if sources.is_empty() {
        return Ok(Vec::new());
    }

    if exact_counts {
        sqlx::query_as!(
            SourceEventAggregateRow,
            r#"
            WITH wanted AS (
                SELECT *
                FROM unnest($1::text[], $2::text[]) AS pair(source, event_type)
            )
            SELECT
                wanted.source as "source!",
                wanted.event_type as "event_type!",
                COUNT(*)::bigint as "event_count!",
                MAX(events.ts_orig) as "last_event_at: _"
            FROM wanted
            JOIN core.events events
              ON events.source = wanted.source
             AND events.event_type = wanted.event_type
            GROUP BY wanted.source, wanted.event_type
            "#,
            &sources,
            &event_types,
        )
        .fetch_all(pool)
        .await
        .map_err(|error| {
            SinexError::database("Failed to compute source event coverage").with_std_error(&error)
        })
    } else {
        sqlx::query_as!(
            SourceEventAggregateRow,
            r#"
            WITH wanted AS (
                SELECT *
                FROM unnest($1::text[], $2::text[]) AS pair(source, event_type)
            )
            SELECT
                wanted.source as "source!",
                wanted.event_type as "event_type!",
                1::bigint as "event_count!",
                latest.ts_orig as "last_event_at: _"
            FROM wanted
            JOIN LATERAL (
                SELECT events.ts_orig
                FROM core.events events
                WHERE events.source = wanted.source
                  AND events.event_type = wanted.event_type
                ORDER BY events.ts_orig DESC
                LIMIT 1
            ) latest ON true
            "#,
            &sources,
            &event_types,
        )
        .fetch_all(pool)
        .await
        .map_err(|error| {
            SinexError::database("Failed to compute source event presence").with_std_error(&error)
        })
    }
}

fn source_event_pairs(contracts: &[&SourceContract]) -> (Vec<String>, Vec<String>) {
    let mut seen = BTreeSet::new();
    let mut sources = Vec::new();
    let mut event_types = Vec::new();
    for contract in contracts {
        for (source, event_type) in contract.event_types {
            if seen.insert((*source, *event_type)) {
                sources.push((*source).to_string());
                event_types.push((*event_type).to_string());
            }
        }
    }
    (sources, event_types)
}

fn source_coverage_view(
    contract: &SourceContract,
    bindings: &[&SourceRuntimeBinding],
    event_aggregates: &HashMap<(String, String), SourceEventAggregateRow>,
    material_aggregates: &HashMap<String, SourceMaterialAggregateRow>,
    confirmation_buffer: &ConfirmationBufferHealth,
    runtime_observations: &HashMap<String, SourceStatus>,
    email_provider_states: &HashMap<String, EmailProviderOperationState>,
    email_projection_states: &HashMap<String, EmailMailboxProjectionState>,
    session_states: &HashMap<String, Vec<sinex_db::repositories::SourceSessionStateRecord>>,
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
    let accepted_binding_count = bindings.iter().filter(|binding| !binding.proposed).count();
    let proposed_binding_count = bindings.len().saturating_sub(accepted_binding_count);
    let has_live_binding = accepted_binding_count > 0;
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
    if contract.id == "email.mailbox" {
        caveats.extend(email_provider_operation_caveats(
            bindings,
            email_provider_states,
        ));
        caveats.extend(email_mailbox_projection_caveats(
            bindings,
            email_projection_states,
        ));
    }
    if let Some(sessions) = session_states.get(contract.id) {
        caveats.extend(sessions.iter().map(session_control_caveat));
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
    let modes = bindings
        .iter()
        .map(|binding| {
            source_mode_status_view(
                contract.id,
                binding,
                runtime_observations,
                email_provider_states,
                email_projection_states,
            )
        })
        .collect();

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
        accepted_binding_count,
        proposed_binding_count,
        gaps,
        caveats,
        privacy: SourcePrivacyPosture {
            tier: format!("{:?}", contract.privacy_tier).to_ascii_lowercase(),
            context: privacy_context,
            proposed: accepted_binding_count == 0 && proposed_binding_count > 0,
        },
        resource_budget,
        modes,
        actions: source_actions(contract.id, bindings, pressure.is_some()),
    }
}

async fn latest_email_provider_operation_states(
    pool: &PgPool,
) -> Result<HashMap<String, EmailProviderOperationState>> {
    let rows = pool
        .email_provider_states()
        .list_current_by_source("email.mailbox")
        .await
        .map_err(|error| {
            SinexError::database("Failed to load email provider runtime state")
                .with_std_error(&error)
        })?;

    Ok(email_provider_operation_states_from_rows(rows))
}

async fn latest_email_mailbox_projection_states(
    pool: &PgPool,
) -> Result<HashMap<String, EmailMailboxProjectionState>> {
    let rows = pool
        .email_mailbox_projections()
        .summarize_by_source("email.mailbox")
        .await
        .map_err(|error| {
            SinexError::database("Failed to load email mailbox projection state")
                .with_std_error(&error)
        })?;

    Ok(email_mailbox_projection_states_from_rows(rows))
}

fn email_provider_operation_states_from_rows(
    rows: Vec<EmailProviderStateRecord>,
) -> HashMap<String, EmailProviderOperationState> {
    let mut states = HashMap::new();
    for row in rows {
        if !states.contains_key(&row.mode_id) {
            states.insert(
                row.mode_id,
                EmailProviderOperationState {
                    operation_id: row.operation_id,
                    result_status: row.result_status.to_string(),
                    provider_runtime: Some(row.provider_runtime),
                    provider_failure: row.provider_failure,
                    failure_class: row.failure_class,
                    required_action: row.required_action,
                    retry_after_secs: row.retry_after_secs,
                    reconnect_state: row.reconnect_state,
                },
            );
        }
    }
    states
}

fn email_mailbox_projection_states_from_rows(
    rows: Vec<EmailMailboxProjectionSummary>,
) -> HashMap<String, EmailMailboxProjectionState> {
    rows.into_iter()
        .map(|row| {
            (
                row.mode_id,
                EmailMailboxProjectionState {
                    message_count: row.message_count,
                    thread_count: row.thread_count,
                    body_bytes: row.body_bytes,
                    attachment_count: row.attachment_count,
                    attachment_observed_count: row.attachment_observed_count,
                    last_observed_at: Timestamp::from(row.last_observed_at),
                },
            )
        })
        .collect()
}

async fn latest_source_session_states(
    pool: &PgPool,
) -> Result<HashMap<String, Vec<sinex_db::repositories::SourceSessionStateRecord>>> {
    let mut by_source: HashMap<String, Vec<sinex_db::repositories::SourceSessionStateRecord>> =
        HashMap::new();
    for record in pool.source_session_states().list_all_current().await? {
        by_source
            .entry(record.source_id.clone())
            .or_default()
            .push(record);
    }
    Ok(by_source)
}

/// Passive operator read of a live-session control row: what state the operator
/// last set, and whether capture is currently suspended. Surfaced in
/// `sinexctl sources status` so the operator can see pause/disable/private
/// posture without invoking the `inspect` operation.
fn session_control_caveat(
    record: &sinex_db::repositories::SourceSessionStateRecord,
) -> CaveatView {
    let suspended = record.private_mode_blocked
        || matches!(record.lifecycle_state.as_str(), "disabled" | "paused");
    let posture = if suspended {
        "capture suspended"
    } else {
        "capture active"
    };
    let private_fragment = if record.private_mode_blocked {
        "; private_mode_blocked=true"
    } else {
        ""
    };
    let reason_fragment = record
        .reason
        .as_deref()
        .map(|reason| format!("; reason={reason}"))
        .unwrap_or_default();
    CaveatView {
        id: format!(
            "source.live_session.{}.{}",
            record.lifecycle_state, record.session_scope
        ),
        message: format!(
            "live-session `{}` (scope `{}`) is {} — {posture}; visibility={}{private_fragment}{reason_fragment}; coverage_ref={}; debt_ref={}",
            record.mode_id,
            record.session_scope,
            record.lifecycle_state,
            record.visibility_state,
            record.coverage_ref,
            record.debt_ref,
        ),
        ref_: Some(SinexObjectRef::new(
            SinexObjectKind::Operation,
            record.operation_id.to_string(),
        )),
    }
}

fn email_provider_operation_caveats(
    bindings: &[&SourceRuntimeBinding],
    states: &HashMap<String, EmailProviderOperationState>,
) -> Vec<CaveatView> {
    bindings
        .iter()
        .filter_map(|binding| {
            let mode_id = binding.subject.as_str();
            let state = states.get(mode_id)?;
            Some(email_provider_operation_caveat(mode_id, state))
        })
        .collect()
}

fn email_mailbox_projection_caveats(
    bindings: &[&SourceRuntimeBinding],
    states: &HashMap<String, EmailMailboxProjectionState>,
) -> Vec<CaveatView> {
    bindings
        .iter()
        .filter_map(|binding| {
            let mode_id = binding.subject.as_str();
            let state = states.get(mode_id)?;
            let mut debts = Vec::new();
            if state.body_bytes > 0 {
                debts.push(format!(
                    "{} message body byte(s) are represented as metadata only",
                    state.body_bytes
                ));
            }
            if state.attachment_count > state.attachment_observed_count {
                debts.push(format!(
                    "{} attachment(s) declared, {} attachment metadata event(s) observed",
                    state.attachment_count, state.attachment_observed_count
                ));
            }
            (!debts.is_empty()).then(|| CaveatView {
                id: format!("email.mailbox_projection.{mode_id}.materialization_debt"),
                message: format!(
                    "{mode_id} has {} projected message(s); {}; debt:email.mailbox.{mode_id}.projection_materialization",
                    state.message_count,
                    debts.join("; ")
                ),
                ref_: None,
            })
        })
        .collect()
}

fn email_provider_operation_caveat(
    mode_id: &str,
    state: &EmailProviderOperationState,
) -> CaveatView {
    let reason = state
        .provider_failure
        .as_ref()
        .and_then(|failure| failure.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or("provider runtime observed");
    let debt_ref = state
        .provider_failure
        .as_ref()
        .and_then(|failure| failure.get("debt_ref"))
        .and_then(Value::as_str)
        .unwrap_or("debt:email.mailbox.provider_runtime");
    let coverage_ref = state
        .provider_failure
        .as_ref()
        .and_then(|failure| failure.get("coverage_ref"))
        .and_then(Value::as_str)
        .or_else(|| provider_runtime_field(state, "coverage_ref"))
        .unwrap_or("coverage:email.mailbox.provider_runtime");
    let auth_state = provider_runtime_field(state, "auth_state").unwrap_or("unknown");
    let network_state = provider_runtime_field(state, "network_state").unwrap_or("unknown");
    let sync_state = provider_runtime_field(state, "sync_state").unwrap_or("unknown");
    let rate_limit_state = provider_runtime_field(state, "rate_limit_state");
    let failure_class = state.failure_class.as_deref();
    let required_action = state.required_action.as_deref();
    let caveat_kind = if state.result_status == "failure" {
        "failed"
    } else {
        "observed"
    };
    let rate_limit_fragment = rate_limit_state
        .map(|state| format!("; rate_limit_state={state}"))
        .unwrap_or_default();
    let failure_fragment = failure_class
        .map(|class| format!("; failure_class={class}"))
        .unwrap_or_default();
    let action_fragment = required_action
        .map(|action| format!("; required_action={action}"))
        .unwrap_or_default();
    let retry_after_fragment = state
        .retry_after_secs
        .map(|secs| format!("; retry_after_secs={secs}"))
        .unwrap_or_default();
    let reconnect_fragment = state
        .reconnect_state
        .as_deref()
        .map(|state| format!("; reconnect_state={state}"))
        .unwrap_or_default();

    CaveatView {
        id: format!("email.provider_runtime.{caveat_kind}.{mode_id}"),
        message: format!(
            "latest provider sync for `{mode_id}` ended with {}; auth_state={auth_state}; network_state={network_state}; sync_state={sync_state}{rate_limit_fragment}{failure_fragment}{action_fragment}{retry_after_fragment}{reconnect_fragment}; coverage_ref={coverage_ref}; debt_ref={debt_ref}; detail={reason}",
            state.result_status
        ),
        ref_: Some(SinexObjectRef::new(
            SinexObjectKind::Operation,
            state.operation_id.to_string(),
        )),
    }
}

fn provider_runtime_field<'a>(
    state: &'a EmailProviderOperationState,
    field: &str,
) -> Option<&'a str> {
    state
        .provider_runtime
        .as_ref()
        .and_then(|runtime| {
            runtime
                .get("runtime_observation_contract")
                .and_then(|contract| contract.get(field))
                .or_else(|| runtime.get(field))
        })
        .and_then(Value::as_str)
}

fn provider_failure_field<'a>(
    state: &'a EmailProviderOperationState,
    field: &str,
) -> Option<&'a str> {
    state
        .provider_failure
        .as_ref()
        .and_then(|failure| failure.get(field))
        .and_then(Value::as_str)
}

fn provider_coverage_ref(state: &EmailProviderOperationState) -> Option<&str> {
    provider_failure_field(state, "coverage_ref")
        .or_else(|| provider_runtime_field(state, "coverage_ref"))
}

fn provider_debt_ref(state: &EmailProviderOperationState) -> Option<&str> {
    provider_failure_field(state, "debt_ref").or_else(|| provider_runtime_field(state, "debt_ref"))
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

fn source_mode_status_view(
    source_id: &str,
    binding: &SourceRuntimeBinding,
    runtime_observations: &HashMap<String, SourceStatus>,
    email_provider_states: &HashMap<String, EmailProviderOperationState>,
    email_projection_states: &HashMap<String, EmailMailboxProjectionState>,
) -> SourceModeStatusView {
    let runtime_observation = runtime_observation_for_source(source_id, runtime_observations);
    let provider_state = (source_id == "email.mailbox")
        .then(|| email_provider_states.get(binding.subject.as_str()))
        .flatten();
    let projection_state = (source_id == "email.mailbox")
        .then(|| email_projection_states.get(binding.subject.as_str()))
        .flatten();
    SourceModeStatusView {
        mode_id: binding.subject.as_str().to_string(),
        binding_id: binding.id.to_string(),
        implementation: binding.implementation.to_string(),
        adapter: binding.adapter.to_string(),
        output_event_type: binding.output_event_type.to_string(),
        proposed: binding.proposed,
        runner_pack: runner_pack_name(binding.runner_pack).to_string(),
        runtime_shape: runtime_shape_name(binding.runtime_shape).to_string(),
        checkpoint_family: checkpoint_family_name(binding.checkpoint_family).to_string(),
        material_lifecycle: material_lifecycle_name(binding.material_lifecycle).to_string(),
        transport: transport_kind_name(binding.transport_semantics.transport).to_string(),
        delivery: delivery_semantics_name(binding.transport_semantics.delivery).to_string(),
        ordering: ordering_semantics_name(binding.transport_semantics.ordering).to_string(),
        replayable: binding.transport_semantics.replayable,
        dlq: binding.transport_semantics.dlq,
        backpressure: binding.transport_semantics.backpressure,
        privacy_context: format!("{:?}", binding.privacy_context).to_ascii_lowercase(),
        resource_budget: source_resource_budget_view(binding),
        runtime_observed: runtime_observation.map(|_| true),
        runtime_live: runtime_observation.map(|observation| observation.live),
        last_heartbeat_at: runtime_observation
            .and_then(|observation| observation.last_heartbeat_at),
        last_output_at: runtime_observation.and_then(|observation| observation.last_output_at),
        recent_output_count: runtime_observation.map(|observation| observation.recent_output_count),
        provider_operation_status: provider_state.map(|state| state.result_status.clone()),
        provider_auth_state: provider_state
            .and_then(|state| provider_runtime_field(state, "auth_state"))
            .map(str::to_string),
        provider_network_state: provider_state
            .and_then(|state| provider_runtime_field(state, "network_state"))
            .map(str::to_string),
        provider_sync_state: provider_state
            .and_then(|state| provider_runtime_field(state, "sync_state"))
            .map(str::to_string),
        provider_rate_limit_state: provider_state
            .and_then(|state| provider_runtime_field(state, "rate_limit_state"))
            .map(str::to_string),
        provider_failure_class: provider_state.and_then(|state| state.failure_class.clone()),
        provider_required_action: provider_state.and_then(|state| state.required_action.clone()),
        provider_retry_after_secs: provider_state.and_then(|state| state.retry_after_secs),
        provider_reconnect_state: provider_state.and_then(|state| state.reconnect_state.clone()),
        provider_operation_id: provider_state.map(|state| state.operation_id.to_string()),
        provider_coverage_ref: provider_state
            .and_then(provider_coverage_ref)
            .map(str::to_string),
        provider_debt_ref: provider_state
            .and_then(provider_debt_ref)
            .map(str::to_string),
        mailbox_projection_message_count: projection_state.map(|state| state.message_count),
        mailbox_projection_thread_count: projection_state.map(|state| state.thread_count),
        mailbox_projection_body_bytes: projection_state.map(|state| state.body_bytes),
        mailbox_projection_attachment_count: projection_state.map(|state| state.attachment_count),
        mailbox_projection_attachment_observed_count: projection_state
            .map(|state| state.attachment_observed_count),
        mailbox_projection_last_observed_at: projection_state.map(|state| state.last_observed_at),
        actions: binding
            .capability_refs()
            .filter(|capability| capability.is_kind(SourceCapabilityKind::Operation))
            .map(|capability| {
                operation_capability_action(
                    capability.target,
                    source_id,
                    Some(binding),
                    operation_action_key(capability.target, source_id, binding),
                )
            })
            .collect(),
    }
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

fn runner_pack_name(runner_pack: RunnerPack) -> &'static str {
    match runner_pack {
        RunnerPack::SinexdSource => "sinexd_source",
        RunnerPack::Live => "live",
        RunnerPack::Staged => "staged",
        RunnerPack::External => "external",
        RunnerPack::InProcess => "in_process",
    }
}

fn runtime_shape_name(shape: sinex_primitives::source_contracts::RuntimeShape) -> &'static str {
    match shape {
        sinex_primitives::source_contracts::RuntimeShape::Continuous => "continuous",
        sinex_primitives::source_contracts::RuntimeShape::OnDemand => "on_demand",
        sinex_primitives::source_contracts::RuntimeShape::Scheduled => "scheduled",
    }
}

fn checkpoint_family_name(family: CheckpointFamily) -> &'static str {
    match family {
        CheckpointFamily::AppendStream => "append_stream",
        CheckpointFamily::MutableSnapshot { .. } => "mutable_snapshot",
        CheckpointFamily::Journal => "journal",
        CheckpointFamily::Polling => "polling",
        CheckpointFamily::LiveObservation => "live_observation",
    }
}

fn material_lifecycle_name(policy: MaterialLifecyclePolicy) -> &'static str {
    match policy {
        MaterialLifecyclePolicy::RetainRaw => "retain_raw",
        MaterialLifecyclePolicy::EphemeralRaw => "ephemeral_raw",
        MaterialLifecyclePolicy::DerivedOnly => "derived_only",
        MaterialLifecyclePolicy::QuarantineUntilReviewed => "quarantine_until_reviewed",
        MaterialLifecyclePolicy::ExternalReferenceOnly => "external_reference_only",
    }
}

fn transport_kind_name(kind: TransportKind) -> &'static str {
    match kind {
        TransportKind::Direct => "direct",
        TransportKind::LocalQueue => "local_queue",
        TransportKind::CoreNats => "core_nats",
        TransportKind::JetStream => "jet_stream",
        TransportKind::Kv => "kv",
        TransportKind::Filesystem => "filesystem",
        TransportKind::ExternalApi => "external_api",
    }
}

fn delivery_semantics_name(delivery: DeliverySemantics) -> &'static str {
    match delivery {
        DeliverySemantics::SameProcess => "same_process",
        DeliverySemantics::AtMostOnce => "at_most_once",
        DeliverySemantics::AtLeastOnce => "at_least_once",
        DeliverySemantics::ExactlyOnceNotClaimed => "exactly_once_not_claimed",
    }
}

fn ordering_semantics_name(ordering: OrderingSemantics) -> &'static str {
    match ordering {
        OrderingSemantics::MaterialOrder => "material_order",
        OrderingSemantics::CursorOrder => "cursor_order",
        OrderingSemantics::BestEffort => "best_effort",
        OrderingSemantics::Unordered => "unordered",
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
            let action_key = operation_action_key(operation, source_id, binding);
            if seen.insert(action_key.clone()) {
                actions.push(operation_capability_action(
                    operation,
                    source_id,
                    Some(binding),
                    action_key,
                ));
            }
        }
    }
    actions
}

fn operation_action_key(
    operation: &str,
    source_id: &str,
    binding: &SourceRuntimeBinding,
) -> String {
    if source_id == "email.mailbox" && email_operation_is_mode_scoped(operation) {
        format!("{}:{}", operation, binding.subject.as_str())
    } else {
        operation.to_string()
    }
}

fn operation_capability_action(
    operation: &str,
    source_id: &str,
    binding: Option<&SourceRuntimeBinding>,
    action_id: String,
) -> ActionAvailability {
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
            email_operation_label(operation, binding).unwrap_or("Inspect Bridge"),
            package_operation_command_hint(operation, source_id, binding).or_else(|| {
                module
                .map(|module| format!("sinexctl runtime status {module}"))
                .or_else(|| Some(format!("sinexctl sources status {source_id} --format json")))
            }),
            package_operation_rpc_method(operation, source_id, binding)
                .or_else(|| module.map(|_| methods::COORDINATION_INSTANCE_HEALTH)),
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
            email_operation_label(operation, binding)
                .or_else(|| package_operation_label(operation, "Pause Package Mode"))
                .unwrap_or("Pause Bridge"),
            package_operation_command_hint(operation, source_id, binding).or_else(|| {
                module.map(|module| format!("sinexctl runtime drain {module} --reason source-paused"))
            }),
            package_operation_rpc_method(operation, source_id, binding)
                .or_else(|| module.map(|_| methods::RUNTIME_DRAIN)),
            ActionSideEffect::Admin,
        ),
        "resume" => (
            email_operation_label(operation, binding)
                .or_else(|| package_operation_label(operation, "Resume Package Mode"))
                .unwrap_or("Resume Bridge"),
            package_operation_command_hint(operation, source_id, binding)
                .or_else(|| module.map(|module| format!("sinexctl runtime resume {module}"))),
            package_operation_rpc_method(operation, source_id, binding)
                .or_else(|| module.map(|_| methods::RUNTIME_RESUME)),
            ActionSideEffect::Admin,
        ),
        "authorize" => (
            email_operation_label(operation, binding).unwrap_or("Authorize Mailbox"),
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Admin,
        ),
        "sync" => (
            email_operation_label(operation, binding).unwrap_or("Sync Mailbox"),
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Write,
        ),
        "fetch-attachments" => (
            email_operation_label(operation, binding).unwrap_or("Fetch Attachments"),
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Write,
        ),
        "rebuild-projection" => (
            email_operation_label(operation, binding).unwrap_or("Rebuild Mailbox Projection"),
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Write,
        ),
        "import-rfc822" => (
            "Import RFC822 Message",
            Some("sinexctl sources stage <path> --binding source:email.mailbox --format json".to_string()),
            Some(methods::SOURCES_STAGE),
            ActionSideEffect::Write,
        ),
        "import-maildir" => (
            "Import Maildir Entry",
            Some(
                "sinexctl sources stage <path> --binding source:email.mailbox.maildir-staged --format json"
                    .to_string(),
            ),
            Some(methods::SOURCES_STAGE),
            ActionSideEffect::Write,
        ),
        "import-mbox" => (
            "Import MBOX",
            Some(
                "sinexctl sources stage <path> --binding source:email.mailbox.mbox-staged --format json"
                    .to_string(),
            ),
            Some(methods::SOURCES_STAGE),
            ActionSideEffect::Write,
        ),
        "import-transcript" => (
            "Import Transcript",
            Some(
                "sinexctl sources stage <path> --binding source:media.audio-transcript --format json"
                    .to_string(),
            ),
            Some(methods::SOURCES_STAGE),
            ActionSideEffect::Write,
        ),
        "import-ocr" => (
            "Import OCR",
            Some(
                "sinexctl sources stage <path> --binding source:media.screen-ocr --format json"
                    .to_string(),
            ),
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
        "import-video" => (
            "Import Screen Video",
            Some(
                "sinexctl sources stage <path> --binding source:media.screen-ocr.video-staged --format json"
                    .to_string(),
            ),
            Some(methods::SOURCES_STAGE),
            ActionSideEffect::Write,
        ),
        "export" => (
            email_operation_label(operation, binding).unwrap_or("Export Source Events"),
            package_operation_command_hint(operation, source_id, binding).or_else(|| {
                Some(format!(
                    "sinexctl privacy export --source {source_id} --output <file>"
                ))
            }),
            package_operation_rpc_method(operation, source_id, binding),
            if source_id == "email.mailbox" {
                ActionSideEffect::Write
            } else {
                ActionSideEffect::Read
            },
        ),
        "replay" => (
            email_operation_label(operation, binding).unwrap_or("Replay Source"),
            package_operation_command_hint(operation, source_id, binding)
                .or_else(|| Some(format!("sinexctl ops replay plan --source {source_id}"))),
            package_operation_rpc_method(operation, source_id, binding)
                .or(Some(methods::REPLAY_CREATE_OPERATION)),
            ActionSideEffect::Write,
        ),
        "delete-material" => (
            "Delete Material",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Destructive,
        ),
        "run-model" => (
            "Run Local Model",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Admin,
        ),
        "run-ocr" => (
            "Run OCR",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Admin,
        ),
        "retry" => (
            "Retry Package Operation",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Write,
        ),
        "rebuild-artifact" => (
            "Rebuild Artifact",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Write,
        ),
        "enable-session" => (
            "Enable Capture Session",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Admin,
        ),
        "disable-session" => (
            "Disable Capture Session",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Admin,
        ),
        "inspect" => (
            "Inspect Live Session",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Read,
        ),
        "capture-region" => (
            "Capture Region",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
            ActionSideEffect::Admin,
        ),
        "record-video" => (
            "Record Screen Video",
            package_operation_command_hint(operation, source_id, binding),
            package_operation_rpc_method(operation, source_id, binding),
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
        id: action_id,
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

fn package_operation_command_hint(
    operation: &str,
    source_id: &str,
    binding: Option<&SourceRuntimeBinding>,
) -> Option<String> {
    let mode_id = package_operation_mode_hint(operation, binding)?;

    Some(format!(
        "sinexctl ops start {operation} --scope '{{\"source_id\":\"{source_id}\",\"mode_id\":\"{mode_id}\"}}' --format json"
    ))
}

fn package_operation_mode_hint(
    operation: &str,
    binding: Option<&SourceRuntimeBinding>,
) -> Option<&'static str> {
    if email_operation_is_mode_scoped(operation)
        && let Some(binding) = binding
        && binding.source_id == "email.mailbox"
    {
        return Some(binding.subject.as_str());
    }

    let mode_id = match operation {
        "media.audio-transcript.run-model"
        | "media.audio-transcript.retry"
        | "media.audio-transcript.rebuild-artifact" => {
            "source:media.audio-transcript.local-model-batch"
        }
        "media.audio-transcript.enable-session"
        | "media.audio-transcript.disable-session"
        | "media.audio-transcript.pause"
        | "media.audio-transcript.resume"
        | "media.audio-transcript.inspect" => "source:media.audio-transcript.live-session",
        "media.audio-transcript.delete-material" => {
            "source:media.audio-transcript.audio-bundle-staged"
        }
        "media.screen-ocr.run-ocr"
        | "media.screen-ocr.retry"
        | "media.screen-ocr.rebuild-artifact" => "source:media.screen-ocr.local-model-batch",
        "media.screen-ocr.capture-region" => "source:media.screen-ocr.on-demand-region",
        "media.screen-ocr.record-video" => "source:media.screen-ocr.on-demand-video",
        "media.screen-ocr.enable-session"
        | "media.screen-ocr.disable-session"
        | "media.screen-ocr.pause"
        | "media.screen-ocr.resume"
        | "media.screen-ocr.inspect" => "source:media.screen-ocr.live-session",
        "media.screen-ocr.delete-material" => "source:media.screen-ocr.screenshot-ocr-staged",
        _ => return None,
    };
    Some(mode_id)
}

fn package_operation_rpc_method(
    operation: &str,
    source_id: &str,
    binding: Option<&SourceRuntimeBinding>,
) -> Option<&'static str> {
    package_operation_command_hint(operation, source_id, binding).map(|_| methods::OPS_START)
}

fn package_operation_label<'a>(operation: &str, label: &'a str) -> Option<&'a str> {
    package_operation_mode_hint(operation, None).map(|_| label)
}

fn email_operation_is_mode_scoped(operation: &str) -> bool {
    matches!(
        operation,
        "email.mailbox.authorize"
            | "email.mailbox.sync"
            | "email.mailbox.pause"
            | "email.mailbox.resume"
            | "email.mailbox.inspect"
            | "email.mailbox.replay"
            | "email.mailbox.fetch-attachments"
            | "email.mailbox.export"
            | "email.mailbox.rebuild-projection"
    )
}

fn email_operation_label(
    operation: &str,
    binding: Option<&SourceRuntimeBinding>,
) -> Option<&'static str> {
    let binding = binding?;
    if binding.source_id != "email.mailbox" {
        return None;
    }
    match (operation, binding.subject.as_str()) {
        ("email.mailbox.authorize", "source:email.mailbox.gmail-api-scheduled-sync") => {
            Some("Authorize Gmail")
        }
        (
            "email.mailbox.authorize",
            "source:email.mailbox.imap-scheduled-sync" | "source:email.mailbox.imap-idle-live",
        ) => Some("Authorize IMAP"),
        ("email.mailbox.sync", "source:email.mailbox.gmail-api-scheduled-sync") => {
            Some("Sync Gmail")
        }
        ("email.mailbox.sync", "source:email.mailbox.imap-scheduled-sync") => Some("Sync IMAP"),
        ("email.mailbox.sync", "source:email.mailbox.imap-idle-live") => Some("Observe IMAP IDLE"),
        ("email.mailbox.pause", "source:email.mailbox.gmail-api-scheduled-sync") => {
            Some("Pause Gmail Sync")
        }
        ("email.mailbox.pause", "source:email.mailbox.imap-scheduled-sync") => {
            Some("Pause IMAP Sync")
        }
        ("email.mailbox.pause", "source:email.mailbox.imap-idle-live") => Some("Pause IMAP IDLE"),
        ("email.mailbox.resume", "source:email.mailbox.gmail-api-scheduled-sync") => {
            Some("Resume Gmail Sync")
        }
        ("email.mailbox.resume", "source:email.mailbox.imap-scheduled-sync") => {
            Some("Resume IMAP Sync")
        }
        ("email.mailbox.resume", "source:email.mailbox.imap-idle-live") => Some("Resume IMAP IDLE"),
        ("email.mailbox.inspect", "source:email.mailbox.gmail-api-scheduled-sync") => {
            Some("Inspect Gmail Sync")
        }
        ("email.mailbox.inspect", "source:email.mailbox.imap-scheduled-sync") => {
            Some("Inspect IMAP Sync")
        }
        ("email.mailbox.inspect", "source:email.mailbox.imap-idle-live") => {
            Some("Inspect IMAP IDLE")
        }
        ("email.mailbox.replay", "source:email.mailbox.maildir-staged") => Some("Replay Maildir"),
        ("email.mailbox.replay", "source:email.mailbox.mbox-staged") => Some("Replay MBOX"),
        ("email.mailbox.replay", "source:email.mailbox.gmail-api-scheduled-sync") => {
            Some("Replay Gmail Sync")
        }
        ("email.mailbox.replay", "source:email.mailbox.imap-scheduled-sync") => {
            Some("Replay IMAP Sync")
        }
        ("email.mailbox.replay", "source:email.mailbox.imap-idle-live") => Some("Replay IMAP IDLE"),
        ("email.mailbox.fetch-attachments", "source:email.mailbox") => {
            Some("Fetch RFC822 Attachments")
        }
        ("email.mailbox.fetch-attachments", "source:email.mailbox.maildir-staged") => {
            Some("Fetch Maildir Attachments")
        }
        ("email.mailbox.fetch-attachments", "source:email.mailbox.mbox-staged") => {
            Some("Fetch MBOX Attachments")
        }
        ("email.mailbox.fetch-attachments", "source:email.mailbox.gmail-api-scheduled-sync") => {
            Some("Fetch Gmail Attachments")
        }
        ("email.mailbox.fetch-attachments", "source:email.mailbox.imap-scheduled-sync") => {
            Some("Fetch IMAP Attachments")
        }
        ("email.mailbox.fetch-attachments", "source:email.mailbox.imap-idle-live") => {
            Some("Fetch IMAP IDLE Attachments")
        }
        ("email.mailbox.export", "source:email.mailbox") => Some("Export RFC822 Mailbox"),
        ("email.mailbox.export", "source:email.mailbox.maildir-staged") => {
            Some("Export Maildir Mailbox")
        }
        ("email.mailbox.export", "source:email.mailbox.mbox-staged") => Some("Export MBOX Mailbox"),
        ("email.mailbox.export", "source:email.mailbox.gmail-api-scheduled-sync") => {
            Some("Export Gmail Mailbox")
        }
        ("email.mailbox.export", "source:email.mailbox.imap-scheduled-sync") => {
            Some("Export IMAP Mailbox")
        }
        ("email.mailbox.export", "source:email.mailbox.imap-idle-live") => {
            Some("Export IMAP IDLE Mailbox")
        }
        ("email.mailbox.rebuild-projection", "source:email.mailbox") => {
            Some("Rebuild RFC822 Projection")
        }
        ("email.mailbox.rebuild-projection", "source:email.mailbox.maildir-staged") => {
            Some("Rebuild Maildir Projection")
        }
        ("email.mailbox.rebuild-projection", "source:email.mailbox.mbox-staged") => {
            Some("Rebuild MBOX Projection")
        }
        ("email.mailbox.rebuild-projection", "source:email.mailbox.gmail-api-scheduled-sync") => {
            Some("Rebuild Gmail Projection")
        }
        ("email.mailbox.rebuild-projection", "source:email.mailbox.imap-scheduled-sync") => {
            Some("Rebuild IMAP Projection")
        }
        ("email.mailbox.rebuild-projection", "source:email.mailbox.imap-idle-live") => {
            Some("Rebuild IMAP IDLE Projection")
        }
        _ => None,
    }
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

    fn session_record(
        lifecycle_state: &str,
        private_mode_blocked: bool,
    ) -> sinex_db::repositories::SourceSessionStateRecord {
        sinex_db::repositories::SourceSessionStateRecord {
            id: uuid::Uuid::now_v7(),
            source_id: "media.screen-ocr".to_string(),
            mode_id: "source:media.screen-ocr.live-session".to_string(),
            session_scope: "default".to_string(),
            operation_id: uuid::Uuid::now_v7(),
            result_status: sinex_primitives::domain::OperationStatus::Success,
            lifecycle_state: lifecycle_state.to_string(),
            visibility_state: "idle".to_string(),
            private_mode_blocked,
            runtime_state_ref: "media.session_runtime.observed:test".to_string(),
            coverage_ref: "coverage:media.screen-ocr.live_session".to_string(),
            debt_ref: "debt:media.screen-ocr.live_session".to_string(),
            requested_by: Some("operator".to_string()),
            reason: Some("operator stepped away".to_string()),
            detail: serde_json::json!({}),
            observed_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[sinex_test]
    async fn session_control_caveat_reports_operator_posture() -> xtask::sandbox::TestResult<()> {
        let paused = session_control_caveat(&session_record("paused", false));
        assert!(paused.message.contains("capture suspended"));
        assert!(paused.message.contains("paused"));
        assert!(paused.message.contains("reason=operator stepped away"));

        let enabled = session_control_caveat(&session_record("enabled", false));
        assert!(enabled.message.contains("capture active"));

        // The per-session private flag suspends even when lifecycle is enabled.
        let private = session_control_caveat(&session_record("enabled", true));
        assert!(private.message.contains("capture suspended"));
        assert!(private.message.contains("private_mode_blocked=true"));
        Ok(())
    }

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
        assert!(
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
            &HashMap::new(),
            &HashMap::new(),
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
            .expect("resource budget expected");
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
            &HashMap::new(),
            &HashMap::new(),
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
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Timestamp::now(),
        );

        let caveat = view
            .caveats
            .iter()
            .find(|caveat| caveat.id == "source.runtime_bridge.unobserved")
            .expect("bridge caveat expected");
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
            .expect("check action expected");
        assert_eq!(check.state, ActionAvailabilityState::Enabled);
        assert_eq!(
            check.command_hint.as_deref(),
            Some("sinexctl sources status terminal.kitty-osc-live --format json")
        );

        let pause = view
            .actions
            .iter()
            .find(|action| action.id == "terminal.activity.pause")
            .expect("pause action expected");
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
            .expect("resume action expected");
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
            .expect("drain action expected");
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
            .expect("reconnect action expected");
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
            .expect("media audio contract expected");
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
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Timestamp::now(),
        );

        let import_transcript = view
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.import-transcript")
            .expect("transcript import action expected");
        assert_eq!(
            import_transcript.command_hint.as_deref(),
            Some(
                "sinexctl sources stage <path> --binding source:media.audio-transcript --format json"
            )
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
            .expect("audio bundle import action expected");
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
            .expect("replay action expected");
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
            .expect("export action expected");
        assert_eq!(
            export.command_hint.as_deref(),
            Some("sinexctl privacy export --source media.audio-transcript --output <file>")
        );

        let run_model = view
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.run-model")
            .expect("model action expected");
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
            .expect("media pause action expected");
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
            .expect("delete material action expected");
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
        let local_model_mode = view
            .modes
            .iter()
            .find(|mode| mode.mode_id == "source:media.audio-transcript.local-model-batch")
            .expect("audio local model mode expected");
        assert_eq!(
            local_model_mode.implementation,
            "local-transcription-worker"
        );
        assert_eq!(local_model_mode.adapter, "LocalProcessWorker");
        assert_eq!(local_model_mode.runtime_shape, "on_demand");
        assert_eq!(local_model_mode.material_lifecycle, "derived_only");
        assert_eq!(local_model_mode.transport, "direct");
        assert_eq!(local_model_mode.resource_budget.work_class, "bulk_import");
        assert!(
            local_model_mode
                .actions
                .iter()
                .any(|action| action.id == "media.audio-transcript.run-model"
                    && action.command_hint.as_deref()
                        == Some(
                            "sinexctl ops start media.audio-transcript.run-model --scope '{\"source_id\":\"media.audio-transcript\",\"mode_id\":\"source:media.audio-transcript.local-model-batch\"}' --format json"
                        ))
        );
        let live_audio_mode = view
            .modes
            .iter()
            .find(|mode| mode.mode_id == "source:media.audio-transcript.live-session")
            .expect("audio live mode expected");
        assert!(live_audio_mode.proposed);
        assert_eq!(live_audio_mode.runner_pack, "live");
        assert_eq!(live_audio_mode.runtime_shape, "continuous");
        assert_eq!(live_audio_mode.material_lifecycle, "ephemeral_raw");
        assert_eq!(live_audio_mode.transport, "local_queue");
        assert!(live_audio_mode.backpressure);

        let screen_contract = all_source_contracts()
            .find(|contract| contract.id == "media.screen-ocr")
            .expect("media screen contract expected");
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
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Timestamp::now(),
        );

        let import_ocr = screen_view
            .actions
            .iter()
            .find(|action| action.id == "media.screen-ocr.import-ocr")
            .expect("OCR import action expected");
        assert_eq!(
            import_ocr.command_hint.as_deref(),
            Some("sinexctl sources stage <path> --binding source:media.screen-ocr --format json")
        );
        assert_eq!(import_ocr.rpc_method.as_deref(), Some("sources.stage"));
        assert_eq!(import_ocr.side_effect, ActionSideEffect::Write);
        assert_eq!(import_ocr.state, ActionAvailabilityState::Enabled);

        let import_screenshots = screen_view
            .actions
            .iter()
            .find(|action| action.id == "media.screen-ocr.import-screenshots")
            .expect("screenshot import action expected");
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

        let import_video = screen_view
            .actions
            .iter()
            .find(|action| action.id == "media.screen-ocr.import-video")
            .expect("screen-video import action expected");
        assert_eq!(
            import_video.command_hint.as_deref(),
            Some(
                "sinexctl sources stage <path> --binding source:media.screen-ocr.video-staged --format json"
            )
        );
        assert_eq!(import_video.rpc_method.as_deref(), Some("sources.stage"));
        assert_eq!(import_video.side_effect, ActionSideEffect::Write);
        assert_eq!(import_video.state, ActionAvailabilityState::Enabled);

        let run_ocr = screen_view
            .actions
            .iter()
            .find(|action| action.id == "media.screen-ocr.run-ocr")
            .expect("run OCR action expected");
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
            .expect("capture-region action expected");
        assert_eq!(capture_region.state, ActionAvailabilityState::Enabled);
        assert_eq!(capture_region.side_effect, ActionSideEffect::Admin);
        assert_eq!(capture_region.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(
            capture_region.command_hint.as_deref(),
            Some(
                "sinexctl ops start media.screen-ocr.capture-region --scope '{\"source_id\":\"media.screen-ocr\",\"mode_id\":\"source:media.screen-ocr.on-demand-region\"}' --format json"
            )
        );

        let record_video = screen_view
            .actions
            .iter()
            .find(|action| action.id == "media.screen-ocr.record-video")
            .expect("record-video action expected");
        assert_eq!(record_video.state, ActionAvailabilityState::Enabled);
        assert_eq!(record_video.side_effect, ActionSideEffect::Admin);
        assert_eq!(record_video.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(
            record_video.command_hint.as_deref(),
            Some(
                "sinexctl ops start media.screen-ocr.record-video --scope '{\"source_id\":\"media.screen-ocr\",\"mode_id\":\"source:media.screen-ocr.on-demand-video\"}' --format json"
            )
        );
        let video_mode = screen_view
            .modes
            .iter()
            .find(|mode| mode.mode_id == "source:media.screen-ocr.video-staged")
            .expect("screen-video staged mode expected");
        assert_eq!(video_mode.implementation, "staged-screen-video-bundle");
        assert_eq!(
            video_mode.output_event_type,
            "media.screen.video_segment_observed"
        );
        assert_eq!(video_mode.material_lifecycle, "retain_raw");
        assert_eq!(video_mode.transport, "direct");
        assert!(
            video_mode
                .actions
                .iter()
                .any(|action| action.id == "media.screen-ocr.import-video"
                    && action.rpc_method.as_deref() == Some("sources.stage"))
        );

        assert_action_rpc_methods_are_cataloged("media.audio-transcript", &view.actions)?;
        assert_action_rpc_methods_are_cataloged("media.screen-ocr", &screen_view.actions)?;

        Ok(())
    }

    #[sinex_test]
    async fn email_package_operations_surface_operator_actions() -> xtask::TestResult<()> {
        let contract = all_source_contracts()
            .find(|contract| contract.id == "email.mailbox")
            .expect("email mailbox contract expected");
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
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Timestamp::now(),
        );

        let authorize_gmail = view
            .actions
            .iter()
            .find(|action| {
                action.command_hint.as_deref()
                    == Some(
                        "sinexctl ops start email.mailbox.authorize --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.gmail-api-scheduled-sync\"}' --format json",
                    )
            })
            .expect("Gmail authorize action expected");
        assert_eq!(authorize_gmail.state, ActionAvailabilityState::Enabled);
        assert_eq!(authorize_gmail.side_effect, ActionSideEffect::Admin);
        assert_eq!(authorize_gmail.rpc_method.as_deref(), Some("ops.start"));
        assert_eq!(authorize_gmail.label, "Authorize Gmail");

        let authorize_imap = view
            .actions
            .iter()
            .find(|action| {
                action.command_hint.as_deref()
                    == Some(
                        "sinexctl ops start email.mailbox.authorize --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.imap-scheduled-sync\"}' --format json",
                    )
            })
            .expect("IMAP authorize action expected");
        assert_eq!(authorize_imap.label, "Authorize IMAP");

        let import_rfc822 = view
            .actions
            .iter()
            .find(|action| {
                action.command_hint.as_deref()
                    == Some(
                        "sinexctl sources stage <path> --binding source:email.mailbox --format json",
                    )
            })
            .expect("RFC822 import action expected");
        assert_eq!(import_rfc822.state, ActionAvailabilityState::Enabled);
        assert_eq!(import_rfc822.side_effect, ActionSideEffect::Write);
        assert_eq!(import_rfc822.rpc_method.as_deref(), Some("sources.stage"));
        assert_eq!(import_rfc822.label, "Import RFC822 Message");

        let import_maildir = view
            .actions
            .iter()
            .find(|action| {
                action.command_hint.as_deref()
                    == Some(
                        "sinexctl sources stage <path> --binding source:email.mailbox.maildir-staged --format json",
                    )
            })
            .expect("Maildir import action expected");
        assert_eq!(import_maildir.state, ActionAvailabilityState::Enabled);
        assert_eq!(import_maildir.side_effect, ActionSideEffect::Write);
        assert_eq!(import_maildir.rpc_method.as_deref(), Some("sources.stage"));
        assert_eq!(import_maildir.label, "Import Maildir Entry");

        let import_mbox = view
            .actions
            .iter()
            .find(|action| {
                action.command_hint.as_deref()
                    == Some(
                        "sinexctl sources stage <path> --binding source:email.mailbox.mbox-staged --format json",
                    )
            })
            .expect("MBOX import action expected");
        assert_eq!(import_mbox.state, ActionAvailabilityState::Enabled);
        assert_eq!(import_mbox.side_effect, ActionSideEffect::Write);
        assert_eq!(import_mbox.rpc_method.as_deref(), Some("sources.stage"));
        assert_eq!(import_mbox.label, "Import MBOX");

        let gmail_sync = view
            .actions
            .iter()
            .find(|action| {
                action.command_hint.as_deref()
                    == Some(
                        "sinexctl ops start email.mailbox.sync --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.gmail-api-scheduled-sync\"}' --format json",
                    )
            })
            .expect("Gmail sync action expected");
        assert_eq!(gmail_sync.label, "Sync Gmail");

        let imap_sync = view
            .actions
            .iter()
            .find(|action| {
                action.command_hint.as_deref()
                    == Some(
                        "sinexctl ops start email.mailbox.sync --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.imap-scheduled-sync\"}' --format json",
                    )
            })
            .expect("IMAP sync action expected");
        assert_eq!(imap_sync.label, "Sync IMAP");
        let gmail_mode = view
            .modes
            .iter()
            .find(|mode| mode.mode_id == "source:email.mailbox.gmail-api-scheduled-sync")
            .expect("Gmail scheduled mode expected");
        assert_eq!(gmail_mode.implementation, "gmail-api-scheduled-sync");
        assert_eq!(gmail_mode.adapter, "GmailApiCursorAdapter");
        assert_eq!(gmail_mode.runtime_shape, "scheduled");
        assert_eq!(gmail_mode.material_lifecycle, "external_reference_only");
        assert_eq!(gmail_mode.transport, "external_api");
        assert!(gmail_mode.dlq);
        assert!(gmail_mode.backpressure);
        assert!(
            gmail_mode
                .actions
                .iter()
                .any(|action| action.label == "Sync Gmail"
                    && action.command_hint.as_deref()
                        == Some(
                            "sinexctl ops start email.mailbox.sync --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.gmail-api-scheduled-sync\"}' --format json"
                        ))
        );
        let imap_idle = view
            .modes
            .iter()
            .find(|mode| mode.mode_id == "source:email.mailbox.imap-idle-live")
            .expect("IMAP IDLE mode expected");
        assert_eq!(imap_idle.runtime_shape, "continuous");
        assert_eq!(imap_idle.resource_budget.work_class, "capture_live");
        assert!(
            imap_idle
                .actions
                .iter()
                .any(|action| action.label == "Observe IMAP IDLE"
                    && action.command_hint.as_deref()
                        == Some(
                            "sinexctl ops start email.mailbox.sync --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.imap-idle-live\"}' --format json"
                        ))
        );
        assert!(
            imap_idle
                .actions
                .iter()
                .any(|action| action.label == "Pause IMAP IDLE")
        );

        assert!(
            view.actions.iter().all(|action| {
                let hint = action.command_hint.as_deref().unwrap_or_default();
                !hint.contains("<email-mode-id>") && !hint.contains("<provider-mode-id>")
            }),
            "email coverage actions should be concrete mode commands"
        );

        let pause = view
            .actions
            .iter()
            .find(|action| {
                action.command_hint.as_deref()
                    == Some(
                        "sinexctl ops start email.mailbox.pause --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.gmail-api-scheduled-sync\"}' --format json",
                    )
            })
            .expect("email pause action expected");
        assert_eq!(pause.state, ActionAvailabilityState::Enabled);
        assert_eq!(pause.side_effect, ActionSideEffect::Admin);
        assert_eq!(pause.rpc_method.as_deref(), Some("ops.start"));

        let check = view
            .actions
            .iter()
            .find(|action| action.id == "email.mailbox.check")
            .expect("email check action expected");
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
    async fn email_provider_failure_operation_surfaces_source_coverage_debt_caveat()
    -> xtask::TestResult<()> {
        let contract = all_source_contracts()
            .find(|contract| contract.id == "email.mailbox")
            .expect("email mailbox contract expected");
        let bindings = source_runtime_bindings()
            .filter(|binding| binding.source_id == "email.mailbox")
            .collect::<Vec<_>>();
        let operation_id = Uuid::now_v7();
        let mut provider_states = HashMap::new();
        provider_states.insert(
            "source:email.mailbox.gmail-api-scheduled-sync".to_string(),
            EmailProviderOperationState {
                operation_id,
                result_status: "failure".to_string(),
                provider_runtime: Some(serde_json::json!({
                    "runtime_observation_contract": {
                        "auth_state": "missing",
                        "network_state": "unknown"
                    }
                })),
                provider_failure: Some(serde_json::json!({
                    "reason": "Gmail token file is unavailable",
                    "coverage_ref": "coverage:email.mailbox.gmail.provider_runtime",
                    "debt_ref": "debt:email.mailbox.gmail.provider_runtime",
                    "actions": ["email.mailbox.authorize", "email.mailbox.sync"]
                })),
                failure_class: Some("authorization-missing".to_string()),
                required_action: Some("email.mailbox.authorize".to_string()),
                retry_after_secs: None,
                reconnect_state: None,
            },
        );

        let view = source_coverage_view(
            contract,
            &bindings,
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
            &HashMap::new(),
            &provider_states,
            &HashMap::new(),
            &HashMap::new(),
            Timestamp::now(),
        );

        let caveat = view
            .caveats
            .iter()
            .find(|caveat| {
                caveat.id
                    == "email.provider_runtime.failed.source:email.mailbox.gmail-api-scheduled-sync"
            })
            .expect("provider runtime failure caveat expected");
        assert!(caveat.message.contains("ended with failure"));
        assert!(caveat.message.contains("auth_state=missing"));
        assert!(caveat.message.contains("network_state=unknown"));
        assert!(
            caveat
                .message
                .contains("failure_class=authorization-missing")
        );
        assert!(
            caveat
                .message
                .contains("required_action=email.mailbox.authorize")
        );
        assert!(
            caveat
                .message
                .contains("debt:email.mailbox.gmail.provider_runtime")
        );
        assert!(caveat.message.contains("Gmail token file is unavailable"));
        let ref_ = caveat
            .ref_
            .as_ref()
            .expect("provider failure caveat should point at the operation");
        assert_eq!(ref_.kind, SinexObjectKind::Operation);
        assert_eq!(ref_.id, operation_id.to_string());
        let gmail_mode = view
            .modes
            .iter()
            .find(|mode| mode.mode_id == "source:email.mailbox.gmail-api-scheduled-sync")
            .expect("Gmail provider mode expected");
        assert_eq!(
            gmail_mode.provider_operation_status.as_deref(),
            Some("failure")
        );
        assert_eq!(gmail_mode.provider_auth_state.as_deref(), Some("missing"));
        assert_eq!(
            gmail_mode.provider_network_state.as_deref(),
            Some("unknown")
        );
        assert_eq!(
            gmail_mode.provider_operation_id.as_deref(),
            Some(ref_.id.as_str())
        );
        assert_eq!(
            gmail_mode.provider_debt_ref.as_deref(),
            Some("debt:email.mailbox.gmail.provider_runtime")
        );
        assert_eq!(
            gmail_mode.provider_failure_class.as_deref(),
            Some("authorization-missing")
        );
        assert_eq!(
            gmail_mode.provider_required_action.as_deref(),
            Some("email.mailbox.authorize")
        );
        Ok(())
    }

    #[sinex_test]
    async fn email_mailbox_projection_surfaces_materialization_debt_and_mode_counts()
    -> xtask::TestResult<()> {
        let contract = all_source_contracts()
            .find(|contract| contract.id == "email.mailbox")
            .expect("email mailbox contract expected");
        let bindings = source_runtime_bindings()
            .filter(|binding| binding.source_id == "email.mailbox")
            .collect::<Vec<_>>();
        let mode_id = "source:email.mailbox.mbox-staged".to_string();
        let mut projection_states = HashMap::new();
        projection_states.insert(
            mode_id.clone(),
            EmailMailboxProjectionState {
                message_count: 3,
                thread_count: 2,
                body_bytes: 128,
                attachment_count: 4,
                attachment_observed_count: 1,
                last_observed_at: Timestamp::now(),
            },
        );

        let view = source_coverage_view(
            contract,
            &bindings,
            &HashMap::new(),
            &HashMap::new(),
            &healthy_confirmation_buffer(),
            &HashMap::new(),
            &HashMap::new(),
            &projection_states,
            &HashMap::new(),
            Timestamp::now(),
        );

        let caveat = view
            .caveats
            .iter()
            .find(|caveat| {
                caveat.id
                    == "email.mailbox_projection.source:email.mailbox.mbox-staged.materialization_debt"
            })
            .expect("projection materialization debt caveat expected");
        assert!(caveat.message.contains("3 projected message"));
        assert!(caveat.message.contains("128 message body byte"));
        assert!(caveat.message.contains("4 attachment(s) declared"));

        let mode = view
            .modes
            .iter()
            .find(|mode| mode.mode_id == mode_id)
            .expect("mbox staged mode expected");
        assert_eq!(mode.mailbox_projection_message_count, Some(3));
        assert_eq!(mode.mailbox_projection_thread_count, Some(2));
        assert_eq!(mode.mailbox_projection_body_bytes, Some(128));
        assert_eq!(mode.mailbox_projection_attachment_count, Some(4));
        assert_eq!(mode.mailbox_projection_attachment_observed_count, Some(1));
        assert!(mode.mailbox_projection_last_observed_at.is_some());
        let fetch_attachments = mode
            .actions
            .iter()
            .find(|action| {
                action.id == "email.mailbox.fetch-attachments:source:email.mailbox.mbox-staged"
            })
            .expect("projection debt should advertise attachment fetch operation");
        assert_eq!(fetch_attachments.state, ActionAvailabilityState::Enabled);
        assert_eq!(fetch_attachments.side_effect, ActionSideEffect::Write);
        assert_eq!(
            fetch_attachments.rpc_method.as_deref(),
            Some(methods::OPS_START)
        );
        assert!(
            fetch_attachments
                .command_hint
                .as_deref()
                .is_some_and(|hint| {
                    hint.contains("email.mailbox.fetch-attachments")
                        && hint.contains("source:email.mailbox.mbox-staged")
                })
        );
        let export = mode
            .actions
            .iter()
            .find(|action| action.id == "email.mailbox.export:source:email.mailbox.mbox-staged")
            .expect("projection debt should advertise scoped mailbox export operation");
        assert_eq!(export.state, ActionAvailabilityState::Enabled);
        assert_eq!(export.side_effect, ActionSideEffect::Write);
        assert_eq!(export.rpc_method.as_deref(), Some(methods::OPS_START));
        let rebuild = mode
            .actions
            .iter()
            .find(|action| {
                action.id == "email.mailbox.rebuild-projection:source:email.mailbox.mbox-staged"
            })
            .expect("projection debt should advertise projection rebuild operation");
        assert_eq!(rebuild.state, ActionAvailabilityState::Enabled);
        assert_eq!(rebuild.side_effect, ActionSideEffect::Write);
        assert_eq!(rebuild.rpc_method.as_deref(), Some(methods::OPS_START));
        Ok(())
    }

    #[sinex_test]
    async fn latest_email_provider_state_prefers_newer_success_over_old_failure()
    -> xtask::TestResult<()> {
        let failed_operation_id = Uuid::now_v7();
        let successful_operation_id = Uuid::now_v7();
        let states = email_provider_operation_states_from_rows(vec![
            EmailProviderStateRecord {
                id: Uuid::now_v7(),
                source_id: "email.mailbox".to_string(),
                operation_id: successful_operation_id,
                result_status: sinex_primitives::domain::OperationStatus::Success,
                mode_id: "source:email.mailbox.imap-scheduled-sync".to_string(),
                provider: "imap".to_string(),
                account_binding_ref: "operator-mailbox:imap-primary".to_string(),
                mailbox_scope: "default".to_string(),
                auth_state: "authorized".to_string(),
                network_state: "online".to_string(),
                sync_state: "synced".to_string(),
                rate_limit_state: None,
                runtime_state_ref: "email.provider_runtime.imap".to_string(),
                coverage_ref: "coverage:email.mailbox.imap.provider_runtime".to_string(),
                debt_ref: "debt:email.mailbox.imap.provider_runtime".to_string(),
                failure_class: None,
                required_action: None,
                retry_after_secs: None,
                reconnect_state: None,
                cursor_kind: None,
                cursor_value: None,
                continuity_state: None,
                provider_runtime: serde_json::json!({
                    "coverage_ref": "coverage:email.mailbox.imap.provider_runtime",
                    "runtime_observation_contract": {
                        "auth_state": "authorized",
                        "network_state": "online",
                        "sync_state": "synced"
                    }
                }),
                provider_cursor: None,
                provider_failure: None,
                observed_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
            },
            EmailProviderStateRecord {
                id: Uuid::now_v7(),
                source_id: "email.mailbox".to_string(),
                operation_id: failed_operation_id,
                result_status: sinex_primitives::domain::OperationStatus::Failed,
                mode_id: "source:email.mailbox.imap-scheduled-sync".to_string(),
                provider: "imap".to_string(),
                account_binding_ref: "operator-mailbox:imap-primary".to_string(),
                mailbox_scope: "default".to_string(),
                auth_state: "authorized".to_string(),
                network_state: "error".to_string(),
                sync_state: "failed".to_string(),
                rate_limit_state: None,
                runtime_state_ref: "email.provider_runtime.imap".to_string(),
                coverage_ref: "coverage:email.mailbox.imap.provider_runtime".to_string(),
                debt_ref: "debt:email.mailbox.imap.provider_runtime".to_string(),
                failure_class: Some("network-reconnect".to_string()),
                required_action: Some("email.mailbox.reconnect".to_string()),
                retry_after_secs: None,
                reconnect_state: Some("reconnect-required".to_string()),
                cursor_kind: None,
                cursor_value: None,
                continuity_state: None,
                provider_runtime: serde_json::json!({
                    "runtime_observation_contract": {
                        "auth_state": "authorized",
                        "network_state": "error",
                        "sync_state": "failed"
                    }
                }),
                provider_cursor: None,
                provider_failure: Some(serde_json::json!({
                    "reason": "older IMAP failure",
                    "debt_ref": "debt:email.mailbox.imap.provider_runtime"
                })),
                observed_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
            },
        ]);

        let state = states
            .get("source:email.mailbox.imap-scheduled-sync")
            .expect("provider state expected");
        assert_eq!(state.operation_id, successful_operation_id);
        assert_eq!(state.result_status, "success");

        let caveat =
            email_provider_operation_caveat("source:email.mailbox.imap-scheduled-sync", state);
        assert_eq!(
            caveat.id,
            "email.provider_runtime.observed.source:email.mailbox.imap-scheduled-sync"
        );
        assert!(caveat.message.contains("ended with success"));
        assert!(caveat.message.contains("network_state=online"));
        assert!(!caveat.message.contains("older IMAP failure"));
        assert_eq!(
            caveat.ref_.as_ref().map(|ref_| ref_.id.clone()),
            Some(successful_operation_id.to_string())
        );
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
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
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
            .expect("observed runtime caveat expected");
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
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
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
            .expect("disconnected caveat expected");
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
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            now,
        );

        let caveat = view
            .caveats
            .iter()
            .find(|caveat| caveat.id == "source.runtime_bridge.health")
            .expect("runtime health caveat expected");
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
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
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
            .expect("stalled caveat expected");
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
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Timestamp::now(),
        );

        let caveat = view
            .caveats
            .iter()
            .find(|caveat| caveat.id == "source.pressure.confirmation_buffer.retained_payload")
            .expect("pressure caveat expected");
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
            &HashMap::new(),
            &HashMap::new(),
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
            pressure_level: sinex_primitives::RuntimePressureLevel::Nominal,
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
