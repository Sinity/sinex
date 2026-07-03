//! Operator-facing source status handler.

use crate::api::service_container::ServiceContainer;
use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_db::repositories::{EmailMailboxProjectionSummary, EmailProviderStateRecord};
use sinex_primitives::SinexError;
use sinex_primitives::domain::SourceIdentifier;
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
    let material_aggregates = material_aggregates_by_logical_source(material_rows);
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

fn material_aggregates_by_logical_source(
    rows: Vec<SourceMaterialAggregateRow>,
) -> HashMap<String, SourceMaterialAggregateRow> {
    let mut aggregates = HashMap::new();
    for row in rows {
        let logical_source_id = SourceIdentifier::from_wire(&row.source_identifier).map_or_else(
            |_| row.source_identifier.clone(),
            |identifier| identifier.logical_id,
        );
        let aggregate = aggregates
            .entry(logical_source_id.clone())
            .or_insert_with(|| SourceMaterialAggregateRow {
                source_identifier: logical_source_id,
                material_count: 0,
                last_material_at: None,
            });
        aggregate.material_count += row.material_count;
        aggregate.last_material_at =
            max_timestamp(aggregate.last_material_at, row.last_material_at);
    }
    aggregates
}

fn source_coverage_view(
    contract: &SourceContract,
    bindings: &[&SourceRuntimeBinding],
    event_aggregates: &HashMap<(String, String), SourceEventAggregateRow>,
    material_aggregates: &HashMap<String, SourceMaterialAggregateRow>,
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
    if has_live_binding {
        if let Some(observation) = runtime_observation_for_source(contract.id, runtime_observations)
        {
            if runtime_observation_is_disconnected(observation) {
                gaps.push(CoverageGapView {
                    kind: runtime_disconnected_gap_kind(contract).to_string(),
                    message: runtime_status_message(contract, observation),
                });
            }
            let verdict = observation.classify_emit_stall(EmitStallThresholds::default(), now);
            if matches!(verdict, EmitStallVerdict::Stalled) {
                gaps.push(CoverageGapView {
                    kind: runtime_stalled_gap_kind(contract).to_string(),
                    message: runtime_stall_message(contract, observation),
                });
            }
            caveats.extend(runtime_observation_caveats(contract, observation, verdict));
        } else {
            caveats.push(runtime_unobserved_caveat(contract));
        }
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
        actions: source_actions(contract.id, bindings),
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
fn session_control_caveat(record: &sinex_db::repositories::SourceSessionStateRecord) -> CaveatView {
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

fn runtime_unobserved_caveat(contract: &SourceContract) -> CaveatView {
    CaveatView {
        id: runtime_caveat_id(contract, "unobserved"),
        message: format!(
            "{} is declared, but no runtime observation, material, or admitted events have been observed for this source",
            runtime_subject(contract)
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
        .filter_map(|row| {
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
            source_status_has_runtime_evidence(&status)
                .then(|| (status.module_name.to_string(), status))
        })
        .collect()
}

fn source_status_has_runtime_evidence(status: &SourceStatus) -> bool {
    status.current_health.is_some()
        || status.last_heartbeat_at.is_some()
        || status.last_output_at.is_some()
        || status.recent_output_count > 0
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

fn runtime_observation_caveats(
    contract: &SourceContract,
    observation: &SourceStatus,
    verdict: EmitStallVerdict,
) -> Vec<CaveatView> {
    let mut caveats = vec![CaveatView {
        id: runtime_caveat_id(contract, "observed"),
        message: runtime_status_message(contract, observation),
        ref_: runtime_ref(contract),
    }];

    if observation.current_health.is_some() || observation.health_reason.is_some() {
        caveats.push(CaveatView {
            id: runtime_caveat_id(contract, "health"),
            message: runtime_health_message(contract, observation),
            ref_: runtime_ref(contract),
        });
    }

    if matches!(verdict, EmitStallVerdict::Stalled) {
        caveats.push(CaveatView {
            id: runtime_caveat_id(contract, "stalled"),
            message: runtime_stall_message(contract, observation),
            ref_: runtime_ref(contract),
        });
    }

    if runtime_observation_is_disconnected(observation) {
        caveats.push(CaveatView {
            id: runtime_caveat_id(contract, "disconnected"),
            message: runtime_status_message(contract, observation),
            ref_: runtime_ref(contract),
        });
    }

    caveats
}

fn runtime_ref(contract: &SourceContract) -> Option<SinexObjectRef> {
    Some(SinexObjectRef::new(
        SinexObjectKind::SourceDriver,
        contract.id.to_string(),
    ))
}

fn runtime_observation_is_disconnected(observation: &SourceStatus) -> bool {
    !observation.live && observation.recent_output_count <= 0
}

fn runtime_bridge_surface(contract: &SourceContract) -> &'static str {
    match contract.access_scope {
        AccessScope::RuntimeBridge { surface } => surface,
        _ => "runtime_bridge",
    }
}

fn runtime_subject(contract: &SourceContract) -> String {
    match contract.access_scope {
        AccessScope::RuntimeBridge { .. } => {
            format!("runtime bridge `{}`", runtime_bridge_surface(contract))
        }
        _ => format!("runtime binding for source `{}`", contract.id),
    }
}

fn runtime_caveat_id(contract: &SourceContract, suffix: &'static str) -> String {
    if matches!(contract.access_scope, AccessScope::RuntimeBridge { .. }) {
        format!("source.runtime_bridge.{suffix}")
    } else {
        format!("source.runtime_binding.{suffix}")
    }
}

fn runtime_disconnected_gap_kind(contract: &SourceContract) -> &'static str {
    if matches!(contract.access_scope, AccessScope::RuntimeBridge { .. }) {
        "runtime_bridge_disconnected"
    } else {
        "runtime_binding_disconnected"
    }
}

fn runtime_stalled_gap_kind(contract: &SourceContract) -> &'static str {
    if matches!(contract.access_scope, AccessScope::RuntimeBridge { .. }) {
        "runtime_bridge_stalled"
    } else {
        "runtime_binding_stalled"
    }
}

fn runtime_status_message(contract: &SourceContract, observation: &SourceStatus) -> String {
    let connection = if observation.live {
        "connected"
    } else if observation.recent_output_count > 0 {
        "output-active without a live heartbeat"
    } else {
        "disconnected"
    };
    format!(
        "{} is {connection} through module `{}`; last heartbeat {}; last output {}; recent output count {}",
        runtime_subject(contract),
        observation.module_name,
        optional_timestamp(observation.last_heartbeat_at),
        optional_timestamp(observation.last_output_at),
        observation.recent_output_count
    )
}

fn runtime_health_message(contract: &SourceContract, observation: &SourceStatus) -> String {
    let health = observation
        .current_health
        .map_or_else(|| "unknown".to_string(), |status| status.to_string());
    let reason = observation
        .health_reason
        .as_deref()
        .unwrap_or("no health reason recorded");
    format!(
        "{} health is {health}; {reason}; health changed {}",
        runtime_subject(contract),
        optional_timestamp(observation.health_changed_at)
    )
}

fn runtime_stall_message(contract: &SourceContract, observation: &SourceStatus) -> String {
    format!(
        "{} is heartbeating but has no recent source output; last output {}; recent output count {}",
        runtime_subject(contract),
        optional_timestamp(observation.last_output_at),
        observation.recent_output_count
    )
}

fn optional_timestamp(timestamp: Option<Timestamp>) -> String {
    timestamp.map_or_else(|| "unknown".to_string(), |timestamp| timestamp.to_string())
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
        "browser.history" => Some("browser.history"),
        "terminal.kitty-osc-live" => Some("terminal-source"),
        _ => None,
    }
}

#[cfg(test)]
#[path = "source_status_test.rs"]
mod tests;
