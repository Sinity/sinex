use crate::api::rpc_server::RpcAuthContext;
use crate::api::service_container::ServiceContainer;
use crate::runtime::nats_payload::ensure_nats_payload_fits;
use serde_json::{Value, json};
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::{
    CategorySet, PrivacyConfig, PrivacyEngine, ProcessingContext, RuntimePrivateModeState,
    builtin_policy_seed_rules, load_private_mode_state, save_private_mode_state,
};
use sinex_primitives::rpc::privacy::{
    PrivacyPolicyBackendAddRequest, PrivacyPolicyDictionary, PrivacyPolicyDictionaryAddRequest,
    PrivacyPolicyFieldBindRequest, PrivacyPolicyFieldBindResponse, PrivacyPolicyFieldScope,
    PrivacyPolicyFieldUnbindRequest, PrivacyPolicyFieldUnbindResponse, PrivacyPolicyKeyNamespace,
    PrivacyPolicyListRequest, PrivacyPolicyListResponse, PrivacyPolicyMutationResponse,
    PrivacyPolicyRecognizerBackend, PrivacyPolicyRule, PrivacyPolicyRuleAddRequest,
    PrivacyPolicyRuleRemoveRequest, PrivacyPolicyRuleRemoveResponse,
    PrivacyPolicyRuleSetEnabledRequest, PrivacyPolicyRuleSetEnabledResponse,
    PrivacyPolicyScopeBindRequest, PrivacyPolicySeedBuiltinRequest,
    PrivacyPolicySeedBuiltinResponse, PrivateModeDisableRequest, PrivateModeEnableRequest,
    PrivateModeStateResponse, PrivateModeStatusRequest, PrivacyInvalidationStatus,
    PrivacyInvalidationSurface, PrivacyShadowAuditFinding, PrivacyShadowAuditRequest,
    PrivacyShadowAuditResponse,
};
use sinex_primitives::temporal::{Timestamp, parse_duration};
use sinex_primitives::transport;
use sqlx::{PgPool, Postgres, Row};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

const PRIVATE_MODE_OPERATION_TYPE: &str = "privacy.private_mode";
const PRIVATE_MODE_CONTROL_SUBJECT: &str = "sinex.control.privacy.private_mode";

const SHADOW_SURFACES: &[(&str, &str)] = &[
    ("core.events", "retention follows event lifecycle"),
    ("reflection.events", "reflection retention window"),
    ("audit.archived_events", "operator-authorized archive retention"),
    ("audit.archived_annotations", "operator-authorized archive retention"),
    ("audit.archived_embeddings", "operator-authorized archive retention"),
    ("audit.archived_tagged_items", "operator-authorized archive retention"),
    ("raw.source_material_registry", "source material retention policy"),
    ("raw.source_material_links", "source material retention policy"),
    ("raw.temporal_ledger", "source material retention policy"),
    ("core.document_chunks", "projection rebuild retention"),
    ("core.email_mailbox_projection", "projection rebuild retention"),
    ("core.entities", "projection rebuild retention"),
    ("core.entity_relations", "projection rebuild retention"),
    ("core.event_annotations", "operator annotation retention"),
    ("core.event_embeddings", "embedding worker retention"),
    ("core.embedding_cache", "cache eviction horizon"),
    ("derivation.lane_outputs", "lane retention and discard policy"),
    ("derivation.projection_registry", "stale registry rebuild horizon"),
    ("core.model_effects", "model-effect retention policy"),
    ("core.operations_log", "immutable operator audit retention"),
    ("sinex_schemas.dlq_events", "DLQ retention window"),
    ("sinex_telemetry.current_health", "telemetry retention window"),
    ("sinex_telemetry.current_window_focus", "telemetry retention window"),
    ("sinex_telemetry.recent_activity_summary", "telemetry retention window"),
];

/// Fold typed Presidio `context_words` into the rule's `matcher_config` under
/// the `"context"` key, so the recognizer-rule compiler and analyzer request
/// can read them from one place. A non-empty list always wins; an empty list
/// leaves any pre-existing `matcher_config["context"]` untouched (callers that
/// want to clear it pass an explicit empty array inside `matcher_config`).
fn fold_context_words(mut matcher_config: Value, context_words: &[String]) -> Value {
    if context_words.is_empty() {
        return matcher_config;
    }
    let context = Value::Array(context_words.iter().cloned().map(Value::String).collect());
    match &mut matcher_config {
        Value::Object(map) => {
            map.insert("context".to_string(), context);
            matcher_config
        }
        _ => json!({ "context": context }),
    }
}

/// Project `matcher_config["context"]` back into a typed `Vec<String>` for the
/// rule list response. Inverse of [`fold_context_words`].
fn project_context_words(matcher_config: &Value) -> Vec<String> {
    matcher_config
        .get("context")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

pub async fn handle_private_mode_status(
    state_dir: &Path,
    _request: PrivateModeStatusRequest,
) -> Result<PrivateModeStateResponse> {
    Ok(private_mode_response(
        load_private_mode_state(state_dir)?.effective_at(Timestamp::now()),
    ))
}

pub async fn handle_private_mode_status_service(
    services: &ServiceContainer,
    request: PrivateModeStatusRequest,
) -> Result<PrivateModeStateResponse> {
    handle_private_mode_status(services.state_dir(), request).await
}

pub async fn handle_privacy_policy_list(
    pool: &PgPool,
    request: PrivacyPolicyListRequest,
) -> Result<PrivacyPolicyListResponse> {
    let repo = pool.privacy_policy();
    let mut rules = repo.list_rules().await?;
    if !request.include_disabled {
        rules.retain(|rule| rule.enabled);
    }
    let enabled_rule_ids = rules.iter().map(|rule| rule.id).collect::<HashSet<_>>();

    let field_scopes = repo
        .list_field_rules(None)
        .await?
        .into_iter()
        .filter(|scope| request.include_disabled || enabled_rule_ids.contains(&scope.rule_id))
        .map(|scope| PrivacyPolicyFieldScope {
            id: scope.id,
            rule_id: scope.rule_id,
            event_source: scope.event_source,
            event_type: scope.event_type,
            field_path: scope.field_path,
            priority: scope.priority,
        })
        .collect();

    let key_namespaces = repo
        .list_keys()
        .await?
        .into_iter()
        .map(|key| PrivacyPolicyKeyNamespace {
            id: key.id,
            name: key.name,
            description: key.description,
        })
        .collect();

    let recognizer_backends = repo
        .list_recognizer_backends()
        .await?
        .into_iter()
        .filter(|backend| request.include_disabled || backend.enabled)
        .map(|backend| PrivacyPolicyRecognizerBackend {
            id: backend.id,
            name: backend.name,
            kind: backend.kind,
            endpoint_url: backend.endpoint_url,
            config: backend.config,
            enabled: backend.enabled,
        })
        .collect();

    let dictionaries = policy_dictionaries(pool, request.include_disabled).await?;

    Ok(PrivacyPolicyListResponse {
        rules: rules
            .into_iter()
            .map(|rule| PrivacyPolicyRule {
                id: rule.id,
                name: rule.name,
                description: rule.description,
                matcher_type: rule.matcher_type,
                matcher_value: rule.matcher_value,
                context_words: project_context_words(&rule.matcher_config),
                matcher_config: rule.matcher_config,
                recognizer_backend_id: rule.recognizer_backend_id,
                recognizer_kind: rule.recognizer_kind,
                case_sensitive: rule.case_sensitive,
                action: rule.action,
                action_label: rule.action_label,
                key_namespace: rule.key_namespace,
                enabled: rule.enabled,
            })
            .collect(),
        field_scopes,
        key_namespaces,
        recognizer_backends,
        dictionaries,
    })
}

/// Stale every current `derivation.projection_registry` row after a privacy
/// policy mutation (sinex-68c.4).
///
/// Redaction is applied at the central persistence chokepoint
/// (`event_engine::policy::PolicyEngine::redact_batch`) — a rule/scope/
/// backend change means content the policy engine already redacted (or
/// left unredacted) under the OLD policy may render differently under the
/// new one. There is no per-projection dependency table tracking which
/// projections read which redacted fields yet, so this conservatively
/// invalidates every tracked projection rather than risk one continuing to
/// serve pre-change content as ready. Best-effort: a failure here must not
/// fail the policy mutation itself (the policy change is the source of
/// truth; a missed stale flag degrades to a manual `xtask`/operator rebuild
/// rather than data loss).
async fn stale_projections_for_policy_change(pool: &PgPool, reason: &str) {
    if let Err(error) = pool.projection_registry().mark_all_stale(reason).await {
        tracing::warn!(
            error = %error,
            reason,
            "Failed to stale projection registry after privacy policy change"
        );
    }
}

pub async fn handle_privacy_policy_rule_add(
    pool: &PgPool,
    request: PrivacyPolicyRuleAddRequest,
) -> Result<PrivacyPolicyMutationResponse> {
    let repo = pool.privacy_policy();
    let matcher_config = fold_context_words(request.matcher_config, &request.context_words);
    let id = repo
        .add_recognizer_rule(
            &request.name,
            &request.description,
            &request.matcher_type,
            &request.matcher_value,
            matcher_config,
            request.recognizer_backend_id,
            &request.recognizer_kind,
            request.case_sensitive,
            &request.action,
            request.action_label.as_deref(),
            &request.key_namespace,
        )
        .await?;
    stale_projections_for_policy_change(
        pool,
        &format!("privacy policy rule '{}' added", request.name),
    )
    .await;
    Ok(PrivacyPolicyMutationResponse {
        id,
        kind: "rule".to_string(),
        name: request.name,
    })
}

pub async fn handle_privacy_policy_backend_add(
    pool: &PgPool,
    request: PrivacyPolicyBackendAddRequest,
) -> Result<PrivacyPolicyMutationResponse> {
    let repo = pool.privacy_policy();
    let id = repo
        .add_recognizer_backend(
            &request.name,
            &request.kind,
            request.endpoint_url.as_deref(),
            request.config,
            request.enabled,
        )
        .await?;
    stale_projections_for_policy_change(
        pool,
        &format!("privacy recognizer backend '{}' added", request.name),
    )
    .await;
    Ok(PrivacyPolicyMutationResponse {
        id,
        kind: "recognizer_backend".to_string(),
        name: request.name,
    })
}

pub async fn handle_privacy_policy_dictionary_add(
    pool: &PgPool,
    request: PrivacyPolicyDictionaryAddRequest,
) -> Result<PrivacyPolicyMutationResponse> {
    let repo = pool.privacy_policy();
    let id = repo
        .add_dictionary(
            &request.name,
            &request.description,
            request.language.as_deref(),
            &request.source_kind,
            &request.tags,
            &request.terms,
        )
        .await?;
    Ok(PrivacyPolicyMutationResponse {
        id,
        kind: "dictionary".to_string(),
        name: request.name,
    })
}

pub async fn handle_privacy_policy_scope_bind(
    pool: &PgPool,
    request: PrivacyPolicyScopeBindRequest,
) -> Result<PrivacyPolicyMutationResponse> {
    let repo = pool.privacy_policy();
    let id = repo
        .bind_field_rule(
            &request.rule_name,
            request.event_source.as_deref(),
            request.event_type.as_deref(),
            request.field_path.as_deref(),
            request.priority,
        )
        .await?;
    Ok(PrivacyPolicyMutationResponse {
        id,
        kind: "field_scope".to_string(),
        name: request.rule_name,
    })
}

pub async fn handle_privacy_policy_seed_builtin(
    pool: &PgPool,
    request: PrivacyPolicySeedBuiltinRequest,
) -> Result<PrivacyPolicySeedBuiltinResponse> {
    let rules = builtin_policy_seed_rules(request.enabled);
    let summary = pool.privacy_policy().seed_rules(&rules).await?;
    if summary.inserted > 0 || summary.updated > 0 {
        stale_projections_for_policy_change(
            pool,
            &format!(
                "privacy policy builtin seed applied ({} inserted, {} updated)",
                summary.inserted, summary.updated
            ),
        )
        .await;
    }
    Ok(PrivacyPolicySeedBuiltinResponse {
        inserted: summary.inserted,
        updated: summary.updated,
        unchanged: summary.unchanged,
        total: rules.len(),
    })
}

async fn policy_dictionaries(
    pool: &PgPool,
    include_disabled: bool,
) -> Result<Vec<PrivacyPolicyDictionary>> {
    let repo = pool.privacy_policy();
    let mut dictionaries = Vec::new();
    for dictionary in repo.list_dictionaries().await? {
        if !include_disabled && !dictionary.enabled {
            continue;
        }
        let enabled_terms = repo
            .list_dictionary_terms(dictionary.id)
            .await?
            .into_iter()
            .filter(|term| term.enabled)
            .count();
        dictionaries.push(PrivacyPolicyDictionary {
            id: dictionary.id,
            name: dictionary.name,
            description: dictionary.description,
            language: dictionary.language,
            source_kind: dictionary.source_kind,
            tags: dictionary.tags,
            enabled: dictionary.enabled,
            enabled_terms,
        });
    }
    Ok(dictionaries)
}

pub async fn handle_private_mode_enable(
    pool: &PgPool,
    state_dir: &Path,
    control: Option<(&async_nats::Client, &SinexEnvironment)>,
    req: PrivateModeEnableRequest,
    auth: &RpcAuthContext,
) -> Result<PrivateModeStateResponse> {
    let mut state =
        RuntimePrivateModeState::enabled_by(req.actor, req.source_classes, Timestamp::now())
            .with_expires_at(req.expires_at);
    state.reason_class = req.reason_class;
    persist_private_mode_state_with_audit(pool, state_dir, control, auth, "enable", &mut state)
        .await?;
    Ok(private_mode_response(state))
}

pub async fn handle_private_mode_enable_service(
    services: &ServiceContainer,
    request: PrivateModeEnableRequest,
    auth: &RpcAuthContext,
) -> Result<PrivateModeStateResponse> {
    let nats = services.nats_client().ok_or_else(|| {
        SinexError::configuration("NATS client is not available for private-mode broadcast")
    })?;
    let control = Some((nats, services.environment()));
    handle_private_mode_enable(
        services.pool(),
        services.state_dir(),
        control,
        request,
        auth,
    )
    .await
}

pub async fn handle_private_mode_disable(
    pool: &PgPool,
    state_dir: &Path,
    control: Option<(&async_nats::Client, &SinexEnvironment)>,
    _request: PrivateModeDisableRequest,
    auth: &RpcAuthContext,
) -> Result<PrivateModeStateResponse> {
    let mut state = load_private_mode_state(state_dir)?.disable();
    persist_private_mode_state_with_audit(pool, state_dir, control, auth, "disable", &mut state)
        .await?;
    Ok(private_mode_response(state))
}

pub async fn handle_private_mode_disable_service(
    services: &ServiceContainer,
    request: PrivateModeDisableRequest,
    auth: &RpcAuthContext,
) -> Result<PrivateModeStateResponse> {
    let nats = services.nats_client().ok_or_else(|| {
        SinexError::configuration("NATS client is not available for private-mode broadcast")
    })?;
    let control = Some((nats, services.environment()));
    handle_private_mode_disable(
        services.pool(),
        services.state_dir(),
        control,
        request,
        auth,
    )
    .await
}

async fn persist_private_mode_state_with_audit(
    pool: &PgPool,
    state_dir: &Path,
    control: Option<(&async_nats::Client, &SinexEnvironment)>,
    auth: &RpcAuthContext,
    action: &'static str,
    state: &mut RuntimePrivateModeState,
) -> Result<()> {
    let scope = private_mode_operation_scope(action, state);
    let operation = pool
        .state()
        .log_operation(Operation {
            id: None,
            operation_type: PRIVATE_MODE_OPERATION_TYPE.to_string(),
            operator: auth.actor_id().to_string(),
            scope: Some(scope.clone()),
            result_status: OperationStatus::Running,
            result_message: Some(format!("private mode {action} requested")),
            preview_summary: Some(scope.clone()),
            duration_ms: None,
        })
        .await?;

    state.updated_by_operation_id = Some(operation.id.to_uuid().to_string());

    if let Err(error) = save_private_mode_state(state_dir, state) {
        pool.state()
            .update_operation_meta(
                &operation.id,
                OperationStatus::Failed,
                Some("private mode state write failed"),
                private_mode_operation_scope(action, state),
            )
            .await?;
        return Err(error);
    }

    if let Some((nats_client, env)) = control
        && let Err(error) = publish_private_mode_control(nats_client, env, action, state).await
    {
        pool.state()
            .update_operation_meta(
                &operation.id,
                OperationStatus::Failed,
                Some("private mode state broadcast failed"),
                private_mode_operation_scope(action, state),
            )
            .await?;
        return Err(error);
    }

    let success_message = if control.is_some() {
        format!("private mode {action} persisted and broadcast")
    } else {
        format!("private mode {action} persisted")
    };
    pool.state()
        .update_operation_meta(
            &operation.id,
            OperationStatus::Success,
            Some(&success_message),
            private_mode_operation_scope(action, state),
        )
        .await?;

    Ok(())
}

async fn publish_private_mode_control(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    action: &'static str,
    state: &RuntimePrivateModeState,
) -> Result<()> {
    let subject = private_mode_control_subject(env);
    let payload = private_mode_control_payload(action, state);
    let mut headers = async_nats::HeaderMap::new();
    transport::insert_transport_class_headers(&mut headers, transport::Class::Control);
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|err| {
            SinexError::serialization("failed to serialize private-mode control payload")
                .with_std_error(&err)
        })?;

    ensure_nats_payload_fits("private-mode control update", &subject, payload_bytes.len())?;

    nats_client
        .publish_with_headers(subject.clone(), headers, payload_bytes.into())
        .await
        .map_err(|err| {
            SinexError::nats_publish("private-mode control update")
                .with_context("subject", &subject)
                .with_std_error(&err)
        })
}

fn private_mode_control_subject(env: &SinexEnvironment) -> String {
    env.nats_subject(PRIVATE_MODE_CONTROL_SUBJECT)
}

fn private_mode_control_payload(action: &'static str, state: &RuntimePrivateModeState) -> Value {
    json!({
        "action": action,
        "timestamp": Timestamp::now(),
        "state": state,
    })
}

fn private_mode_operation_scope(action: &'static str, state: &RuntimePrivateModeState) -> Value {
    json!({
        "action": action,
        "enabled": state.enabled,
        "reason_class": state.reason_class.to_string(),
        "actor": state.actor.as_str(),
        "affected_source_classes": &state.affected_source_classes,
        "updated_by_operation_id": state.updated_by_operation_id.as_deref(),
    })
}

fn private_mode_response(state: RuntimePrivateModeState) -> PrivateModeStateResponse {
    PrivateModeStateResponse { state }
}

pub async fn handle_privacy_policy_rule_remove(
    pool: &PgPool,
    request: PrivacyPolicyRuleRemoveRequest,
) -> Result<PrivacyPolicyRuleRemoveResponse> {
    let name = required_policy_text(request.name, "privacy policy rule name")?;
    let rows = pool.privacy_policy().remove_rule(&name).await?;
    if rows == 0 {
        return Err(SinexError::not_found(format!(
            "privacy policy rule not found: {name}"
        )));
    }
    stale_projections_for_policy_change(pool, &format!("privacy policy rule '{name}' removed"))
        .await;
    Ok(PrivacyPolicyRuleRemoveResponse {
        name,
        removed: true,
    })
}

pub async fn handle_privacy_policy_rule_set_enabled(
    pool: &PgPool,
    request: PrivacyPolicyRuleSetEnabledRequest,
) -> Result<PrivacyPolicyRuleSetEnabledResponse> {
    let name = required_policy_text(request.name, "privacy policy rule name")?;
    let rows = pool
        .privacy_policy()
        .set_rule_enabled(&name, request.enabled)
        .await?;
    if rows == 0 {
        return Err(SinexError::not_found(format!(
            "privacy policy rule not found: {name}"
        )));
    }
    stale_projections_for_policy_change(
        pool,
        &format!(
            "privacy policy rule '{name}' set enabled={}",
            request.enabled
        ),
    )
    .await;
    Ok(PrivacyPolicyRuleSetEnabledResponse {
        name,
        enabled: request.enabled,
    })
}

pub async fn handle_privacy_policy_field_bind(
    pool: &PgPool,
    request: PrivacyPolicyFieldBindRequest,
) -> Result<PrivacyPolicyFieldBindResponse> {
    let rule_name = required_policy_text(request.rule_name, "privacy policy rule name")?;
    let field_path = normalize_optional_text(request.field_path);
    if let Some(path) = field_path.as_deref()
        && !path.starts_with('/')
    {
        return Err(SinexError::validation(
            "privacy policy field_path must be a JSON Pointer beginning with '/'",
        ));
    }
    let event_source = normalize_optional_text(request.event_source);
    let event_type = normalize_optional_text(request.event_type);
    let id = pool
        .privacy_policy()
        .bind_field_rule(
            &rule_name,
            event_source.as_deref(),
            event_type.as_deref(),
            field_path.as_deref(),
            request.priority,
        )
        .await?;
    let scope = pool
        .privacy_policy()
        .list_field_rules(Some(&rule_name))
        .await?
        .into_iter()
        .find(|scope| scope.id == id)
        .ok_or_else(|| {
            SinexError::database("privacy policy field scope was inserted but not readable")
                .with_context("scope_id", id.to_string())
        })?;
    stale_projections_for_policy_change(
        pool,
        &format!("privacy policy field scope bound to rule '{rule_name}'"),
    )
    .await;
    Ok(PrivacyPolicyFieldBindResponse {
        scope: PrivacyPolicyFieldScope {
            id: scope.id,
            rule_id: scope.rule_id,
            event_source: scope.event_source,
            event_type: scope.event_type,
            field_path: scope.field_path,
            priority: scope.priority,
        },
    })
}

pub async fn handle_privacy_policy_field_unbind(
    pool: &PgPool,
    request: PrivacyPolicyFieldUnbindRequest,
) -> Result<PrivacyPolicyFieldUnbindResponse> {
    let rows = pool
        .privacy_policy()
        .unbind_field_rule(request.scope_id)
        .await?;
    if rows == 0 {
        return Err(SinexError::not_found(format!(
            "privacy policy field scope not found: {}",
            request.scope_id
        )));
    }
    stale_projections_for_policy_change(
        pool,
        &format!("privacy policy field scope '{}' unbound", request.scope_id),
    )
    .await;
    Ok(PrivacyPolicyFieldUnbindResponse {
        scope_id: request.scope_id,
        removed: true,
    })
}

/// Run the bounded report-only privacy audit over the live database.
///
/// The audit opens a PostgreSQL read-only transaction, samples both event
/// lanes, recursively inspects only JSON string leaves, and reports rule
/// metadata/counts. Other registered surfaces are enumerated with bounded row
/// counts so the output distinguishes "scanned and empty" from "not covered".
pub async fn handle_privacy_shadow_audit(
    pool: &PgPool,
    mut request: PrivacyShadowAuditRequest,
) -> Result<PrivacyShadowAuditResponse> {
    if request.limit_events <= 0 || request.limit_rows_per_surface <= 0 {
        return Err(SinexError::validation(
            "privacy shadow audit limits must be positive",
        ));
    }
    request.limit_events = request.limit_events.min(10_000);
    request.limit_rows_per_surface = request.limit_rows_per_surface.min(1_000);

    let since = parse_audit_bound(request.since.as_deref(), false)?;
    let until = parse_audit_bound(request.until.as_deref(), true)?;
    if since.is_some() && until.is_some() && since >= until {
        return Err(SinexError::validation(
            "privacy shadow audit since must be before until",
        ));
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| SinexError::database("failed to begin privacy shadow audit").with_source(e.to_string()))?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(|e| SinexError::database("failed to set privacy shadow audit read-only").with_source(e.to_string()))?;
    let read_only: bool = sqlx::query_scalar("SELECT current_setting('transaction_read_only') = 'on'")
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| SinexError::database("failed to prove privacy shadow audit read-only").with_source(e.to_string()))?;
    if !read_only {
        return Err(SinexError::invalid_state(
            "privacy shadow audit could not prove a read-only transaction",
        ));
    }

    let engine = PrivacyEngine::new(PrivacyConfig {
        enabled: true,
        builtin_categories: CategorySet::All,
        ..PrivacyConfig::default()
    })
    .map_err(|e| SinexError::processing("failed to compile privacy shadow recognizers").with_source(e.to_string()))?;
    let mut aggregate = BTreeMap::<(String, String, String, String, String, String), PrivacyShadowAuditFinding>::new();
    let mut surfaces = Vec::new();
    let mut scanned_events = 0_u64;
    let mut scanned_rows = 0_u64;

    for lane in ["core.events", "reflection.events"] {
        let (schema, table) = lane.split_once('.').expect("static lane has schema");
        let mut sql = format!(
            "SELECT source, event_type, payload, ts_orig FROM {schema}.{table} WHERE TRUE"
        );
        if request.source.is_some() {
            sql.push_str(" AND source = $1");
        }
        if request.event_type.is_some() {
            sql.push_str(if request.source.is_some() { " AND event_type = $2" } else { " AND event_type = $1" });
        }
        if since.is_some() {
            let index = 1 + request.source.is_some() as usize + request.event_type.is_some() as usize;
            sql.push_str(&format!(" AND ts_orig >= ${index}"));
        }
        if until.is_some() {
            let index = 1
                + request.source.is_some() as usize
                + request.event_type.is_some() as usize
                + since.is_some() as usize;
            sql.push_str(&format!(" AND ts_orig < ${index}"));
        }
        sql.push_str(" ORDER BY id DESC LIMIT $");
        sql.push_str(&(1 + request.source.is_some() as usize + request.event_type.is_some() as usize + since.is_some() as usize + until.is_some() as usize).to_string());

        let mut query = sqlx::query(&sql);
        if let Some(source) = request.source.as_deref() {
            query = query.bind(source);
        }
        if let Some(event_type) = request.event_type.as_deref() {
            query = query.bind(event_type);
        }
        if let Some(since) = since {
            query = query.bind(since);
        }
        if let Some(until) = until {
            query = query.bind(until);
        }
        let rows = query
            .bind(request.limit_events)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| SinexError::database(format!("privacy shadow audit failed on {lane}")).with_source(e.to_string()))?;
        for row in rows {
            scanned_events += 1;
            scanned_rows += 1;
            let source: String = row.try_get("source").map_err(|e| SinexError::database("privacy shadow audit source read failed").with_source(e.to_string()))?;
            let event_type: String = row.try_get("event_type").map_err(|e| SinexError::database("privacy shadow audit event type read failed").with_source(e.to_string()))?;
            let payload: Value = row.try_get("payload").map_err(|e| SinexError::database("privacy shadow audit payload read failed").with_source(e.to_string()))?;
            let ts_orig: Option<time::OffsetDateTime> = row.try_get("ts_orig").ok();
            let timestamp = ts_orig.map(|value| value.format(&time::format_description::well_known::Rfc3339).unwrap_or_default());
            let mut leaves = Vec::new();
            collect_string_leaves(&payload, "$", &mut leaves);
            for (path, value) in leaves {
                let context = audit_context_for_field(&path);
                for finding in engine.detect_matches(&value, context) {
                    let key = (
                        finding.rule_name.clone(),
                        format!("{:?}", finding.category),
                        lane.to_string(),
                        source.clone(),
                        event_type.clone(),
                        path.clone(),
                    );
                    let entry = aggregate.entry(key).or_insert_with(|| PrivacyShadowAuditFinding {
                        recognizer: finding.rule_name,
                        category: finding.category,
                        surface: lane.to_string(),
                        source: source.clone(),
                        event_type: event_type.clone(),
                        field_path: path.clone(),
                        sampled_row_count: 0,
                        matched_row_count: 0,
                        match_count: 0,
                        first_seen: timestamp.clone(),
                        last_seen: timestamp.clone(),
                    });
                    entry.matched_row_count += 1;
                    entry.match_count += finding.match_count;
                    if entry.first_seen.is_none() {
                        entry.first_seen.clone_from(&timestamp);
                    }
                    entry.last_seen.clone_from(&timestamp);
                }
            }
        }
        surfaces.push(PrivacyInvalidationSurface {
            surface: lane.to_string(),
            status: PrivacyInvalidationStatus::Scanned,
            before_count: count_rows(&mut tx, lane, request.limit_events).await?,
            after_count: 0,
            affected_count: 0,
            residual_horizon: Some("event lifecycle retention".to_string()),
            detail: Some("bounded payload scan; values omitted".to_string()),
        });
    }

    for &(surface, horizon) in SHADOW_SURFACES {
        if surfaces.iter().any(|row| row.surface == surface) {
            continue;
        }
        let count = count_rows(&mut tx, surface, request.limit_rows_per_surface).await?;
        scanned_rows += count;
        surfaces.push(PrivacyInvalidationSurface {
            surface: surface.to_string(),
            status: PrivacyInvalidationStatus::Scanned,
            before_count: count,
            after_count: count,
            affected_count: 0,
            residual_horizon: Some(horizon.to_string()),
            detail: Some("bounded row inventory; payload values omitted".to_string()),
        });
    }

    tx.rollback().await.map_err(|e| SinexError::database("failed to roll back privacy shadow audit").with_source(e.to_string()))?;
    let generated_at = Timestamp::now().format_rfc3339();
    Ok(PrivacyShadowAuditResponse {
        schema_version: "sinex.privacy-shadow-audit/v1".to_string(),
        generated_at,
        read_only_proven: true,
        scanned_events,
        scanned_rows,
        scope: request,
        surfaces,
        findings: aggregate.into_values().collect(),
        caveats: vec![
            "default-zero-redaction is measured, not changed by this command".to_string(),
            "NATS retained frames, recovery spool, journald, xtask history, exports, backups, WAL, and physical database remnants require separate retention inspection".to_string(),
        ],
    })
}

async fn count_rows(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    surface: &str,
    limit: i64,
) -> Result<u64> {
    let query = format!("SELECT COUNT(*)::bigint FROM (SELECT 1 FROM {surface} LIMIT $1) bounded");
    let count: i64 = sqlx::query_scalar(&query)
        .bind(limit)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| SinexError::database(format!("privacy shadow audit surface read failed for {surface}")).with_source(e.to_string()))?;
    Ok(count.max(0) as u64)
}

fn parse_audit_bound(value: Option<&str>, _upper: bool) -> Result<Option<time::OffsetDateTime>> {
    let Some(value) = value else { return Ok(None); };
    if let Ok(timestamp) = Timestamp::parse_rfc3339(value) {
        return Ok(Some(timestamp.into()));
    }
    let Some(duration) = parse_duration(value) else {
        return Err(SinexError::validation(format!("invalid privacy audit time bound: {value}")));
    };
    Ok(Some((Timestamp::now() - duration).into()))
}

fn collect_string_leaves(value: &Value, path: &str, output: &mut Vec<(String, String)>) {
    match value {
        Value::String(value) => output.push((path.to_string(), value.clone())),
        Value::Array(values) => values.iter().enumerate().for_each(|(index, value)| {
            collect_string_leaves(value, &format!("{path}/{index}"), output);
        }),
        Value::Object(values) => values.iter().for_each(|(key, value)| {
            collect_string_leaves(value, &format!("{path}/{}", key.replace('~', "~0").replace('/', "~1")), output);
        }),
        _ => {}
    }
}

fn audit_context_for_field(path: &str) -> ProcessingContext {
    let lower = path.to_ascii_lowercase();
    if lower.contains("command") || lower.contains("shell") {
        ProcessingContext::Command
    } else if lower.contains("clipboard") {
        ProcessingContext::Clipboard
    } else if lower.contains("message") || lower.contains("body") || lower.contains("text") || lower.contains("content") || lower.contains("note") {
        ProcessingContext::Document
    } else if lower.contains("path") || lower.contains("url") || lower.contains("host") || lower.contains("title") {
        ProcessingContext::Metadata
    } else {
        ProcessingContext::Document
    }
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn required_policy_text(value: String, field: &'static str) -> Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        Err(SinexError::validation(format!("{field} must not be empty")))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
#[path = "privacy_test.rs"]
mod tests;
