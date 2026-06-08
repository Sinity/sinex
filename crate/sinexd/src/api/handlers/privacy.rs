use crate::api::rpc_server::RpcAuthContext;
use crate::api::service_container::ServiceContainer;
use serde_json::{Value, json};
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::{
    RuntimePrivateModeState, builtin_policy_seed_rules, load_private_mode_state,
    save_private_mode_state,
};
use sinex_primitives::rpc::privacy::{
    PrivacyPolicyBackendAddRequest, PrivacyPolicyDictionary, PrivacyPolicyDictionaryAddRequest,
    PrivacyPolicyFieldBindRequest, PrivacyPolicyFieldBindResponse, PrivacyPolicyFieldScope,
    PrivacyPolicyFieldUnbindRequest, PrivacyPolicyFieldUnbindResponse,
    PrivacyPolicyKeyNamespace, PrivacyPolicyListRequest, PrivacyPolicyListResponse,
    PrivacyPolicyMutationResponse, PrivacyPolicyRecognizerBackend, PrivacyPolicyRule,
    PrivacyPolicyRuleAddRequest, PrivacyPolicyRuleRemoveRequest, PrivacyPolicyRuleRemoveResponse,
    PrivacyPolicyRuleSetEnabledRequest, PrivacyPolicyRuleSetEnabledResponse,
    PrivacyPolicyScopeBindRequest, PrivacyPolicySeedBuiltinRequest, PrivacyPolicySeedBuiltinResponse,
    PrivateModeDisableRequest, PrivateModeEnableRequest, PrivateModeStateResponse,
    PrivateModeStatusRequest,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::transport;
use sqlx::PgPool;
use std::collections::HashSet;
use std::path::Path;

const PRIVATE_MODE_OPERATION_TYPE: &str = "privacy.private_mode";
const PRIVATE_MODE_CONTROL_SUBJECT: &str = "sinex.control.privacy.private_mode";

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

    nats_client
        .publish_with_headers(
            subject.clone(),
            headers,
            serde_json::to_vec(&payload)
                .map_err(|err| {
                    SinexError::serialization("failed to serialize private-mode control payload")
                        .with_std_error(&err)
                })?
                .into(),
        )
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
    Ok(PrivacyPolicyFieldUnbindResponse {
        scope_id: request.scope_id,
        removed: true,
    })
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
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_primitives::privacy::PrivateModeReasonClass;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn private_mode_status_defaults_disabled() -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;

        let response =
            handle_private_mode_status(dir.path(), PrivateModeStatusRequest::default()).await?;

        assert!(!response.state.enabled);
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_status_treats_expired_state_as_disabled() -> xtask::sandbox::TestResult<()>
    {
        let dir = tempfile::tempdir()?;
        let expired = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        )
        .with_expires_at(Timestamp::from_unix_timestamp(1));
        save_private_mode_state(dir.path(), &expired)?;

        let response =
            handle_private_mode_status(dir.path(), PrivateModeStatusRequest::default()).await?;

        assert!(!response.state.enabled);
        assert_eq!(response.state.actor, "sinity");
        assert_eq!(response.state.expires_at, Timestamp::from_unix_timestamp(1));
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_enable_and_disable_round_trip(
        ctx: TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let auth = RpcAuthContext::system();

        let enabled = handle_private_mode_enable(
            ctx.pool(),
            dir.path(),
            None,
            PrivateModeEnableRequest {
                actor: "sinity".to_string(),
                reason_class: PrivateModeReasonClass::PolicyHold,
                source_classes: vec!["desktop".to_string()],
                expires_at: None,
            },
            &auth,
        )
        .await?;

        assert!(enabled.state.enabled);
        assert_eq!(enabled.state.actor, "sinity");
        assert_eq!(
            enabled.state.reason_class,
            PrivateModeReasonClass::PolicyHold
        );
        assert_eq!(enabled.state.affected_source_classes, vec!["desktop"]);
        assert!(enabled.state.updated_by_operation_id.is_some());

        let disabled = handle_private_mode_disable(
            ctx.pool(),
            dir.path(),
            None,
            PrivateModeDisableRequest::default(),
            &auth,
        )
        .await?;

        assert!(!disabled.state.enabled);
        assert_eq!(disabled.state.actor, "sinity");
        assert_eq!(disabled.state.affected_source_classes, vec!["desktop"]);
        assert!(disabled.state.updated_by_operation_id.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_enable_null_params_uses_operator_defaults(
        ctx: TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let auth = RpcAuthContext::system();

        let enabled = handle_private_mode_enable(
            ctx.pool(),
            dir.path(),
            None,
            PrivateModeEnableRequest::default(),
            &auth,
        )
        .await?;

        assert!(enabled.state.enabled);
        assert_eq!(enabled.state.actor, "operator");
        assert_eq!(
            enabled.state.reason_class,
            PrivateModeReasonClass::OperatorPrivate
        );
        assert!(enabled.state.affected_source_classes.is_empty());
        assert!(enabled.state.updated_by_operation_id.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_toggle_writes_operation_audit(
        ctx: TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let auth = RpcAuthContext::system();

        let enabled = handle_private_mode_enable(
            ctx.pool(),
            dir.path(),
            None,
            PrivateModeEnableRequest {
                actor: "sinity".to_string(),
                reason_class: PrivateModeReasonClass::OperatorPrivate,
                source_classes: vec!["desktop".to_string()],
                expires_at: None,
            },
            &auth,
        )
        .await?;
        let operation_id = enabled
            .state
            .updated_by_operation_id
            .as_ref()
            .expect("private-mode operation id should be recorded")
            .parse()?;

        let operation = ctx
            .pool()
            .state()
            .get_operation(&operation_id)
            .await?
            .expect("operation row should exist");

        assert_eq!(operation.operation_type, PRIVATE_MODE_OPERATION_TYPE);
        assert_eq!(operation.result_status, OperationStatus::Success);
        assert_eq!(operation.scope.as_ref().unwrap()["action"], "enable");
        assert_eq!(
            operation.scope.as_ref().unwrap()["affected_source_classes"],
            json!(["desktop"])
        );
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_control_payload_is_coarse() -> xtask::sandbox::TestResult<()> {
        let env = SinexEnvironment::new("dev")?;
        let state = RuntimePrivateModeState::enabled_by(
            "sinity".to_string(),
            vec!["desktop".to_string()],
            Timestamp::now(),
        );

        let subject = private_mode_control_subject(&env);
        let payload = private_mode_control_payload("enable", &state);

        assert_eq!(subject, "dev.sinex.control.privacy.private_mode");
        assert_eq!(payload["action"], "enable");
        assert_eq!(payload["state"]["enabled"], true);
        assert_eq!(payload["state"]["actor"], "sinity");
        assert_eq!(
            payload["state"]["affected_source_classes"],
            json!(["desktop"])
        );
        assert!(payload.get("reason").is_none());
        Ok(())
    }
}
