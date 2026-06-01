use crate::api::rpc_server::RpcAuthContext;
use crate::api::service_container::ServiceContainer;
use serde_json::{Value, json};
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::{
    Matcher, RuntimePrivateModeState, Strategy, StructuralDetector, builtin_rules,
    load_private_mode_state, save_private_mode_state,
};
use sinex_primitives::rpc::privacy::{
    PrivacyDictionary, PrivacyDictionaryTerm, PrivacyFieldRule, PrivacyKeyNamespace,
    PrivacyPolicyAddDictionaryTermRequest, PrivacyPolicyBindRuleRequest,
    PrivacyPolicyCreateBackendRequest,
    PrivacyPolicyCreateDictionaryRequest, PrivacyPolicyCreateKeyRequest,
    PrivacyPolicyCreateRuleRequest, PrivacyPolicyIdResponse, PrivacyPolicyListRequest,
    PrivacyPolicyListResponse, PrivacyPolicySeedCatalogRequest, PrivacyPolicySeedCatalogResponse,
    PrivacyRecognizerBackend, PrivacyRule,
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
    let rules = repo.list_rules().await?;
    let field_rules = repo.list_field_rules(None).await?;
    let recognizer_backends = repo.list_recognizer_backends().await?;
    let dictionaries = repo.list_dictionaries().await?;
    let key_namespaces = repo.list_keys().await?;

    let mut dictionary_terms = Vec::new();
    for dictionary in &dictionaries {
        dictionary_terms.extend(repo.list_dictionary_terms(dictionary.id).await?);
    }
    let enabled_rule_ids = rules
        .iter()
        .filter(|rule| rule.enabled)
        .map(|rule| rule.id)
        .collect::<HashSet<_>>();
    let enabled_dictionary_ids = dictionaries
        .iter()
        .filter(|dictionary| dictionary.enabled)
        .map(|dictionary| dictionary.id)
        .collect::<HashSet<_>>();

    Ok(PrivacyPolicyListResponse {
        rules: rules
            .into_iter()
            .filter(|rule| request.include_disabled || rule.enabled)
            .map(|rule| PrivacyRule {
                id: rule.id,
                name: rule.name,
                description: rule.description,
                recognizer_backend_id: rule.recognizer_backend_id,
                recognizer_kind: rule.recognizer_kind,
                matcher_type: rule.matcher_type,
                matcher_value: rule.matcher_value,
                matcher_config: rule.matcher_config,
                case_sensitive: rule.case_sensitive,
                action: rule.action,
                action_label: rule.action_label,
                key_namespace: rule.key_namespace,
                enabled: rule.enabled,
            })
            .collect(),
        field_rules: field_rules
            .into_iter()
            .filter(|field_rule| {
                request.include_disabled || enabled_rule_ids.contains(&field_rule.rule_id)
            })
            .map(|field_rule| PrivacyFieldRule {
                id: field_rule.id,
                rule_id: field_rule.rule_id,
                event_source: field_rule.event_source,
                event_type: field_rule.event_type,
                field_path: field_rule.field_path,
                priority: field_rule.priority,
            })
            .collect(),
        recognizer_backends: recognizer_backends
            .into_iter()
            .filter(|backend| request.include_disabled || backend.enabled)
            .map(|backend| PrivacyRecognizerBackend {
                id: backend.id,
                name: backend.name,
                kind: backend.kind,
                endpoint_url: backend.endpoint_url,
                config: backend.config,
                enabled: backend.enabled,
            })
            .collect(),
        dictionaries: dictionaries
            .into_iter()
            .filter(|dictionary| request.include_disabled || dictionary.enabled)
            .map(|dictionary| PrivacyDictionary {
                id: dictionary.id,
                name: dictionary.name,
                description: dictionary.description,
                language: dictionary.language,
                source_kind: dictionary.source_kind,
                tags: dictionary.tags,
                enabled: dictionary.enabled,
            })
            .collect(),
        dictionary_terms: dictionary_terms
            .into_iter()
            .filter(|term| {
                request.include_disabled
                    || (term.enabled && enabled_dictionary_ids.contains(&term.dictionary_id))
            })
            .map(|term| PrivacyDictionaryTerm {
                id: term.id,
                dictionary_id: term.dictionary_id,
                term: term.term,
                metadata: term.metadata,
                enabled: term.enabled,
            })
            .collect(),
        key_namespaces: key_namespaces
            .into_iter()
            .map(|key| PrivacyKeyNamespace {
                id: key.id,
                name: key.name,
                description: key.description,
            })
            .collect(),
    })
}

pub async fn handle_privacy_policy_create_dictionary(
    pool: &PgPool,
    request: PrivacyPolicyCreateDictionaryRequest,
) -> Result<PrivacyPolicyIdResponse> {
    let id = pool
        .privacy_policy()
        .add_dictionary(
            request.name.as_str(),
            request.description.as_str(),
            request.language.as_deref(),
            request.source_kind.as_str(),
            &request.tags,
        )
        .await?;
    Ok(PrivacyPolicyIdResponse { id })
}

pub async fn handle_privacy_policy_create_backend(
    pool: &PgPool,
    request: PrivacyPolicyCreateBackendRequest,
) -> Result<PrivacyPolicyIdResponse> {
    let id = pool
        .privacy_policy()
        .add_recognizer_backend(
            request.name.as_str(),
            request.kind.as_str(),
            request.endpoint_url.as_deref(),
            request.config,
        )
        .await?;
    Ok(PrivacyPolicyIdResponse { id })
}

pub async fn handle_privacy_policy_create_key(
    pool: &PgPool,
    request: PrivacyPolicyCreateKeyRequest,
) -> Result<PrivacyPolicyIdResponse> {
    let id = pool
        .privacy_policy()
        .add_key(request.name.as_str(), request.description.as_str())
        .await?;
    Ok(PrivacyPolicyIdResponse { id })
}

pub async fn handle_privacy_policy_add_dictionary_term(
    pool: &PgPool,
    request: PrivacyPolicyAddDictionaryTermRequest,
) -> Result<PrivacyPolicyIdResponse> {
    let id = pool
        .privacy_policy()
        .add_dictionary_term(
            request.dictionary_id,
            request.term.as_str(),
            request.metadata,
        )
        .await?;
    Ok(PrivacyPolicyIdResponse { id })
}

pub async fn handle_privacy_policy_create_rule(
    pool: &PgPool,
    request: PrivacyPolicyCreateRuleRequest,
) -> Result<PrivacyPolicyIdResponse> {
    let id = pool
        .privacy_policy()
        .add_recognizer_rule(
            request.name.as_str(),
            request.description.as_str(),
            request.recognizer_backend_id,
            request.recognizer_kind.as_str(),
            request.matcher_type.as_str(),
            request.matcher_value.as_str(),
            request.matcher_config,
            request.case_sensitive,
            request.action.as_str(),
            request.action_label.as_deref(),
            request.key_namespace.as_str(),
        )
        .await?;
    Ok(PrivacyPolicyIdResponse { id })
}

pub async fn handle_privacy_policy_bind_rule(
    pool: &PgPool,
    request: PrivacyPolicyBindRuleRequest,
) -> Result<PrivacyPolicyIdResponse> {
    let id = pool
        .privacy_policy()
        .bind_field_rule(
            request.rule_name.as_str(),
            request.event_source.as_deref(),
            request.event_type.as_deref(),
            request.field_path.as_deref(),
            request.priority,
        )
        .await?;
    Ok(PrivacyPolicyIdResponse { id })
}

pub async fn handle_privacy_policy_seed_catalog(
    pool: &PgPool,
    _request: PrivacyPolicySeedCatalogRequest,
) -> Result<PrivacyPolicySeedCatalogResponse> {
    let repo = pool.privacy_policy();
    let mut seeded_rules = 0usize;

    for rule in builtin_rules() {
        let Some((matcher_type, matcher_value, matcher_config, case_sensitive)) =
            catalog_matcher_to_db(&rule.matcher)
        else {
            continue;
        };
        let (action, action_label) = catalog_strategy_to_db(&rule.strategy);
        repo.upsert_recognizer_rule(
            rule.name.as_str(),
            rule.description.as_str(),
            None,
            "local_pattern",
            matcher_type,
            matcher_value.as_str(),
            matcher_config,
            case_sensitive,
            action,
            action_label.as_deref(),
            "default",
            rule.enabled,
        )
        .await?;
        seeded_rules += 1;
    }

    Ok(PrivacyPolicySeedCatalogResponse { seeded_rules })
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

fn catalog_matcher_to_db(matcher: &Matcher) -> Option<(&'static str, String, Value, bool)> {
    match matcher {
        Matcher::Regex { pattern } => Some((
            "regex",
            pattern.clone(),
            Value::Object(serde_json::Map::new()),
            false,
        )),
        Matcher::Literal {
            text,
            case_sensitive,
        } => Some((
            "literal",
            text.clone(),
            Value::Object(serde_json::Map::new()),
            *case_sensitive,
        )),
        Matcher::Structural { detector } => Some((
            "structural",
            structural_detector_name(*detector).to_string(),
            Value::Object(serde_json::Map::new()),
            false,
        )),
        Matcher::All(_) | Matcher::Any(_) => None,
    }
}

fn structural_detector_name(detector: StructuralDetector) -> &'static str {
    match detector {
        StructuralDetector::CreditCard => "credit_card",
        StructuralDetector::Email => "email",
        StructuralDetector::PhoneNumber => "phone_number",
        StructuralDetector::Iban => "iban",
        StructuralDetector::Ipv4 => "ipv4",
        StructuralDetector::Ipv6 => "ipv6",
        StructuralDetector::MacAddress => "mac_address",
        StructuralDetector::UserHomePath => "user_home_path",
        StructuralDetector::LocalHostname => "local_hostname",
        StructuralDetector::Ssn => "ssn",
        StructuralDetector::Pesel => "pesel",
        StructuralDetector::Nip => "nip",
        StructuralDetector::Regon => "regon",
    }
}

fn catalog_strategy_to_db(strategy: &Strategy) -> (&'static str, Option<String>) {
    match strategy {
        Strategy::Redact { label } => ("redact", label.clone()),
        Strategy::Encrypt => ("encrypt", None),
        Strategy::Hash => ("hash", None),
        Strategy::Suppress => ("suppress", None),
        Strategy::Mask { .. } => ("mask", None),
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
        ctx: xtask::sandbox::TestContext,
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
        ctx: xtask::sandbox::TestContext,
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
        ctx: xtask::sandbox::TestContext,
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
    async fn privacy_policy_list_returns_db_managed_policy(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let repo = ctx.pool().privacy_policy();
        let tags = vec!["seed".to_string(), "nsfw".to_string()];
        let dictionary_name = format!("seed_nsfw_terms_{}", Uuid::now_v7().simple());
        let dictionary_id = repo
            .add_dictionary(
                &dictionary_name,
                "Seeded NSFW vocabulary",
                Some("en"),
                "seed",
                &tags,
            )
            .await?;
        repo.add_dictionary_term(dictionary_id, "explicit-term", json!({"source": "seed"}))
            .await?;
        repo.add_recognizer_rule(
            "seed_nsfw_dictionary",
            "Seed dictionary rule",
            None,
            "dictionary",
            "dictionary",
            &dictionary_name,
            json!({"dictionary": dictionary_name.clone()}),
            false,
            "redact",
            Some("[NSFW]"),
            "default",
        )
        .await?;
        repo.bind_field_rule("seed_nsfw_dictionary", None, None, Some("title"), 10)
            .await?;
        handle_privacy_policy_create_backend(
            ctx.pool(),
            PrivacyPolicyCreateBackendRequest {
                name: "presidio-local".to_string(),
                kind: "presidio".to_string(),
                endpoint_url: Some("http://127.0.0.1:5001/analyze".to_string()),
                config: json!({"language": "en"}),
            },
        )
        .await?;
        handle_privacy_policy_create_key(
            ctx.pool(),
            PrivacyPolicyCreateKeyRequest {
                name: "local-pii".to_string(),
                description: "Local PII hash namespace".to_string(),
            },
        )
        .await?;

        let response = handle_privacy_policy_list(
            ctx.pool(),
            PrivacyPolicyListRequest {
                include_disabled: false,
            },
        )
        .await?;

        assert!(response.rules.iter().any(
            |rule| rule.name == "seed_nsfw_dictionary" && rule.recognizer_kind == "dictionary"
        ));
        assert!(
            response
                .dictionaries
                .iter()
                .any(|dictionary| dictionary.name == dictionary_name)
        );
        assert!(
            response
                .dictionary_terms
                .iter()
                .any(|term| term.term == "explicit-term")
        );
        assert!(
            response
                .field_rules
                .iter()
                .any(|field_rule| field_rule.field_path.as_deref() == Some("/title"))
        );
        assert!(
            response
                .recognizer_backends
                .iter()
                .any(|backend| backend.name == "presidio-local" && backend.kind == "presidio")
        );
        assert!(
            response
                .key_namespaces
                .iter()
                .any(|namespace| namespace.name == "local-pii")
        );
        Ok(())
    }

    #[sinex_test]
    async fn privacy_policy_seed_catalog_is_idempotent_db_seed(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let first = handle_privacy_policy_seed_catalog(
            ctx.pool(),
            PrivacyPolicySeedCatalogRequest::default(),
        )
        .await?;
        let second = handle_privacy_policy_seed_catalog(
            ctx.pool(),
            PrivacyPolicySeedCatalogRequest::default(),
        )
        .await?;
        let response = handle_privacy_policy_list(
            ctx.pool(),
            PrivacyPolicyListRequest {
                include_disabled: true,
            },
        )
        .await?;
        let seeded_count = response
            .rules
            .iter()
            .filter(|rule| rule.recognizer_kind == "local_pattern")
            .count();

        assert!(first.seeded_rules > 0);
        assert_eq!(first.seeded_rules, second.seeded_rules);
        assert_eq!(seeded_count, first.seeded_rules);
        assert!(response.rules.iter().any(|rule| rule.name == "github_token"));
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
