use crate::rpc_server::RpcAuthContext;
use crate::service_container::ServiceContainer;
use serde_json::{Value, json};
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::{
    RuntimePrivateModeState, load_private_mode_state, save_private_mode_state,
};
use sinex_primitives::rpc::privacy::{
    PrivateModeDisableRequest, PrivateModeEnableRequest, PrivateModeStateResponse,
    PrivateModeStatusRequest,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::transport;
use sqlx::PgPool;
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
