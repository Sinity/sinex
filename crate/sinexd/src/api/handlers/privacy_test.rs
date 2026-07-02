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
