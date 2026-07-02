use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn runtime_control_operation_records_actor_scope_and_preview(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let module_name = sinex_primitives::domain::ModuleName::from("terminal-source");
    let operation_id = start_runtime_control_operation(
        ctx.pool(),
        "runtime.drain",
        "operator:alice",
        json!({
            "surface": RUNTIME_CONTROL_SURFACE,
            "action": "drain",
            "module_name": module_name,
            "reason": "maintenance",
            "control_subject": "dev.sinex.control.sources.terminal-source.drain",
        }),
        runtime_control_preview(
            "drain",
            &module_name,
            "dev.sinex.control.sources.terminal-source.drain",
        ),
    )
    .await?;

    let operation_id = operation_id.parse()?;
    let operation = ctx
        .pool()
        .state()
        .get_operation(&operation_id)
        .await?
        .expect("runtime control operation should be persisted");

    assert_eq!(operation.operation_type, "runtime.drain");
    assert_eq!(operation.operator, "operator:alice");
    assert_eq!(operation.result_status, OperationStatus::Running);
    assert_eq!(
        operation.scope.as_ref().unwrap()["surface"],
        RUNTIME_CONTROL_SURFACE
    );
    assert_eq!(operation.scope.as_ref().unwrap()["action"], "drain");
    assert_eq!(operation.scope.as_ref().unwrap()["reason"], "maintenance");
    assert_eq!(
        operation.preview_summary.as_ref().unwrap()["control_subject"],
        "dev.sinex.control.sources.terminal-source.drain"
    );
    Ok(())
}

#[sinex_test]
async fn runtime_control_operation_records_publish_failure(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let module_name = sinex_primitives::domain::ModuleName::from("terminal-source");
    let operation_id = start_runtime_control_operation(
        ctx.pool(),
        "runtime.resume",
        "operator:alice",
        json!({
            "surface": RUNTIME_CONTROL_SURFACE,
            "action": "resume",
            "module_name": module_name,
            "control_subject": "dev.sinex.control.sources.terminal-source.resume",
        }),
        runtime_control_preview(
            "resume",
            &module_name,
            "dev.sinex.control.sources.terminal-source.resume",
        ),
    )
    .await?;

    finalize_runtime_control_operation(
        ctx.pool(),
        &operation_id,
        OperationStatus::Failed,
        "publish failed",
        json!({
            "surface": RUNTIME_CONTROL_SURFACE,
            "action": "resume",
            "module_name": module_name,
            "error": "publish failed",
        }),
    )
    .await?;

    let operation_id = operation_id.parse()?;
    let operation = ctx
        .pool()
        .state()
        .get_operation(&operation_id)
        .await?
        .expect("runtime control operation should be persisted");

    assert_eq!(operation.operation_type, "runtime.resume");
    assert_eq!(operation.result_status, OperationStatus::Failed);
    assert_eq!(operation.result_message.as_deref(), Some("publish failed"));
    assert_eq!(
        operation.preview_summary.as_ref().unwrap()["error"],
        "publish failed"
    );
    Ok(())
}
