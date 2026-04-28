//! Tests role-based access control (RBAC) on replay RPC endpoints.
//!
//! Verifies that the RPC registry's role assignments are enforced:
//! - `ReadOnly` tokens can list/status but not create/approve/execute
//! - Write tokens can create/preview but not approve/execute
//! - Admin tokens have full access
//!
//! Each test starts a `LiveGateway` with the specific role token under test.

use serde_json::json;
use sinex_primitives::rpc::methods;
use sinex_primitives::temporal::Timestamp;
use xtask::sandbox::{EnvGuard, prelude::*};

mod common;
use common::LiveGateway;

/// Convenience: bridge `RoleGateway`-shaped call sites to `LiveGateway`.
async fn start_role_gateway(
    database_url: &str,
    nats_url: &str,
    role_token: &str,
    env_guard: &mut EnvGuard,
) -> TestResult<LiveGateway> {
    LiveGateway::start_with(database_url, role_token, Some(nats_url), env_guard).await
}

/// Check if a JSON-RPC response is a permission-denied error.
fn is_permission_denied(response: &serde_json::Value) -> bool {
    response
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .is_some_and(|msg| msg.contains("requires") && msg.contains("role"))
}

/// Check if a JSON-RPC response has a result (success).
fn has_result(response: &serde_json::Value) -> bool {
    response.get("result").is_some()
}

fn test_scope_params() -> serde_json::Value {
    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(10);
    let scope_end = ts + time::Duration::seconds(10);
    json!({
        "scope": {
            "node_id": "auth-test-node",
            "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
        },
        "actor": "test:auth-tester"
    })
}

// ── ReadOnly role tests ─────────────────────────────────────────────

#[sinex_test(timeout = 60)]
async fn readonly_can_list_operations(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = start_role_gateway(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:readonly",
        &mut env_guard,
    )
    .await?;

    let resp = gw
        .rpc_envelope(methods::REPLAY_LIST_OPERATIONS, json!({}))
        .await?;
    assert!(
        has_result(&resp),
        "ReadOnly should be able to list operations: {resp}"
    );
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn readonly_cannot_create_operation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = start_role_gateway(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:readonly",
        &mut env_guard,
    )
    .await?;

    let resp = gw
        .rpc_envelope(methods::REPLAY_CREATE_OPERATION, test_scope_params())
        .await?;
    assert!(
        is_permission_denied(&resp),
        "ReadOnly should not be able to create operations: {resp}"
    );
    Ok(())
}

// ── Write role tests ────────────────────────────────────────────────

#[sinex_test(timeout = 60)]
async fn write_can_create_operation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = start_role_gateway(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:write",
        &mut env_guard,
    )
    .await?;

    let resp = gw
        .rpc_envelope(methods::REPLAY_CREATE_OPERATION, test_scope_params())
        .await?;
    assert!(
        has_result(&resp),
        "Write should be able to create operations: {resp}"
    );
    assert_eq!(
        resp["result"]["operation"]["actor"].as_str(),
        Some("operator:token:auth-tes"),
        "gateway must persist the authenticated replay actor, not caller-supplied params"
    );
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn write_cannot_approve_operation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = start_role_gateway(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:write",
        &mut env_guard,
    )
    .await?;

    // Approve requires a real operation_id — just pass a dummy UUID.
    // The role check happens before the operation lookup, so we get
    // permission denied before "operation not found".
    let resp = gw
        .rpc_envelope(
            methods::REPLAY_APPROVE_OPERATION,
            json!({
                "operation_id": "00000000-0000-0000-0000-000000000001",
                "approver": "admin:superuser"
            }),
        )
        .await?;
    assert!(
        is_permission_denied(&resp),
        "Write should not be able to approve operations: {resp}"
    );
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn write_cannot_submit_operation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = start_role_gateway(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:write",
        &mut env_guard,
    )
    .await?;

    let resp = gw
        .rpc_envelope(
            methods::REPLAY_SUBMIT_OPERATION,
            json!({
                "operation_id": "00000000-0000-0000-0000-000000000001"
            }),
        )
        .await?;
    assert!(
        is_permission_denied(&resp),
        "Write should not be able to submit operations: {resp}"
    );
    Ok(())
}

// ── Admin role tests ────────────────────────────────────────────────

#[sinex_test(timeout = 120)]
async fn admin_full_lifecycle(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = start_role_gateway(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:admin",
        &mut env_guard,
    )
    .await?;

    // Admin should be able to: list, create, preview, approve, cancel
    let list_resp = gw
        .rpc_envelope(methods::REPLAY_LIST_OPERATIONS, json!({}))
        .await?;
    assert!(
        has_result(&list_resp),
        "Admin list failed: {list_resp}"
    );

    let create_resp = gw
        .rpc_envelope(methods::REPLAY_CREATE_OPERATION, test_scope_params())
        .await?;
    assert!(
        has_result(&create_resp),
        "Admin create failed: {create_resp}"
    );
    assert_eq!(
        create_resp["result"]["operation"]["actor"].as_str(),
        Some("admin:token:auth-tes"),
        "create must derive replay actor from the authenticated admin token"
    );

    let op_id = create_resp["result"]["operation"]["operation_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("operation_id missing from create response"))?
        .to_string();

    let preview_resp = gw
        .rpc_envelope(
            methods::REPLAY_PREVIEW_OPERATION,
            json!({ "operation_id": op_id }),
        )
        .await?;
    assert!(
        has_result(&preview_resp),
        "Admin preview failed: {preview_resp}"
    );

    let approve_resp = gw
        .rpc_envelope(
            methods::REPLAY_APPROVE_OPERATION,
            json!({ "operation_id": op_id, "approver": "admin:superuser" }),
        )
        .await?;
    assert!(
        has_result(&approve_resp),
        "Admin approve failed: {approve_resp}"
    );
    assert_eq!(
        approve_resp["result"]["operation"]["approved_by"].as_str(),
        Some("admin:token:auth-tes"),
        "approve must use the authenticated admin actor rather than request params"
    );

    // Cancel instead of execute (no fake node to handle scan)
    let cancel_resp = gw
        .rpc_envelope(
            methods::REPLAY_CANCEL_OPERATION,
            json!({ "operation_id": op_id, "reason": "auth test cleanup" }),
        )
        .await?;
    assert!(
        has_result(&cancel_resp),
        "Admin cancel failed: {cancel_resp}"
    );

    Ok(())
}
