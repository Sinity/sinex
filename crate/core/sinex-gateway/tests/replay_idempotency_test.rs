//! Tests replay idempotency guard: rejects duplicate operations for the same node.
//!
//! Verifies the guard added in `create_operation()` that prevents concurrent
//! replay operations targeting the same `node_id`.

use serde_json::json;
use sinex_primitives::rpc::methods;
use sinex_primitives::temporal::Timestamp;
use xtask::sandbox::{EnvGuard, prelude::*};

mod common;
use common::LiveGateway;

const RPC_TOKEN: &str = "idempotency-test-token:admin";

fn scope_for_node(node_id: &str) -> serde_json::Value {
    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(10);
    let scope_end = ts + time::Duration::seconds(10);
    json!({
        "scope": {
            "node_id": node_id,
            "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
        },
        "actor": "test:idempotency-tester"
    })
}

/// Creating two operations for the same node should fail on the second.
#[sinex_test(timeout = 60)]
async fn duplicate_plan_for_same_node_rejected(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());
    env_guard.set("SINEX_ALLOW_TEST_ACTORS", "1");

    let gw = LiveGateway::start(ctx.database_url(), RPC_TOKEN, &mut env_guard).await?;

    // First creation: succeeds
    let first = gw
        .rpc_envelope(
            methods::REPLAY_CREATE_OPERATION,
            scope_for_node("idem-node"),
        )
        .await?;
    assert!(
        first.get("result").is_some(),
        "First create should succeed: {first}"
    );

    // Second creation for same node: should fail
    let second = gw
        .rpc_envelope(
            methods::REPLAY_CREATE_OPERATION,
            scope_for_node("idem-node"),
        )
        .await?;
    // Check the JSON-RPC error code: -32803 maps to SinexError::InvalidState.
    // This is more robust than matching the human-readable message string.
    let error_code = second
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_i64());
    assert_eq!(
        error_code,
        Some(-32803),
        "Second create should be rejected with InvalidState (code -32803), got: {second}"
    );

    Ok(())
}

/// Concurrent creates for the same node should still admit only one active operation.
#[sinex_test(timeout = 60)]
async fn concurrent_duplicate_plan_for_same_node_rejected(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());
    env_guard.set("SINEX_ALLOW_TEST_ACTORS", "1");

    let gw = LiveGateway::start(ctx.database_url(), RPC_TOKEN, &mut env_guard).await?;

    let first = gw.rpc_envelope(
        methods::REPLAY_CREATE_OPERATION,
        scope_for_node("idem-race-node"),
    );
    let second = gw.rpc_envelope(
        methods::REPLAY_CREATE_OPERATION,
        scope_for_node("idem-race-node"),
    );
    let (first, second) = tokio::join!(first, second);
    let first = first?;
    let second = second?;

    let successes = [&first, &second]
        .into_iter()
        .filter(|response| response.get("result").is_some())
        .count();
    let errors: Vec<&str> = [&first, &second]
        .into_iter()
        .filter_map(|response| {
            response
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(|message| message.as_str())
        })
        .collect();

    assert_eq!(
        successes, 1,
        "exactly one concurrent create should succeed: first={first}, second={second}"
    );
    // Check the JSON-RPC error code: -32803 maps to SinexError::InvalidState.
    let error_codes: Vec<i64> = [&first, &second]
        .into_iter()
        .filter_map(|response| {
            response
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(|code| code.as_i64())
        })
        .collect();
    assert!(
        error_codes.contains(&-32803),
        "one concurrent create should be rejected with InvalidState (code -32803): first={first}, second={second}"
    );

    Ok(())
}

/// Different nodes can have concurrent operations.
#[sinex_test(timeout = 60)]
async fn different_nodes_allowed_concurrent(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());
    env_guard.set("SINEX_ALLOW_TEST_ACTORS", "1");

    let gw = LiveGateway::start(ctx.database_url(), RPC_TOKEN, &mut env_guard).await?;

    let first = gw
        .rpc_envelope(methods::REPLAY_CREATE_OPERATION, scope_for_node("node-a"))
        .await?;
    assert!(
        first.get("result").is_some(),
        "First node create should succeed: {first}"
    );

    let second = gw
        .rpc_envelope(methods::REPLAY_CREATE_OPERATION, scope_for_node("node-b"))
        .await?;
    assert!(
        second.get("result").is_some(),
        "Different node create should succeed: {second}"
    );

    Ok(())
}

/// After cancelling, a new operation for the same node should succeed.
#[sinex_test(timeout = 60)]
async fn cancelled_allows_new_operation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());
    env_guard.set("SINEX_ALLOW_TEST_ACTORS", "1");

    let gw = LiveGateway::start(ctx.database_url(), RPC_TOKEN, &mut env_guard).await?;

    // Create and cancel
    let create_resp = gw
        .rpc_envelope(
            methods::REPLAY_CREATE_OPERATION,
            scope_for_node("cancel-node"),
        )
        .await?;
    let op_id = create_resp["result"]["operation"]["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();

    let cancel_resp = gw
        .rpc_envelope(
            methods::REPLAY_CANCEL_OPERATION,
            json!({ "operation_id": op_id, "reason": "testing idempotency" }),
        )
        .await?;
    assert!(
        cancel_resp.get("result").is_some(),
        "Cancel should succeed: {cancel_resp}"
    );

    // New operation for same node after cancel: should succeed
    let new_resp = gw
        .rpc_envelope(
            methods::REPLAY_CREATE_OPERATION,
            scope_for_node("cancel-node"),
        )
        .await?;
    assert!(
        new_resp.get("result").is_some(),
        "New operation after cancel should succeed: {new_resp}"
    );

    Ok(())
}
