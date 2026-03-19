use color_eyre::eyre::bail;
use serde_json::json;
use sinex_gateway::ServiceContainer;
use sinex_primitives::temporal::Timestamp;
use xtask::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn replay_cancel_from_previewed_state(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let nats_url = ctx.nats_handle()?.client_url().to_string();
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", &nats_url);

    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let nats = services
        .nats_client()
        .expect("NATS required for replay test")
        .clone();
    let control_subject = services.environment().nats_subject("sinex.control.replay");

    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(1);
    let scope_end = ts + time::Duration::seconds(1);

    // Create plan
    let plan_req = json!({
        "command": "plan",
        "actor": "admin:test-user",
        "scope": {
            "node_id": "test-node-1",
            "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
            "filters": {}
        }
    });
    let plan_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&plan_req)?.into(),
        )
        .await?;
    let plan_resp: serde_json::Value = serde_json::from_slice(&plan_msg.payload)?;
    if plan_resp["status"].as_str() == Some("error") {
        bail!("plan failed: {plan_resp:?}");
    }
    let op_id = plan_resp["operation"]["operation_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("operation id missing"))?
        .to_string();

    // Preview the operation
    let preview_req = json!({
        "command": "preview",
        "operation_id": op_id,
    });
    let preview_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&preview_req)?.into(),
        )
        .await?;
    let preview_resp: serde_json::Value = serde_json::from_slice(&preview_msg.payload)?;
    if preview_resp["status"].as_str() == Some("error") {
        bail!("preview failed: {preview_resp:?}");
    }

    // Cancel from previewed state
    let cancel_req = json!({
        "command": "cancel",
        "operation_id": op_id,
        "canceller": "admin:test-user",
        "reason": "User requested cancellation"
    });
    let cancel_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&cancel_req)?.into(),
        )
        .await?;
    let cancel_resp: serde_json::Value = serde_json::from_slice(&cancel_msg.payload)?;
    if cancel_resp["status"].as_str() == Some("error") {
        bail!("cancel failed: {cancel_resp:?}");
    }

    // Verify state is Cancelled
    assert_eq!(
        cancel_resp["operation"]["state"].as_str(),
        Some("Cancelled"),
        "operation should be in Cancelled state"
    );

    // Verify cancellation reason is preserved (stored in error_details)
    let error_details = cancel_resp["operation"]["error_details"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("error_details missing from cancelled operation"))?;
    assert_eq!(error_details, "User requested cancellation");

    // Verify status shows Cancelled
    let status_req = json!({
        "command": "status",
        "operation_id": op_id,
    });
    let status_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&status_req)?.into(),
        )
        .await?;
    let status_resp: serde_json::Value = serde_json::from_slice(&status_msg.payload)?;
    assert_eq!(
        status_resp["operation"]["state"].as_str(),
        Some("Cancelled")
    );

    Ok(())
}

#[sinex_test]
async fn replay_cancel_from_approved_state(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let nats_url = ctx.nats_handle()?.client_url().to_string();
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", &nats_url);

    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let nats = services
        .nats_client()
        .expect("NATS required for replay test")
        .clone();
    let control_subject = services.environment().nats_subject("sinex.control.replay");

    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(1);
    let scope_end = ts + time::Duration::seconds(1);

    // Create plan
    let plan_req = json!({
        "command": "plan",
        "actor": "admin:test-user",
        "scope": {
            "node_id": "test-node-2",
            "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
            "filters": {}
        }
    });
    let plan_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&plan_req)?.into(),
        )
        .await?;
    let plan_resp: serde_json::Value = serde_json::from_slice(&plan_msg.payload)?;
    let op_id = plan_resp["operation"]["operation_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("operation id missing"))?
        .to_string();

    // Preview the operation
    let preview_req = json!({
        "command": "preview",
        "operation_id": op_id,
    });
    let preview_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&preview_req)?.into(),
        )
        .await?;
    let _preview_resp: serde_json::Value = serde_json::from_slice(&preview_msg.payload)?;

    // Approve the operation
    let approve_req = json!({
        "command": "approve",
        "operation_id": op_id,
        "approver": "admin:superuser",
    });
    let approve_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&approve_req)?.into(),
        )
        .await?;
    let approve_resp: serde_json::Value = serde_json::from_slice(&approve_msg.payload)?;
    if approve_resp["status"].as_str() == Some("error") {
        bail!("approve failed: {approve_resp:?}");
    }

    // Cancel from approved state
    let cancel_req = json!({
        "command": "cancel",
        "operation_id": op_id,
        "canceller": "admin:test-user"
    });
    let cancel_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&cancel_req)?.into(),
        )
        .await?;
    let cancel_resp: serde_json::Value = serde_json::from_slice(&cancel_msg.payload)?;
    if cancel_resp["status"].as_str() == Some("error") {
        bail!("cancel failed: {cancel_resp:?}");
    }

    // Verify state is Cancelled
    assert_eq!(
        cancel_resp["operation"]["state"].as_str(),
        Some("Cancelled"),
        "operation should be in Cancelled state after cancel"
    );

    // Try to execute the cancelled operation — should fail
    let execute_req = json!({
        "command": "execute",
        "operation_id": op_id,
        "executor": "service:worker-1",
    });
    let execute_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&execute_req)?.into(),
        )
        .await?;
    let execute_resp: serde_json::Value = serde_json::from_slice(&execute_msg.payload)?;

    // Expect error since operation is already cancelled
    assert_eq!(
        execute_resp["status"].as_str(),
        Some("error"),
        "executing a cancelled operation should fail"
    );

    Ok(())
}

#[sinex_test]
async fn replay_list_filters_by_state(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let nats_url = ctx.nats_handle()?.client_url().to_string();
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", &nats_url);

    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let nats = services
        .nats_client()
        .expect("NATS required for replay test")
        .clone();
    let control_subject = services.environment().nats_subject("sinex.control.replay");

    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(1);
    let scope_end = ts + time::Duration::seconds(1);

    // Create three operations with different node_ids
    let mut op_ids = Vec::new();
    for i in 1..=3 {
        let plan_req = json!({
            "command": "plan",
            "actor": "admin:test-user",
            "scope": {
                "node_id": format!("test-node-{}", i),
                "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
                "filters": {}
            }
        });
        let plan_msg = nats
            .request(
                control_subject.clone(),
                serde_json::to_vec(&plan_req)?.into(),
            )
            .await?;
        let plan_resp: serde_json::Value = serde_json::from_slice(&plan_msg.payload)?;
        let op_id = plan_resp["operation"]["operation_id"]
            .as_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("operation id missing"))?
            .to_string();
        op_ids.push(op_id);
    }

    // Cancel the first operation
    let cancel_req = json!({
        "command": "cancel",
        "operation_id": op_ids[0],
        "canceller": "admin:test-user"
    });
    let cancel_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&cancel_req)?.into(),
        )
        .await?;
    let cancel_resp: serde_json::Value = serde_json::from_slice(&cancel_msg.payload)?;
    if cancel_resp["status"].as_str() == Some("error") {
        bail!("cancel failed: {cancel_resp:?}");
    }

    // List operations with state filter "Cancelled"
    let list_cancelled_req = json!({
        "command": "list",
        "state": "Cancelled"
    });
    let list_cancelled_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&list_cancelled_req)?.into(),
        )
        .await?;
    let list_cancelled_resp: serde_json::Value =
        serde_json::from_slice(&list_cancelled_msg.payload)?;
    let cancelled_ops = list_cancelled_resp["operations"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("operations array missing"))?;
    assert_eq!(
        cancelled_ops.len(),
        1,
        "should have exactly 1 cancelled operation"
    );

    // List operations with state filter "Planning"
    let list_planning_req = json!({
        "command": "list",
        "state": "Planning"
    });
    let list_planning_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&list_planning_req)?.into(),
        )
        .await?;
    let list_planning_resp: serde_json::Value = serde_json::from_slice(&list_planning_msg.payload)?;
    let planning_ops = list_planning_resp["operations"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("operations array missing"))?;
    assert_eq!(
        planning_ops.len(),
        2,
        "should have exactly 2 planning operations"
    );

    // List operations without filter
    let list_all_req = json!({
        "command": "list"
    });
    let list_all_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&list_all_req)?.into(),
        )
        .await?;
    let list_all_resp: serde_json::Value = serde_json::from_slice(&list_all_msg.payload)?;
    let all_ops = list_all_resp["operations"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("operations array missing"))?;
    assert!(
        all_ops.len() >= 3,
        "should have at least 3 operations total (may have others from other tests)"
    );

    Ok(())
}

#[sinex_test]
async fn replay_create_with_empty_scope_fails_gracefully(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let nats_url = ctx.nats_handle()?.client_url().to_string();
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", &nats_url);

    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let nats = services
        .nats_client()
        .expect("NATS required for replay test")
        .clone();
    let control_subject = services.environment().nats_subject("sinex.control.replay");

    // Send plan with empty scope (no node_id, no time_window)
    let plan_req = json!({
        "command": "plan",
        "actor": "admin:test-user",
        "scope": {
            "filters": {}
        }
    });
    let plan_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&plan_req)?.into(),
        )
        .await?;
    let plan_resp: serde_json::Value = serde_json::from_slice(&plan_msg.payload)?;

    // Expect error response
    assert_eq!(
        plan_resp["status"].as_str(),
        Some("error"),
        "plan with empty scope should return error status"
    );
    assert!(
        plan_resp["message"].as_str().is_some(),
        "error response should have a message"
    );

    Ok(())
}
