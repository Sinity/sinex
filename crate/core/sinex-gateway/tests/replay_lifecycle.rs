use color_eyre::Result;
use sinex_gateway::ServiceContainer;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_replay_lifecycle_full_flow(ctx: TestContext) -> Result<()> {
    // Replay control requires NATS. Set up ephemeral NATS before creating ServiceContainer.
    let ctx = ctx.with_nats().shared().await?;

    let nats_url = ctx.nats_handle()?.client_url().to_string();
    let mut env = EnvGuard::new();
    env.set("SINEX_NATS_URL", &nats_url);

    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let nats = services
        .nats_client()
        .expect("NATS required for replay test")
        .clone();

    // Initial health check
    let _health = services.health_report().await;

    let subject = services.environment().nats_subject("sinex.control.replay");

    // 1. Plan a replay operation
    let plan_req = serde_json::json!({
        "command": "plan",
        "actor": "admin:test-user",
        "scope": {
            "processor_id": "test-processor"
        }
    });

    let resp_msg = nats
        .request(subject.clone(), serde_json::to_vec(&plan_req)?.into())
        .await
        .map_err(|e| color_eyre::eyre::eyre!("NATS Plan request failed: {}", e))?;

    let resp: serde_json::Value = serde_json::from_slice(&resp_msg.payload)?;
    if resp["status"].as_str() == Some("error") {
        bail!("Plan failed: {:?}", resp);
    }

    let op_id = resp["operation"]["id"]
        .as_str()
        .expect("Operation ID should be present");
    println!("Planned replay operation: {}", op_id);

    // 2. Approve
    let approve_req = serde_json::json!({
        "command": "approve",
        "operation_id": op_id,
        "approver": "admin:superuser"
    });
    let resp_msg = nats
        .request(subject.clone(), serde_json::to_vec(&approve_req)?.into())
        .await
        .map_err(|e| color_eyre::eyre::eyre!("NATS Approve request failed: {}", e))?;

    let resp: serde_json::Value = serde_json::from_slice(&resp_msg.payload)?;
    if resp["status"].as_str() == Some("error") {
        bail!("Approve failed: {:?}", resp);
    }

    // 3. Preview (required before execute)
    let preview_req = serde_json::json!({
        "command": "preview",
        "operation_id": op_id
    });
    let resp_msg = nats
        .request(subject.clone(), serde_json::to_vec(&preview_req)?.into())
        .await
        .map_err(|e| color_eyre::eyre::eyre!("NATS Preview request failed: {}", e))?;

    let resp: serde_json::Value = serde_json::from_slice(&resp_msg.payload)?;
    if resp["status"].as_str() == Some("error") {
        bail!("Preview failed: {:?}", resp);
    }

    // 4. Execute
    let execute_req = serde_json::json!({
        "command": "execute",
        "operation_id": op_id,
        "executor": "node:worker-1"
    });
    let resp_msg = nats
        .request(subject.clone(), serde_json::to_vec(&execute_req)?.into())
        .await
        .map_err(|e| color_eyre::eyre::eyre!("NATS Execute request failed: {}", e))?;

    let resp: serde_json::Value = serde_json::from_slice(&resp_msg.payload)?;
    if resp["status"].as_str() == Some("error") {
        bail!("Execute failed: {:?}", resp);
    }

    // 5. Verify final state. Since there are no events for "test-processor", replay
    //    finishes immediately and should reach Completed.
    let status_req = serde_json::json!({
        "command": "status",
        "operation_id": op_id
    });
    let resp_msg = nats
        .request(subject.clone(), serde_json::to_vec(&status_req)?.into())
        .await?;
    let resp: serde_json::Value = serde_json::from_slice(&resp_msg.payload)?;

    let state = resp["operation"]["state"].as_str().unwrap();
    println!("Final operation state: {}", state);

    assert_eq!(
        state, "Completed",
        "Empty replay should complete immediately"
    );

    Ok(())
}
