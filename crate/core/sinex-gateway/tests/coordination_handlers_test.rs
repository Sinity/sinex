use async_nats::jetstream;
use serde_json::json;
use sinex_gateway::handlers::{
    handle_coordination_get_leader, handle_coordination_instance_health,
    handle_coordination_list_instances,
};
use sinex_primitives::coordination::{CoordinationKvClient, InstanceMetadata};
use sinex_primitives::temporal;
use xtask::sandbox::prelude::*;

fn build_coordination_client(ctx: &TestContext, service_name: &str) -> TestResult<CoordinationKvClient> {
    let js = jetstream::new(ctx.nats_client());
    Ok(CoordinationKvClient::new(js, service_name.to_string()))
}

#[sinex_test]
async fn coordination_instance_health_uses_configured_stale_timeout(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env = EnvGuard::new();
    env.set("SINEX_COORDINATION_HEARTBEAT", "5");
    env.set("SINEX_COORDINATION_TIMEOUT", "120");

    let kv_client = build_coordination_client(&ctx, "gateway-health-threshold")?;
    let now = temporal::now().unix_timestamp();
    let metadata = InstanceMetadata {
        instance_id: "instance-a".to_string(),
        hostname: "host-a".to_string(),
        version: "1.0.0-test".to_string(),
        started_at: now - 120,
        last_heartbeat: now - 90,
    };
    kv_client.register_instance(&metadata).await?;

    let response = handle_coordination_instance_health(
        &kv_client,
        json!({ "instance_id": metadata.instance_id }),
    )
    .await?;
    assert_eq!(response["healthy"].as_bool(), Some(true));
    assert_eq!(response["instance"]["instance_id"].as_str(), Some("instance-a"));
    assert_eq!(response["last_error"], serde_json::Value::Null);

    Ok(())
}

#[sinex_test]
async fn coordination_instance_health_rejects_missing_instance(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let kv_client = build_coordination_client(&ctx, "gateway-health-missing")?;

    let error = handle_coordination_instance_health(&kv_client, json!({ "instance_id": "missing" }))
        .await
        .expect_err("missing coordination instances must fail loudly");
    assert!(error.to_string().contains("Instance not found: missing"));

    Ok(())
}

#[sinex_test]
async fn coordination_list_instances_marks_current_leader(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let kv_client = build_coordination_client(&ctx, "gateway-coordination-list")?;
    let now = temporal::now().unix_timestamp();

    let leader = InstanceMetadata {
        instance_id: "leader-a".to_string(),
        hostname: "host-a".to_string(),
        version: "1.0.0-test".to_string(),
        started_at: now - 30,
        last_heartbeat: now,
    };
    let follower = InstanceMetadata {
        instance_id: "follower-b".to_string(),
        hostname: "host-b".to_string(),
        version: "1.0.0-test".to_string(),
        started_at: now - 30,
        last_heartbeat: now,
    };

    kv_client.register_instance(&leader).await?;
    kv_client.register_instance(&follower).await?;
    assert!(kv_client.acquire_leadership(&leader.instance_id).await?);

    let listed = handle_coordination_list_instances(&kv_client, json!({})).await?;
    let instances = listed["instances"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("instances should serialize as an array"))?;
    assert_eq!(instances.len(), 2);
    assert!(
        instances.iter().any(|instance| {
            instance["instance_id"].as_str() == Some("leader-a")
                && instance["is_leader"].as_bool() == Some(true)
        }),
        "leader instance should be marked in list output"
    );
    assert!(
        instances.iter().any(|instance| {
            instance["instance_id"].as_str() == Some("follower-b")
                && instance["is_leader"].as_bool() == Some(false)
        }),
        "non-leader instance should stay non-leader in list output"
    );

    let leader_result = handle_coordination_get_leader(&kv_client, json!({})).await?;
    assert_eq!(leader_result["leader"].as_str(), Some("leader-a"));

    Ok(())
}
