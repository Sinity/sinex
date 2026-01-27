use chrono::Utc;
use sinex_core::coordination::kv_client::{CoordinationKvClient, InstanceMetadata};
use xtask::sandbox::nats::ensure_coordination_buckets;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn kv_leadership_handoff(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;
    ensure_coordination_buckets(&js).await?;

    let service_name = "kv-handoff-service".to_string();
    let kv_client = CoordinationKvClient::new(js.clone(), service_name);

    let leader_id = "leader-1";
    let standby_id = "standby-1";

    let leader_meta = InstanceMetadata {
        instance_id: leader_id.to_string(),
        hostname: "leader-host".to_string(),
        version: "0.1.0".to_string(),
        started_at: Utc::now().timestamp(),
        last_heartbeat: Utc::now().timestamp(),
    };
    kv_client.register_instance(&leader_meta).await?;
    assert!(
        kv_client.acquire_leadership(leader_id).await?,
        "leader should acquire first"
    );

    kv_client.release_leadership(leader_id).await?;

    let standby_meta = InstanceMetadata {
        instance_id: standby_id.to_string(),
        hostname: "standby-host".to_string(),
        version: "0.1.0".to_string(),
        started_at: Utc::now().timestamp(),
        last_heartbeat: Utc::now().timestamp(),
    };
    kv_client.register_instance(&standby_meta).await?;
    assert!(
        kv_client.acquire_leadership(standby_id).await?,
        "standby should acquire after release"
    );

    Ok(())
}
