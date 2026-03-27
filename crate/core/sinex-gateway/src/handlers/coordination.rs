//! Coordination RPC handlers.

use super::rpc_handlers::RpcParams;
use color_eyre::eyre::{Result, eyre};
use serde_json::Value;
use sinex_primitives::coordination::{CoordinationKvClient, InstanceMetadata};
use sinex_primitives::rpc::coordination::{
    GetLeaderResponse, InstanceHealthResponse, InstanceInfo, ListInstancesResponse,
};
use sinex_primitives::{
    domain::{HostName, InstanceId, NodeType},
    temporal,
    temporal::Timestamp,
};

fn metadata_to_instance_info(meta: &InstanceMetadata, is_leader: bool) -> InstanceInfo {
    InstanceInfo {
        instance_id: InstanceId::new(&meta.instance_id),
        node_type: NodeType::Service,
        hostname: HostName::new(&meta.hostname).ok(),
        last_heartbeat: Timestamp::from_unix_timestamp(meta.last_heartbeat),
        is_leader,
    }
}

pub async fn handle_coordination_list_instances(
    kv_client: &CoordinationKvClient,
    _params: Value,
) -> Result<Value> {
    let instances = kv_client.list_instances().await?;
    let leader = kv_client.get_leader().await?.unwrap_or_default();

    let instance_infos: Vec<InstanceInfo> = instances
        .iter()
        .map(|meta| metadata_to_instance_info(meta, meta.instance_id == leader))
        .collect();

    Ok(serde_json::to_value(ListInstancesResponse {
        instances: instance_infos,
    })?)
}

pub async fn handle_coordination_get_leader(
    kv_client: &CoordinationKvClient,
    _params: Value,
) -> Result<Value> {
    let leader = match kv_client.get_leader().await? {
        Some(instance_id) => {
            let metadata = kv_client
                .get_instance(&instance_id)
                .await?
                .ok_or_else(|| eyre!("Leader metadata missing for instance: {instance_id}"))?;
            Some(metadata_to_instance_info(&metadata, true))
        }
        None => None,
    };

    Ok(serde_json::to_value(GetLeaderResponse { leader })?)
}

pub async fn handle_coordination_instance_health(
    kv_client: &CoordinationKvClient,
    params: Value,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let instance_id = params.require_str("instance_id")?;

    let metadata = kv_client.get_instance(instance_id).await?;
    let leader = kv_client.get_leader().await?.unwrap_or_default();

    match metadata {
        Some(meta) => {
            let now = temporal::now().unix_timestamp();
            let heartbeat_age_secs = now - meta.last_heartbeat;
            let is_healthy =
                heartbeat_age_secs < kv_client.instance_stale_timeout().as_secs() as i64;
            let is_leader = meta.instance_id == leader;

            Ok(serde_json::to_value(InstanceHealthResponse {
                instance: metadata_to_instance_info(&meta, is_leader),
                healthy: is_healthy,
                last_error: None,
            })?)
        }
        None => Err(eyre!("Instance not found: {}", instance_id)),
    }
}
