//! Coordination RPC handlers.

use sinex_primitives::coordination::{CoordinationKvClient, InstanceMetadata};
use sinex_primitives::rpc::coordination::{
    GetLeaderRequest, GetLeaderResponse, InstanceHealthRequest, InstanceHealthResponse,
    InstanceInfo, ListInstancesRequest, ListInstancesResponse,
};
use sinex_primitives::{
    Result, SinexError,
    domain::{HostName, InstanceId, NodeType},
    temporal,
    temporal::Timestamp,
};

fn metadata_to_instance_info(meta: &InstanceMetadata, is_leader: bool) -> Result<InstanceInfo> {
    let hostname = HostName::new(&meta.hostname).map_err(|error| {
        error
            .with_context("instance_id", &meta.instance_id)
            .with_context("hostname", &meta.hostname)
    })?;

    Ok(InstanceInfo {
        instance_id: InstanceId::new(&meta.instance_id),
        node_type: NodeType::Service,
        hostname: Some(hostname),
        last_heartbeat: Timestamp::from_unix_timestamp(meta.last_heartbeat),
        is_leader,
    })
}

pub async fn handle_coordination_list_instances(
    kv_client: &CoordinationKvClient,
    _req: ListInstancesRequest,
) -> Result<ListInstancesResponse> {
    let instances = kv_client.list_instances().await?;
    let leader = kv_client.get_leader().await?;

    let instance_infos: Vec<InstanceInfo> = instances
        .iter()
        .map(|meta| {
            metadata_to_instance_info(meta, leader.as_deref() == Some(meta.instance_id.as_str()))
        })
        .collect::<Result<_>>()?;

    Ok(ListInstancesResponse {
        instances: instance_infos,
    })
}

pub async fn handle_coordination_get_leader(
    kv_client: &CoordinationKvClient,
    _req: GetLeaderRequest,
) -> Result<GetLeaderResponse> {
    let leader = match kv_client.get_leader().await? {
        Some(instance_id) => {
            let metadata = kv_client.get_instance(&instance_id).await?.ok_or_else(|| {
                SinexError::not_found("Leader metadata missing for instance")
                    .with_context("instance_id", &instance_id)
            })?;
            Some(metadata_to_instance_info(&metadata, true)?)
        }
        None => None,
    };

    Ok(GetLeaderResponse { leader })
}

pub async fn handle_coordination_instance_health(
    kv_client: &CoordinationKvClient,
    req: InstanceHealthRequest,
) -> Result<InstanceHealthResponse> {
    let metadata = kv_client.get_instance(req.instance_id.as_str()).await?;
    let leader = kv_client.get_leader().await?;

    match metadata {
        Some(meta) => {
            let now = temporal::now().unix_timestamp();
            let heartbeat_age_secs = now - meta.last_heartbeat;
            let is_healthy =
                heartbeat_age_secs < kv_client.instance_stale_timeout().as_secs() as i64;
            let is_leader = leader.as_deref() == Some(meta.instance_id.as_str());

            Ok(InstanceHealthResponse {
                instance: metadata_to_instance_info(&meta, is_leader)?,
                healthy: is_healthy,
                last_error: None,
            })
        }
        None => Err(SinexError::not_found("Instance not found")
            .with_context("instance_id", req.instance_id.as_str())),
    }
}
