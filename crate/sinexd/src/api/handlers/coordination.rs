//! Read-only coordination views backed by the live runtime presence registry.

use super::runtime_presence::handle_runtime_list_active;
use sinex_primitives::domain::{HostName, InstanceId};
use sinex_primitives::rpc::coordination::{
    GetLeaderRequest, GetLeaderResponse, InstanceHealthRequest, InstanceHealthResponse,
    InstanceInfo, ListInstancesRequest, ListInstancesResponse,
};
use sinex_primitives::rpc::runtime::{RuntimeInfo, RuntimeListActiveRequest};
use sinex_primitives::{
    Result, RuntimeLivenessPolicy, RuntimeLivenessSignals, SinexError, evaluate_runtime_liveness,
    Timestamp,
};
use sqlx::PgPool;

async fn active_runtime_modules(pool: &PgPool) -> Result<Vec<RuntimeInfo>> {
    Ok(handle_runtime_list_active(pool, RuntimeListActiveRequest::default())
        .await?
        .modules)
}

fn instance_info(module: RuntimeInfo, is_leader: bool) -> InstanceInfo {
    let instance_id = module
        .instance_id
        .as_deref()
        .unwrap_or(module.module_name.as_ref());
    InstanceInfo {
        instance_id: InstanceId::new(instance_id),
        module_kind: module.module_kind,
        hostname: module.host.as_deref().and_then(|host| HostName::new(host).ok()),
        last_heartbeat: module.last_heartbeat_at,
        is_leader,
    }
}

pub async fn handle_coordination_list_instances(
    pool: &PgPool,
    request: ListInstancesRequest,
) -> Result<ListInstancesResponse> {
    let mut modules = active_runtime_modules(pool).await?;
    if let Some(module_kind) = request.module_kind {
        modules.retain(|module| module.module_kind == module_kind);
    }
    modules.sort_by(|left, right| {
        left.instance_id
            .as_deref()
            .unwrap_or(left.module_name.as_ref())
            .cmp(
                right
                    .instance_id
                    .as_deref()
                    .unwrap_or(right.module_name.as_ref()),
            )
    });

    let instances = modules
        .into_iter()
        .enumerate()
        .map(|(index, module)| instance_info(module, index == 0))
        .collect();
    Ok(ListInstancesResponse { instances })
}

pub async fn handle_coordination_get_leader(
    pool: &PgPool,
    request: GetLeaderRequest,
) -> Result<GetLeaderResponse> {
    let response = handle_coordination_list_instances(
        pool,
        ListInstancesRequest {
            module_kind: Some(request.module_kind),
        },
    )
    .await?;
    Ok(GetLeaderResponse {
        leader: response
            .instances
            .into_iter()
            .find(|instance| instance.is_leader),
    })
}

pub async fn handle_coordination_instance_health(
    pool: &PgPool,
    request: InstanceHealthRequest,
) -> Result<InstanceHealthResponse> {
    let modules = active_runtime_modules(pool).await?;
    let module = modules.into_iter().find(|module| {
        module
            .instance_id
            .as_deref()
            .unwrap_or(module.module_name.as_ref())
            == request.instance_id.as_ref()
    });
    let Some(module) = module else {
        return Err(SinexError::not_found("coordination instance not found")
            .with_context("instance_id", request.instance_id.to_string()));
    };
    let healthy = evaluate_runtime_liveness(
        RuntimeLivenessSignals {
            run_status: Some(module.status.as_str()),
            health_status: None,
            last_heartbeat_at: module.last_heartbeat_at,
            last_output_at: None,
        },
        RuntimeLivenessPolicy::default(),
        Timestamp::now(),
    )
    .status
    .is_live();
    Ok(InstanceHealthResponse {
        instance: instance_info(module, true),
        healthy,
        last_error: None,
    })
}
