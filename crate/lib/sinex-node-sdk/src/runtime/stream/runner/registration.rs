//! Database-backed node identity registration helpers for `NodeRunner<T>`.
//!
//! These methods are only compiled with the `db` feature and update the
//! `core.manifests` / `core.runs` tables to expose the running node
//! identity to operators and downstream automation.

#[cfg(feature = "db")]
use super::{Node, NodeRunner, NodeType, ServiceInfo};
#[cfg(feature = "db")]
use crate::{NodeResult, SinexError};
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
#[cfg(feature = "db")]
use sinex_db::repositories::DbPoolExt;
#[cfg(feature = "db")]
use sinex_primitives::domain::{NodeName, NodeState};
#[cfg(feature = "db")]
use sinex_primitives::{Id, Uuid};
#[cfg(feature = "db")]
use std::collections::HashMap;
#[cfg(feature = "db")]
use tracing::warn;

#[cfg(feature = "db")]
impl<T: Node + 'static> NodeRunner<T> {
    pub(super) async fn register_runtime_identity(
        &self,
        pool: &PgPool,
        service_name: &str,
        instance_id: &str,
        host: &str,
        version: &str,
        _raw_config: &HashMap<String, serde_json::Value>,
    ) -> NodeResult<Option<Uuid>> {
        let node_name = NodeName::new(self.node.node_name());
        let node_type = self.node.node_type();
        let manifest = pool
            .state()
            .register_node(&node_name, node_type, version, None)
            .await
            .map_err(|error| {
                SinexError::processing(format!(
                    "Failed to register manifest for {service_name}: {error}"
                ))
            })?;
        let run = pool
            .state()
            .start_run(Some(manifest.id), service_name, instance_id, host)
            .await
            .map_err(|error| {
                SinexError::processing(format!(
                    "Failed to start run for {service_name}/{instance_id}: {error}"
                ))
            })?;
        Ok(Some(run.id.to_uuid()))
    }

    pub(super) async fn update_registered_run_status(
        pool: &PgPool,
        service_info: &ServiceInfo,
        status: NodeState,
    ) {
        let Some(source_run_id) = service_info.source_run_id() else {
            return;
        };
        let source_run_id = Id::<sinex_db::repositories::state::NodeRun>::from_uuid(source_run_id);
        if let Err(error) = pool
            .state()
            .update_node_run_status(source_run_id, status)
            .await
        {
            warn!(
                node = %service_info.node_name(),
                service = %service_info.service_name(),
                source_run_id = %source_run_id,
                target_status = %status,
                error = %error,
                "Failed to persist node run terminal status"
            );
        }
    }
}
