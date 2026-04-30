//! Database-backed node identity registration helpers for `NodeRunner<T>`.
//!
//! These methods are only compiled with the `db` feature and update the
//! `core.node_manifests` / `core.node_runs` tables to expose the running node
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
use sinex_primitives::Uuid;
#[cfg(feature = "db")]
use sinex_primitives::domain::{NodeName, NodeState};
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
        raw_config: &HashMap<String, serde_json::Value>,
    ) -> NodeResult<Option<Uuid>> {
        let node_name = NodeName::new(self.node.node_name());
        let node_type = match self.node.node_type() {
            NodeType::Ingestor => sinex_primitives::domain::NodeType::Ingestor,
            NodeType::Automaton => sinex_primitives::domain::NodeType::Automaton,
        };
        let manifest = pool
            .state()
            .register_node(&node_name, node_type, version, None)
            .await
            .map_err(|error| {
                SinexError::processing(format!(
                    "Failed to register node manifest for {}: {error}",
                    self.node.node_name()
                ))
            })?;
        let (config_hash, effective_config) = Self::effective_config(raw_config)?;
        let node_run = pool
            .state()
            .start_node_run(
                manifest.id,
                service_name,
                instance_id,
                host,
                config_hash.as_deref(),
                effective_config.as_ref(),
            )
            .await
            .map_err(|error| {
                SinexError::processing(format!(
                    "Failed to register node run for {}: {error}",
                    self.node.node_name()
                ))
            })?;
        Ok(Some(node_run.id))
    }

    pub(super) async fn update_registered_run_status(
        pool: &PgPool,
        service_info: &ServiceInfo,
        status: NodeState,
    ) {
        let Some(node_run_id) = service_info.node_run_id() else {
            return;
        };
        if let Err(error) = pool
            .state()
            .update_node_run_status(node_run_id, status)
            .await
        {
            warn!(
                node = %service_info.node_name(),
                service = %service_info.service_name(),
                node_run_id = %node_run_id,
                target_status = %status,
                error = %error,
                "Failed to persist node run terminal status"
            );
        }
    }

}
