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
        raw_config: &HashMap<String, serde_json::Value>,
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

        // Persist effective-config provenance on the run row so config-drift
        // and audit workflows can reconstruct what version + config a process
        // started with. Hash is BLAKE3 over the canonical-JSON serialization
        // of the config map plus the version string.
        let effective_config_value = serde_json::to_value(raw_config).ok();
        let effective_config_hash = effective_config_value.as_ref().map(|cfg| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(version.as_bytes());
            hasher.update(b"\0");
            // Canonical JSON: serde_json::to_string sorts map keys when the
            // map type does (HashMap doesn't, but the input is small enough
            // that ordering instability across versions is acceptable for
            // hashing — the hash exists to detect changes, not to be a
            // cross-host invariant.)
            if let Ok(serialized) = serde_json::to_string(cfg) {
                hasher.update(serialized.as_bytes());
            }
            hasher.finalize().to_hex().to_string()
        });

        let run = pool
            .state()
            .start_run(
                Some(manifest.id),
                service_name,
                instance_id,
                host,
                effective_config_hash.as_deref(),
                effective_config_value.as_ref(),
            )
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
