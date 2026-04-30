//! Constructors and simple accessors for `NodeRunner<T>`.
//!
//! Holds the runner constructors (`new`, `new_with_factory`) and the
//! cheap accessors (`lifecycle`, `node_type`, `runtime_state`) plus the two
//! private helpers (`config_identity_value`,
//! `drain_completion_checkpoint_description`) that only touch `&self` fields.

use super::{
    Checkpoint, Node, NodeFactory, NodeRunner, NodeRuntimeState, NodeType, ProcessingModel,
    RunnerLifecycle,
};
use std::collections::HashMap;

impl<T: Node + 'static> NodeRunner<T> {
    /// Create a new node runner
    pub fn new(node: T) -> Self {
        Self::new_with_optional_factory(node, None)
    }

    /// Create a node runner with a factory for fresh worker instances.
    pub fn new_with_factory(node: T, node_factory: NodeFactory<T>) -> Self {
        Self::new_with_optional_factory(node, Some(node_factory))
    }

    pub(super) fn new_with_optional_factory(node: T, node_factory: Option<NodeFactory<T>>) -> Self {
        Self {
            node,
            node_factory,
            lifecycle: RunnerLifecycle::Created,
            handles: None,
            service_info: None,
            raw_config: None,
            work_dir_utf8: None,
            event_batcher_handle: None,
            event_batcher_shutdown: None,
            schema_listener_shutdown: None,
            schema_listener_handle: None,
            checkpoint_cleanup_shutdown: None,
            checkpoint_cleanup_handle: None,
            consumer_handle: None,
            command_listener_shutdown: None,
            command_listener_handle: None,
            processing_model: ProcessingModel::StatelessWorker,
            leader_state: None,
        }
    }

    pub(super) fn config_identity_value(
        raw_config: &HashMap<String, serde_json::Value>,
        key: &str,
    ) -> Option<String> {
        raw_config
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    }

    pub(super) async fn drain_completion_checkpoint_description(&self) -> Option<String> {
        let node_checkpoint = self.node.current_checkpoint().await.ok();
        if let Some(checkpoint) = node_checkpoint.clone()
            && !matches!(checkpoint, Checkpoint::None)
        {
            return Some(checkpoint.description());
        }

        if let Some(handles) = &self.handles
            && let Ok(checkpoint_state) = handles.checkpoint_manager().load_checkpoint().await
            && !matches!(checkpoint_state.checkpoint, Checkpoint::None)
        {
            return Some(checkpoint_state.checkpoint.description());
        }

        node_checkpoint.map(|checkpoint| checkpoint.description())
    }

    /// Returns the current lifecycle state of this runner.
    pub fn lifecycle(&self) -> RunnerLifecycle {
        self.lifecycle
    }

    /// Return the underlying node type.
    pub fn node_type(&self) -> NodeType {
        self.node.node_type()
    }

    /// Reconstruct the current runtime state if the runner has been initialized
    pub fn runtime_state(&self) -> Option<NodeRuntimeState> {
        let handles = self.handles.clone()?;
        let service_info = self.service_info.clone()?;
        let raw_config = self.raw_config.clone()?;
        let work_dir_utf8 = self.work_dir_utf8.clone()?;

        Some(NodeRuntimeState::new(
            service_info,
            handles,
            raw_config,
            work_dir_utf8,
        ))
    }

}
