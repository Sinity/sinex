//! The unified `Node` trait that ingestors and automata implement.

use super::{
    Checkpoint, NodeCapabilities, NodeType, NodeInitContext, ProcessingStats, ScanArgs,
    ScanEstimate, ScanReport, TimeHorizon,
};
use crate::{NodeResult, SinexError};
use serde::Deserialize;
use sinex_primitives::events::Event;
use sinex_primitives::JsonValue;
use tracing::info;

/// Unified trait for all stream nodes (ingestors and automata).
pub trait Node: Send + Sync {
    type Config: for<'de> Deserialize<'de> + Default + Send + Sync;

    fn initialize(
        &mut self,
        init: NodeInitContext<Self::Config>,
    ) -> impl std::future::Future<Output = NodeResult<()>> + Send;

    fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> impl std::future::Future<Output = NodeResult<ScanReport>> + Send;

    fn node_name(&self) -> &str;
    fn node_type(&self) -> NodeType;

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    fn current_checkpoint(
        &self,
    ) -> impl std::future::Future<Output = NodeResult<Checkpoint>> + Send;

    fn health_check(&self) -> impl std::future::Future<Output = NodeResult<bool>> + Send {
        async { Ok(true) }
    }

    fn process_event_batch(
        &mut self,
        _events: Vec<Event<JsonValue>>,
    ) -> impl std::future::Future<Output = NodeResult<ProcessingStats>> + Send {
        async {
            Err(SinexError::processing(
                "This node does not support event batch processing. Only automata should implement this method.".to_string()
            ))
        }
    }

    fn shutdown(&mut self) -> impl std::future::Future<Output = NodeResult<()>> + Send {
        async {
            info!(node = %self.node_name(), "Node shutting down");
            Ok(())
        }
    }

    fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> impl std::future::Future<Output = NodeResult<ScanEstimate>> + Send {
        async { Ok(ScanEstimate::default()) }
    }

    fn config_schema(&self) -> Option<serde_json::Value> {
        None
    }
}
