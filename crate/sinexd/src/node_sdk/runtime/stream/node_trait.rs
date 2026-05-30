//! The unified `Node` trait that ingestors and automata implement.

use super::{
    Checkpoint, NodeCapabilities, NodeInitContext, NodeType, ProcessingStats, ScanArgs,
    ScanEstimate, ScanReport, TimeHorizon,
};
use crate::node_sdk::{NodeResult, SinexError};
use serde::Deserialize;
use sinex_primitives::JsonValue;
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
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

    /// Clock-driven trailing-bucket flush for `Windowed` derived nodes.
    ///
    /// Called by the runtime on a periodic timer to allow `Windowed` nodes to
    /// emit trailing buckets (the current, latest hour/day) without waiting for
    /// the next bucket's first event. Returns the count of output events emitted.
    ///
    /// Default: no-op (returns 0). Only `AutomatonRuntime<WindowedWrapper<N>>`
    /// provides a meaningful implementation via the inner `flush_due` predicate.
    fn periodic_flush(
        &mut self,
        _now: Timestamp,
    ) -> impl std::future::Future<Output = NodeResult<u64>> + Send {
        async { Ok(0) }
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
