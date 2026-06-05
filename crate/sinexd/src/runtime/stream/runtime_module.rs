//! The unified `RuntimeModule` trait implemented by source drivers and automata.

use super::{
    Checkpoint, ModuleKind, ProcessingStats, RuntimeCapabilities, RuntimeInitContext, ScanArgs,
    ScanEstimate, ScanReport, TimeHorizon,
};
use crate::runtime::{RuntimeResult, SinexError};
use serde::Deserialize;
use sinex_primitives::JsonValue;
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use tracing::info;

/// Unified trait for runtime modules that participate in event streams.
pub trait RuntimeModule: Send + Sync {
    type Config: for<'de> Deserialize<'de> + Default + Send + Sync;

    fn initialize(
        &mut self,
        init: RuntimeInitContext<Self::Config>,
    ) -> impl std::future::Future<Output = RuntimeResult<()>> + Send;

    fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> impl std::future::Future<Output = RuntimeResult<ScanReport>> + Send;

    fn module_name(&self) -> &str;
    fn module_kind(&self) -> ModuleKind;

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::default()
    }

    fn current_checkpoint(
        &self,
    ) -> impl std::future::Future<Output = RuntimeResult<Checkpoint>> + Send;

    fn health_check(&self) -> impl std::future::Future<Output = RuntimeResult<bool>> + Send {
        async { Ok(true) }
    }

    fn process_event_batch(
        &mut self,
        _events: Vec<Event<JsonValue>>,
    ) -> impl std::future::Future<Output = RuntimeResult<ProcessingStats>> + Send {
        async {
            Err(SinexError::processing(
                "This runtime actor does not support event batch processing. Only automata should implement this method.".to_string()
            ))
        }
    }

    fn shutdown(&mut self) -> impl std::future::Future<Output = RuntimeResult<()>> + Send {
        async {
            info!(module = %self.module_name(), "Runtime actor shutting down");
            Ok(())
        }
    }

    /// Clock-driven trailing-bucket flush for `Windowed` automata.
    ///
    /// Called by the runtime on a periodic timer to allow `Windowed` automata to
    /// emit trailing buckets (the current, latest hour/day) without waiting for
    /// the next bucket's first event. Returns the count of output events emitted.
    ///
    /// Default: no-op (returns 0). Only `AutomatonRuntime<WindowedWrapper<N>>`
    /// provides a meaningful implementation via the inner `flush_due` predicate.
    fn periodic_flush(
        &mut self,
        _now: Timestamp,
    ) -> impl std::future::Future<Output = RuntimeResult<u64>> + Send {
        async { Ok(0) }
    }

    fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> impl std::future::Future<Output = RuntimeResult<ScanEstimate>> + Send {
        async { Ok(ScanEstimate::default()) }
    }

    fn config_schema(&self) -> Option<serde_json::Value> {
        None
    }
}
