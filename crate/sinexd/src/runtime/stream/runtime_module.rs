//! The unified `RuntimeModule` trait implemented by source drivers and automata.

use super::{
    Checkpoint, ModuleKind, ProcessingStats, RuntimeCapabilities, RuntimeHandles,
    RuntimeInitContext, ScanArgs, ScanEstimate, ScanReport, ServiceInfo, TimeHorizon,
};
use crate::runtime::automaton::traits::InputProvenanceFilter;
use crate::runtime::{RuntimeResult, SinexError};
use camino::Utf8PathBuf;
use futures::future::BoxFuture;
use serde::Deserialize;
use sinex_primitives::JsonValue;
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use std::collections::HashMap;
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

    /// Single concrete raw event type this module consumes, if any.
    ///
    /// When `Some(t)`, the raw-event consumer can filter the stream server-side
    /// to `events.raw.*.<t>` instead of subscribing to the whole `events.raw.>`
    /// firehose — so this module no longer receives and decodes events it would
    /// immediately discard. Returns `None` for wildcard consumers that need
    /// every event (the default), preserving existing behavior. Ref #2187.
    fn raw_event_type_filter(&self) -> Option<&'static str> {
        None
    }

    /// Concrete event types this module consumes, if it can express a finite
    /// set. Empty means wildcard.
    fn event_type_filters(&self) -> Vec<&'static str> {
        self.raw_event_type_filter().into_iter().collect()
    }

    /// Provenance class this module consumes from the confirmed-event stream.
    ///
    /// Most runtime modules do not consume confirmed events directly. Automata
    /// override this so the confirmed-event durable consumer can filter
    /// material-only or synthesized-only inputs at the NATS subject level.
    fn confirmed_event_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
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

/// Non-typed inputs the runner assembles for module initialization.
///
/// The typed `RuntimeModule::Config` is reconstructed from `raw_config` INSIDE
/// the erased boundary ([`ErasedRuntimeModule::initialize`]), which is what lets
/// [`ErasedRuntimeModule`] stay object-safe — no associated `Config` type ever
/// escapes into the trait-object surface.
pub struct ErasedInitContext {
    pub raw_config: HashMap<String, serde_json::Value>,
    pub service: ServiceInfo,
    pub handles: RuntimeHandles,
    pub work_dir_utf8: Utf8PathBuf,
}

/// Object-safe face of [`RuntimeModule`].
///
/// `RuntimeModule` is intentionally NOT object-safe: it carries an associated
/// `Config` type and RPITIT (`-> impl Future`) async methods so each module
/// keeps a typed, zero-overhead surface. But `RuntimeRunner` — the ~15-file
/// runtime kernel — only ever drives modules through this fixed method set, and
/// making the kernel generic over the concrete module (`RuntimeRunner<T>`)
/// forced the ENTIRE kernel to be monomorphized once per module type: 46% of
/// the crate's monomorphized LLVM IR (measured via `cargo llvm-lines`;
/// sinex-qabz), driving the sinexd lib rustc peak to ~9.8 GiB.
///
/// This trait erases the module behind `Box<dyn ErasedRuntimeModule>` so the
/// kernel compiles ONCE. The blanket impl below forwards every call to the
/// typed `RuntimeModule`, boxing its futures and reconstructing the typed config
/// from raw JSON at the initialization boundary. Dispatch changes from static to
/// dynamic; there is NO behavior change.
pub trait ErasedRuntimeModule: Send + Sync {
    fn module_name(&self) -> &str;
    fn module_kind(&self) -> ModuleKind;
    fn capabilities(&self) -> RuntimeCapabilities;
    fn raw_event_type_filter(&self) -> Option<&'static str>;
    fn event_type_filters(&self) -> Vec<&'static str>;
    fn confirmed_event_provenance_filter(&self) -> InputProvenanceFilter;
    fn config_schema(&self) -> Option<serde_json::Value>;

    fn initialize<'a>(&'a mut self, ctx: ErasedInitContext) -> BoxFuture<'a, RuntimeResult<()>>;
    fn scan<'a>(
        &'a mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> BoxFuture<'a, RuntimeResult<ScanReport>>;
    fn current_checkpoint(&self) -> BoxFuture<'_, RuntimeResult<Checkpoint>>;
    fn health_check(&self) -> BoxFuture<'_, RuntimeResult<bool>>;
    fn process_event_batch<'a>(
        &'a mut self,
        events: Vec<Event<JsonValue>>,
    ) -> BoxFuture<'a, RuntimeResult<ProcessingStats>>;
    fn shutdown(&mut self) -> BoxFuture<'_, RuntimeResult<()>>;
    fn periodic_flush(&mut self, now: Timestamp) -> BoxFuture<'_, RuntimeResult<u64>>;
    fn estimate_scan_scope<'a>(
        &'a self,
        from: &'a Checkpoint,
        until: &'a TimeHorizon,
        args: &'a ScanArgs,
    ) -> BoxFuture<'a, RuntimeResult<ScanEstimate>>;
}

impl<T: RuntimeModule> ErasedRuntimeModule for T {
    fn module_name(&self) -> &str {
        RuntimeModule::module_name(self)
    }
    fn module_kind(&self) -> ModuleKind {
        RuntimeModule::module_kind(self)
    }
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeModule::capabilities(self)
    }
    fn raw_event_type_filter(&self) -> Option<&'static str> {
        RuntimeModule::raw_event_type_filter(self)
    }
    fn event_type_filters(&self) -> Vec<&'static str> {
        RuntimeModule::event_type_filters(self)
    }
    fn confirmed_event_provenance_filter(&self) -> InputProvenanceFilter {
        RuntimeModule::confirmed_event_provenance_filter(self)
    }
    fn config_schema(&self) -> Option<serde_json::Value> {
        RuntimeModule::config_schema(self)
    }

    fn initialize<'a>(&'a mut self, ctx: ErasedInitContext) -> BoxFuture<'a, RuntimeResult<()>> {
        Box::pin(async move {
            // Config deserialization lives here (was in RuntimeRunner::initialize
            // and the replay-worker dispatch path) so the typed `T::Config` never
            // crosses the object-safe boundary.
            let typed_config: T::Config = if ctx.raw_config.is_empty() {
                T::Config::default()
            } else {
                let config_value = serde_json::to_value(&ctx.raw_config).map_err(|e| {
                    SinexError::configuration(format!("Failed to serialize runtime config: {e}"))
                })?;
                serde_json::from_value(config_value).map_err(|e| {
                    SinexError::configuration(format!("Failed to parse runtime config: {e}"))
                })?
            };
            let init_context = RuntimeInitContext::new(
                typed_config,
                ctx.raw_config,
                ctx.service,
                ctx.handles,
                ctx.work_dir_utf8,
            );
            RuntimeModule::initialize(self, init_context).await
        })
    }
    fn scan<'a>(
        &'a mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> BoxFuture<'a, RuntimeResult<ScanReport>> {
        Box::pin(RuntimeModule::scan(self, from, until, args))
    }
    fn current_checkpoint(&self) -> BoxFuture<'_, RuntimeResult<Checkpoint>> {
        Box::pin(RuntimeModule::current_checkpoint(self))
    }
    fn health_check(&self) -> BoxFuture<'_, RuntimeResult<bool>> {
        Box::pin(RuntimeModule::health_check(self))
    }
    fn process_event_batch<'a>(
        &'a mut self,
        events: Vec<Event<JsonValue>>,
    ) -> BoxFuture<'a, RuntimeResult<ProcessingStats>> {
        Box::pin(RuntimeModule::process_event_batch(self, events))
    }
    fn shutdown(&mut self) -> BoxFuture<'_, RuntimeResult<()>> {
        Box::pin(RuntimeModule::shutdown(self))
    }
    fn periodic_flush(&mut self, now: Timestamp) -> BoxFuture<'_, RuntimeResult<u64>> {
        Box::pin(RuntimeModule::periodic_flush(self, now))
    }
    fn estimate_scan_scope<'a>(
        &'a self,
        from: &'a Checkpoint,
        until: &'a TimeHorizon,
        args: &'a ScanArgs,
    ) -> BoxFuture<'a, RuntimeResult<ScanEstimate>> {
        Box::pin(RuntimeModule::estimate_scan_scope(self, from, until, args))
    }
}
