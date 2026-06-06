mod checkpoint;
mod control_protocol;
mod handles;
mod kernel;
mod listener;
mod runner;
mod runtime_module;
mod runtime_state;
mod stats;
#[cfg(test)]
pub(crate) mod test_support;
mod time_horizon;
mod wire_types;

pub use checkpoint::Checkpoint;
pub use handles::{
    EventEmitter, EventSender, EventStream, RuntimeDrainController, RuntimeHandles,
    RuntimeInitContext, ServiceInfo,
};
pub use kernel::{
    PullConsumerSpec, PullConsumerStartupSnapshot, ShadowConsumerSpec, consume_pull_loop,
    create_shadow_consumer, delete_consumer, ensure_pull_consumer, list_consumers, pull_batch,
    validate_pull_consumer_config,
};
pub use runner::RuntimeRunner;
pub use runtime_module::RuntimeModule;
pub use runtime_state::RuntimeContext;
pub use stats::ProcessingStats;
pub use time_horizon::TimeHorizon;
pub use wire_types::{
    ContinuousStart, MaterialReplayContext, ModuleKind, ReplayScopeFilters, ResolvedReplayMaterial,
    RunnerLifecycle, RuntimeCapabilities, ScanArgs, ScanEstimate, ScanReport, SchemaBroadcastCache,
    SchemaBroadcastEntry, SourceScanAck, SourceScanCommand, SourceScanProgress,
};
