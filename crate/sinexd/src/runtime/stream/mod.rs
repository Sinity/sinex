mod checkpoint;
mod control_protocol;
mod handles;
mod kernel;
mod listener;
mod runtime_actor;
mod runner;
mod runtime_state;
mod stats;
#[cfg(test)]
pub(crate) mod test_support;
mod time_horizon;
mod wire_types;

pub use checkpoint::Checkpoint;
pub use handles::{
    EventEmitter, EventSender, EventStream, RuntimeHandles, RuntimeInitContext, RuntimeDrainController,
    ServiceInfo,
};
pub use kernel::{
    PullConsumerSpec, PullConsumerStartupSnapshot, ShadowConsumerSpec, consume_pull_loop,
    create_shadow_consumer, delete_consumer, ensure_pull_consumer, list_consumers, pull_batch,
    validate_pull_consumer_config,
};
pub use runtime_actor::RuntimeActor;
pub use runner::RuntimeRunner;
pub use runtime_state::RuntimeContext;
pub use stats::ProcessingStats;
pub use time_horizon::TimeHorizon;
pub use wire_types::{
    ContinuousStart, MaterialReplayContext, RuntimeCapabilities, SourceScanAck, SourceScanCommand,
    SourceScanProgress, ModuleKind, ReplayScopeFilters, ResolvedReplayMaterial, RunnerLifecycle,
    ScanArgs, ScanEstimate, ScanReport, SchemaBroadcastCache, SchemaBroadcastEntry,
};
