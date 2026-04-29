#![doc = include_str!("../../../docs/stream_node.md")]

mod checkpoint;
mod control_protocol;
mod handles;
mod kernel;
mod listener;
mod node_trait;
mod runner;
mod runtime_state;
mod stats;
mod time_horizon;
mod wire_types;

pub use checkpoint::Checkpoint;
pub use handles::{
    EventEmitter, EventSender, EventStream, NodeHandles, NodeInitContext, RuntimeDrainController,
    ServiceInfo,
};
pub use kernel::{
    PullConsumerSpec, ShadowConsumerSpec, consume_pull_loop, create_shadow_consumer,
    delete_consumer, ensure_pull_consumer, list_consumers, pull_batch,
    validate_pull_consumer_config,
};
pub use node_trait::Node;
pub use runner::NodeRunner;
pub use runtime_state::NodeRuntimeState;
pub use stats::ProcessingStats;
pub use time_horizon::TimeHorizon;
pub use wire_types::{
    ContinuousStart, MaterialReplayContext, NodeCapabilities, NodeScanAck, NodeScanCommand,
    NodeScanProgress, NodeType, ReplayScopeFilters, ResolvedReplayMaterial, RunnerLifecycle,
    ScanArgs, ScanEstimate, ScanReport, SchemaBroadcastCache, SchemaBroadcastEntry,
};
