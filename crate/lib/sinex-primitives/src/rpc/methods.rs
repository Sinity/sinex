//! RPC method name constants

// System
pub const SYSTEM_HEALTH: &str = "system.health";

// Analytics
pub const ANALYTICS_EVENT_COUNT_BY_SOURCE: &str = "analytics.event_count_by_source";
pub const ANALYTICS_ACTIVITY_HEATMAP: &str = "analytics.activity_heatmap";
pub const ANALYTICS_SOURCES_STATISTICS: &str = "analytics.sources_statistics";

// PKM
pub const PKM_CREATE_NOTE: &str = "pkm.create_note";
pub const PKM_CREATE_ENTITIES: &str = "pkm.create_entities_from_list";
pub const PKM_LINK_ENTITIES: &str = "pkm.link_entities";

// Search
pub const SEARCH_EVENTS: &str = "search.search_events";

// Content
pub const CONTENT_STORE_BLOB: &str = "content.store_blob";
pub const CONTENT_RETRIEVE_BLOB: &str = "content.retrieve_blob";

// Replay
pub const REPLAY_CREATE_OPERATION: &str = "replay.create_operation";
pub const REPLAY_PREVIEW_OPERATION: &str = "replay.preview_operation";
pub const REPLAY_APPROVE_OPERATION: &str = "replay.approve_operation";
pub const REPLAY_EXECUTE_OPERATION: &str = "replay.execute_operation";
pub const REPLAY_CANCEL_OPERATION: &str = "replay.cancel_operation";
pub const REPLAY_OPERATION_STATUS: &str = "replay.operation_status";
pub const REPLAY_LIST_OPERATIONS: &str = "replay.list_operations";

// Replay aliases
pub const REPLAY_CREATE: &str = REPLAY_CREATE_OPERATION;
pub const REPLAY_PREVIEW: &str = REPLAY_PREVIEW_OPERATION;
pub const REPLAY_APPROVE: &str = REPLAY_APPROVE_OPERATION;
pub const REPLAY_EXECUTE: &str = REPLAY_EXECUTE_OPERATION;
pub const REPLAY_CANCEL: &str = REPLAY_CANCEL_OPERATION;
pub const REPLAY_STATUS: &str = REPLAY_OPERATION_STATUS;
pub const REPLAY_LIST: &str = REPLAY_LIST_OPERATIONS;

// Coordination
pub const COORDINATION_LIST_INSTANCES: &str = "coordination.list_instances";
pub const COORDINATION_GET_LEADER: &str = "coordination.get_leader";
pub const COORDINATION_INSTANCE_HEALTH: &str = "coordination.instance_health";

// DLQ
pub const DLQ_LIST: &str = "dlq.list";
pub const DLQ_PEEK: &str = "dlq.peek";
pub const DLQ_REQUEUE: &str = "dlq.requeue";
pub const DLQ_PURGE: &str = "dlq.purge";

// Nodes
pub const NODES_LIST: &str = "nodes.list";
pub const NODES_DRAIN: &str = "nodes.drain";
pub const NODES_RESUME: &str = "nodes.resume";
pub const NODES_SET_HORIZON: &str = "nodes.set_horizon";

// Ops
pub const OPS_START: &str = "ops.start";
pub const OPS_LIST: &str = "ops.list";
pub const OPS_GET: &str = "ops.get";
pub const OPS_CANCEL: &str = "ops.cancel";

// Audit
pub const AUDIT_GET: &str = "audit.get";

// Shadow
pub const SHADOW_CREATE: &str = "shadow.create";
pub const SHADOW_LIST: &str = "shadow.list";
pub const SHADOW_DELETE: &str = "shadow.delete";

// Lifecycle
pub const LIFECYCLE_STATUS: &str = "lifecycle.status";
pub const LIFECYCLE_ARCHIVE: &str = "lifecycle.archive";
pub const LIFECYCLE_RESTORE: &str = "lifecycle.restore";

// GitOps
pub const GITOPS_LIST_SOURCES: &str = "gitops.list_sources";
pub const GITOPS_CREATE_SOURCE: &str = "gitops.create_source";
pub const GITOPS_DELETE_SOURCE: &str = "gitops.delete_source";
pub const GITOPS_TRIGGER_SYNC: &str = "gitops.trigger_sync";

// Tombstone (two-step)
pub const LIFECYCLE_TOMBSTONE_CREATE: &str = "lifecycle.tombstone.create";
pub const LIFECYCLE_TOMBSTONE_PREVIEW: &str = "lifecycle.tombstone.preview";
pub const LIFECYCLE_TOMBSTONE_APPROVE: &str = "lifecycle.tombstone.approve";
pub const LIFECYCLE_TOMBSTONE_CANCEL: &str = "lifecycle.tombstone.cancel";
pub const LIFECYCLE_TOMBSTONE_LIST: &str = "lifecycle.tombstone.list";
pub const LIFECYCLE_TOMBSTONE_STATUS: &str = "lifecycle.tombstone.status";
