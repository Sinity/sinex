//! RPC method name constants

// System
pub const SYSTEM_PING: &str = "system.ping";
pub const SYSTEM_VERSION: &str = "system.version";
pub const SYSTEM_HEALTH: &str = "system.health";

// Privacy
pub const PRIVACY_PRIVATE_MODE_STATUS: &str = "privacy.private_mode.status";
pub const PRIVACY_PRIVATE_MODE_ENABLE: &str = "privacy.private_mode.enable";
pub const PRIVACY_PRIVATE_MODE_DISABLE: &str = "privacy.private_mode.disable";
pub const PRIVACY_POLICY_LIST: &str = "privacy.policy.list";
pub const PRIVACY_POLICY_BACKEND_ADD: &str = "privacy.policy.backend.add";
pub const PRIVACY_POLICY_DICTIONARY_ADD: &str = "privacy.policy.dictionary.add";
pub const PRIVACY_POLICY_RULE_ADD: &str = "privacy.policy.rule.add";
pub const PRIVACY_POLICY_SEED_BUILTIN: &str = "privacy.policy.seed.builtin";
pub const PRIVACY_POLICY_SCOPE_BIND: &str = "privacy.policy.scope.bind";

// Events (composable query engine)
pub const EVENTS_QUERY: &str = "events.query";
pub const EVENTS_CARDS: &str = "events.cards";
pub const EVENTS_LINEAGE: &str = "events.lineage";

// Curation
pub const CURATION_PROPOSALS_LIST: &str = "curation.proposals.list";
pub const CURATION_JUDGMENTS_RECORD: &str = "curation.judgments.record";
pub const CURATION_DUPLICATE_CANDIDATES_LIST: &str = "curation.duplicate_candidates.list";
pub const CURATION_DUPLICATE_JUDGMENTS_RECORD: &str = "curation.duplicate_judgments.record";
pub const CURATION_FINALIZE: &str = "curation.finalize";

// LLM prompt/router/budget
pub const LLM_PROMPTS_LIST: &str = "llm.prompts.list";
pub const LLM_ROUTE_EXPLAIN: &str = "llm.route.explain";
pub const LLM_BUDGET_REPORT: &str = "llm.budget.report";

// Tasks
pub const TASKS_CREATE: &str = "tasks.create";
pub const TASKS_UPDATE: &str = "tasks.update";
pub const TASKS_STATUS_SET: &str = "tasks.status.set";
pub const TASKS_COMPLETE: &str = "tasks.complete";
pub const TASKS_CANCEL: &str = "tasks.cancel";
pub const TASKS_STATE_GET: &str = "tasks.state.get";
pub const TASKS_LIST: &str = "tasks.list";

// Health declarations
pub const HEALTH_INTAKE_RECORD: &str = "health.intake.record";
pub const HEALTH_EFFECT_RECORD: &str = "health.effect.record";

// PKM
pub const PKM_CREATE_NOTE: &str = "pkm.create_note";
pub const PKM_CREATE_ENTITIES: &str = "pkm.create_entities_from_list";
pub const PKM_LINK_ENTITIES: &str = "pkm.link_entities";

// Content
pub const CONTENT_STORE_BLOB: &str = "content.store_blob";
pub const CONTENT_RETRIEVE_BLOB: &str = "content.retrieve_blob";

// Replay
pub const REPLAY_CREATE_OPERATION: &str = "replay.create_operation";
pub const REPLAY_PREVIEW_OPERATION: &str = "replay.preview_operation";
pub const REPLAY_APPROVE_OPERATION: &str = "replay.approve_operation";
pub const REPLAY_SUBMIT_OPERATION: &str = "replay.submit_operation";
pub const REPLAY_EXECUTE_OPERATION: &str = "replay.execute_operation";
pub const REPLAY_CANCEL_OPERATION: &str = "replay.cancel_operation";
pub const REPLAY_OPERATION_STATUS: &str = "replay.operation_status";
pub const REPLAY_LIST_OPERATIONS: &str = "replay.list_operations";

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
pub const RUNTIME_LIST: &str = "runtime.list";
pub const RUNTIME_LIST_ACTIVE: &str = "runtime.list_active";
pub const RUNTIME_HEALTH: &str = "runtime.health";
pub const RUNTIME_DRAIN: &str = "runtime.drain";
pub const RUNTIME_RESUME: &str = "runtime.resume";
pub const RUNTIME_SET_HORIZON: &str = "runtime.set_horizon";

// Automata
pub const AUTOMATA_STATUS: &str = "automata.status";

// Ingestors
pub const INGESTORS_STATUS: &str = "ingestors.status";

// Instructions
pub const INSTRUCTIONS_HYPRLAND_WORKSPACE_SWITCH: &str = "instructions.hyprland.workspace_switch";

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

// Semantic epochs and shadow lanes
pub const SEMANTIC_EPOCHS_CREATE: &str = "semantic.epochs.create";
pub const SEMANTIC_EPOCHS_LIST: &str = "semantic.epochs.list";
pub const SEMANTIC_LANES_CREATE: &str = "semantic.lanes.create";
pub const SEMANTIC_LANES_LIST: &str = "semantic.lanes.list";
pub const SEMANTIC_LANES_SET_STATUS: &str = "semantic.lanes.set_status";
pub const SEMANTIC_LANES_DISCARD: &str = "semantic.lanes.discard";
pub const SEMANTIC_LANE_OUTPUTS_LIST: &str = "semantic.lane_outputs.list";
pub const SEMANTIC_LANE_OUTPUTS_WRITE: &str = "semantic.lane_outputs.write";
pub const SEMANTIC_LANE_OUTPUTS_SEED_CANONICAL_GRAPH: &str =
    "semantic.lane_outputs.seed_canonical_graph";
pub const SEMANTIC_LANE_OUTPUTS_SEED_ENTITY_EVENTS: &str =
    "semantic.lane_outputs.seed_entity_events";
pub const SEMANTIC_LANE_DIFFS_LIST: &str = "semantic.lane_diffs.list";
pub const SEMANTIC_LANE_DIFFS_RECORD_ENTITY_RELATION: &str =
    "semantic.lane_diffs.record_entity_relation";

// Lifecycle
pub const LIFECYCLE_STATUS: &str = "lifecycle.status";
pub const LIFECYCLE_ARCHIVE: &str = "lifecycle.archive";
pub const LIFECYCLE_RESTORE: &str = "lifecycle.restore";

// GitOps
pub const GITOPS_LIST_SOURCES: &str = "gitops.list_sources";
pub const GITOPS_CREATE_SOURCE: &str = "gitops.create_source";
pub const GITOPS_DELETE_SOURCE: &str = "gitops.delete_source";
pub const GITOPS_TRIGGER_SYNC: &str = "gitops.trigger_sync";

// Telemetry
pub const TELEMETRY_CURRENT_HEALTH: &str = "telemetry.current_health";
pub const TELEMETRY_CURRENT_DEVICE_STATE: &str = "telemetry.current_device_state";
pub const TELEMETRY_WINDOW_FOCUS: &str = "telemetry.window_focus";
pub const TELEMETRY_COMMAND_FREQUENCY: &str = "telemetry.command_frequency";
pub const TELEMETRY_FILE_ACTIVITY: &str = "telemetry.file_activity";
pub const TELEMETRY_RECENT_ACTIVITY: &str = "telemetry.recent_activity";
pub const TELEMETRY_SYSTEM_STATE: &str = "telemetry.system_state";
pub const TELEMETRY_GATEWAY_STATS: &str = "telemetry.gateway_stats";
pub const TELEMETRY_STREAM_STATS: &str = "telemetry.stream_stats";
pub const TELEMETRY_ASSEMBLY_STATS: &str = "telemetry.assembly_stats";
pub const TELEMETRY_SOURCE_STATS: &str = "telemetry.source_stats";
pub const TELEMETRY_METRIC_COUNTERS: &str = "telemetry.metric_counters";
pub const TELEMETRY_EVENT_ENGINE_BATCH_STATS: &str = "telemetry.event_engine_batch_stats";
pub const TELEMETRY_EVENT_ENGINE_VALIDATION: &str = "telemetry.event_engine_validation";
pub const TELEMETRY_THROUGHPUT: &str = "telemetry.throughput";

// Annotations
pub const EVENTS_ANNOTATE: &str = "events.annotate";

// Sources
pub const SOURCES_STAGE: &str = "sources.stage";
pub const SOURCES_LIST: &str = "sources.list";
pub const SOURCES_SHOW: &str = "sources.show";
pub const SOURCES_COVERAGE: &str = "sources.coverage";
pub const SOURCES_ANNOTATE: &str = "sources.annotate";
pub const SOURCES_ARCHIVE: &str = "sources.archive";
pub const SOURCES_CONTINUITY: &str = "sources.continuity";
pub const SOURCES_CONTINUITY_LIST: &str = "sources.continuity.list";
pub const SOURCES_CONTINUITY_GET: &str = "sources.continuity.get";
pub const SOURCES_CONTINUITY_EXPLAIN_GAP: &str = "sources.continuity.explain_gap";
pub const SOURCES_PRESETS_LIST: &str = "sources.presets.list";
pub const SOURCES_BINDINGS_LIST: &str = "sources.bindings.list";
pub const SOURCES_BINDINGS_CREATE: &str = "sources.bindings.create";
pub const SOURCES_BINDINGS_UPDATE: &str = "sources.bindings.update";
pub const SOURCES_BINDINGS_RESOLVE: &str = "sources.bindings.resolve";
pub const SOURCES_READINESS_LIST: &str = "sources.readiness.list";
pub const SOURCES_READINESS_GET: &str = "sources.readiness.get";
pub const SOURCES_DRIFT_LIST: &str = "sources.drift.list";

// Tombstone (two-step)
pub const LIFECYCLE_TOMBSTONE_CREATE: &str = "lifecycle.tombstone.create";
pub const LIFECYCLE_TOMBSTONE_PREVIEW: &str = "lifecycle.tombstone.preview";
pub const LIFECYCLE_TOMBSTONE_APPROVE: &str = "lifecycle.tombstone.approve";
pub const LIFECYCLE_TOMBSTONE_CANCEL: &str = "lifecycle.tombstone.cancel";
pub const LIFECYCLE_TOMBSTONE_LIST: &str = "lifecycle.tombstone.list";
pub const LIFECYCLE_TOMBSTONE_STATUS: &str = "lifecycle.tombstone.status";

// Documents (A2 — #332 part 2)
pub const DOCUMENTS_SEARCH: &str = "documents.search";
pub const DOCUMENTS_GET: &str = "documents.get";
pub const DOCUMENTS_GET_CHUNKS: &str = "documents.get_chunks";
pub const DOCUMENTS_GET_CHUNKS_REDACTED: &str = "documents.get_chunks_redacted";
