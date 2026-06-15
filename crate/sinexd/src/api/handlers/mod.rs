//! RPC method handlers organized by domain
//!
//! This module organizes handlers into domain-specific submodules.

pub mod audit;
pub mod automata;
pub mod content;
pub mod coordination;
pub mod curation;
pub mod dlq;
pub mod documents;
pub mod health;
pub mod instructions;
pub mod lifecycle;
pub mod llm;
pub mod ops;
pub mod pkm;
pub mod privacy;
pub mod query;
pub mod replay;
pub mod rpc_handlers;
pub mod runtime_presence;
pub mod runtime_registry;
pub mod semantic;
pub mod shadow;
pub mod source_status;
pub mod sources;
pub mod system;
pub mod tasks;
pub mod telemetry;

pub use curation::{
    handle_curation_finalize, handle_curation_list_duplicate_candidates,
    handle_curation_list_proposals, handle_curation_record_duplicate_judgment,
    handle_curation_record_judgment,
};
pub use query::{
    handle_events_annotate, handle_events_cards, handle_events_lineage, handle_events_query,
    handle_events_relation_evidence,
};
pub use replay::{
    handle_replay_approve_operation, handle_replay_cancel_operation,
    handle_replay_create_operation, handle_replay_execute_operation, handle_replay_list_operations,
    handle_replay_operation_status, handle_replay_preview_operation,
    handle_replay_submit_operation,
};

// Re-export new domain-specific handler functions
pub use audit::handle_audit_get;
pub use automata::handle_automata_status;
pub use dlq::{handle_dlq_list, handle_dlq_peek, handle_dlq_purge, handle_dlq_requeue};
pub use instructions::handle_hyprland_workspace_switch;
pub use lifecycle::{
    handle_lifecycle_archive,
    handle_lifecycle_restore,
    handle_lifecycle_status,
    // Tombstone operations (two-step flow)
    handle_tombstone_approve,
    handle_tombstone_cancel,
    handle_tombstone_create,
    handle_tombstone_list,
    handle_tombstone_preview,
    handle_tombstone_status,
};
pub use llm::{handle_llm_budget_report, handle_llm_prompts_list, handle_llm_route_explain};
pub use ops::{handle_ops_cancel, handle_ops_get, handle_ops_list, handle_ops_start};
pub use runtime_registry::{
    handle_runtime_drain, handle_runtime_list, handle_runtime_resume, handle_runtime_set_horizon,
};
pub use semantic::{
    handle_semantic_epoch_create, handle_semantic_epoch_list, handle_semantic_lane_create,
    handle_semantic_lane_diff_record_entity_relation, handle_semantic_lane_diffs_list,
    handle_semantic_lane_discard, handle_semantic_lane_outputs_list,
    handle_semantic_lane_outputs_seed_canonical_graph,
    handle_semantic_lane_outputs_seed_entity_events, handle_semantic_lane_outputs_write,
    handle_semantic_lane_set_status, handle_semantic_lanes_list,
};
pub use shadow::{handle_shadow_create, handle_shadow_delete, handle_shadow_list};
pub use source_status::{handle_sources_status, handle_sources_status_view};

pub use content::{handle_retrieve_blob, handle_store_blob};
pub use coordination::{
    handle_coordination_get_leader, handle_coordination_instance_health,
    handle_coordination_list_instances,
};
pub use documents::{
    handle_documents_get, handle_documents_get_chunks, handle_documents_get_chunks_redacted,
    handle_documents_search,
};
pub use health::{handle_health_effect_record, handle_health_intake_record};
pub use pkm::{handle_create_entities, handle_create_note, handle_link_entities};
pub use privacy::{
    handle_privacy_policy_backend_add, handle_privacy_policy_dictionary_add,
    handle_privacy_policy_field_bind, handle_privacy_policy_field_unbind,
    handle_privacy_policy_list, handle_privacy_policy_rule_add, handle_privacy_policy_rule_remove,
    handle_privacy_policy_rule_set_enabled, handle_privacy_policy_scope_bind,
    handle_privacy_policy_seed_builtin, handle_private_mode_disable,
    handle_private_mode_disable_service, handle_private_mode_enable,
    handle_private_mode_enable_service, handle_private_mode_status,
    handle_private_mode_status_service,
};
pub use runtime_presence::{handle_runtime_health, handle_runtime_list_active};
pub use sources::{
    handle_sources_annotate, handle_sources_archive, handle_sources_bindings_create,
    handle_sources_bindings_list, handle_sources_bindings_resolve, handle_sources_continuity,
    handle_sources_continuity_explain_gap, handle_sources_continuity_get,
    handle_sources_continuity_list, handle_sources_coverage, handle_sources_drift_list,
    handle_sources_list, handle_sources_presets_list, handle_sources_readiness_get,
    handle_sources_readiness_list, handle_sources_show, handle_sources_stage,
};
pub use system::{handle_system_health, handle_system_ping, handle_system_version};
pub use tasks::{
    handle_tasks_cancel, handle_tasks_complete, handle_tasks_create, handle_tasks_list,
    handle_tasks_state_get, handle_tasks_status_set, handle_tasks_update,
};
pub use telemetry::{
    handle_telemetry_assembly_stats, handle_telemetry_command_frequency,
    handle_telemetry_current_device_state, handle_telemetry_current_health,
    handle_telemetry_event_engine_batch_stats, handle_telemetry_event_engine_validation,
    handle_telemetry_file_activity, handle_telemetry_gateway_stats,
    handle_telemetry_metric_counters, handle_telemetry_recent_activity,
    handle_telemetry_source_stats, handle_telemetry_stream_stats, handle_telemetry_system_state,
    handle_telemetry_throughput, handle_telemetry_window_focus,
};
