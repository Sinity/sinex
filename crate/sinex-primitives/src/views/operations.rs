use super::{ActionAvailability, ActionAvailabilityState, ActionSideEffect};
use crate::JsonValue;
use crate::domain::{OperationKind, OperationStatus};
use crate::rpc::dlq::{DlqListResponse, DlqMessagePeek};
use crate::rpc::lifecycle::LifecycleStatusResponse;
use crate::rpc::replay::{ReplayOperation, ReplayState};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::common::truncate_chars;

pub const OPERATION_JOB_LIST_SCHEMA_VERSION: &str = "sinex.operation-job-list/v1";
pub const OPERATION_CONTROL_CARD_SCHEMA_VERSION: &str = "sinex.operation-control-card/v1";
pub const OPERATION_VIEW_SCHEMA_VERSION: &str = "sinex.operation-view/v1";

/// Read-only view of a single `core.operations_log` row rendered for operator
/// and agent consumption.
///
/// Wraps the raw `OperationRecord` from `sinex-db`, replacing the untyped
/// `operation_type: String` with the typed [`OperationKind`] registry and
/// surfacing stable, named fields without exposing DB-internal identifiers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OperationView {
    /// Stable hex ID of this operation (UUID, opaque to callers).
    pub id: String,
    /// Typed classification of the operation.
    pub kind: OperationKind,
    /// Actor that submitted the operation (actor_id from auth context).
    pub operator: String,
    /// Terminal result status of the operation.
    pub status: OperationStatus,
    /// Wall-clock duration in milliseconds, `null` while still running.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i32>,
    /// Human-readable result message set on completion or failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_message: Option<String>,
    /// JSONB scope payload that scoped this operation (e.g. event ID range).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<JsonValue>,
    /// Summary JSONB produced at completion, suitable for display.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_summary: Option<JsonValue>,
    /// Quick-access action hints for operator UIs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

impl OperationView {
    /// Construct from the RPC `Operation` type from `sinex-primitives::rpc::ops`.
    ///
    /// Accepts the raw `operation_type` string and converts it to [`OperationKind`].
    #[must_use]
    pub fn from_rpc(
        id: String,
        operation_type: &str,
        operator: String,
        status: OperationStatus,
        duration_ms: Option<i32>,
        result_message: Option<String>,
        scope: Option<JsonValue>,
        preview_summary: Option<JsonValue>,
    ) -> Self {
        let kind = OperationKind::from(operation_type);
        let actions = operation_actions(&id, &kind, &status);
        Self {
            id,
            kind,
            operator,
            status,
            duration_ms,
            result_message,
            scope,
            preview_summary,
            actions,
        }
    }
}

fn operation_actions(
    id: &str,
    kind: &OperationKind,
    status: &OperationStatus,
) -> Vec<ActionAvailability> {
    let is_terminal = matches!(
        status,
        OperationStatus::Success | OperationStatus::Failed | OperationStatus::Cancelled
    );
    let can_cancel =
        !is_terminal && matches!(status, OperationStatus::Running | OperationStatus::Pending);

    vec![
        ActionAvailability::read("ops.show", "Show", ActionAvailabilityState::Enabled)
            .with_command_hint(format!("sinexctl ops get {id}")),
        ActionAvailability {
            id: "ops.cancel".to_string(),
            label: "Cancel".to_string(),
            state: if can_cancel {
                ActionAvailabilityState::Enabled
            } else {
                ActionAvailabilityState::Disabled
            },
            reason: if is_terminal {
                Some("operation is already in a terminal state".to_string())
            } else {
                None
            },
            command_hint: Some(format!("sinexctl ops cancel {id}")),
            rpc_method: None,
            side_effect: ActionSideEffect::Write,
            requires_confirmation: false,
            dry_run_available: false,
            audit_output_ref: None,
        },
        ActionAvailability {
            id: "ops.replay".to_string(),
            label: "Replay".to_string(),
            state: if matches!(kind, OperationKind::Replay)
                && matches!(status, OperationStatus::Failed | OperationStatus::Cancelled)
            {
                ActionAvailabilityState::Enabled
            } else {
                ActionAvailabilityState::Unavailable
            },
            reason: if !matches!(kind, OperationKind::Replay) {
                Some("replay action only available for replay operations".to_string())
            } else {
                None
            },
            command_hint: Some(format!("sinexctl ops replay submit --ref-op {id}")),
            rpc_method: None,
            side_effect: ActionSideEffect::Write,
            requires_confirmation: true,
            dry_run_available: true,
            audit_output_ref: None,
        },
    ]
}

/// Payload carried inside a [`super::ViewEnvelope`] for `sinexctl ops jobs list`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OperationJobListView {
    pub schema_version: String,
    pub count: usize,
    pub jobs: Vec<OperationView>,
}

/// Shared read-model card for operation-room style control panels.
///
/// This keeps TUI/MCP/CLI-facing operation panels on the same action grammar
/// even when the underlying RPC still has a domain-specific DTO.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OperationControlCardView {
    pub schema_version: String,
    pub title: String,
    pub authority: String,
    pub phase: String,
    pub progress: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_refs: Vec<String>,
}

impl OperationControlCardView {
    #[must_use]
    pub fn from_replay_operation(operation: &ReplayOperation) -> Self {
        let progress = format!(
            "{} / {} events, batch {}",
            operation.checkpoint.processed_events,
            operation.checkpoint.total_events,
            operation.checkpoint.batch_number
        );
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: format!("ops replay {}", operation.operation_id),
            authority: "write".to_string(),
            phase: format!("{:?}", operation.state).to_lowercase(),
            progress,
            affected_refs: replay_scope_refs(operation),
            caveats: replay_caveats(operation),
            actions: replay_actions(operation),
            audit_refs: vec![format!("sinexctl ops audit {}", operation.operation_id)],
        }
    }

    #[must_use]
    pub fn from_dlq_status(stats: &DlqListResponse) -> Self {
        let total = stats.total_messages;
        let bytes = stats.total_bytes;
        let mut caveats = Vec::new();
        if total > 0 {
            caveats.push(
                "requeue/purge is mutating; inspect peek output and source readiness first"
                    .to_string(),
            );
            caveats.push(format!(
                "pressure: {}, recommended action: {}",
                stats.pressure_level, stats.recommended_action
            ));
        }
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "raw-ingest DLQ".to_string(),
            authority: if total > 0 { "admin" } else { "read" }.to_string(),
            phase: if total > 0 { "blocked" } else { "clear" }.to_string(),
            progress: format!("{total} message(s), {bytes} byte(s)"),
            affected_refs: vec![format!("seq {}..{}", stats.first_seq, stats.last_seq)],
            caveats,
            actions: dlq_actions(total > 0),
            audit_refs: vec!["sinexctl ops dlq list".to_string()],
        }
    }

    #[must_use]
    pub fn dlq_unavailable() -> Self {
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "raw-ingest DLQ".to_string(),
            authority: "read".to_string(),
            phase: "unknown".to_string(),
            progress: "DLQ status unavailable".to_string(),
            affected_refs: Vec::new(),
            caveats: vec!["DLQ status has not loaded yet".to_string()],
            actions: vec![read_action(
                "dlq.list",
                "List",
                ActionAvailabilityState::Enabled,
                "sinexctl ops dlq list",
                "dlq.list",
            )],
            audit_refs: vec!["sinexctl ops dlq list".to_string()],
        }
    }

    #[must_use]
    pub fn from_automaton_dlq_message(message: &DlqMessagePeek) -> Option<Self> {
        if !is_automaton_material_dlq(message) {
            return None;
        }
        Some(Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "automaton telemetry DLQ material gap".to_string(),
            authority: "admin".to_string(),
            phase: "blocked".to_string(),
            progress: format!(
                "sample seq {}, retry {}",
                message.sequence, message.retry_count
            ),
            affected_refs: vec![
                format!("subject: {}", message.subject),
                format!(
                    "original: {}",
                    message.original_subject.as_deref().unwrap_or("unknown")
                ),
                format!(
                    "failed event sample: {}",
                    truncate_chars(&message.payload_preview, 96)
                ),
            ],
            caveats: vec![
                "first-class DLQ class: likely missing source-material registration for derived telemetry".to_string(),
                "requeue will probably re-DLQ until the Source Readiness Cockpit row is fixed".to_string(),
                "downstream projections may miss automaton telemetry until repaired".to_string(),
            ],
            actions: vec![
                read_action(
                    "source.inspect",
                    "Inspect source",
                    ActionAvailabilityState::Enabled,
                    "sinexctl tui --tab sources",
                    "sources.coverage",
                ),
                read_action(
                    "dlq.peek",
                    "Peek",
                    ActionAvailabilityState::Enabled,
                    "sinexctl ops dlq peek --limit 10",
                    "dlq.peek",
                ),
                write_action(
                    "dlq.requeue.after_repair",
                    "Requeue after repair",
                    ActionAvailabilityState::Dangerous,
                    "sinexctl ops dlq requeue --all",
                    "dlq.requeue",
                    ActionSideEffect::Admin,
                )
                .with_reason("repair source-material registration before requeue"),
            ],
            audit_refs: vec!["Ref #1241 automaton telemetry DLQ verification".to_string()],
        })
    }

    #[must_use]
    pub fn from_lifecycle_status(status: &LifecycleStatusResponse) -> Self {
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "ops lifecycle archive/restore/tombstone".to_string(),
            authority: "admin".to_string(),
            phase: "guarded".to_string(),
            progress: format!("{} event(s) across lifecycle tiers", status.total_events),
            affected_refs: status
                .tiers
                .iter()
                .map(|tier| {
                    format!(
                        "{:?}: {} event(s), {} source(s)",
                        tier.tier, tier.event_count, tier.distinct_sources
                    )
                })
                .collect(),
            caveats: vec![
                "archive/restore supports dry-run; tombstone is destructive and preview/approve gated"
                    .to_string(),
            ],
            actions: lifecycle_actions(),
            audit_refs: vec!["sinexctl ops lifecycle status".to_string()],
        }
    }

    #[must_use]
    pub fn lifecycle_unavailable() -> Self {
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "ops lifecycle archive/restore/tombstone".to_string(),
            authority: "admin".to_string(),
            phase: "unknown".to_string(),
            progress: "lifecycle status unavailable".to_string(),
            affected_refs: Vec::new(),
            caveats: vec!["lifecycle status has not loaded yet".to_string()],
            actions: lifecycle_actions(),
            audit_refs: vec!["sinexctl ops lifecycle status".to_string()],
        }
    }
}

fn replay_scope_refs(operation: &ReplayOperation) -> Vec<String> {
    let scope = &operation.scope;
    let mut refs = vec![format!("source: {}", scope.source_name)];
    if let Some((start, end)) = &scope.time_window {
        refs.push(format!("time: {start} -> {end}"));
    }
    if let Some(materials) = &scope.material_filter {
        refs.push(format!("materials: {}", materials.len()));
    }
    if let Some(source_id) = &scope.source_id {
        refs.push(format!("source: {source_id}"));
    }
    if let Some(source_material_id) = &scope.source_material_id {
        refs.push(format!("source-material: {source_material_id}"));
    }
    if let Some(parser_id) = &scope.parser_id {
        refs.push(format!("parser: {parser_id}"));
    }
    refs
}

fn replay_caveats(operation: &ReplayOperation) -> Vec<String> {
    let mut caveats = Vec::new();
    if operation.scope.is_staged_source_scope() {
        caveats.push("staged-source replay: inspect source readiness before execute".to_string());
    }
    if !operation.state.is_terminal()
        && matches!(
            operation.state,
            ReplayState::Previewed | ReplayState::Approved | ReplayState::Executing
        )
    {
        caveats.push("mutating replay phase: confirmation/audit trail required".to_string());
    }
    if let Some(error) = &operation.error_details {
        caveats.push(format!("error: {}", truncate_chars(error, 96)));
    }
    caveats
}

fn replay_actions(operation: &ReplayOperation) -> Vec<ActionAvailability> {
    let id = &operation.operation_id;
    let mut actions = vec![
        read_action(
            "replay.watch",
            "Monitor",
            ActionAvailabilityState::Enabled,
            format!("sinexctl ops replay watch {id}"),
            "replay.status",
        ),
        read_action(
            "replay.status",
            "Status",
            ActionAvailabilityState::Enabled,
            format!("sinexctl ops replay status {id}"),
            "replay.status",
        ),
    ];
    match operation.state {
        ReplayState::Planning => actions.push(write_action(
            "replay.preview",
            "Preview",
            ActionAvailabilityState::Enabled,
            format!("sinexctl ops replay preview {id}"),
            "replay.preview",
            ActionSideEffect::Write,
        )),
        ReplayState::Previewed => actions.push(
            write_action(
                "replay.approve",
                "Confirm",
                ActionAvailabilityState::Dangerous,
                format!("sinexctl ops replay approve {id}"),
                "replay.approve",
                ActionSideEffect::Admin,
            )
            .with_reason("approval changes replay authority state"),
        ),
        ReplayState::Approved => actions.push(
            write_action(
                "replay.execute",
                "Execute",
                ActionAvailabilityState::Dangerous,
                format!("sinexctl ops replay execute {id}"),
                "replay.execute",
                ActionSideEffect::Admin,
            )
            .with_reason("execution mutates admitted events/projections"),
        ),
        ReplayState::Executing | ReplayState::Cancelling | ReplayState::Committing => actions.push(
            write_action(
                "replay.cancel",
                "Cancel",
                ActionAvailabilityState::Dangerous,
                format!("sinexctl ops replay cancel {id} --reason <reason>"),
                "replay.cancel",
                ActionSideEffect::Admin,
            )
            .with_reason("cancellation changes an active replay operation"),
        ),
        ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled => {}
    }
    actions
}

fn dlq_actions(has_messages: bool) -> Vec<ActionAvailability> {
    vec![
        read_action(
            "dlq.peek",
            "Peek",
            ActionAvailabilityState::Enabled,
            "sinexctl ops dlq peek --limit 10",
            "dlq.peek",
        ),
        write_action(
            "dlq.requeue",
            "Requeue",
            if has_messages {
                ActionAvailabilityState::Dangerous
            } else {
                ActionAvailabilityState::Disabled
            },
            "sinexctl ops dlq requeue --all",
            "dlq.requeue",
            ActionSideEffect::Admin,
        )
        .with_reason(if has_messages {
            "requeue mutates pending DLQ messages"
        } else {
            "DLQ is empty"
        }),
        write_action(
            "dlq.purge",
            "Purge",
            if has_messages {
                ActionAvailabilityState::Dangerous
            } else {
                ActionAvailabilityState::Disabled
            },
            "sinexctl ops dlq purge --confirm",
            "dlq.purge",
            ActionSideEffect::Destructive,
        )
        .with_reason(if has_messages {
            "purge deletes pending DLQ messages"
        } else {
            "DLQ is empty"
        }),
    ]
}

fn lifecycle_actions() -> Vec<ActionAvailability> {
    vec![
        write_action(
            "lifecycle.archive.dry_run",
            "Archive dry-run",
            ActionAvailabilityState::Enabled,
            "sinexctl ops lifecycle archive --limit 1000",
            "lifecycle.archive",
            ActionSideEffect::Admin,
        )
        .with_dry_run(),
        write_action(
            "lifecycle.restore.dry_run",
            "Restore dry-run",
            ActionAvailabilityState::Enabled,
            "sinexctl ops lifecycle restore <event-id>...",
            "lifecycle.restore",
            ActionSideEffect::Admin,
        )
        .with_dry_run(),
        write_action(
            "lifecycle.tombstone.preview",
            "Tombstone preview",
            ActionAvailabilityState::Dangerous,
            "sinexctl ops lifecycle tombstone preview <operation-id>",
            "lifecycle.tombstone.preview",
            ActionSideEffect::Destructive,
        )
        .with_reason("tombstone is destructive; preview before approve"),
        write_action(
            "lifecycle.tombstone.approve",
            "Tombstone approve",
            ActionAvailabilityState::Dangerous,
            "sinexctl ops lifecycle tombstone approve <operation-id>",
            "lifecycle.tombstone.approve",
            ActionSideEffect::Destructive,
        )
        .with_reason("approval commits a destructive tombstone operation"),
    ]
}

fn read_action(
    id: impl Into<String>,
    label: impl Into<String>,
    state: ActionAvailabilityState,
    command: impl Into<String>,
    rpc_method: impl Into<String>,
) -> ActionAvailability {
    ActionAvailability::read(id, label, state)
        .with_command_hint(command)
        .with_rpc_method(rpc_method)
}

fn write_action(
    id: impl Into<String>,
    label: impl Into<String>,
    state: ActionAvailabilityState,
    command: impl Into<String>,
    rpc_method: impl Into<String>,
    side_effect: ActionSideEffect,
) -> ActionAvailability {
    ActionAvailability {
        id: id.into(),
        label: label.into(),
        state,
        reason: None,
        command_hint: Some(command.into()),
        rpc_method: Some(rpc_method.into()),
        side_effect,
        requires_confirmation: matches!(
            side_effect,
            ActionSideEffect::Admin | ActionSideEffect::Destructive
        ),
        dry_run_available: false,
        audit_output_ref: None,
    }
}

trait ActionAvailabilityExt {
    fn with_dry_run(self) -> Self;
}

impl ActionAvailabilityExt for ActionAvailability {
    fn with_dry_run(mut self) -> Self {
        self.dry_run_available = true;
        self
    }
}

fn is_automaton_material_dlq(message: &DlqMessagePeek) -> bool {
    let haystack = format!(
        "{} {} {}",
        message.subject,
        message.original_subject.as_deref().unwrap_or_default(),
        message.payload_preview
    )
    .to_ascii_lowercase();
    haystack.contains("derived")
        && (haystack.contains("source_material")
            || haystack.contains("source material")
            || haystack.contains("material"))
}

impl OperationJobListView {
    #[must_use]
    pub fn new(jobs: Vec<OperationView>) -> Self {
        let count = jobs.len();
        Self {
            schema_version: OPERATION_JOB_LIST_SCHEMA_VERSION.to_string(),
            count,
            jobs,
        }
    }
}
