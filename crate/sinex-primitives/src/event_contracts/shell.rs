use super::{
    EventContract, EventContractId, EventOccurrenceContract, EventProvenanceRequirement,
    EventTemporalContract, PayloadSchemaContract,
};
use crate::output_kind::OutputKind;
use crate::source_contracts::OccurrenceIdentity;

/// Existing terminal-history event contract used as the first registry entry.
pub const SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID: EventContractId =
    "event-contract:shell.history/command.imported@v1";
pub const SHELL_ATUIN_COMMAND_EXECUTED_CONTRACT_ID: EventContractId =
    "event-contract:shell.atuin/command.executed@v1";
pub const SHELL_KITTY_COMMAND_EXECUTED_CONTRACT_ID: EventContractId =
    "event-contract:shell.kitty/command.executed@v1";
const SHELL_HISTORY_PACKAGES: &[&str] = &[
    "terminal.bash-history",
    "terminal.zsh-history",
    "terminal.text-history",
    "terminal.fish-history",
];
const SHELL_HISTORY_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Anchor, OccurrenceIdentity::Natural];
const SHELL_ATUIN_PACKAGES: &[&str] = &["terminal.atuin-history"];
const SHELL_ATUIN_SOURCE_OCCURRENCES: &[OccurrenceIdentity] = &[OccurrenceIdentity::Natural];
const SHELL_KITTY_PACKAGES: &[&str] = &["terminal.kitty-osc-live"];
const SHELL_KITTY_SOURCE_OCCURRENCES: &[OccurrenceIdentity] = &[OccurrenceIdentity::Uuid5From(
    "(terminal_session, sequence, command, cwd, ts)",
)];
inventory::submit! {
    EventContract {
        id: SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID,
        event_source: "shell.history",
        event_type: "command.imported",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "shell.history",
            event_type: "command.imported",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: SHELL_HISTORY_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.shell-history.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: SHELL_HISTORY_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: SHELL_ATUIN_COMMAND_EXECUTED_CONTRACT_ID,
        event_source: "shell.atuin",
        event_type: "command.executed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "shell.atuin",
            event_type: "command.executed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: SHELL_ATUIN_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.shell-history.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: SHELL_ATUIN_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: SHELL_KITTY_COMMAND_EXECUTED_CONTRACT_ID,
        event_source: "shell.kitty",
        event_type: "command.executed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "shell.kitty",
            event_type: "command.executed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: SHELL_KITTY_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.terminal-live.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: SHELL_KITTY_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
