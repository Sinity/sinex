//! EventContract registry.
//!
//! Event contracts give admitted events a contract identity that is separate
//! from `source + event_type`. The source/type pair remains the namespace that
//! an event matches; the contract id is the semantic coordinate that package
//! and admission policy code can reference.

use crate::domain::{EventSource, EventType};
use crate::events::Event;
use crate::output_kind::OutputKind;
use crate::source_contracts::OccurrenceIdentity;
use schemars::JsonSchema;
use serde::Serialize;

/// Stable identifier for an event contract.
pub type EventContractId = &'static str;

/// Payload schema coordinate used by an [`EventContract`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PayloadSchemaContract {
    /// Contract uses the payload inventory schema for `(source, event_type)`.
    PayloadInventory {
        source: &'static str,
        event_type: &'static str,
        version: &'static str,
    },
    /// Contract requires an explicit persisted payload schema id.
    ExplicitSchemaId { schema_id: &'static str },
}

/// Occurrence identity requirements for the event payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventOccurrenceContract {
    /// Occurrence is declared by the source package contract.
    SourceDeclared,
    /// Material-provenance events use the material id plus byte/row anchor.
    MaterialAnchor,
    /// Explicit field list forms the occurrence key.
    Fields { fields: &'static [&'static str] },
}

/// Temporal expectations for admitted events under this contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventTemporalContract {
    /// Parser supplies intrinsic event time when present; admission may derive
    /// from material timing when absent.
    IntrinsicOrMaterial,
    /// Event must carry intrinsic timestamp evidence.
    IntrinsicRequired,
    /// Event time is derived from source-material timing.
    MaterialDerived,
}

/// Provenance requirements for events under this contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventProvenanceRequirement {
    Material,
    Derived,
    MaterialOrDerived,
}

/// Code-coupled semantic contract for an admitted event family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct EventContract {
    /// Stable contract id. This is the semantic coordinate other contracts
    /// should reference instead of treating `source + event_type` as authority.
    pub id: EventContractId,
    /// Namespace/display event source matched by this contract.
    pub event_source: &'static str,
    /// Event type matched by this contract.
    pub event_type: &'static str,
    /// Payload schema coordinate.
    pub payload_schema: PayloadSchemaContract,
    /// Occurrence identity contract.
    pub occurrence: EventOccurrenceContract,
    /// Existing source-contract occurrence declarations this contract accepts.
    /// Multi-package contracts can cover sources with different declared
    /// occurrence strategies without collapsing them into one false identity.
    pub source_occurrences: &'static [OccurrenceIdentity],
    /// Temporal policy.
    pub temporal: EventTemporalContract,
    /// Provenance policy.
    pub provenance: EventProvenanceRequirement,
    /// Optional operator-controlled disclosure policy ref.
    pub disclosure_policy_ref: Option<&'static str>,
    /// Admission policy expected to govern this contract.
    pub admission_policy_ref: Option<&'static str>,
    /// Package/source ids currently allowed to emit this event contract.
    pub package_refs: &'static [&'static str],
    /// Output-kind classification for this contract's primary durable output.
    pub output_kind: OutputKind,
}

impl EventContract {
    #[must_use]
    pub fn matches_namespace<T>(&self, event: &Event<T>) -> bool {
        event.source.as_str() == self.event_source && event.event_type.as_str() == self.event_type
    }

    #[must_use]
    pub fn matches_pair(&self, source: &EventSource, event_type: &EventType) -> bool {
        source.as_str() == self.event_source && event_type.as_str() == self.event_type
    }

    #[must_use]
    pub const fn is_canonical_event(&self) -> bool {
        self.output_kind.is_canonical_event()
    }
}

inventory::collect!(EventContract);

/// Existing terminal-history event contract used as the first registry entry.
pub const SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID: EventContractId =
    "event-contract:shell.history/command.imported@v1";

const SHELL_HISTORY_PACKAGES: &[&str] = &[
    "terminal.bash-history",
    "terminal.zsh-history",
    "terminal.text-history",
    "terminal.fish-history",
];
const SHELL_HISTORY_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Anchor, OccurrenceIdentity::Natural];

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

pub fn event_contracts() -> impl Iterator<Item = &'static EventContract> {
    inventory::iter::<EventContract>()
}

#[must_use]
pub fn find_event_contract(id: &str) -> Option<&'static EventContract> {
    event_contracts().find(|contract| contract.id == id)
}

#[must_use]
pub fn find_event_contract_for_pair(
    source: &EventSource,
    event_type: &EventType,
) -> Option<&'static EventContract> {
    event_contracts().find(|contract| contract.matches_pair(source, event_type))
}
