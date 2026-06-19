//! Durable output-kind vocabulary for Sinex boundaries.
//!
//! This module is classification metadata, not a runtime router. It gives new
//! event, projection, artifact, proposal, operation, and view work a shared
//! vocabulary so derived outputs are not admitted as canonical events by
//! default.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Classifies the semantic role of a Sinex output boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OutputKind {
    /// Immutable admitted fact in the event spine.
    CanonicalEvent,
    /// Rebuildable state computed from events or material.
    ProjectionRow,
    /// Persisted generated report, bundle, catalog, or export.
    Artifact,
    /// Candidate change or truth claim requiring authority.
    Proposal,
    /// Explicit authority decision over a proposal.
    Judgment,
    /// Intentional control-plane activity or finalization record.
    OperationRecord,
    /// Read result delivered to CLI, API, TUI, MCP, or another view surface.
    EphemeralView,
}

impl OutputKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CanonicalEvent => "canonical_event",
            Self::ProjectionRow => "projection_row",
            Self::Artifact => "artifact",
            Self::Proposal => "proposal",
            Self::Judgment => "judgment",
            Self::OperationRecord => "operation_record",
            Self::EphemeralView => "ephemeral_view",
        }
    }

    #[must_use]
    pub const fn is_canonical_event(self) -> bool {
        matches!(self, Self::CanonicalEvent)
    }
}

/// Checked-in output-kind classification for an existing boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct OutputKindDeclaration {
    /// Stable identifier for the table, DTO, artifact, or view boundary.
    pub output_id: &'static str,
    /// The output-kind law classification for this boundary.
    pub kind: OutputKind,
    /// Owning module, crate, or architectural surface.
    pub owner: &'static str,
    /// Short reason this boundary belongs to `kind`.
    pub rationale: &'static str,
}

/// Initial registry for current Sinex outputs that are commonly confused with
/// canonical events.
pub const OUTPUT_KIND_DECLARATIONS: &[OutputKindDeclaration] = &[
    OutputKindDeclaration {
        output_id: "core.events",
        kind: OutputKind::CanonicalEvent,
        owner: "sinex-db/core schema",
        rationale: "immutable admitted event facts",
    },
    OutputKindDeclaration {
        output_id: "domain.current_objects",
        kind: OutputKind::ProjectionRow,
        owner: "sinex-primitives::domain_reducer",
        rationale: "rebuildable reducer state keyed by domain object",
    },
    OutputKindDeclaration {
        output_id: "source.coverage",
        kind: OutputKind::ProjectionRow,
        owner: "sinex-primitives::views::SourceCoverageView",
        rationale: "operator coverage state computed from contracts, bindings, and runtime observations",
    },
    OutputKindDeclaration {
        output_id: "artifacts.source_catalog",
        kind: OutputKind::Artifact,
        owner: "sinexd source catalog export",
        rationale: "generated deployment/catalog file, not a fact in the event spine",
    },
    OutputKindDeclaration {
        output_id: "curation.proposal",
        kind: OutputKind::Proposal,
        owner: "sinex-primitives::authority",
        rationale: "candidate truth or change pending judgment",
    },
    OutputKindDeclaration {
        output_id: "curation.judgment",
        kind: OutputKind::Judgment,
        owner: "sinex-primitives::authority",
        rationale: "explicit authority decision over a proposal",
    },
    OutputKindDeclaration {
        output_id: "operations_log",
        kind: OutputKind::OperationRecord,
        owner: "sinex-db::repositories::state",
        rationale: "intentional control-plane activity and finalization history",
    },
    OutputKindDeclaration {
        output_id: "relations.evidence_window",
        kind: OutputKind::EphemeralView,
        owner: "sinex-primitives::relations",
        rationale: "read/query payload delivered through ViewEnvelope",
    },
    OutputKindDeclaration {
        output_id: "views.view_envelope",
        kind: OutputKind::EphemeralView,
        owner: "sinex-primitives::views",
        rationale: "delivery envelope for operator-visible read results",
    },
    OutputKindDeclaration {
        output_id: "views.debt_list",
        kind: OutputKind::EphemeralView,
        owner: "sinex-primitives::views::DebtListView",
        rationale: "operator debt read model for capture, admission, and projection gaps",
    },
];

#[must_use]
pub fn declared_output_kind(output_id: &str) -> Option<OutputKind> {
    OUTPUT_KIND_DECLARATIONS
        .iter()
        .find(|declaration| declaration.output_id == output_id)
        .map(|declaration| declaration.kind)
}
