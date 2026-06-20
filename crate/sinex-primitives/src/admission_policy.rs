//! AdmissionPolicy and AdmissionOutcome contracts.
//!
//! These types define the shared admission vocabulary used by package
//! contracts, runtime admission code, debt views, and operation surfaces.
//! They are not a hidden policy engine: suppression, sampling, quarantine, and
//! deferral are explicit outcomes with policy ids and operator-visible reasons.

use crate::authority::ProposalKind;
use crate::event_contracts::EventContractId;
use crate::events::builder::EventId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Stable identifier for an admission policy.
pub type AdmissionPolicyId = &'static str;

/// Baseline policy for ordinary schema-validated canonical event admission.
pub const STANDARD_EVENT_ADMISSION_POLICY_ID: AdmissionPolicyId =
    "admission-policy:standard-event@v1";

/// Scope of material/candidates governed by an admission policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdmissionPolicyScope {
    PackageMode {
        package_id: &'static str,
        mode_id: &'static str,
    },
    EventContract {
        event_contract_id: EventContractId,
    },
    GlobalDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaValidationBehavior {
    RequirePayloadSchemaId,
    AllowPayloadInventoryLookup,
    QuarantineOnMissingSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OccurrenceAdmissionBehavior {
    RequireOccurrenceKey,
    AllowSourceContractIdentity,
    AllowMaterialAnchor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MalformedMaterialBehavior {
    Reject,
    Quarantine,
    Defer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePressureBehavior {
    DeferAndSurfaceDebt,
    PausePackageMode,
    QuarantineUntilOperatorAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProposalRoutingBehavior {
    None,
    Route { proposal_kind: ProposalKind },
}

/// Code-coupled admission policy declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct AdmissionPolicy {
    pub id: AdmissionPolicyId,
    pub scope: AdmissionPolicyScope,
    pub accepted_event_contracts: &'static [EventContractId],
    pub schema_validation: SchemaValidationBehavior,
    pub occurrence: OccurrenceAdmissionBehavior,
    pub disclosure_policy_ref: Option<&'static str>,
    pub malformed_material: MalformedMaterialBehavior,
    pub resource_pressure: ResourcePressureBehavior,
    pub proposal_routing: ProposalRoutingBehavior,
}

inventory::collect!(AdmissionPolicy);

inventory::submit! {
    AdmissionPolicy {
        id: STANDARD_EVENT_ADMISSION_POLICY_ID,
        scope: AdmissionPolicyScope::GlobalDefault,
        accepted_event_contracts: &[
            crate::event_contracts::SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID,
            crate::event_contracts::SHELL_KITTY_COMMAND_EXECUTED_CONTRACT_ID,
            crate::event_contracts::BROWSER_PAGE_VISITED_CONTRACT_ID,
            crate::event_contracts::EMAIL_MESSAGE_RECEIVED_CONTRACT_ID,
            crate::event_contracts::EMAIL_MESSAGE_SENT_CONTRACT_ID,
            crate::event_contracts::EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID,
            crate::event_contracts::MEDIA_AUDIO_RECORDING_OBSERVED_CONTRACT_ID,
            crate::event_contracts::MEDIA_AUDIO_CAPTURE_SESSION_STARTED_CONTRACT_ID,
            crate::event_contracts::MEDIA_AUDIO_CAPTURE_SESSION_ENDED_CONTRACT_ID,
            crate::event_contracts::MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID,
            crate::event_contracts::MEDIA_AUDIO_TRANSCRIPTION_RUN_OBSERVED_CONTRACT_ID,
            crate::event_contracts::MEDIA_SCREEN_SCREENSHOT_OBSERVED_CONTRACT_ID,
            crate::event_contracts::MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID,
            crate::event_contracts::MEDIA_SCREEN_OCR_RUN_OBSERVED_CONTRACT_ID,
        ],
        schema_validation: SchemaValidationBehavior::AllowPayloadInventoryLookup,
        occurrence: OccurrenceAdmissionBehavior::AllowSourceContractIdentity,
        disclosure_policy_ref: Some("operator.default-disclosure"),
        malformed_material: MalformedMaterialBehavior::Quarantine,
        resource_pressure: ResourcePressureBehavior::DeferAndSurfaceDebt,
        proposal_routing: ProposalRoutingBehavior::None,
    }
}

pub fn admission_policies() -> impl Iterator<Item = &'static AdmissionPolicy> {
    inventory::iter::<AdmissionPolicy>()
}

#[must_use]
pub fn find_admission_policy(id: &str) -> Option<&'static AdmissionPolicy> {
    admission_policies().find(|policy| policy.id == id)
}

/// References surfaced on non-admitted outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum AdmissionOutcomeRef {
    SourceMaterial(String),
    Event(String),
    EventContract(String),
    PackageMode(String),
    Operation(String),
    Policy(String),
}

/// Operator-visible reason a candidate did not become an admitted event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AdmissionOutcomeReason {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_owner: Option<String>,
}

impl AdmissionOutcomeReason {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            policy_owner: None,
        }
    }

    #[must_use]
    pub fn with_policy_owner(mut self, policy_owner: impl Into<String>) -> Self {
        self.policy_owner = Some(policy_owner.into());
        self
    }
}

/// Shared vocabulary for admission decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AdmissionOutcome {
    Admitted {
        policy_id: String,
        event_contract_id: Option<String>,
        event_ids: Vec<EventId>,
    },
    Rejected {
        policy_id: String,
        reason: AdmissionOutcomeReason,
        refs: Vec<AdmissionOutcomeRef>,
    },
    Quarantined {
        policy_id: String,
        reason: AdmissionOutcomeReason,
        refs: Vec<AdmissionOutcomeRef>,
    },
    Suppressed {
        policy_id: String,
        reason: AdmissionOutcomeReason,
        refs: Vec<AdmissionOutcomeRef>,
    },
    Deferred {
        policy_id: String,
        reason: AdmissionOutcomeReason,
        refs: Vec<AdmissionOutcomeRef>,
    },
    Deduplicated {
        policy_id: String,
        reason: AdmissionOutcomeReason,
        existing_event_id: Option<EventId>,
        refs: Vec<AdmissionOutcomeRef>,
    },
    Proposed {
        policy_id: String,
        proposal_id: String,
        proposal_kind: ProposalKind,
        refs: Vec<AdmissionOutcomeRef>,
    },
}

impl AdmissionOutcome {
    #[must_use]
    pub fn policy_id(&self) -> &str {
        match self {
            Self::Admitted { policy_id, .. }
            | Self::Rejected { policy_id, .. }
            | Self::Quarantined { policy_id, .. }
            | Self::Suppressed { policy_id, .. }
            | Self::Deferred { policy_id, .. }
            | Self::Deduplicated { policy_id, .. }
            | Self::Proposed { policy_id, .. } => policy_id,
        }
    }

    #[must_use]
    pub const fn is_admitted(&self) -> bool {
        matches!(self, Self::Admitted { .. })
    }

    #[must_use]
    pub const fn creates_proposal(&self) -> bool {
        matches!(self, Self::Proposed { .. })
    }
}
