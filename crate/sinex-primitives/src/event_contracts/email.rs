use super::{
    EventContract, EventContractId, EventOccurrenceContract, EventProvenanceRequirement,
    EventTemporalContract, PayloadSchemaContract,
};
use crate::output_kind::OutputKind;
use crate::source_contracts::OccurrenceIdentity;

pub const EMAIL_MESSAGE_RECEIVED_CONTRACT_ID: EventContractId =
    "event-contract:email/message.received@v1";
pub const EMAIL_MESSAGE_SENT_CONTRACT_ID: EventContractId = "event-contract:email/message.sent@v1";
pub const EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:email/attachment.observed@v1";
pub const EMAIL_THREAD_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:email/thread.observed@v1";
pub const EMAIL_SYNC_CURSOR_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:email/sync_cursor.observed@v1";
pub const EMAIL_CAPTURE_RUNTIME_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:email/capture_runtime.observed@v1";
const EMAIL_MAILBOX_PACKAGES: &[&str] = &["email.mailbox"];
const EMAIL_MAILBOX_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From("(message_id, folder)")];
const EMAIL_ATTACHMENT_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(message_occurrence, attachment_index, filename, content_id)",
    )];
const EMAIL_THREAD_SOURCE_OCCURRENCES: &[OccurrenceIdentity] = &[OccurrenceIdentity::Uuid5From(
    "(thread_key, message_id_or_material)",
)];
const EMAIL_SYNC_CURSOR_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(provider, account_binding_ref, mailbox_scope, cursor_kind)",
    )];
const EMAIL_CAPTURE_RUNTIME_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(provider, account_binding_ref, mode_id, observed_at)",
    )];
inventory::submit! {
    EventContract {
        id: EMAIL_MESSAGE_RECEIVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.message.received",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.message.received",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: EMAIL_MAILBOX_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: EMAIL_MESSAGE_SENT_CONTRACT_ID,
        event_source: "email",
        event_type: "email.message.sent",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.message.sent",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: EMAIL_MAILBOX_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.attachment.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.attachment.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: EMAIL_ATTACHMENT_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: EMAIL_THREAD_OBSERVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.thread.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.thread.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["thread_key", "message_id"],
        },
        source_occurrences: EMAIL_THREAD_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: EMAIL_SYNC_CURSOR_OBSERVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.sync_cursor.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.sync_cursor.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &[
                "provider",
                "account_binding_ref",
                "mailbox_scope",
                "cursor_kind",
            ],
        },
        source_occurrences: EMAIL_SYNC_CURSOR_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: EMAIL_CAPTURE_RUNTIME_OBSERVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.capture_runtime.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.capture_runtime.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["provider", "account_binding_ref", "mode_id", "observed_at"],
        },
        source_occurrences: EMAIL_CAPTURE_RUNTIME_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
