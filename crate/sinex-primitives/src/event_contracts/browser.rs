use super::{
    EventContract, EventContractId, EventOccurrenceContract, EventProvenanceRequirement,
    EventTemporalContract, PayloadSchemaContract,
};
use crate::output_kind::OutputKind;
use crate::source_contracts::OccurrenceIdentity;

pub const BROWSER_PAGE_VISITED_CONTRACT_ID: EventContractId =
    "event-contract:webhistory/page.visited@v1";
pub const BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:browser/navigation.observed@v1";
pub const BROWSER_TAB_ACTIVATED_CONTRACT_ID: EventContractId =
    "event-contract:browser/tab.activated@v1";
pub const BROWSER_DOWNLOAD_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:browser/download.observed@v1";
const BROWSER_HISTORY_PACKAGES: &[&str] = &["browser.history"];
const BROWSER_HISTORY_SOURCE_OCCURRENCES: &[OccurrenceIdentity] = &[OccurrenceIdentity::Uuid5From(
    "(source, browser_profile, visit_id)",
)];
const BROWSER_WEBEXTENSION_PACKAGES: &[&str] = &["browser.webextension-live"];
const BROWSER_NAVIGATION_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(profile_id, tab_id, url, observed_at)",
    )];
const BROWSER_TAB_ACTIVATED_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(profile_id, tab_id, window_id, observed_at)",
    )];
const BROWSER_DOWNLOAD_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(profile_id, download_id, url, observed_at)",
    )];
inventory::submit! {
    EventContract {
        id: BROWSER_PAGE_VISITED_CONTRACT_ID,
        event_source: "webhistory",
        event_type: "page.visited",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "webhistory",
            event_type: "page.visited",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: BROWSER_HISTORY_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.browser-history.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: BROWSER_HISTORY_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID,
        event_source: "browser",
        event_type: "navigation.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "browser",
            event_type: "navigation.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["profile_id", "tab_id", "url", "observed_at"],
        },
        source_occurrences: BROWSER_NAVIGATION_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.browser-web.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: BROWSER_WEBEXTENSION_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: BROWSER_TAB_ACTIVATED_CONTRACT_ID,
        event_source: "browser",
        event_type: "tab.activated",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "browser",
            event_type: "tab.activated",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["profile_id", "tab_id", "window_id", "observed_at"],
        },
        source_occurrences: BROWSER_TAB_ACTIVATED_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.browser-web.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: BROWSER_WEBEXTENSION_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: BROWSER_DOWNLOAD_OBSERVED_CONTRACT_ID,
        event_source: "browser",
        event_type: "download.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "browser",
            event_type: "download.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["profile_id", "download_id", "url", "observed_at"],
        },
        source_occurrences: BROWSER_DOWNLOAD_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.browser-web.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: BROWSER_WEBEXTENSION_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
