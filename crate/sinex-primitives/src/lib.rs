//! Core domain primitives for Sinex.
#![feature(never_type)]
#![allow(async_fn_in_trait)]

extern crate self as sinex_primitives;

pub mod activity;
pub mod admission_policy;
pub mod authority;
pub mod constants;
#[cfg(feature = "nats")]
pub mod coordination;
pub mod deployment_readiness;
pub mod derivations;
pub mod domain;
pub mod domain_reducer;
pub mod env;
pub mod environment;
pub mod error;
pub mod event_contracts;
pub mod events;
pub mod evidence_bundle;
pub mod fs;
pub mod ids;
pub mod llm;
#[cfg(feature = "nats")]
pub mod nats;
pub mod non_empty;
pub mod otel_projection;
pub mod output_kind;
pub mod parser;
pub mod primitives;
pub mod privacy;
pub mod public_ref;
pub mod query;
pub mod query_units;
pub mod relations;
pub mod rpc;
pub mod runtime_target;
pub mod schema_constraints;
pub mod semantic;
pub mod source_contracts;
pub mod views;

/// Re-exports used by macros generated from `sinex-macros`.
/// Not part of the stable public surface; do not depend on this from
/// hand-written code.
#[doc(hidden)]
pub mod __sinex_macros_reexport {
    pub use async_trait::async_trait;
    pub use inventory;
}
pub mod settlement;
pub mod sources;
pub mod task_domain;
pub mod temporal;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
#[cfg(feature = "nats")]
pub mod transport;
pub mod units;
pub mod utils;
pub mod validation;

pub mod buffers {
    pub use crate::constants::buffers::*;
}

pub mod prelude {
    pub use crate::domain::{
        EventName, EventSource, EventType, HostName, OperationRunStatus, RecordedPath, ServiceName,
        SourceIdentifier, SourceMaterialFormat, SourceMaterialTimingInfoType,
    };
    pub use crate::environment::SinexEnvironment;
    pub use crate::error::{Result, SinexError};
    pub use crate::events::builder::{OffsetKind, Provenance};
    pub use crate::events::occurrence::MaterialOccurrenceKey;
    pub use crate::events::{Event, SourceMaterial, Timestamp};
    pub use crate::ids::Id;
    pub use crate::primitives::Uuid;
    pub use crate::query::{
        AggregationMode, Cursor, EventQuery, EventQueryResult, GroupByField, GroupedCount,
        LineageDirection, LineageNode, LineageQuery, LineageResult, Pagination, PayloadFilter,
        QueryResultEvent, SortDirection, SourceMaterialLinkInfo, SourceStatsEntry,
        SubscriptionFilter, TimeBucketEntry, TimeRange, TimeSeriesOrder,
    };
    pub use crate::relations::{
        EventRelationExpr, EvidenceRef, EvidenceRole, EvidenceWindow, ExpansionTrace,
        ObservedRange, SameField, TimeBasis, TimeQuality,
    };
    pub use crate::temporal::OffsetDateTime;
}

/// Expected binary schema version — checked at startup against `sinex_schemas.binary_schema_version`.
/// Bump when the DB schema changes in a backward-incompatible way.
pub const EXPECTED_BINARY_SCHEMA_VERSION: &str = "1";

// Re-export commonly used types at crate root
pub use activity::{ActivitySourceKind, classify_trusted_activity_signal, primary_activity_source};
pub use admission_policy::{
    AdmissionOutcome, AdmissionOutcomeReason, AdmissionOutcomeRef, AdmissionPolicy,
    AdmissionPolicyId, AdmissionPolicyScope, MalformedMaterialBehavior,
    OccurrenceAdmissionBehavior, ProposalRoutingBehavior, ResourcePressureBehavior,
    STANDARD_EVENT_ADMISSION_POLICY_ID, SchemaValidationBehavior, admission_policies,
    find_admission_policy,
};
pub use authority::{
    DuplicateCandidatePayload, FinalizerRegistration, Judgment, JudgmentVerdict, ProposalKind,
};
pub use constants::filesystem;
pub use deployment_readiness::{
    AutomataDeploymentSurface, BrowserDeploymentSurface, BrowserSqliteSource,
    DeploymentDatabaseRuntime, DeploymentExpectations, DeploymentGatewayRuntime,
    DeploymentNatsRuntime, DeploymentReadinessDescriptor, DeploymentReadinessMode,
    DeploymentSecrets, DeploymentSurface, DeploymentTarget, DesktopDeploymentSurface,
    DocumentDeploymentSurface, TerminalDeploymentSurface, TerminalHistorySource,
};
pub use derivations::{
    DERIVATION_SPECS, DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION,
    DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID, DESKTOP_FOCUS_SESSION_DERIVATION,
    DESKTOP_FOCUS_SESSION_DERIVATION_ID, DESKTOP_NOTIFICATION_PRESSURE_DERIVATION,
    DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID, DESKTOP_PROJECT_CONTEXT_DERIVATION,
    DESKTOP_PROJECT_CONTEXT_DERIVATION_ID, DerivationInputScope, DerivationOperationHook,
    DerivationSpec, DerivationSpecId, FreshnessPolicy, InvalidationTrigger,
    TASK_CURRENT_OBJECTS_DERIVATION, TASK_CURRENT_OBJECTS_DERIVATION_ID, affected_derivations,
    derivation_specs, derivations_for_output, find_derivation_spec,
};
pub use domain::{
    ControlSubject, EventName, EventSource, EventType, HostName, MaterialStatus, OperationKind,
    OperationRunStatus, RecordedPath, SanitizedPath, ServiceName, SourceIdentifier,
    SourceMaterialFormat, SourceMaterialTimingInfoType,
};
pub use domain_reducer::{
    DomainProjectionSpec, ProjectionConflictPolicy, ProjectionOrderingPolicy,
    ProjectionOutputShape, ProjectionSettlementPolicy,
};
pub use env::strict_env_filter_source;
pub use environment::{SinexEnvironment, environment};
pub use error::{Result, SinexError};
pub use event_contracts::{
    EventContract, EventContractId, EventOccurrenceContract, EventProvenanceRequirement,
    EventTemporalContract, PayloadSchemaContract, event_contracts, find_event_contract,
    find_event_contract_for_pair,
};
pub use events::builder::{OffsetKind, Provenance};
pub use events::occurrence::MaterialOccurrenceKey;
pub use events::payload::DynamicPayload;
pub use events::{Event, SourceMaterial, Timestamp};
pub use ids::Id;
pub use llm::*;
pub use output_kind::{
    OUTPUT_KIND_DECLARATIONS, OutputKind, OutputKindDeclaration, declared_output_kind,
};
pub use primitives::Uuid;
pub use public_ref::{
    PublicSinexRef, PublicSinexRefParseError, RESOLVED_OBJECT_VIEW_SCHEMA_VERSION,
    ResolvedObjectStatus, ResolvedObjectView, parse_public_kind, public_kind_name,
};
pub use query::{
    AggregationMode, Cursor, EventQuery, EventQueryResult, GroupByField, GroupedCount,
    LineageDirection, LineageNode, LineageQuery, LineageResult, Pagination, PathOp, PayloadFilter,
    QueryResultEvent, SortDirection, SourceMaterialLinkInfo, SourceStatsEntry, SubscriptionFilter,
    TimeBucketEntry, TimeRange, TimeSeriesOrder,
};
pub use query_units::{
    QueryFieldDescriptor, QueryFieldType, QueryOperator, QueryPagination, QuerySortDescriptor,
    QueryUnitDescriptor, QueryUnitId, QueryValue, SinexQuery, SinexQueryPredicate, SinexQuerySort,
    parse_sinex_query, query_unit_descriptor, query_unit_descriptors,
};
pub use runtime_target::{
    RuntimeStatusSignal, RuntimeStatusSignalStatus, RuntimeStatusSnapshot, RuntimeStatusWarning,
    RuntimeTargetDatabase, RuntimeTargetDescriptor, RuntimeTargetGateway,
    RuntimeTargetGatewayTokenRole, RuntimeTargetKind, RuntimeTargetNats, RuntimeTargetServices,
    RuntimeTargetState,
};
pub use semantic::*;
pub use serde_json::Value as JsonValue;
pub use source_contracts::{SourceRuntimeBinding, SubjectQuery, SubjectRef};
pub use task_domain::*;
pub use temporal::{OffsetDateTime, now};
pub use units::{Bytes, Seconds};
pub use validation::{
    sanitize_filename_component, validate_json, validate_json_value, validate_path,
    validate_path_within_root,
};
pub use views::*;
