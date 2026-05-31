//! Core domain primitives for Sinex.
#![feature(never_type)]
#![allow(async_fn_in_trait)]

extern crate self as sinex_primitives;

pub mod activity;
pub mod constants;
#[cfg(feature = "nats")]
pub mod coordination;
pub mod deployment_readiness;
pub mod domain;
pub mod domain_reducer;
pub mod env;
pub mod environment;
pub mod error;
pub mod events;
pub mod fs;
pub mod ids;
pub mod llm;
#[cfg(feature = "nats")]
pub mod nats;
pub mod non_empty;
pub mod parser;
pub mod primitives;
pub mod privacy;
pub mod proof;
pub mod query;
pub mod rpc;
pub mod runtime_target;
pub mod schema_constraints;
pub mod semantic;
pub mod views;

/// Re-exports used by macros generated from `sinex-macros`.
/// Not part of the stable public surface; do not depend on this from
/// hand-written code.
#[doc(hidden)]
pub mod __sinex_macros_reexport {
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
    pub use crate::temporal::OffsetDateTime;
}

/// Expected binary schema version — checked at startup against `sinex_schemas.binary_schema_version`.
/// Bump when the DB schema changes in a backward-incompatible way.
pub const EXPECTED_BINARY_SCHEMA_VERSION: &str = "1";

// Re-export commonly used types at crate root
pub use activity::{ActivitySourceKind, classify_trusted_activity_signal, primary_activity_source};
pub use constants::filesystem;
pub use deployment_readiness::{
    AutomataDeploymentSurface, BrowserDeploymentSurface, BrowserSqliteSource,
    DeploymentDatabaseRuntime, DeploymentExpectations, DeploymentGatewayRuntime,
    DeploymentNatsRuntime, DeploymentReadinessDescriptor, DeploymentReadinessMode,
    DeploymentSecrets, DeploymentSurface, DeploymentTarget, DesktopDeploymentSurface,
    DocumentDeploymentSurface, TerminalDeploymentSurface, TerminalHistorySource,
};
pub use domain::{
    ControlSubject, EventName, EventSource, EventType, HostName, MaterialStatus,
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
pub use events::builder::{OffsetKind, Provenance};
pub use events::occurrence::MaterialOccurrenceKey;
pub use events::payload::DynamicPayload;
pub use events::{Event, SourceMaterial, Timestamp};
pub use ids::Id;
pub use llm::*;
pub use primitives::Uuid;
pub use proof::{
    Claim, EvidenceEnvelope, Exemption, PROOF_CATALOG_SCHEMA_VERSION, ProofClaimKind,
    ProofObligation, ProofObligationLevel, RunnerBinding, SourceUnitBinding, SubjectQuery,
    SubjectRef,
};
pub use query::{
    AggregationMode, Cursor, EventQuery, EventQueryResult, GroupByField, GroupedCount,
    LineageDirection, LineageNode, LineageQuery, LineageResult, Pagination, PathOp, PayloadFilter,
    QueryResultEvent, SortDirection, SourceMaterialLinkInfo, SourceStatsEntry, SubscriptionFilter,
    TimeBucketEntry, TimeRange, TimeSeriesOrder,
};
pub use runtime_target::{
    RuntimeStatusSignal, RuntimeStatusSignalStatus, RuntimeStatusSnapshot, RuntimeStatusWarning,
    RuntimeTargetDatabase, RuntimeTargetDescriptor, RuntimeTargetGateway,
    RuntimeTargetGatewayTokenRole, RuntimeTargetKind, RuntimeTargetNats, RuntimeTargetServices,
    RuntimeTargetState,
};
pub use semantic::*;
pub use serde_json::Value as JsonValue;
pub use task_domain::*;
pub use temporal::{OffsetDateTime, now};
pub use units::{Bytes, Seconds};
pub use validation::{
    sanitize_filename_component, validate_json, validate_json_value, validate_path,
    validate_path_within_root,
};
pub use views::*;
