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
pub mod env;
pub mod environment;
pub mod error;
pub mod events;
pub mod fs;
pub mod ids;
#[cfg(feature = "nats")]
pub mod nats;
pub mod non_empty;
pub mod primitives;
pub mod privacy;
pub mod proof;
pub mod query;
pub mod rpc;
pub mod runtime_target;
pub mod settlement;
pub mod source_unit;
pub mod temporal;
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
    pub use crate::domain::{EventSource, EventType, HostName, RecordedPath};
    pub use crate::environment::SinexEnvironment;
    pub use crate::error::{Result, SinexError};
    pub use crate::events::builder::{OffsetKind, Provenance};
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
pub use domain::{EventSource, EventType, HostName, RecordedPath, SanitizedPath};
pub use env::strict_env_filter_source;
pub use environment::{SinexEnvironment, environment};
pub use error::{Result, SinexError};
pub use events::builder::{OffsetKind, Provenance};
pub use events::payload::DynamicPayload;
pub use events::{Event, SourceMaterial, Timestamp};
pub use ids::Id;
pub use primitives::Uuid;
pub use proof::{
    Claim, EvidenceEnvelope, Exemption, PROOF_CATALOG_SCHEMA_VERSION, ProofClaimKind,
    ProofObligation, ProofObligationLevel, RunnerBinding, RuntimeUnitDescriptor,
    SourceUnitDescriptor, SubjectQuery, SubjectRef,
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
pub use serde_json::Value as JsonValue;
pub use temporal::{OffsetDateTime, now};
pub use units::{Bytes, Seconds};
pub use validation::{
    sanitize_filename_component, validate_json, validate_json_value, validate_path,
    validate_path_within_root,
};
