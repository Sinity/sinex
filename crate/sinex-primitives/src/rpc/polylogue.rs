//! Exact revision publication over the authenticated local material boundary.

use serde::{Deserialize, Serialize};

use super::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use crate::Uuid;
pub use crate::polylogue_protocol::{PolylogueAcquisition, PolylogueOriginalRole};

pub const POLYLOGUE_REVISION_PUBLISH_METHOD: RpcMethod<
    PolylogueRevisionPublishRequest,
    PolylogueRevisionPublicationResponse,
> = RpcMethod::new(
    methods::POLYLOGUE_REVISION_PUBLISH,
    RpcRole::Write,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const POLYLOGUE_REVISION_STATUS_METHOD: RpcMethod<
    PolylogueRevisionStatusRequest,
    PolylogueRevisionPublicationResponse,
> = RpcMethod::new(
    methods::POLYLOGUE_REVISION_STATUS,
    RpcRole::Write,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

/// A locator is an input to immutable custody, never the artifact's identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolylogueArtifactInput {
    pub file_path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolylogueSegmentInput {
    pub name: String,
    pub file_path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolylogueOriginalInput {
    pub role: PolylogueOriginalRole,
    /// Exact Source-owned retained-version reference, not an inferred name.
    pub source_ref: String,
    pub file_path: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub media_type: String,
    pub acquisition: PolylogueAcquisition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolylogueRevisionPublishRequest {
    pub request_id: String,
    pub normalizer_version: String,
    pub manifest: PolylogueArtifactInput,
    pub segments: Vec<PolylogueSegmentInput>,
    pub originals: Vec<PolylogueOriginalInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolylogueRevisionStatusRequest {
    pub request_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolyloguePublicationState {
    RawAccepted,
    PersistedConfirmed,
    DurableDebt,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolyloguePublicationPhase {
    Prepared,
    MaterialsReady,
    Published,
    Settled,
}

/// The request's immutable material coordinates survive a missing RPC reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolyloguePublicationMaterials {
    pub manifest_material_id: Uuid,
    pub segment_material_ids: std::collections::BTreeMap<i32, Uuid>,
    pub original_material_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolylogueRevisionPublicationResponse {
    pub request_id: String,
    /// None means no native publication/settlement evidence yet. It must not
    /// unlock producer progress or turn a slow accepted request into failure.
    pub state: Option<PolyloguePublicationState>,
    pub detail: String,
    pub phase: PolyloguePublicationPhase,
    pub operation_id: Uuid,
    pub material_refs: PolyloguePublicationMaterials,
    pub event_ids: Vec<Uuid>,
}
