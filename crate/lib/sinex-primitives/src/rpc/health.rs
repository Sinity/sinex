//! Health declaration RPC types for `health.*` methods.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::events::{
    SourceMaterial,
    payloads::{
        HealthEffectObservationRecordedPayload, HealthQuantity,
        HealthSubstanceIntakeRecordedPayload, HealthTimingQuality,
    },
};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use crate::{Id, Timestamp, Uuid};

pub const HEALTH_INTAKE_RECORD_METHOD: RpcMethod<
    HealthIntakeRecordRequest,
    HealthIntakeRecordResponse,
> = RpcMethod::new(
    methods::HEALTH_INTAKE_RECORD,
    RpcRole::Write,
    RpcDomain::Health,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const HEALTH_EFFECT_RECORD_METHOD: RpcMethod<
    HealthEffectRecordRequest,
    HealthEffectRecordResponse,
> = RpcMethod::new(
    methods::HEALTH_EFFECT_RECORD,
    RpcRole::Write,
    RpcDomain::Health,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthIntakeRecordRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intake_id: Option<Uuid>,
    pub substance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dose: Option<HealthQuantity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub form: Option<String>,
    pub occurred_at: Timestamp,
    pub timing_quality: HealthTimingQuality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthEffectRecordRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_intake_id: Option<Uuid>,
    pub effect: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
    pub observed_at: Timestamp,
    pub timing_quality: HealthTimingQuality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthDeclarationResponse<T> {
    pub payload: T,
    pub event: Value,
    pub material_id: Id<SourceMaterial>,
}

pub type HealthIntakeRecordResponse =
    HealthDeclarationResponse<HealthSubstanceIntakeRecordedPayload>;
pub type HealthEffectRecordResponse =
    HealthDeclarationResponse<HealthEffectObservationRecordedPayload>;
