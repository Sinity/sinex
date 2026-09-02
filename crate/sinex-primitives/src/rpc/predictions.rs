//! Prediction-domain RPC contracts.
use crate::prediction_domain::{PredictionState, PredictorCalibration};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use crate::{Timestamp, Uuid};
use serde::{Deserialize, Serialize};

pub const PREDICTIONS_REGISTER_METHOD: RpcMethod<
    PredictionRegisterRequest,
    PredictionEventResponse,
> = RpcMethod::new(
    methods::PREDICTIONS_REGISTER,
    RpcRole::Write,
    RpcDomain::Predictions,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);
pub const PREDICTIONS_RESOLVE_METHOD: RpcMethod<PredictionResolveRequest, PredictionEventResponse> =
    RpcMethod::new(
        methods::PREDICTIONS_RESOLVE,
        RpcRole::Write,
        RpcDomain::Predictions,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );
pub const PREDICTIONS_REPORT_METHOD: RpcMethod<PredictionReportRequest, PredictionReportResponse> =
    RpcMethod::new(
        methods::PREDICTIONS_REPORT,
        RpcRole::ReadOnly,
        RpcDomain::Predictions,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PredictionRegisterRequest {
    pub prediction_id: Option<Uuid>,
    pub statement: String,
    pub probability: f64,
    pub resolution_criteria: String,
    pub due_at: Timestamp,
    pub predictor: Option<String>,
    pub horizon: Option<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PredictionResolveRequest {
    pub prediction_id: Uuid,
    pub outcome: bool,
    pub resolved_at: Option<Timestamp>,
    pub evidence_refs: Vec<String>,
}
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PredictionReportRequest {
    pub predictor: Option<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PredictionEventResponse {
    pub prediction: PredictionState,
    pub event: serde_json::Value,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PredictionReportResponse {
    pub predictions: Vec<PredictionState>,
    pub calibration: Vec<PredictorCalibration>,
    pub resolved_count: usize,
    pub unresolved_count: usize,
}
