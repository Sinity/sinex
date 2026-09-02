//! Prediction lifecycle event payloads.
use crate::prediction_domain::{PredictionRegisteredInput, PredictionResolvedInput};
use crate::{Timestamp, Uuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "prediction",
    event_type = "prediction.registered",
    version = "1.0.0"
)]
pub struct PredictionRegisteredPayload {
    pub prediction_id: Uuid,
    pub statement: String,
    pub probability: f64,
    pub resolution_criteria: String,
    pub due_at: Timestamp,
    pub predictor: String,
    pub horizon: Option<String>,
}
impl From<PredictionRegisteredPayload> for PredictionRegisteredInput {
    fn from(p: PredictionRegisteredPayload) -> Self {
        Self {
            prediction_id: p.prediction_id,
            statement: p.statement,
            probability: p.probability,
            resolution_criteria: p.resolution_criteria,
            due_at: p.due_at,
            predictor: p.predictor,
            horizon: p.horizon,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "prediction",
    event_type = "prediction.resolved",
    version = "1.0.0"
)]
pub struct PredictionResolvedPayload {
    pub prediction_id: Uuid,
    pub outcome: bool,
    pub resolved_at: Timestamp,
    pub resolver: String,
    pub evidence_refs: Vec<String>,
}
impl From<PredictionResolvedPayload> for PredictionResolvedInput {
    fn from(p: PredictionResolvedPayload) -> Self {
        Self {
            prediction_id: p.prediction_id,
            outcome: p.outcome,
            resolved_at: p.resolved_at,
            resolver: p.resolver,
            evidence_refs: p.evidence_refs,
        }
    }
}
