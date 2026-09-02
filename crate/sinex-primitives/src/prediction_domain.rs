//! Event-native prediction registration, resolution, and calibration.

use crate::{Result, SinexError, Timestamp, Uuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const PREDICTION_REDUCER_DOMAIN_ID: &str = "predictions.current";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PredictionState {
    pub prediction_id: Uuid,
    pub statement: String,
    pub probability: f64,
    pub resolution_criteria: String,
    pub due_at: Timestamp,
    pub predictor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub horizon: Option<String>,
    pub outcome: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolution_evidence_refs: Vec<String>,
    pub last_event_id: Uuid,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PredictionRegisteredInput {
    pub prediction_id: Uuid,
    pub statement: String,
    pub probability: f64,
    pub resolution_criteria: String,
    pub due_at: Timestamp,
    pub predictor: String,
    pub horizon: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PredictionResolvedInput {
    pub prediction_id: Uuid,
    pub outcome: bool,
    pub resolved_at: Timestamp,
    pub resolver: String,
    pub evidence_refs: Vec<String>,
}

pub fn validate_probability(probability: f64) -> Result<()> {
    if probability.is_finite() && (0.0..=1.0).contains(&probability) {
        Ok(())
    } else {
        Err(SinexError::validation(
            "prediction probability must be between 0 and 1",
        ))
    }
}

pub fn reduce_prediction_registered(
    event_id: Uuid,
    input: PredictionRegisteredInput,
    observed_at: Timestamp,
) -> Result<PredictionState> {
    validate_probability(input.probability)?;
    if input.statement.trim().is_empty() || input.resolution_criteria.trim().is_empty() {
        return Err(SinexError::validation(
            "prediction statement and resolution criteria must not be empty",
        ));
    }
    Ok(PredictionState {
        prediction_id: input.prediction_id,
        statement: input.statement,
        probability: input.probability,
        resolution_criteria: input.resolution_criteria,
        due_at: input.due_at,
        predictor: input.predictor,
        horizon: input.horizon,
        outcome: None,
        resolution_evidence_refs: Vec::new(),
        last_event_id: event_id,
        updated_at: observed_at,
    })
}

pub fn reduce_prediction_resolved(
    mut state: PredictionState,
    event_id: Uuid,
    input: PredictionResolvedInput,
) -> Result<PredictionState> {
    if state.outcome.is_some() {
        return Err(SinexError::validation("prediction is already resolved"));
    }
    state.outcome = Some(input.outcome);
    state.resolution_evidence_refs = input.evidence_refs;
    state.last_event_id = event_id;
    state.updated_at = input.resolved_at;
    Ok(state)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CalibrationBucket {
    pub lower_probability: f64,
    pub upper_probability: f64,
    pub count: usize,
    pub mean_predicted_probability: f64,
    pub observed_frequency: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PredictorCalibration {
    pub predictor: String,
    pub resolved_count: usize,
    pub brier_score: f64,
    pub buckets: Vec<CalibrationBucket>,
}

pub fn calibration_report(states: &[PredictionState]) -> Vec<PredictorCalibration> {
    let mut predictors: Vec<String> = states.iter().map(|s| s.predictor.clone()).collect();
    predictors.sort();
    predictors.dedup();
    predictors
        .into_iter()
        .map(|predictor| {
            let resolved: Vec<&PredictionState> = states
                .iter()
                .filter(|s| s.predictor == predictor && s.outcome.is_some())
                .collect();
            let brier_score = if resolved.is_empty() {
                0.0
            } else {
                resolved
                    .iter()
                    .map(|s| (s.probability - f64::from(s.outcome == Some(true))).powi(2))
                    .sum::<f64>()
                    / resolved.len() as f64
            };
            let buckets = (0..10)
                .filter_map(|index| {
                    let lower = index as f64 / 10.0;
                    let upper = if index == 9 {
                        1.0
                    } else {
                        (index + 1) as f64 / 10.0
                    };
                    let rows: Vec<&PredictionState> = resolved
                        .iter()
                        .copied()
                        .filter(|s| s.probability >= lower && (s.probability < upper || index == 9))
                        .collect();
                    if rows.is_empty() {
                        return None;
                    }
                    Some(CalibrationBucket {
                        lower_probability: lower,
                        upper_probability: upper,
                        count: rows.len(),
                        mean_predicted_probability: rows.iter().map(|s| s.probability).sum::<f64>()
                            / rows.len() as f64,
                        observed_frequency: rows.iter().filter(|s| s.outcome == Some(true)).count()
                            as f64
                            / rows.len() as f64,
                    })
                })
                .collect();
            PredictorCalibration {
                predictor,
                resolved_count: resolved.len(),
                brier_score,
                buckets,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calibration_report_computes_known_brier_score() {
        let now = Timestamp::now();
        let states = [
            PredictionState {
                prediction_id: Uuid::now_v7(),
                statement: "a".into(),
                probability: 0.8,
                resolution_criteria: "x".into(),
                due_at: now,
                predictor: "agent".into(),
                horizon: None,
                outcome: Some(true),
                resolution_evidence_refs: vec![],
                last_event_id: Uuid::now_v7(),
                updated_at: now,
            },
            PredictionState {
                prediction_id: Uuid::now_v7(),
                statement: "b".into(),
                probability: 0.4,
                resolution_criteria: "x".into(),
                due_at: now,
                predictor: "agent".into(),
                horizon: None,
                outcome: Some(false),
                resolution_evidence_refs: vec![],
                last_event_id: Uuid::now_v7(),
                updated_at: now,
            },
        ];
        let report = calibration_report(&states);
        assert_eq!(report[0].resolved_count, 2);
        assert!((report[0].brier_score - 0.20).abs() < f64::EPSILON);
    }
}
