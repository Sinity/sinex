//! Prediction-domain RPC handlers.
use crate::api::rpc_server::RpcAuthContext;
use serde_json::{Value, json};
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial as DbSourceMaterial;
use sinex_primitives::events::payloads::{PredictionRegisteredPayload, PredictionResolvedPayload};
use sinex_primitives::events::{EventPayload, SourceMaterial};
use sinex_primitives::prediction_domain::{
    PredictionState, calibration_report, reduce_prediction_registered, reduce_prediction_resolved,
    validate_probability,
};
use sinex_primitives::rpc::predictions::*;
use sinex_primitives::{Id, Result, SinexError, Timestamp, Uuid};
use sqlx::PgPool;

pub async fn handle_predictions_register(
    pool: &PgPool,
    req: PredictionRegisterRequest,
    auth: &RpcAuthContext,
) -> Result<PredictionEventResponse> {
    validate_probability(req.probability)?;
    let prediction_id = req.prediction_id.unwrap_or_else(Uuid::now_v7);
    if req.statement.trim().is_empty() || req.resolution_criteria.trim().is_empty() {
        return Err(SinexError::validation(
            "prediction statement and resolution criteria must not be empty",
        ));
    }
    if query_prediction(pool, prediction_id).await?.is_some() {
        return Err(SinexError::validation("prediction id already exists"));
    }
    let material_id =
        register_material(pool, auth, prediction_id, "registered", &req.statement).await?;
    let payload = PredictionRegisteredPayload {
        prediction_id,
        statement: req.statement.trim().to_string(),
        probability: req.probability,
        resolution_criteria: req.resolution_criteria.trim().to_string(),
        due_at: req.due_at,
        predictor: req.predictor.unwrap_or_else(|| auth.actor_id().to_string()),
        horizon: req.horizon,
    };
    let event = payload
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(Timestamp::now())
        .build()?;
    let inserted = pool.events().insert(event).await?;
    let event_id = *inserted
        .id
        .as_ref()
        .ok_or_else(|| SinexError::invalid_state("prediction.registered event missing id"))?
        .as_uuid();
    let prediction = reduce_prediction_registered(
        event_id,
        payload.clone().into(),
        inserted.ts_orig.unwrap_or_else(Timestamp::now),
    )?;
    Ok(PredictionEventResponse {
        prediction,
        event: serde_json::to_value(inserted).map_err(|e| {
            SinexError::serialization("failed to serialize prediction event").with_std_error(&e)
        })?,
    })
}

pub async fn handle_predictions_resolve(
    pool: &PgPool,
    req: PredictionResolveRequest,
    auth: &RpcAuthContext,
) -> Result<PredictionEventResponse> {
    let prior = query_prediction(pool, req.prediction_id)
        .await?
        .ok_or_else(|| SinexError::not_found("prediction not found"))?;
    if prior.outcome.is_some() {
        return Err(SinexError::validation("prediction is already resolved"));
    }
    let resolved_at = req.resolved_at.unwrap_or_else(Timestamp::now);
    let material_id =
        register_material(pool, auth, req.prediction_id, "resolved", &prior.statement).await?;
    let payload = PredictionResolvedPayload {
        prediction_id: req.prediction_id,
        outcome: req.outcome,
        resolved_at,
        resolver: auth.actor_id().to_string(),
        evidence_refs: req.evidence_refs,
    };
    let event = payload
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(resolved_at)
        .build()?;
    let inserted = pool.events().insert(event).await?;
    let event_id = *inserted
        .id
        .as_ref()
        .ok_or_else(|| SinexError::invalid_state("prediction.resolved event missing id"))?
        .as_uuid();
    let prediction = reduce_prediction_resolved(prior, event_id, payload.clone().into())?;
    Ok(PredictionEventResponse {
        prediction,
        event: serde_json::to_value(inserted).map_err(|e| {
            SinexError::serialization("failed to serialize prediction event").with_std_error(&e)
        })?,
    })
}

pub async fn handle_predictions_report(
    pool: &PgPool,
    req: PredictionReportRequest,
) -> Result<PredictionReportResponse> {
    let rows = sqlx::query!(r#"SELECT id as "id!: Uuid", event_type, payload, ts_orig as "ts_orig!: Timestamp" FROM core.events WHERE source = 'prediction' AND event_type IN ('prediction.registered', 'prediction.resolved') ORDER BY ts_orig ASC, id ASC"#).fetch_all(pool).await.map_err(|e| SinexError::database("failed to query prediction events").with_std_error(&e))?;
    let mut states = Vec::new();
    for row in rows {
        if row.event_type == "prediction.registered" {
            let p: PredictionRegisteredPayload =
                serde_json::from_value(row.payload).map_err(|e| {
                    SinexError::serialization("invalid prediction.registered payload")
                        .with_std_error(&e)
                })?;
            if req.predictor.as_deref().is_none_or(|v| v == p.predictor) {
                states.push(reduce_prediction_registered(row.id, p.into(), row.ts_orig)?);
            }
        } else if let Some(state) = states.iter_mut().find(|s: &&mut PredictionState| {
            s.prediction_id
                == row
                    .payload
                    .get("prediction_id")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(Uuid::nil)
        }) {
            let p: PredictionResolvedPayload =
                serde_json::from_value(row.payload).map_err(|e| {
                    SinexError::serialization("invalid prediction.resolved payload")
                        .with_std_error(&e)
                })?;
            *state = reduce_prediction_resolved((*state).clone(), row.id, p.into())?;
        }
    }
    let resolved_count = states.iter().filter(|s| s.outcome.is_some()).count();
    let unresolved_count = states.len() - resolved_count;
    Ok(PredictionReportResponse {
        calibration: calibration_report(&states),
        predictions: states,
        resolved_count,
        unresolved_count,
    })
}

async fn query_prediction(pool: &PgPool, id: Uuid) -> Result<Option<PredictionState>> {
    let req = PredictionReportRequest::default();
    Ok(handle_predictions_report(pool, req)
        .await?
        .predictions
        .into_iter()
        .find(|p| p.prediction_id == id))
}

async fn register_material(
    pool: &PgPool,
    auth: &RpcAuthContext,
    id: Uuid,
    action: &str,
    detail: &str,
) -> Result<Uuid> {
    let material_id = Uuid::now_v7();
    let uri = format!("sinexctl://predictions/{id}/{action}/{material_id}");
    let material = DbSourceMaterial::blob_text(uri.clone())
        .with_content_preview(detail.to_string())
        .with_metadata(
            json!({"prediction_id": id, "action": action, "capture_surface": "sinexctl"}),
        )
        .with_staged_by(auth.actor_id().to_string());
    Ok(pool
        .source_materials()
        .register_external_material(material_id, material)
        .await
        .map_err(|e| {
            SinexError::processing("failed to register prediction source material")
                .with_std_error(&e)
        })?
        .id)
}
