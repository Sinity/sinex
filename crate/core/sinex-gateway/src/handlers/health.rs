//! Health declaration RPC handlers.

use serde_json::{Value, json};
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial as DbSourceMaterial;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::payloads::{
    HealthEffectObservationRecordedPayload, HealthSubstanceIntakeRecordedPayload,
};
use sinex_primitives::rpc::health::{
    HealthDeclarationResponse, HealthEffectRecordRequest, HealthEffectRecordResponse,
    HealthIntakeRecordRequest, HealthIntakeRecordResponse,
};
use sinex_primitives::{Id, Result, SinexError, Timestamp, Uuid};
use sqlx::PgPool;

use crate::rpc_server::RpcAuthContext;

pub async fn handle_health_intake_record(
    pool: &PgPool,
    req: HealthIntakeRecordRequest,
    auth: &RpcAuthContext,
) -> Result<HealthIntakeRecordResponse> {
    let substance = req.substance.trim();
    if substance.is_empty() {
        return Err(SinexError::validation(
            "health.intake.record: substance must not be empty",
        ));
    }
    validate_confidence(req.confidence, "health.intake.record")?;

    let intake_id = req.intake_id.unwrap_or_else(Uuid::now_v7);
    let note_redacted = has_note(&req.note);
    let material_id = register_health_material(
        pool,
        auth,
        "intake",
        intake_id,
        json!({
            "substance": substance,
            "note_redacted": note_redacted,
        }),
    )
    .await?;
    let payload = HealthSubstanceIntakeRecordedPayload {
        intake_id,
        substance: substance.to_string(),
        dose: req.dose,
        route: trim_optional(req.route),
        form: trim_optional(req.form),
        occurred_at: req.occurred_at,
        timing_quality: req.timing_quality,
        confidence: req.confidence,
        note_redacted,
    };
    persist_health_event(pool, payload, material_id, req.occurred_at).await
}

pub async fn handle_health_effect_record(
    pool: &PgPool,
    req: HealthEffectRecordRequest,
    auth: &RpcAuthContext,
) -> Result<HealthEffectRecordResponse> {
    let effect = req.effect.trim();
    if effect.is_empty() {
        return Err(SinexError::validation(
            "health.effect.record: effect must not be empty",
        ));
    }
    validate_confidence(req.confidence, "health.effect.record")?;

    let observation_id = req.observation_id.unwrap_or_else(Uuid::now_v7);
    let note_redacted = has_note(&req.note);
    let material_id = register_health_material(
        pool,
        auth,
        "effect",
        observation_id,
        json!({
            "effect": effect,
            "related_intake_id": req.related_intake_id.map(|id| id.to_string()),
            "note_redacted": note_redacted,
        }),
    )
    .await?;
    let payload = HealthEffectObservationRecordedPayload {
        observation_id,
        related_intake_id: req.related_intake_id,
        effect: effect.to_string(),
        severity: trim_optional(req.severity),
        observed_at: req.observed_at,
        timing_quality: req.timing_quality,
        confidence: req.confidence,
        note_redacted,
    };
    persist_health_event(pool, payload, material_id, req.observed_at).await
}

async fn persist_health_event<T>(
    pool: &PgPool,
    payload: T,
    material_id: Uuid,
    ts_orig: Timestamp,
) -> Result<HealthDeclarationResponse<T>>
where
    T: EventPayload + Clone + serde::Serialize,
{
    let event = payload
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(ts_orig)
        .build()?;
    let inserted = pool.events().insert(event).await?;
    let _inserted_id = inserted.id.ok_or_else(|| {
        SinexError::invalid_state("health declaration persisted event missing id")
    })?;

    Ok(HealthDeclarationResponse {
        payload,
        event: serde_json::to_value(inserted).map_err(|error| {
            SinexError::serialization("health declaration: failed to serialize event")
                .with_std_error(&error)
        })?,
        material_id: Id::<SourceMaterial>::from_uuid(material_id),
    })
}

async fn register_health_material(
    pool: &PgPool,
    auth: &RpcAuthContext,
    kind: &str,
    declaration_id: Uuid,
    metadata: Value,
) -> Result<Uuid> {
    let material_id = Uuid::now_v7();
    let source_uri = format!("sinexctl://health/{kind}/{declaration_id}/{material_id}");
    let material = DbSourceMaterial::blob_text(source_uri.clone())
        .with_content_preview(format!(
            "{kind} declaration {declaration_id}; notes redacted"
        ))
        .with_metadata(json!({
            "source_uri": source_uri,
            "health_declaration_id": declaration_id,
            "health_declaration_kind": kind,
            "capture_surface": "sinexctl",
            "freeform_notes_policy": "redacted",
            "fields": metadata,
        }))
        .with_staged_by(auth.actor_id().to_string());
    let record = pool
        .source_materials()
        .register_external_material(material_id, material)
        .await
        .map_err(|error| {
            SinexError::processing("failed to register health declaration source material")
                .with_context("declaration_id", declaration_id.to_string())
                .with_context("kind", kind)
                .with_std_error(&error)
        })?;
    Ok(record.id)
}

fn validate_confidence(confidence: Option<f64>, method: &str) -> Result<()> {
    if let Some(value) = confidence
        && !(0.0..=1.0).contains(&value)
    {
        return Err(SinexError::validation(format!(
            "{method}: confidence must be between 0 and 1"
        ))
        .with_context("confidence", value.to_string()));
    }
    Ok(())
}

fn trim_optional(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn has_note(value: &Option<String>) -> bool {
    value.as_ref().is_some_and(|note| !note.trim().is_empty())
}
