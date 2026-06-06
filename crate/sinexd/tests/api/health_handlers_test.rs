use sinex_db::{DbPoolExt, SourceMaterialRecord};
use sinex_primitives::events::payloads::{HealthQuantity, HealthTimingQuality};
use sinex_primitives::rpc::health::{HealthEffectRecordRequest, HealthIntakeRecordRequest};
use sinex_primitives::{Id, Timestamp};
use sinexd::api::handlers::{handle_health_effect_record, handle_health_intake_record};
use sinexd::api::rpc_server::RpcAuthContext;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn health_intake_persists_material_event_without_raw_note(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let note = "sensitive freeform health note";

    let response = handle_health_intake_record(
        ctx.pool(),
        HealthIntakeRecordRequest {
            intake_id: None,
            substance: "caffeine".to_string(),
            dose: Some(HealthQuantity {
                value: 100.0,
                unit: "mg".to_string(),
                precision: Some("approximate".to_string()),
            }),
            route: Some("oral".to_string()),
            form: Some("coffee".to_string()),
            occurred_at: Timestamp::UNIX_EPOCH,
            timing_quality: HealthTimingQuality::Approximate,
            confidence: Some(0.8),
            note: Some(note.to_string()),
        },
        &auth,
    )
    .await?;

    assert_eq!(response.payload.substance, "caffeine");
    assert!(response.payload.note_redacted);
    assert_eq!(
        response.payload.timing_quality,
        HealthTimingQuality::Approximate
    );

    let event_id = response.event["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("health response event missing id"))?;
    let persisted = ctx
        .pool()
        .events()
        .get_by_id(Id::from_uuid(event_id.parse()?))
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("health intake event not persisted"))?;
    assert_eq!(persisted.source.as_str(), "manual-health");
    assert_eq!(
        persisted.event_type.as_str(),
        "health.substance.intake_recorded"
    );
    assert!(persisted.is_first_order_event());
    assert!(!serde_json::to_string(&persisted.payload)?.contains(note));

    let material = ctx
        .pool()
        .source_materials()
        .get_by_id(Id::<SourceMaterialRecord>::from_uuid(
            response.material_id.to_uuid(),
        ))
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("health source material not persisted"))?;
    assert_eq!(material.staged_by.as_deref(), Some(auth.actor_id()));
    assert_eq!(material.metadata["health_declaration_kind"], "intake");
    assert_eq!(material.metadata["freeform_notes_policy"], "redacted");
    assert!(
        !material.metadata["content_preview"]
            .as_str()
            .unwrap_or("")
            .contains(note)
    );
    assert!(!serde_json::to_string(&material.metadata)?.contains(note));

    Ok(())
}

#[sinex_test]
async fn health_effect_persists_material_event_without_raw_note(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let note = "sensitive effect note";

    let response = handle_health_effect_record(
        ctx.pool(),
        HealthEffectRecordRequest {
            observation_id: None,
            related_intake_id: None,
            effect: "calm".to_string(),
            severity: Some("mild".to_string()),
            observed_at: Timestamp::UNIX_EPOCH,
            timing_quality: HealthTimingQuality::Exact,
            confidence: Some(1.0),
            note: Some(note.to_string()),
        },
        &auth,
    )
    .await?;

    assert_eq!(response.payload.effect, "calm");
    assert!(response.payload.note_redacted);
    assert_eq!(response.payload.timing_quality, HealthTimingQuality::Exact);

    let event_id = response.event["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("health response event missing id"))?;
    let persisted = ctx
        .pool()
        .events()
        .get_by_id(Id::from_uuid(event_id.parse()?))
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("health effect event not persisted"))?;
    assert_eq!(persisted.source.as_str(), "manual-health");
    assert_eq!(persisted.event_type.as_str(), "health.effect.observed");
    assert!(persisted.is_first_order_event());
    assert!(!serde_json::to_string(&persisted.payload)?.contains(note));
    assert_eq!(
        persisted.payload["timing_quality"],
        serde_json::json!("exact")
    );

    Ok(())
}
