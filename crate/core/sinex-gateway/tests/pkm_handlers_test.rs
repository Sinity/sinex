use base64::Engine;
use sinex_gateway::handlers::{handle_create_entities, handle_create_note, handle_link_entities};
use sinex_db::DbPoolExt;
use sinex_services::PkmService;
use sinex_primitives::{Uuid, events::DynamicPayload};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn pkm_create_note_rejects_malformed_optional_tags(ctx: TestContext) -> TestResult<()> {
    let service = PkmService::new(ctx.pool().clone());
    let material_id = ctx.create_source_material(Some("pkm-note-test")).await?;
    let event = DynamicPayload::new(
        "gateway.test",
        "gateway.inline",
        serde_json::json!({ "message": "pkm" }),
    )
    .from_material(material_id)
    .build()?;
    let event = ctx.pool().events().insert(event).await?;

    let error = handle_create_note(
        &service,
        serde_json::json!({
            "event_id": event.id.expect("published event must have id"),
            "content": base64::engine::general_purpose::STANDARD.encode("note"),
            "tags": "not-an-array"
        }),
    )
    .await
    .expect_err("malformed tags must fail");

    assert!(error.to_string().contains("tags"));
    Ok(())
}

#[sinex_test]
async fn pkm_create_entities_rejects_malformed_entities_param(ctx: TestContext) -> TestResult<()> {
    let service = PkmService::new(ctx.pool().clone());

    let error = handle_create_entities(
        &service,
        serde_json::json!({
            "source_material_id": Uuid::now_v7(),
            "entities": "not-an-array"
        }),
    )
    .await
    .expect_err("malformed entities must fail");

    assert!(error.to_string().contains("entities"));
    Ok(())
}

#[sinex_test]
async fn pkm_link_entities_rejects_malformed_properties(ctx: TestContext) -> TestResult<()> {
    let service = PkmService::new(ctx.pool().clone());

    let error = handle_link_entities(
        &service,
        serde_json::json!({
            "from_entity_id": Uuid::now_v7(),
            "to_entity_id": Uuid::now_v7(),
            "relationship_type": "related_to",
            "properties": ["not-an-object"]
        }),
    )
    .await
    .expect_err("malformed relation properties must fail");

    assert!(error.to_string().contains("properties"));
    Ok(())
}
