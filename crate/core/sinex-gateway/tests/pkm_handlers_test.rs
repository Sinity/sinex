use base64::Engine;
use sinex_gateway::{
    auth::Role,
    handlers::{handle_create_entities, handle_create_note, handle_link_entities},
    rpc_server::RpcAuthContext,
};
use sinex_db::DbPoolExt;
use sinex_services::PkmService;
use sinex_primitives::{Uuid, events::DynamicPayload, temporal};
use xtask::sandbox::prelude::*;

fn write_auth() -> RpcAuthContext {
    RpcAuthContext {
        token_prefix: "pkmtest".to_string(),
        actor_id: "token:pkmtest".to_string(),
        authenticated_at: temporal::now(),
        role: Role::Write,
    }
}

#[sinex_test]
async fn pkm_create_note_rejects_malformed_optional_tags(ctx: TestContext) -> TestResult<()> {
    let service = PkmService::new(ctx.pool().clone());
    let auth = write_auth();
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
        &auth,
    )
    .await
    .expect_err("malformed tags must fail");

    assert!(error.to_string().contains("tags"));
    Ok(())
}

#[sinex_test]
async fn pkm_create_entities_rejects_malformed_entities_param(ctx: TestContext) -> TestResult<()> {
    let service = PkmService::new(ctx.pool().clone());
    let auth = write_auth();

    let error = handle_create_entities(
        &service,
        serde_json::json!({
            "source_material_id": Uuid::now_v7(),
            "entities": "not-an-array"
        }),
        &auth,
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

#[sinex_test]
async fn pkm_create_note_uses_authenticated_actor_over_payload_created_by(
    ctx: TestContext,
) -> TestResult<()> {
    let service = PkmService::new(ctx.pool().clone());
    let auth = write_auth();
    let material_id = ctx
        .create_source_material(Some("pkm-note-auth-created-by"))
        .await?;
    let event = DynamicPayload::new(
        "gateway.test",
        "gateway.inline",
        serde_json::json!({ "message": "pkm auth" }),
    )
    .from_material(material_id)
    .build()?;
    let event = ctx.pool().events().insert(event).await?;
    let event_id = event.id.expect("published event must have id");

    let response = handle_create_note(
        &service,
        serde_json::json!({
            "event_id": event_id,
            "content": base64::engine::general_purpose::STANDARD.encode("note"),
            "created_by": "forged-payload-user"
        }),
        &auth,
    )
    .await?;
    let annotation_id = response["annotation_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("annotation_id missing from response"))?
        .parse::<Uuid>()?;

    let annotations = ctx.pool().events().get_annotations(event_id).await?;
    let annotation = annotations
        .into_iter()
        .find(|annotation| *annotation.id.as_uuid() == annotation_id)
        .ok_or_else(|| color_eyre::eyre::eyre!("created annotation missing from database"))?;

    assert_eq!(annotation.created_by, auth.actor_id());
    Ok(())
}

#[sinex_test]
async fn pkm_create_entities_uses_authenticated_actor_over_payload_created_by(
    ctx: TestContext,
) -> TestResult<()> {
    let service = PkmService::new(ctx.pool().clone());
    let auth = write_auth();
    let material_id = ctx
        .create_source_material(Some("pkm-entities-auth-created-by"))
        .await?;

    let response = handle_create_entities(
        &service,
        serde_json::json!({
            "source_material_id": material_id.as_uuid(),
            "entities": [{ "name": "Sinex", "type": "project" }],
            "created_by": "forged-payload-user"
        }),
        &auth,
    )
    .await?;
    let entity_id = response["entity_ids"][0]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("entity_ids missing from response"))?
        .parse::<Uuid>()?;

    let entity = ctx
        .pool()
        .knowledge_graph()
        .get_entity(entity_id.into())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("created entity missing from database"))?;

    assert_eq!(entity.properties["created_by"].as_str(), Some(auth.actor_id()));
    Ok(())
}
