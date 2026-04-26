use base64::Engine;
use sinex_db::DbPoolExt;
use sinex_db::pkm::PkmService;
use sinex_db::repositories::knowledge_graph::CreateEntity;
use sinex_gateway::{
    auth::Role,
    handlers::{handle_create_entities, handle_create_note, handle_link_entities},
    rpc_server::RpcAuthContext,
};
use sinex_primitives::rpc::pkm::{
    CreateEntitiesResponse, CreateNoteResponse, LinkEntitiesResponse,
};
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
    let auth = write_auth();

    let error = handle_link_entities(
        &service,
        serde_json::json!({
            "from_entity_id": Uuid::now_v7(),
            "to_entity_id": Uuid::now_v7(),
            "relation_type": "related_to",
            "metadata": ["not-an-object"]
        }),
        &auth,
    )
    .await
    .expect_err("malformed relation properties must fail");

    assert!(error.to_string().contains("metadata"));
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
    let response: CreateNoteResponse = serde_json::from_value(response)?;
    let annotation_id = *response.annotation_id.as_uuid();

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
            "entities": [{ "name": "Sinex", "entity_type": "project" }],
            "created_by": "forged-payload-user"
        }),
        &auth,
    )
    .await?;
    let response: CreateEntitiesResponse = serde_json::from_value(response)?;
    let entity_id = *response.entity_ids[0].as_uuid();

    let entity = ctx
        .pool()
        .knowledge_graph()
        .get_entity(entity_id.into())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("created entity missing from database"))?;

    assert_eq!(
        entity.properties["created_by"].as_str(),
        Some(auth.actor_id())
    );
    Ok(())
}

#[sinex_test]
async fn pkm_link_entities_uses_typed_contract_and_preserves_source_material(
    ctx: TestContext,
) -> TestResult<()> {
    let service = PkmService::new(ctx.pool().clone());
    let auth = write_auth();
    let material_id = ctx
        .create_source_material(Some("pkm-link-source-material"))
        .await?;
    let from = ctx
        .pool()
        .knowledge_graph()
        .create_entity(CreateEntity::project("Sinex"))
        .await?;
    let to = ctx
        .pool()
        .knowledge_graph()
        .create_entity(CreateEntity::tool("Codex"))
        .await?;

    let response = handle_link_entities(
        &service,
        serde_json::json!({
            "from_entity_id": from.id,
            "to_entity_id": to.id,
            "relation_type": "uses",
            "metadata": { "note": "operator supplied" },
            "source_material_id": material_id,
        }),
        &auth,
    )
    .await?;
    let response: LinkEntitiesResponse = serde_json::from_value(response)?;

    let relations = ctx
        .pool()
        .knowledge_graph()
        .get_entity_relations(from.id, Some("uses"), true)
        .await?;
    let relation = relations
        .into_iter()
        .find(|relation| relation.id == response.relation_id)
        .ok_or_else(|| color_eyre::eyre::eyre!("created relation missing from database"))?;

    assert_eq!(
        relation.properties["note"].as_str(),
        Some("operator supplied")
    );
    let expected_source_material_id = material_id.to_string();
    assert_eq!(
        relation.properties["_system_metadata"]["source_material_id"].as_str(),
        Some(expected_source_material_id.as_str())
    );

    let audit: (String,) = sqlx::query_as(
        "SELECT operator FROM core.operations_log \
         WHERE operation_type = 'pkm.entity.link' ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(audit.0, auth.actor_id());

    Ok(())
}
