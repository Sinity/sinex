//! PKM RPC handlers.

use super::rpc_handlers::{
    RpcParams, decode_note_content, validate_entity_link_ids, validate_entity_name,
};
use crate::rpc_server::RpcAuthContext;
use color_eyre::eyre::{Context, Result, eyre};
use serde_json::{Value, json};
use sinex_primitives::rpc::pkm::{
    CreateEntitiesRequest, CreateEntitiesResponse, CreateNoteRequest, CreateNoteResponse,
    LinkEntitiesRequest, LinkEntitiesResponse,
};
use sinex_primitives::{Event, Id, JsonValue, domain::EntityRelation};
use sinex_services::PkmService;

pub async fn handle_create_note(
    service: &PkmService,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    RpcParams::new(&params)
        .optional_array("tags")
        .wrap_err("invalid `tags` parameter")?;
    let request: CreateNoteRequest =
        serde_json::from_value(params).wrap_err("invalid `pkm.create_note` request")?;
    let content = decode_note_content(&request.content)?;

    let annotation_id = service
        .create_note(
            request.event_id,
            &content,
            request.tags,
            auth.actor_id(),
            None,
        )
        .await?;
    Ok(serde_json::to_value(CreateNoteResponse {
        annotation_id: Id::<Event<JsonValue>>::from_uuid(annotation_id),
    })?)
}

pub async fn handle_create_entities(
    service: &PkmService,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    RpcParams::new(&params)
        .optional_array("entities")
        .wrap_err("invalid `entities` parameter")?;
    let request: CreateEntitiesRequest =
        serde_json::from_value(params).wrap_err("invalid `pkm.create_entities` request")?;
    let entities = request
        .entities
        .iter()
        .map(|entity| {
            validate_entity_name(&entity.name)?;
            Ok((entity.name.clone(), entity.entity_type.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;

    let entity_ids = service
        .create_entities_from_source_material(
            *request.source_material_id.as_uuid(),
            entities,
            auth.actor_id(),
        )
        .await?;
    Ok(serde_json::to_value(CreateEntitiesResponse {
        entity_ids: entity_ids.into_iter().map(Id::from_uuid).collect(),
    })?)
}

pub async fn handle_link_entities(service: &PkmService, params: Value) -> Result<Value> {
    RpcParams::new(&params)
        .optional_object("metadata")
        .wrap_err("invalid `metadata` parameter")?;
    let request: LinkEntitiesRequest =
        serde_json::from_value(params).wrap_err("invalid `pkm.link_entities` request")?;
    validate_entity_link_ids(&request.from_entity_id, &request.to_entity_id)?;
    let metadata = request.metadata.unwrap_or_else(|| json!({}));
    let properties = metadata
        .as_object()
        .ok_or_else(|| eyre!("metadata must be an object"))?
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    let relation_id = service
        .link_entities(
            request.from_entity_id,
            request.to_entity_id,
            &request.relation_type.to_string(),
            properties,
            request.source_material_id.map(|id| *id.as_uuid()),
        )
        .await?;

    Ok(serde_json::to_value(LinkEntitiesResponse {
        relation_id: Id::<EntityRelation>::from_uuid(relation_id),
    })?)
}
