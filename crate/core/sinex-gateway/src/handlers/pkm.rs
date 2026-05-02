//! PKM RPC handlers.

use super::rpc_handlers::{
    RpcParams, decode_note_content, validate_entity_link_ids, validate_entity_name,
};
use crate::rpc_server::RpcAuthContext;
use serde_json::{Value, json};
use sinex_db::pkm::PkmService;
use sinex_primitives::rpc::pkm::{
    CreateEntitiesRequest, CreateEntitiesResponse, CreateNoteRequest, CreateNoteResponse,
    LinkEntitiesRequest, LinkEntitiesResponse,
};
use sinex_primitives::{Event, Id, JsonValue, Result, SinexError, domain::EntityRelation};

fn pkm_validation_error(message: &'static str, error: color_eyre::eyre::Report) -> SinexError {
    SinexError::validation(message).with_source(error.to_string())
}

pub async fn handle_create_note(
    service: &PkmService,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    RpcParams::new(&params)
        .optional_array("tags")
        .map_err(|error| pkm_validation_error("invalid `tags` parameter", error))?;
    let request: CreateNoteRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("invalid `pkm.create_note` request").with_std_error(&error)
    })?;
    let content = decode_note_content(&request.content)
        .map_err(|error| pkm_validation_error("invalid note content", error))?;

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
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize `pkm.create_note` response")
            .with_std_error(&error)
    })?)
}

pub async fn handle_create_entities(
    service: &PkmService,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    RpcParams::new(&params)
        .optional_array("entities")
        .map_err(|error| pkm_validation_error("invalid `entities` parameter", error))?;
    let request: CreateEntitiesRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("invalid `pkm.create_entities` request").with_std_error(&error)
    })?;
    let entities = request
        .entities
        .iter()
        .map(|entity| {
            validate_entity_name(&entity.name)
                .map_err(|error| pkm_validation_error("invalid entity name", error))?;
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
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize `pkm.create_entities` response")
            .with_std_error(&error)
    })?)
}

pub async fn handle_link_entities(
    service: &PkmService,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    RpcParams::new(&params)
        .optional_object("metadata")
        .map_err(|error| pkm_validation_error("invalid `metadata` parameter", error))?;
    let request: LinkEntitiesRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("invalid `pkm.link_entities` request").with_std_error(&error)
    })?;
    validate_entity_link_ids(&request.from_entity_id, &request.to_entity_id)
        .map_err(|error| pkm_validation_error("invalid entity link", error))?;
    let metadata = request.metadata.unwrap_or_else(|| json!({}));
    let properties = metadata
        .as_object()
        .ok_or_else(|| SinexError::validation("metadata must be an object"))?
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    let relation_id = service
        .link_entities(
            request.from_entity_id,
            request.to_entity_id,
            request.relation_type.as_ref(),
            properties,
            request.source_material_id.map(|id| *id.as_uuid()),
            auth.actor_id(),
        )
        .await?;

    Ok(serde_json::to_value(LinkEntitiesResponse {
        relation_id: Id::<EntityRelation>::from_uuid(relation_id),
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize `pkm.link_entities` response")
            .with_std_error(&error)
    })?)
}
