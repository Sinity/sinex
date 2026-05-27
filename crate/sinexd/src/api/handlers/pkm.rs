//! PKM RPC handlers.

use super::rpc_handlers::{decode_note_content, validate_entity_link_ids, validate_entity_name};
use crate::api::rpc_server::RpcAuthContext;
use serde_json::json;
use sinex_db::pkm::PkmService;
use sinex_primitives::rpc::pkm::{
    CreateEntitiesRequest, CreateEntitiesResponse, CreateNoteRequest, CreateNoteResponse,
    LinkEntitiesRequest, LinkEntitiesResponse,
};
use sinex_primitives::{Event, Id, JsonValue, Result, SinexError, domain::EntityRelation};

pub async fn handle_create_note(
    service: &PkmService,
    request: CreateNoteRequest,
    auth: &RpcAuthContext,
) -> Result<CreateNoteResponse> {
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
    Ok(CreateNoteResponse {
        annotation_id: Id::<Event<JsonValue>>::from_uuid(annotation_id),
    })
}

pub async fn handle_create_entities(
    service: &PkmService,
    request: CreateEntitiesRequest,
    auth: &RpcAuthContext,
) -> Result<CreateEntitiesResponse> {
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
    Ok(CreateEntitiesResponse {
        entity_ids: entity_ids.into_iter().map(Id::from_uuid).collect(),
    })
}

pub async fn handle_link_entities(
    service: &PkmService,
    request: LinkEntitiesRequest,
    auth: &RpcAuthContext,
) -> Result<LinkEntitiesResponse> {
    validate_entity_link_ids(&request.from_entity_id, &request.to_entity_id)?;
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

    Ok(LinkEntitiesResponse {
        relation_id: Id::<EntityRelation>::from_uuid(relation_id),
    })
}
