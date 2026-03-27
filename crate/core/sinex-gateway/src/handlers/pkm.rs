//! PKM RPC handlers.

use super::rpc_handlers::{RpcParams, decode_note_content, validate_entity_link_ids, validate_entity_name};
use crate::rpc_server::RpcAuthContext;
use color_eyre::eyre::{Context, ContextCompat, Result};
use serde_json::{Value, json};
use sinex_primitives::{Event, Id, JsonValue, domain::Entity};
use sinex_services::PkmService;

pub async fn handle_create_note(
    service: &PkmService,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let event_id = Id::<Event<JsonValue>>::from_uuid(params.require_uuid("event_id")?);
    let content_b64 = params.require_str("content").wrap_err("Missing content")?;

    let content = decode_note_content(content_b64)?;

    let tags = params
        .optional_array("tags")
        ?
        .map(|arr| {
            arr.iter()
                .filter_map(|value| value.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let annotation_id = service
        .create_note(event_id, &content, tags, auth.actor_id(), None)
        .await?;
    Ok(json!({ "annotation_id": annotation_id.to_string() }))
}

pub async fn handle_create_entities(
    service: &PkmService,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let source_material_id = params.require_uuid("source_material_id")?;

    let entities = params
        .optional_array("entities")
        ?
        .map(|arr| {
            arr.iter()
                .map(|value| {
                    let name = value
                        .get("name")
                        .and_then(|field| field.as_str())
                        .wrap_err("Missing entity name")?;
                    validate_entity_name(name)?;
                    let entity_type = value
                        .get("type")
                        .and_then(|field| field.as_str())
                        .wrap_err("Missing entity type")?;
                    Ok((name.to_string(), entity_type.to_string()))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();

    let entity_ids = service
        .create_entities_from_source_material(source_material_id, entities, auth.actor_id())
        .await?;
    Ok(
        json!({ "entity_ids": entity_ids.iter().map(std::string::ToString::to_string).collect::<Vec<_>>() }),
    )
}

pub async fn handle_link_entities(service: &PkmService, params: Value) -> Result<Value> {
    let params = RpcParams::new(&params);
    let from_entity_id = Id::<Entity>::from_uuid(params.require_uuid("from_entity_id")?);
    let to_entity_id = Id::<Entity>::from_uuid(params.require_uuid("to_entity_id")?);
    validate_entity_link_ids(&from_entity_id, &to_entity_id)?;

    let relationship_type = params.require_str("relationship_type")?;
    let properties = params
        .optional_object("properties")
        ?
        .map(|obj| {
            obj.iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default();

    let relation_id = service
        .link_entities(
            from_entity_id,
            to_entity_id,
            relationship_type,
            properties,
            None,
        )
        .await?;

    Ok(json!({ "relation_id": relation_id.to_string() }))
}
