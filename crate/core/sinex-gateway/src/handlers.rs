//! Shared RPC method handlers

use color_eyre::eyre::{Context, Result, WrapErr};
use serde_json::{json, Value};
use sinex_db::models::Entity;
use sinex_db::models::Event;
use sinex_services::{AnalyticsService, ContentService, PkmService, SearchQuery, SearchService};
use sinex_types::{ulid::Ulid, Id};

// Analytics handlers

pub async fn handle_event_count_by_source(
    service: &AnalyticsService,
    params: Value,
) -> Result<Value> {
    use chrono::{Duration, Utc};

    let days_back = params
        .get("days_back")
        .and_then(|v| v.as_i64())
        .unwrap_or(7);

    let end_time = Utc::now();
    let start_time = end_time - Duration::days(days_back);

    let counts = service
        .get_event_count_by_source(Some(start_time), Some(end_time))
        .await?;
    Ok(json!(counts))
}

pub async fn handle_activity_heatmap(service: &AnalyticsService, params: Value) -> Result<Value> {
    let bucket_size_minutes = params
        .get("bucket_size_minutes")
        .and_then(|v| v.as_i64())
        .unwrap_or(60) as i32;

    let limit = params.get("limit").and_then(|v| v.as_i64()).unwrap_or(100) as i32;

    let heatmap = service.activity_heatmap(bucket_size_minutes, limit).await?;
    Ok(json!(heatmap))
}

// PKM handlers

pub async fn handle_create_note(service: &PkmService, params: Value) -> Result<Value> {
    let event_id = params
        .get("event_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Ulid>().ok())
        .map(Id::<Event>::from_ulid)
        .wrap_err("Invalid or missing event_id")?;

    let content = params
        .get("content")
        .and_then(|v| v.as_str())
        .wrap_err("Missing content")?;

    let tags = params
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let created_by = params
        .get("created_by")
        .and_then(|v| v.as_str())
        .unwrap_or("sinex-host");

    let annotation_id = service
        .create_note(event_id, content, tags, created_by, None)
        .await?;
    Ok(json!({ "annotation_id": annotation_id.to_string() }))
}

pub async fn handle_create_entities(service: &PkmService, params: Value) -> Result<Value> {
    let source_material_id = params
        .get("source_material_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Ulid>().ok())
        .wrap_err("Invalid or missing source_material_id")?;

    let entities = params
        .get("entities")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let name = v.get("name")?.as_str()?;
                    let entity_type = v.get("type")?.as_str()?;
                    Some((name.to_string(), entity_type.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();

    let created_by = params
        .get("created_by")
        .and_then(|v| v.as_str())
        .unwrap_or("sinex-gateway");

    let entity_ids = service
        .create_entities_from_source_material(source_material_id, entities, created_by)
        .await?;
    Ok(json!({ "entity_ids": entity_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>() }))
}

pub async fn handle_link_entities(service: &PkmService, params: Value) -> Result<Value> {
    let from_entity_id = params
        .get("from_entity_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Ulid>().ok())
        .map(Id::<Entity>::from_ulid)
        .wrap_err("Invalid or missing from_entity_id")?;

    let to_entity_id = params
        .get("to_entity_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Ulid>().ok())
        .map(Id::<Entity>::from_ulid)
        .wrap_err("Invalid or missing to_entity_id")?;

    let relationship_type = params
        .get("relationship_type")
        .and_then(|v| v.as_str())
        .wrap_err("Missing relationship_type")?;

    let properties = params
        .get("properties")
        .and_then(|v| v.as_object())
        .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
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

// Search handlers

pub async fn handle_search_events(service: &SearchService, params: Value) -> Result<Value> {
    let query: SearchQuery =
        serde_json::from_value(params).wrap_err("Invalid search query parameters")?;

    let results = service.search_events(query).await?;
    Ok(json!(results))
}

// Content handlers

pub async fn handle_store_blob(service: &ContentService, params: Value) -> Result<Value> {
    let content = params
        .get("content")
        .and_then(|v| v.as_str())
        .wrap_err("Missing content")?;

    let filename = params
        .get("filename")
        .and_then(|v| v.as_str())
        .unwrap_or("content.txt");

    let content_type = params
        .get("content_type")
        .and_then(|v| v.as_str())
        .unwrap_or("text/plain");

    let source = params
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("sinex-host");

    let annex_key = service
        .store_large_content(content.as_bytes(), filename, content_type, source)
        .await?;

    Ok(json!({ "annex_key": annex_key }))
}

pub async fn handle_retrieve_blob(service: &ContentService, params: Value) -> Result<Value> {
    let annex_key = params
        .get("annex_key")
        .and_then(|v| v.as_str())
        .wrap_err("Missing annex_key")?;

    let content = service.retrieve_content(annex_key).await?;
    let content_str = String::from_utf8(content).unwrap_or_else(|_| "<binary content>".to_string());

    Ok(json!({ "content": content_str }))
}
