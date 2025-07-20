// # RPC Server Request/Response Handler Tests
//
// Comprehensive tests for the sinex-gateway JSON-RPC server that verify end-to-end
// request/response handling including serialization, method routing, error handling,
// and JSON-RPC 2.0 specification compliance.

use crate::common::prelude::*;
use sinex_events::{EventFactory, services, event_types};
use serde_json::{json, Value};
use sinex_gateway::service_container::ServiceContainer;
use sinex_gateway::handlers::{
    handle_event_count_by_source, handle_activity_heatmap, handle_search_events,
    handle_create_note, handle_create_entities, handle_link_entities,
    handle_store_blob, handle_retrieve_blob
};
use std::str::FromStr;

/// JSON-RPC 2.0 request structure for test requests
#[derive(Debug, Clone, serde::Serialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    params: Value,
    id: Option<Value>,
}

/// JSON-RPC 2.0 response structure for test validation
#[derive(Debug, Clone, serde::Deserialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
    id: Option<Value>,
}

/// JSON-RPC error structure
#[derive(Debug, Clone, serde::Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    data: Option<Value>,
}

impl JsonRpcRequest {
    fn new(method: &str, params: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
            id: Some(json!(1)),
        }
    }

    fn with_id(mut self, id: Value) -> Self {
        self.id = Some(id);
        self
    }

    fn without_id(mut self) -> Self {
        self.id = None;
        self
    }
}

/// Create test events for analytics testing
async fn create_test_events_for_analytics(pool: &DbPool) -> TestResult {
    let test_events = vec![
        ("fs", "file.created", json!({"path": "/test1.txt"})),
        ("fs", "file.modified", json!({"path": "/test2.txt"})),
        (
            "shell.kitty",
            "command.executed",
            json!({"command": "ls -la"}),
        ),
        (
            "shell.kitty",
            "command.executed",
            json!({"command": "git status"}),
        ),
        (
            "wm.hyprland",
            "window.opened",
            json!({"window_title": "Browser"}),
        ),
        ("clipboard", "copied", json!({"content": "test content"})),
    ];

    for (source, event_type, payload) in test_events {
        let event = EventFactory::new(source)
            .create_event(event_type, payload);
        insert_event(pool, &event).await?;
    }

    Ok(())
}

/// Create test events for search testing
async fn create_test_events_for_search(pool: &DbPool) -> TestResult {
    let test_events = vec![
        (
            "fs",
            "file.created",
            json!({
                "path": "/home/user/important.txt",
                "content": "This contains secret information"
            }),
        ),
        (
            "shell.kitty",
            "command.executed",
            json!({
                "command": "grep -r secret .",
                "output": "Found secret files"
            }),
        ),
        (
            "clipboard",
            "copied",
            json!({
                "content": "password123"
            }),
        ),
    ];

    for (source, event_type, payload) in test_events {
        let event = EventFactory::new(source)
            .create_event(event_type, payload);
        insert_event(pool, &event).await?;
    }

    Ok(())
}

/// Test helper to directly invoke RPC handlers via service container
/// This approach avoids HTTP server complexity while still testing the RPC logic
async fn invoke_rpc_method(
    services: &ServiceContainer,
    method: &str,
    params: Value,
) -> AnyhowResult<Value> {
    // Direct handler invocation for testing RPC logic without HTTP server complexity

    match method {
        "analytics.event_count_by_source" => {
            handle_event_count_by_source(services.analytics.as_ref(), params)
                .await
                .map_err(|e| {
                    Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
                        as Box<dyn std::error::Error>
                })
        }
        "analytics.activity_heatmap" => {
            handle_activity_heatmap(services.analytics.as_ref(), params)
                .await
                .map_err(|e| {
                    Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
                        as Box<dyn std::error::Error>
                })
        }
        "search.search_events" => handle_search_events(services.search.as_ref(), params)
            .await
            .map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
                    as Box<dyn std::error::Error>
            }),
        "pkm.create_note" => handle_create_note(services.pkm.as_ref(), params)
            .await
            .map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
                    as Box<dyn std::error::Error>
            }),
        "pkm.create_entities_from_list" => handle_create_entities(services.pkm.as_ref(), params)
            .await
            .map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
                    as Box<dyn std::error::Error>
            }),
        "pkm.link_entities" => handle_link_entities(services.pkm.as_ref(), params)
            .await
            .map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
                    as Box<dyn std::error::Error>
            }),
        "content.store_blob" => handle_store_blob(services.content.as_ref(), params)
            .await
            .map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
                    as Box<dyn std::error::Error>
            }),
        "content.retrieve_blob" => handle_retrieve_blob(services.content.as_ref(), params)
            .await
            .map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
                    as Box<dyn std::error::Error>
            }),
        _ => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Method not found: {}", method),
        ))),
    }
}

/// Create service container for testing
async fn create_test_services() -> AnyhowResult<ServiceContainer> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    ServiceContainer::new(Some(database_url))
        .await
        .map_err(|e| {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
                as Box<dyn std::error::Error>
        })
}

/// Simulate a JSON-RPC request/response cycle
async fn simulate_rpc_request(
    services: &ServiceContainer,
    request: JsonRpcRequest,
) -> JsonRpcResponse {
    match invoke_rpc_method(services, &request.method, request.params).await {
        Ok(result) => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id: request.id,
        },
        Err(err) => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code: -32603,
                message: format!("Internal error: {}", err),
                data: None,
            }),
            id: request.id,
        },
    }
}

// ===============================
// Analytics Service RPC Tests
// ===============================

#[sinex_test]
async fn test_analytics_event_count_by_source_success(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;
    create_test_events_for_analytics(ctx.pool()).await?;

    let request = JsonRpcRequest::new("analytics.event_count_by_source", json!({ "days_back": 1 }));

    let response = simulate_rpc_request(&services, request).await;

    // Verify JSON-RPC 2.0 compliance
    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.error.is_none());
    assert!(response.result.is_some());
    assert_eq!(response.id, Some(json!(1)));

    // Verify response structure
    let result = response.result.unwrap();
    assert!(result.is_object());

    let counts = result.as_object().unwrap();
    assert!(counts.contains_key("fs"));
    assert!(counts.contains_key("shell.kitty"));
    assert!(counts.contains_key("wm.hyprland"));
    assert!(counts.contains_key("clipboard"));

    // Verify count values are positive integers
    assert!(counts["fs"].as_i64().unwrap() > 0);
    assert!(counts["shell.kitty"].as_i64().unwrap() > 0);

    Ok(())
}

#[sinex_test]
async fn test_analytics_event_count_by_source_defaults(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;
    create_test_events_for_analytics(ctx.pool()).await?;

    // Test with empty params - should use default 7 days
    let request = JsonRpcRequest::new("analytics.event_count_by_source", json!({}));

    let response = simulate_rpc_request(&services, request).await;

    assert!(response.error.is_none());
    assert!(response.result.is_some());

    let result = response.result.unwrap();
    assert!(result.is_object());

    Ok(())
}

#[sinex_test]
async fn test_analytics_activity_heatmap_success(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;
    create_test_events_for_analytics(ctx.pool()).await?;

    let request = JsonRpcRequest::new(
        "analytics.activity_heatmap",
        json!({
            "bucket_size_minutes": 60,
            "limit": 10
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    // Verify JSON-RPC response structure
    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.error.is_none());
    assert!(response.result.is_some());

    // Verify result is an array of time buckets
    let result = response.result.unwrap();
    assert!(result.is_array());

    Ok(())
}

// ===============================
// Search Service RPC Tests
// ===============================

#[sinex_test]
async fn test_search_events_success(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;
    create_test_events_for_search(ctx.pool()).await?;

    let request = JsonRpcRequest::new(
        "search.search_events",
        json!({
            "text": "secret",
            "sources": [],
            "event_types": [],
            "start_time": null,
            "end_time": null,
            "limit": 10,
            "offset": 0
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    // Verify JSON-RPC response structure
    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.error.is_none());
    assert!(response.result.is_some());

    // Verify search results structure
    let result = response.result.unwrap();
    assert!(result.is_array());

    let results = result.as_array().unwrap();
    assert!(!results.is_empty());

    // Verify each result has required fields
    for result_item in results {
        assert!(result_item["event_id"].is_string());
        assert!(result_item["source"].is_string());
        assert!(result_item["event_type"].is_string());
        assert!(result_item["timestamp"].is_string());
        assert!(result_item["snippet"].is_string());
        assert!(result_item["score"].is_number());
    }

    Ok(())
}

#[sinex_test]
async fn test_search_events_with_filters(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;
    create_test_events_for_search(ctx.pool()).await?;

    let request = JsonRpcRequest::new(
        "search.search_events",
        json!({
            "text": null,
            "sources": ["fs"],
            "event_types": ["file.created"],
            "start_time": null,
            "end_time": null,
            "limit": 5,
            "offset": 0
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    assert!(response.error.is_none());
    assert!(response.result.is_some());

    let result = response.result.unwrap();
    let results = result.as_array().unwrap();

    // Verify all results match the filters
    for result_item in results {
        assert_eq!(result_item["source"].as_str().unwrap(), "fs");
        assert_eq!(result_item["event_type"].as_str().unwrap(), "file.created");
    }

    Ok(())
}

#[sinex_test]
async fn test_search_events_invalid_params(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    // Test with invalid parameter structure
    let request = JsonRpcRequest::new(
        "search.search_events",
        json!({
            "invalid_field": "value",
            "limit": "not_a_number"
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    // Should return JSON-RPC error
    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.result.is_none());
    assert!(response.error.is_some());

    let error = response.error.unwrap();
    assert_eq!(error.code, -32603); // Internal error
    assert!(error.message.contains("Internal error"));

    Ok(())
}

// ===============================
// PKM Service RPC Tests
// ===============================

#[sinex_test]
async fn test_pkm_create_note_success(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    // Create a test event to annotate
    let event = EventFactory::new("test")
        .create_event("test.event", json!({"data": "test"}));
    let event_id = event.id;
    insert_event(ctx.pool(), &event).await?;

    let request = JsonRpcRequest::new(
        "pkm.create_note",
        json!({
            "event_id": event_id.to_string(),
            "content": "This is a test note",
            "tags": ["test", "important"],
            "created_by": "test-user"
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    // Verify JSON-RPC response structure
    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.error.is_none());
    assert!(response.result.is_some());

    // Verify annotation ID returned
    let result = response.result.unwrap();
    assert!(result["annotation_id"].is_string());

    let annotation_id_str = result["annotation_id"].as_str().unwrap();
    assert!(Ulid::from_str(annotation_id_str).is_ok());

    Ok(())
}

#[sinex_test]
async fn test_pkm_create_note_missing_event_id(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    let request = JsonRpcRequest::new(
        "pkm.create_note",
        json!({
            "content": "This is a test note",
            "tags": ["test"]
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    // Should return error for missing event_id
    assert!(response.error.is_some());
    let error = response.error.unwrap();
    assert_eq!(error.code, -32603);
    assert!(error.message.contains("Invalid or missing event_id"));

    Ok(())
}

#[sinex_test]
async fn test_pkm_create_entities_from_list_success(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    let event = EventFactory::new("test")
        .create_event("test.event", json!({"data": "test"}));
    let event_id = event.id;
    insert_event(ctx.pool(), &event).await?;

    let request = JsonRpcRequest::new(
        "pkm.create_entities_from_list",
        json!({
            "event_id": event_id.to_string(),
            "entities": [
                {"name": "Alice", "type": "person"},
                {"name": "OpenAI", "type": "organization"},
                {"name": "Machine Learning", "type": "concept"}
            ]
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.error.is_none());
    assert!(response.result.is_some());

    let result = response.result.unwrap();
    assert!(result["entity_ids"].is_array());

    let entity_ids = result["entity_ids"].as_array().unwrap();
    assert_eq!(entity_ids.len(), 3);

    // Verify all returned IDs are valid ULIDs
    for id in entity_ids {
        let id_str = id.as_str().unwrap();
        assert!(Ulid::from_str(id_str).is_ok());
    }

    Ok(())
}

// ===============================
// Content Service RPC Tests
// ===============================

#[sinex_test]
async fn test_content_store_blob_success(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    let request = JsonRpcRequest::new(
        "content.store_blob",
        json!({
            "content": "This is test content",
            "filename": "test.txt",
            "content_type": "text/plain",
            "source": "test-client"
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.error.is_none());
    assert!(response.result.is_some());

    let result = response.result.unwrap();
    assert!(result["annex_key"].is_string());

    let annex_key = result["annex_key"].as_str().unwrap();
    assert!(!annex_key.is_empty());

    Ok(())
}

#[sinex_test]
async fn test_content_store_blob_missing_content(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    let request = JsonRpcRequest::new(
        "content.store_blob",
        json!({
            "filename": "test.txt"
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    assert!(response.error.is_some());
    let error = response.error.unwrap();
    assert_eq!(error.code, -32603);
    assert!(error.message.contains("Missing content"));

    Ok(())
}

// ===============================
// JSON-RPC Protocol Tests
// ===============================

#[sinex_test]
async fn test_rpc_method_not_found(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    let request = JsonRpcRequest::new("non.existent.method", json!({}));

    let response = simulate_rpc_request(&services, request).await;

    // Verify JSON-RPC error response
    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.result.is_none());
    assert!(response.error.is_some());

    let error = response.error.unwrap();
    assert_eq!(error.code, -32603); // Internal error (method not found routing)
    assert!(error.message.contains("Method not found"));

    Ok(())
}

#[sinex_test]
async fn test_rpc_request_without_id(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;
    create_test_events_for_analytics(ctx.pool()).await?;

    let request = JsonRpcRequest::new("analytics.event_count_by_source", json!({})).without_id();

    let response = simulate_rpc_request(&services, request).await;

    // For notification requests (no id), response should still work
    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.id.is_none());

    Ok(())
}

#[sinex_test]
async fn test_rpc_request_with_string_id(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;
    create_test_events_for_analytics(ctx.pool()).await?;

    let request = JsonRpcRequest::new("analytics.event_count_by_source", json!({}))
        .with_id(json!("string-id-123"));

    let response = simulate_rpc_request(&services, request).await;

    assert_eq!(response.jsonrpc, "2.0");
    assert_eq!(response.id, Some(json!("string-id-123")));

    Ok(())
}

// ===============================
// Parameter Validation Tests
// ===============================

#[sinex_test]
async fn test_parameter_serialization_all_types(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    let event = EventFactory::new("test")
        .create_event("test.event", json!({"data": "test"}));
    let event_id = event.id;
    insert_event(ctx.pool(), &event).await?;

    // Test complex parameter serialization
    let request = JsonRpcRequest::new(
        "pkm.create_note",
        json!({
            "event_id": event_id.to_string(),
            "content": "Test with various types",
            "tags": ["string", "array", "values"],
            "created_by": "test-user"
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    assert!(response.error.is_none());
    assert!(response.result.is_some());

    Ok(())
}

#[sinex_test]
async fn test_parameter_validation_ulid_parsing(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    // Test with invalid ULID format
    let request = JsonRpcRequest::new(
        "pkm.create_note",
        json!({
            "event_id": "not-a-valid-ulid",
            "content": "Test note"
        }),
    );

    let response = simulate_rpc_request(&services, request).await;

    assert!(response.error.is_some());
    let error = response.error.unwrap();
    assert_eq!(error.code, -32603);
    assert!(error.message.contains("Invalid or missing event_id"));

    Ok(())
}

// ===============================
// Integration Tests
// ===============================

#[sinex_test]
async fn test_full_workflow_integration(ctx: TestContext) -> TestResult {
    let services = create_test_services().await?;

    // 1. Create test data
    let event = EventFactory::new("fs")
        .create_event("file.created", json!({
            "path": "/important/document.txt",
            "content": "This document contains sensitive information"
        }));
    let event_id = event.id;
    insert_event(ctx.pool(), &event).await?;

    // 2. Search for the event
    let search_request = JsonRpcRequest::new(
        "search.search_events",
        json!({
            "text": "sensitive",
            "sources": [],
            "event_types": [],
            "start_time": null,
            "end_time": null,
            "limit": 10,
            "offset": 0
        }),
    );

    let search_response = simulate_rpc_request(&services, search_request).await;
    assert!(search_response.error.is_none());
    let search_result = search_response.result.unwrap();
    let search_results = search_result.as_array().unwrap();
    assert!(!search_results.is_empty());

    // 3. Create annotation on found event
    let note_request = JsonRpcRequest::new(
        "pkm.create_note",
        json!({
            "event_id": event_id.to_string(),
            "content": "Marked as sensitive document",
            "tags": ["sensitive", "document"],
            "created_by": "automated-scan"
        }),
    );

    let note_response = simulate_rpc_request(&services, note_request).await;
    assert!(note_response.error.is_none());

    // 4. Store large content
    let content_request = JsonRpcRequest::new(
        "content.store_blob",
        json!({
            "content": "This is the full content of the sensitive document with much more detail...",
            "filename": "document_full.txt",
            "content_type": "text/plain",
            "source": "file-scanner"
        }),
    );

    let content_response = simulate_rpc_request(&services, content_request).await;
    assert!(content_response.error.is_none());

    let annex_key = content_response.result.unwrap()["annex_key"]
        .as_str()
        .unwrap()
        .to_string();

    // 5. Retrieve stored content
    let retrieve_request = JsonRpcRequest::new(
        "content.retrieve_blob",
        json!({
            "annex_key": annex_key
        }),
    );

    let retrieve_response = simulate_rpc_request(&services, retrieve_request).await;
    assert!(retrieve_response.error.is_none());

    Ok(())
}
