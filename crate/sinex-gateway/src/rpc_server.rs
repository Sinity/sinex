//! JSON-RPC server for CLI communication

use anyhow::Result;
use axum::{extract::State, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info};

use crate::handlers::*;
use crate::service_container::ServiceContainer;

#[derive(Debug, Clone, Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    method: String,
    params: Value,
    id: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
    id: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    data: Option<Value>,
}

impl JsonRpcResponse {
    fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    fn error(id: Option<Value>, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }
}

/// State shared between handlers
#[derive(Clone)]
struct AppState {
    services: ServiceContainer,
}

/// Main RPC handler
async fn handle_rpc(
    State(state): State<AppState>,
    Json(request): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    debug!(
        "Received RPC request: method={}, params={:?}",
        request.method, request.params
    );

    let start = std::time::Instant::now();
    let method = request.method.clone();

    let result = match request.method.as_str() {
        // Analytics methods
        "analytics.event_count_by_source" => {
            handle_event_count_by_source(state.services.analytics.as_ref(), request.params).await
        }

        "analytics.activity_heatmap" => {
            handle_activity_heatmap(state.services.analytics.as_ref(), request.params).await
        }

        // PKM methods
        "pkm.create_note" => handle_create_note(state.services.pkm.as_ref(), request.params).await,

        "pkm.create_entities_from_list" => {
            handle_create_entities(state.services.pkm.as_ref(), request.params).await
        }

        "pkm.link_entities" => {
            handle_link_entities(state.services.pkm.as_ref(), request.params).await
        }

        // Search methods
        "search.search_events" => {
            handle_search_events(state.services.search.as_ref(), request.params).await
        }

        // Content methods
        "content.store_blob" => {
            handle_store_blob(state.services.content.as_ref(), request.params).await
        }

        "content.retrieve_blob" => {
            handle_retrieve_blob(state.services.content.as_ref(), request.params).await
        }

        _ => {
            return Json(JsonRpcResponse::error(
                request.id,
                -32601,
                format!("Method not found: {}", request.method),
            ));
        }
    };

    // Record telemetry
    if let Some(ref telemetry) = state.services.telemetry {
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        telemetry.record_operation_latency(&format!("rpc.{}", method), duration_ms);

        if result.is_err() {
            telemetry.record_error("rpc_error");
        }
    }

    match result {
        Ok(value) => Json(JsonRpcResponse::success(request.id, value)),
        Err(err) => {
            error!("RPC method {} failed: {}", method, err);
            Json(JsonRpcResponse::error(
                request.id,
                -32603,
                format!("Internal error: {}", err),
            ))
        }
    }
}

/// Run the RPC server on the specified socket
pub async fn run(socket_path: PathBuf, services: ServiceContainer) -> Result<()> {
    // Remove existing socket if it exists
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    // Create parent directory if needed
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let state = AppState { services };

    let app = Router::new()
        .route("/rpc", post(handle_rpc))
        .layer(
            ServiceBuilder::new()
                .layer(CorsLayer::permissive())
                .into_inner(),
        )
        .with_state(state);

    // For simplicity, bind to TCP instead of Unix socket for now
    let addr = "127.0.0.1:9999";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("RPC server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
