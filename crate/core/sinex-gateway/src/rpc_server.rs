//! JSON-RPC server for CLI communication

use axum::{extract::State, routing::post, Json, Router};
use camino::Utf8PathBuf;
use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
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

/// RPC method handler type
type RpcHandler = fn(
    &ServiceContainer,
    Value,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send>>;

/// Create RPC method dispatch table
fn create_dispatch_table() -> HashMap<&'static str, RpcHandler> {
    let mut table = HashMap::new();

    // Analytics methods
    table.insert("analytics.event_count_by_source", |services, params| {
        Box::pin(handle_event_count_by_source(
            services.analytics.as_ref(),
            params,
        ))
    });
    table.insert("analytics.activity_heatmap", |services, params| {
        Box::pin(handle_activity_heatmap(services.analytics.as_ref(), params))
    });

    // PKM methods
    table.insert("pkm.create_note", |services, params| {
        Box::pin(handle_create_note(services.pkm.as_ref(), params))
    });
    table.insert("pkm.create_entities_from_list", |services, params| {
        Box::pin(handle_create_entities(services.pkm.as_ref(), params))
    });
    table.insert("pkm.link_entities", |services, params| {
        Box::pin(handle_link_entities(services.pkm.as_ref(), params))
    });

    // Search methods
    table.insert("search.search_events", |services, params| {
        Box::pin(handle_search_events(services.search.as_ref(), params))
    });

    // Content methods
    table.insert("content.store_blob", |services, params| {
        Box::pin(handle_store_blob(services.content.as_ref(), params))
    });
    table.insert("content.retrieve_blob", |services, params| {
        Box::pin(handle_retrieve_blob(services.content.as_ref(), params))
    });

    table
}

/// Main RPC handler using dispatch table
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

    // Use dispatch table for method routing
    let dispatch_table = create_dispatch_table();
    let result = if let Some(handler) = dispatch_table.get(request.method.as_str()) {
        handler(&state.services, request.params).await
    } else {
        return Json(JsonRpcResponse::error(
            request.id,
            -32601,
            format!("Method not found: {}", request.method),
        ));
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

/// Server bind address configuration
#[derive(Debug)]
enum BindAddress {
    Tcp { host: String, port: u16 },
    UnixSocket { path: Utf8PathBuf },
}

impl BindAddress {
    /// Create bind address from environment variables or defaults
    fn from_env_or_socket_path(socket_path: Utf8PathBuf) -> Self {
        // Check for explicit TCP configuration
        if let Ok(host) = std::env::var("SINEX_GATEWAY_HOST") {
            let port = std::env::var("SINEX_GATEWAY_PORT")
                .and_then(|p| p.parse().map_err(|_| std::env::VarError::NotPresent))
                .unwrap_or(9999);
            return BindAddress::Tcp { host, port };
        }

        // Default to Unix socket
        BindAddress::UnixSocket { path: socket_path }
    }
}

/// Run the RPC server with configurable binding
pub async fn run(socket_path: Utf8PathBuf, services: ServiceContainer) -> Result<()> {
    let bind_address = BindAddress::from_env_or_socket_path(socket_path);

    let state = AppState { services };

    let app = Router::new()
        .route("/rpc", post(handle_rpc))
        .layer(
            ServiceBuilder::new()
                .layer(CorsLayer::permissive())
                .into_inner(),
        )
        .with_state(state);

    match bind_address {
        BindAddress::Tcp { host, port } => {
            let addr = format!("{}:{}", host, port);
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            info!("RPC server listening on TCP {}", addr);
            axum::serve(listener, app).await?;
        }
        BindAddress::UnixSocket { path } => {
            // Remove existing socket if it exists
            if path.exists() {
                std::fs::remove_file(&path)?;
            }

            // Create parent directory if needed
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            #[cfg(unix)]
            {
                let listener = tokio::net::UnixListener::bind(&path)?;
                info!("RPC server listening on Unix socket {}", path);

                let service =
                    tower::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let app = app.clone();
                        async move { app.oneshot(req).await }
                    });

                loop {
                    let (stream, _) = listener.accept().await?;
                    let service = service.clone();

                    tokio::spawn(async move {
                        if let Err(e) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(stream, service)
                            .await
                        {
                            error!("Error serving connection: {}", e);
                        }
                    });
                }
            }

            #[cfg(not(unix))]
            {
                // Fall back to TCP on non-Unix systems
                let addr = "127.0.0.1:9999";
                let listener = tokio::net::TcpListener::bind(addr).await?;
                info!(
                    "RPC server listening on TCP {} (Unix socket not available)",
                    addr
                );
                axum::serve(listener, app).await?;
            }
        }
    }

    Ok(())
}
