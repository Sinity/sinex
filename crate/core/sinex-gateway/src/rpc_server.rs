#![doc = include_str!("../doc/rpc_server.md")]

// Local crate imports
use crate::{handlers::*, service_container::ServiceContainer};

// External crates
use axum::{extract::State, routing::post, Json, Router};
use camino::Utf8PathBuf;
use color_eyre::eyre::{Result, WrapErr};
use futures::StreamExt;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperBuilder;
use hyper_util::service::TowerToHyperService;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

// Standard library
use sinex_core::environment::environment;
use tracing::{debug, error, info};

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

/// Shared dispatch function for RPC methods (used by both rpc_server and native_messaging)
pub async fn dispatch_rpc_method(
    services: &ServiceContainer,
    method: &str,
    params: serde_json::Value,
) -> color_eyre::eyre::Result<serde_json::Value> {
    match method {
        // Analytics methods
        "analytics.event_count_by_source" => {
            handle_event_count_by_source(services.analytics.as_ref(), params).await
        }

        "analytics.activity_heatmap" => {
            handle_activity_heatmap(services.analytics.as_ref(), params).await
        }

        // PKM methods
        "pkm.create_note" => handle_create_note(services.pkm.as_ref(), params).await,

        "pkm.create_entities_from_list" => {
            handle_create_entities(services.pkm.as_ref(), params).await
        }

        "pkm.link_entities" => handle_link_entities(services.pkm.as_ref(), params).await,

        // Search methods
        "search.search_events" => handle_search_events(services.search.as_ref(), params).await,

        // Content methods
        "content.store_blob" => handle_store_blob(services.content.as_ref(), params).await,

        "content.retrieve_blob" => handle_retrieve_blob(services.content.as_ref(), params).await,

        _ => Err(color_eyre::eyre::eyre!("Unknown method: {}", method)),
    }
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

    let _start = std::time::Instant::now();
    let method = request.method.clone();

    // Use shared dispatch function
    let result = dispatch_rpc_method(&state.services, &request.method, request.params).await;

    // Telemetry disabled in this build; keep handler lightweight

    match result {
        Ok(value) => Json(JsonRpcResponse::success(request.id, value)),
        Err(err) if err.to_string().contains("Unknown method:") => {
            Json(JsonRpcResponse::error(request.id, -32601, err.to_string()))
        }
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

        // In development, prefer TCP 127.0.0.1:9999 for CLI friendliness
        let env = environment();
        if env.is_dev() {
            return BindAddress::Tcp {
                host: "127.0.0.1".to_string(),
                port: 9999,
            };
        }

        // Default to Unix socket elsewhere
        BindAddress::UnixSocket { path: socket_path }
    }
}

/// Run the RPC server with configurable binding
pub async fn run(socket_path: sinex_core::SanitizedPath, services: ServiceContainer) -> Result<()> {
    let bind_address =
        BindAddress::from_env_or_socket_path(Utf8PathBuf::from(socket_path.as_str()));

    let state = AppState { services };

    let app = Router::new()
        .route("/rpc", post(handle_rpc))
        .route("/", post(handle_rpc)) // Accept RPC calls at base path for CLI compatibility
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
            let path_str = path.as_str();
            let socket_path = std::path::Path::new(path_str);

            if let Some(parent) = socket_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .wrap_err("Failed to create Unix socket directory")?;
            }

            if socket_path.exists() {
                if let Err(e) = tokio::fs::remove_file(socket_path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(color_eyre::eyre::eyre!(
                            "Failed to remove existing Unix socket {}: {}",
                            path_str,
                            e
                        ));
                    }
                }
            }

            let listener = tokio::net::UnixListener::bind(socket_path)
                .wrap_err("Failed to bind Unix socket")?;
            info!("RPC server listening on Unix socket {}", path_str);

            let mut incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
            let app = app;

            while let Some(stream) = incoming.next().await {
                match stream {
                    Ok(stream) => {
                        let service_app = app.clone();
                        tokio::spawn(async move {
                            let builder = HyperBuilder::new(TokioExecutor::new());
                            let service = TowerToHyperService::new(service_app);
                            let io = TokioIo::new(stream);
                            if let Err(err) = builder.serve_connection(io, service).await {
                                error!(?err, "Unix RPC connection closed with error");
                            }
                        });
                    }
                    Err(err) => {
                        error!(?err, "Failed to accept Unix socket connection");
                    }
                }
            }
        }
    }

    Ok(())
}
