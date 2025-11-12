#![doc = include_str!("../doc/rpc_server.md")]

// Local crate imports
use crate::{
    handlers::*, replay_control::ReplayControlClient, service_container::ServiceContainer,
};

// External crates
use axum::{
    error_handling::HandleErrorLayer, extract::State, http::StatusCode, response::IntoResponse,
    routing::post, BoxError, Json, Router,
};
use camino::Utf8PathBuf;
use color_eyre::eyre::{eyre, WrapErr};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperBuilder;
use hyper_util::service::TowerToHyperService;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_stream::StreamExt;
use tower::{
    limit::ConcurrencyLimitLayer,
    load_shed::{error::Overloaded, LoadShedLayer},
    timeout::TimeoutLayer,
    ServiceBuilder,
};
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;

// Standard library
use sinex_core::environment::environment;
use thiserror::Error;
use tracing::{debug, error, info, warn};

use std::time::Duration;

pub const DEFAULT_SOCKET_PATH: &str = "/tmp/sinex-host.sock";

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

#[derive(Debug, Error)]
#[error("Unknown method: {method}")]
struct UnknownMethodError {
    method: String,
}

#[derive(Debug, Clone, Copy)]
struct RpcServerLimits {
    concurrency_limit: usize,
    request_timeout: Duration,
    max_body_bytes: usize,
}

impl RpcServerLimits {
    fn from_env() -> Self {
        Self {
            concurrency_limit: env_var_usize("SINEX_GATEWAY_MAX_CONCURRENCY", 32),
            request_timeout: Duration::from_secs(env_var_u64(
                "SINEX_GATEWAY_REQUEST_TIMEOUT_SECS",
                30,
            )),
            max_body_bytes: env_var_usize("SINEX_GATEWAY_MAX_BODY_BYTES", 2 * 1024 * 1024),
        }
    }

    #[cfg(test)]
    fn test_limits(concurrency_limit: usize, timeout: Duration, max_body_bytes: usize) -> Self {
        Self {
            concurrency_limit,
            request_timeout: timeout,
            max_body_bytes,
        }
    }
}

fn env_var_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_var_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(default)
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

        // Replay control surface
        "replay.create_operation" => {
            let control = replay_control_client(services)?;
            handle_replay_create_operation(control, params).await
        }
        "replay.preview_operation" => {
            let control = replay_control_client(services)?;
            handle_replay_preview_operation(control, params).await
        }
        "replay.approve_operation" => {
            let control = replay_control_client(services)?;
            handle_replay_approve_operation(control, params).await
        }
        "replay.execute_operation" => {
            let control = replay_control_client(services)?;
            handle_replay_execute_operation(control, params).await
        }
        "replay.cancel_operation" => {
            let control = replay_control_client(services)?;
            handle_replay_cancel_operation(control, params).await
        }
        "replay.operation_status" => {
            let control = replay_control_client(services)?;
            handle_replay_operation_status(control, params).await
        }
        "replay.list_operations" => {
            let control = replay_control_client(services)?;
            handle_replay_list_operations(control, params).await
        }

        _ => Err(color_eyre::Report::new(UnknownMethodError {
            method: method.to_string(),
        })),
    }
}

fn replay_control_client<'a>(
    services: &'a ServiceContainer,
) -> color_eyre::eyre::Result<&'a ReplayControlClient> {
    services
        .replay_control
        .as_ref()
        .ok_or_else(|| eyre!("Replay control bus is not initialised"))
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
        Err(err) if err.downcast_ref::<UnknownMethodError>().is_some() => {
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

        let env = environment();
        if env.is_dev() && socket_path.as_str() == DEFAULT_SOCKET_PATH {
            return BindAddress::Tcp {
                host: "127.0.0.1".to_string(),
                port: 9999,
            };
        }

        // Default to Unix socket otherwise
        BindAddress::UnixSocket { path: socket_path }
    }
}

fn apply_rpc_layers<S>(router: Router<S>, limits: &RpcServerLimits) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(
        ServiceBuilder::new()
            .layer(HandleErrorLayer::new(handle_layer_error))
            .layer(LoadShedLayer::new())
            .layer(ConcurrencyLimitLayer::new(limits.concurrency_limit))
            .layer(TimeoutLayer::new(limits.request_timeout))
            .layer(RequestBodyLimitLayer::new(limits.max_body_bytes))
            .layer(CorsLayer::permissive())
            .into_inner(),
    )
}

async fn handle_layer_error(err: BoxError) -> impl IntoResponse {
    if err.is::<tower::timeout::error::Elapsed>() {
        return rpc_layer_error_response(
            StatusCode::GATEWAY_TIMEOUT,
            -32000,
            "RPC request exceeded timeout".to_string(),
        );
    }

    if err.is::<Overloaded>() {
        return rpc_layer_error_response(
            StatusCode::TOO_MANY_REQUESTS,
            -32001,
            "RPC server is busy; please retry".to_string(),
        );
    }

    let message = format!("Unhandled middleware error: {}", err);
    rpc_layer_error_response(StatusCode::INTERNAL_SERVER_ERROR, -32099, message)
}

fn rpc_layer_error_response(status: StatusCode, code: i32, message: String) -> impl IntoResponse {
    (status, Json(JsonRpcResponse::error(None, code, message)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json, Router};
    use reqwest::Client;
    use serde_json::json;
    use sinex_test_utils::sinex_test;
    use std::net::SocketAddr;
    use tokio::task::JoinHandle;

    fn build_test_router(limits: RpcServerLimits) -> Router {
        let base = Router::new().route(
            "/",
            post(|| async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Json(json!({"status": "ok"}))
            }),
        );
        apply_rpc_layers(base, &limits)
    }

    async fn spawn_router(router: Router) -> (SocketAddr, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, router.into_make_service())
                .await
                .unwrap();
        });
        (addr, handle)
    }

    #[sinex_test]
    async fn concurrency_limit_returns_429() -> color_eyre::eyre::Result<()> {
        let limits = RpcServerLimits::test_limits(1, Duration::from_secs(5), 1024 * 1024);
        let router = build_test_router(limits);
        let (addr, handle) = spawn_router(router).await;
        let client = Client::new();

        let first = {
            let client = client.clone();
            let url = format!("http://{addr}/");
            tokio::spawn(async move {
                client
                    .post(&url)
                    .header("content-type", "application/json")
                    .body("{}")
                    .send()
                    .await
                    .unwrap()
            })
        };

        tokio::time::sleep(Duration::from_millis(10)).await;

        let resp = client
            .post(format!("http://{addr}/"))
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let as_str = resp.text().await.unwrap();
        assert!(as_str.contains("server is busy"));

        first.await.unwrap();
        handle.abort();
        Ok(())
    }

    #[sinex_test]
    async fn timeout_layer_returns_504() -> color_eyre::eyre::Result<()> {
        let limits = RpcServerLimits::test_limits(8, Duration::from_millis(20), 1024 * 1024);
        let router = build_test_router(limits);
        let (addr, handle) = spawn_router(router).await;
        let client = Client::new();

        let resp = client
            .post(format!("http://{addr}/"))
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        let body = resp.text().await.unwrap();
        assert!(body.contains("timeout"));

        handle.abort();
        Ok(())
    }

    #[sinex_test]
    async fn body_limit_returns_413() -> color_eyre::eyre::Result<()> {
        let limits = RpcServerLimits::test_limits(8, Duration::from_secs(5), 16);
        let router = build_test_router(limits);
        let big_payload = format!("{{\"payload\":\"{}\"}}", "x".repeat(32));

        let (addr, handle) = spawn_router(router).await;
        let client = Client::new();

        let resp = client
            .post(format!("http://{addr}/"))
            .header("content-type", "application/json")
            .body(big_payload)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

        handle.abort();
        Ok(())
    }

    #[sinex_test]
    async fn rpc_responses_include_request_id_header() -> color_eyre::eyre::Result<()> {
        let limits = RpcServerLimits::test_limits(4, Duration::from_secs(1), 1024);
        let router = build_test_router(limits);
        let (addr, handle) = spawn_router(router).await;
        let client = Client::new();

        let resp = client
            .post(format!("http://{addr}/"))
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await?;

        assert!(
            resp.headers().contains_key("x-request-id"),
            "Gateway RPC responses should include an x-request-id header for structured logging"
        );

        handle.abort();
        Ok(())
    }

    #[sinex_test]
    async fn tcp_binding_requires_opt_in() -> color_eyre::eyre::Result<()> {
        std::env::remove_var("SINEX_GATEWAY_HOST");
        std::env::remove_var("SINEX_GATEWAY_PORT");

        let addr = BindAddress::from_env_or_socket_path(Utf8PathBuf::from(DEFAULT_SOCKET_PATH));

        assert!(
            matches!(addr, BindAddress::UnixSocket { .. }),
            "TCP binding should remain disabled unless explicitly opted in"
        );

        Ok(())
    }
}

/// Run the RPC server with configurable binding
pub async fn run(
    socket_path: sinex_core::SanitizedPath,
    services: ServiceContainer,
) -> color_eyre::eyre::Result<()> {
    let bind_address =
        BindAddress::from_env_or_socket_path(Utf8PathBuf::from(socket_path.as_str()));

    let state = AppState { services };

    let limits = RpcServerLimits::from_env();

    let base_router = Router::new()
        .route("/rpc", post(handle_rpc))
        .route("/", post(handle_rpc)); // Accept RPC calls at base path for CLI compatibility

    let app = apply_rpc_layers(base_router, &limits).with_state(state);

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

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Err(e) =
                    tokio::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
                        .await
                {
                    warn!("Failed to set Unix socket permissions to 0600: {}", e);
                }
            }

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
