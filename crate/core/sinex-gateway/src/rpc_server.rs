#![doc = include_str!("../doc/rpc_server.md")]

// Local crate imports
use crate::{
    handlers::*, replay_control::ReplayControlClient, service_container::ServiceContainer,
};

// External crates
use axum::{
    error_handling::HandleErrorLayer,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    BoxError, Json, Router,
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
use std::{net::SocketAddr, str::FromStr};

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

#[derive(Clone)]
struct GatewayAuth {
    mode: GatewayAuthMode,
}

#[derive(Clone)]
enum GatewayAuthMode {
    Disabled,
    StaticToken(String),
}

impl GatewayAuth {
    fn from_env() -> color_eyre::eyre::Result<Self> {
        match read_token_from_env()? {
            Some(token) => {
                if token.trim().is_empty() {
                    Err(eyre!(
                        "SINEX_RPC_TOKEN (or SINEX_RPC_TOKEN_FILE) is set but empty; refusing to start without a token"
                    ))
                } else {
                    Ok(Self {
                        mode: GatewayAuthMode::StaticToken(token),
                    })
                }
            }
            None => {
                if insecure_auth_allowed() {
                    warn!("SINEX_GATEWAY_ALLOW_INSECURE=1 detected - RPC authentication disabled (dev/test only).");
                    Ok(Self {
                        mode: GatewayAuthMode::Disabled,
                    })
                } else {
                    Err(eyre!(
                        "SINEX_RPC_TOKEN is not set. Export a token (or SINEX_RPC_TOKEN_FILE) so the gateway can authenticate RPC clients."
                    ))
                }
            }
        }
    }

    fn verify(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        match &self.mode {
            GatewayAuthMode::Disabled => Ok(()),
            GatewayAuthMode::StaticToken(expected) => {
                let provided = extract_token(headers).ok_or(AuthError::Missing)?;
                if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
                    Ok(())
                } else {
                    Err(AuthError::Invalid)
                }
            }
        }
    }

    #[cfg(test)]
    fn disabled_for_tests() -> Self {
        Self {
            mode: GatewayAuthMode::Disabled,
        }
    }

    #[cfg(test)]
    fn with_test_token(token: &str) -> Self {
        Self {
            mode: GatewayAuthMode::StaticToken(token.to_string()),
        }
    }
}

fn read_token_from_env() -> color_eyre::eyre::Result<Option<String>> {
    if let Ok(path) = std::env::var("SINEX_RPC_TOKEN_FILE") {
        let contents = std::fs::read_to_string(path).wrap_err("Failed to read SINEX_RPC_TOKEN_FILE")?;
        return Ok(Some(contents.trim().to_string()));
    }

    if let Ok(token) = std::env::var("SINEX_RPC_TOKEN") {
        return Ok(Some(token.trim().to_string()));
    }

    Ok(None)
}

fn insecure_auth_allowed() -> bool {
    matches!(
        std::env::var("SINEX_GATEWAY_ALLOW_INSECURE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str(),
        "1" | "true" | "yes"
    )
}

fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(as_str) = value.to_str() {
            let trimmed = as_str.trim();
            if let Some(rest) = trimmed.strip_prefix("Bearer ") {
                return Some(rest.trim().to_string());
            }
        }
    }

    headers
        .get("x-sinex-rpc-token")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.trim().to_string())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

enum AuthError {
    Missing,
    Invalid,
}

impl AuthError {
    fn into_response(self) -> (StatusCode, Json<JsonRpcResponse>) {
        let message = match self {
            AuthError::Missing => "Authentication required. Provide SINEX_RPC_TOKEN via Authorization header.",
            AuthError::Invalid => "Authentication failed: invalid token.",
        };

        (
            StatusCode::UNAUTHORIZED,
            Json(JsonRpcResponse::error(None, -32002, message.to_string())),
        )
    }
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
    headers: HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    if let Err(err) = state.auth.verify(&headers) {
        return err.into_response();
    }

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
    fn from_env_or_socket_path(
        socket_path: Utf8PathBuf,
        cli_tcp: Option<&str>,
    ) -> color_eyre::eyre::Result<Self> {
        if let Some(spec) = cli_tcp {
            let (host, port) = parse_tcp_listen(spec)?;
            return Ok(BindAddress::Tcp { host, port });
        }

        if let Ok(spec) = std::env::var("SINEX_GATEWAY_TCP_LISTEN") {
            let (host, port) = parse_tcp_listen(&spec)?;
            return Ok(BindAddress::Tcp { host, port });
        }

        if let Ok(host) = std::env::var("SINEX_GATEWAY_HOST") {
            let port = std::env::var("SINEX_GATEWAY_PORT")
                .ok()
                .and_then(|p| p.parse::<u16>().ok())
                .unwrap_or(9999);
            return Ok(BindAddress::Tcp { host, port });
        }

        Ok(BindAddress::UnixSocket { path: socket_path })
    }
}

fn parse_tcp_listen(spec: &str) -> color_eyre::eyre::Result<(String, u16)> {
    if let Ok(addr) = SocketAddr::from_str(spec) {
        return Ok((addr.ip().to_string(), addr.port()));
    }

    if let Some(idx) = spec.rfind(':') {
        let (host_part, port_part) = spec.split_at(idx);
        let port = port_part[1..]
            .parse::<u16>()
            .map_err(|_| eyre!("Invalid TCP port in {spec}"))?;
        let host = host_part.trim_matches(|c| c == '[' || c == ']').trim();
        if host.is_empty() {
            return Err(eyre!("TCP host is empty in {spec}"));
        }
        return Ok((host.to_string(), port));
    }

    Err(eyre!(
        "Invalid TCP listen specification '{spec}'. Expected host:port."
    ))
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
    use axum::{
        http::{HeaderMap, HeaderValue},
        routing::post,
        Json, Router,
    };
    use reqwest::Client;
    use serde_json::json;
    use sinex_test_utils::sinex_test;
    use std::net::SocketAddr;
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;
    static ENV_LOCK: once_cell::sync::Lazy<Mutex<()>> =
        once_cell::sync::Lazy::new(|| Mutex::new(()));

    fn clear_tcp_env() {
        std::env::remove_var("SINEX_GATEWAY_TCP_LISTEN");
        std::env::remove_var("SINEX_GATEWAY_HOST");
        std::env::remove_var("SINEX_GATEWAY_PORT");
    }

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
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();
        let addr =
            BindAddress::from_env_or_socket_path(Utf8PathBuf::from(DEFAULT_SOCKET_PATH), None)?;

        assert!(
            matches!(addr, BindAddress::UnixSocket { .. }),
            "TCP binding should remain disabled unless explicitly opted in"
        );

        Ok(())
    }

    #[sinex_test]
    async fn tcp_binding_env_opt_in_respected() -> color_eyre::eyre::Result<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();
        std::env::set_var("SINEX_GATEWAY_TCP_LISTEN", "127.0.0.1:7777");

        let addr =
            BindAddress::from_env_or_socket_path(Utf8PathBuf::from(DEFAULT_SOCKET_PATH), None)?;

        match addr {
            BindAddress::Tcp { host, port } => {
                assert_eq!(&host, "127.0.0.1");
                assert_eq!(port, 7777);
            }
            _ => panic!("expected TCP bind"),
        }

        clear_tcp_env();
        Ok(())
    }

    #[sinex_test]
    async fn tcp_binding_cli_override_wins() -> color_eyre::eyre::Result<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();
        std::env::set_var("SINEX_GATEWAY_TCP_LISTEN", "127.0.0.1:7777");

        let addr = BindAddress::from_env_or_socket_path(
            Utf8PathBuf::from(DEFAULT_SOCKET_PATH),
            Some("127.0.0.1:8888"),
        )?;

        match addr {
            BindAddress::Tcp { host, port } => {
                assert_eq!(&host, "127.0.0.1");
                assert_eq!(port, 8888);
            }
            _ => panic!("expected TCP bind"),
        }

        clear_tcp_env();
        Ok(())
    }

    #[sinex_test]
    async fn tcp_binding_invalid_cli_spec_rejected() -> color_eyre::eyre::Result<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();

        let result = BindAddress::from_env_or_socket_path(
            Utf8PathBuf::from(DEFAULT_SOCKET_PATH),
            Some("not-a-valid-spec"),
        );

        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn gateway_auth_blocks_missing_token() -> color_eyre::eyre::Result<()> {
        let auth = GatewayAuth::with_test_token("secret");
        let headers = HeaderMap::new();
        assert!(matches!(auth.verify(&headers), Err(AuthError::Missing)));
        Ok(())
    }

    #[sinex_test]
    async fn gateway_auth_accepts_bearer_header() -> color_eyre::eyre::Result<()> {
        let auth = GatewayAuth::with_test_token("secret");
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );
        assert!(auth.verify(&headers).is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn gateway_auth_accepts_custom_header() -> color_eyre::eyre::Result<()> {
        let auth = GatewayAuth::with_test_token("secret");
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-sinex-rpc-token",
            HeaderValue::from_static("secret"),
        );
        assert!(auth.verify(&headers).is_ok());
        Ok(())
    }
}

/// Run the RPC server with configurable binding
pub async fn run(
    socket_path: sinex_core::SanitizedPath,
    tcp_listen: Option<&str>,
    services: ServiceContainer,
) -> color_eyre::eyre::Result<()> {
    let bind_address =
        BindAddress::from_env_or_socket_path(Utf8PathBuf::from(socket_path.as_str()), tcp_listen)?;

    let auth = GatewayAuth::from_env()?;
    let state = AppState { services, auth };

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
