#![doc = include_str!("../docs/rpc_server.md")]

// Local crate imports
use crate::{
    handlers::*, replay_control::ReplayControlClient, service_container::ServiceContainer,
};

// External crates
use axum::{
    error_handling::HandleErrorLayer,
    extract::State,
    http::{HeaderMap, HeaderName, Request, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    BoxError, Json, Router,
};
use color_eyre::eyre::{eyre, WrapErr};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperBuilder;
use hyper_util::service::TowerToHyperService;
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_core::coordination::CoordinationKvClient;
use sinex_core::types::Bytes;
use std::convert::TryFrom;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::{rustls, TlsAcceptor};
use tower::{
    limit::ConcurrencyLimitLayer,
    load_shed::{error::Overloaded, LoadShedLayer},
    timeout::TimeoutLayer,
    ServiceBuilder,
};
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::TraceLayer;

// Standard library
use thiserror::Error;
use tracing::{debug, error, info, warn};

use std::time::Duration;
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    str::FromStr,
};
use tokio::sync::RwLock;

pub const DEFAULT_TCP_LISTEN: &str = "127.0.0.1:9999";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    params: Value,
    id: Option<Value>,
}

pub(crate) fn validate_jsonrpc_request(request: &JsonRpcRequest) -> color_eyre::eyre::Result<()> {
    if request.jsonrpc != "2.0" {
        return Err(eyre!("jsonrpc must be '2.0'"));
    }
    if request.method.trim().is_empty() {
        return Err(eyre!("method must be a non-empty string"));
    }
    match request.params {
        Value::Object(_) | Value::Array(_) | Value::Null => Ok(()),
        _ => Err(eyre!("params must be an object, array, or null")),
    }
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
pub(crate) struct RpcServerLimits {
    pub(crate) concurrency_limit: usize,
    pub(crate) request_timeout: Duration,
    pub(crate) max_body_bytes: Bytes,
}

impl RpcServerLimits {
    pub(crate) fn from_env() -> Self {
        Self {
            concurrency_limit: env_var_usize("SINEX_GATEWAY_MAX_CONCURRENCY", 32),
            request_timeout: Duration::from_secs(env_var_u64(
                "SINEX_GATEWAY_REQUEST_TIMEOUT_SECS",
                30,
            )),
            max_body_bytes: Bytes::from_bytes(env_var_u64(
                "SINEX_GATEWAY_MAX_BODY_BYTES",
                2 * 1024 * 1024,
            )),
        }
    }

    fn apply_pool_limit(self, pool_max: usize) -> Self {
        if pool_max == 0 {
            return self;
        }

        Self {
            concurrency_limit: self.concurrency_limit.min(pool_max),
            ..self
        }
    }

    #[cfg(test)]
    fn test_limits(concurrency_limit: usize, timeout: Duration, max_body_bytes: Bytes) -> Self {
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
    token: Arc<RwLock<Option<String>>>,
    token_path: Option<PathBuf>,
}

impl GatewayAuth {
    fn from_env() -> color_eyre::eyre::Result<Self> {
        let (token, token_path) = read_token_and_path_from_env()?;

        if let Some(ref t) = token {
            if t.trim().is_empty() {
                return Err(eyre!(
                    "SINEX_RPC_TOKEN (or token file) is set but empty; refusing to start without a token"
                ));
            }
        } else {
            return Err(eyre!(
                "SINEX_RPC_TOKEN is not set. Export a token (or SINEX_GATEWAY_ADMIN_TOKEN_FILE / SINEX_RPC_TOKEN_FILE) so the gateway can authenticate RPC clients."
            ));
        }

        Ok(Self {
            token: Arc::new(RwLock::new(token)),
            token_path,
        })
    }

    fn start_file_watcher(self) -> color_eyre::eyre::Result<Self> {
        if let Some(ref path) = self.token_path {
            let token_clone = Arc::clone(&self.token);
            let path_clone = path.clone();
            let path_for_closure = path.clone();

            std::thread::spawn(move || {
                use notify::{Event, EventKind, RecursiveMode, Watcher};

                let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
                    match res {
                        Ok(event) => {
                            match event.kind {
                                EventKind::Modify(_) | EventKind::Create(_) => {
                                    // File was modified or created - reload token
                                    match std::fs::read_to_string(&path_for_closure) {
                                        Ok(new_token) => {
                                            let trimmed = new_token.trim().to_string();
                                            if !trimmed.is_empty() {
                                                let mut token_lock = token_clone.blocking_write();
                                                *token_lock = Some(trimmed);
                                                info!("RPC token reloaded from {:?}", path_for_closure);
                                            } else {
                                                warn!("Token file {:?} is empty after reload", path_for_closure);
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to read token file {:?} after modification: {}", path_for_closure, e);
                                        }
                                    }
                                }
                                EventKind::Remove(_) => {
                                    // File was deleted - disable auth (with warning)
                                    let mut token_lock = token_clone.blocking_write();
                                    *token_lock = None;
                                    warn!("RPC token file {:?} deleted - authentication disabled!", path_for_closure);
                                }
                                _ => {
                                    // Ignore other events (access, metadata changes, etc.)
                                }
                            }
                        }
                        Err(e) => {
                            error!("Token file watch error: {}", e);
                        }
                    }
                }).expect("Failed to create file watcher");

                if let Err(e) = watcher.watch(&path_clone, RecursiveMode::NonRecursive) {
                    error!("Failed to watch token file {:?}: {}", path_clone, e);
                    return;
                }

                info!("Watching token file {:?} for changes", path_clone);

                // Keep the watcher alive
                loop {
                    std::thread::sleep(Duration::from_secs(60));
                }
            });
        }

        Ok(self)
    }

    async fn verify(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        let provided = extract_token(headers).ok_or(AuthError::Missing)?;

        let token_guard = self.token.read().await;
        match token_guard.as_ref() {
            Some(expected) => {
                if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
                    Ok(())
                } else {
                    Err(AuthError::Invalid)
                }
            }
            None => {
                warn!("No token configured - rejecting request");
                Err(AuthError::Missing)
            }
        }
    }

    #[cfg(test)]
    fn with_test_token(token: &str) -> Self {
        Self {
            token: Arc::new(RwLock::new(Some(token.to_string()))),
            token_path: None,
        }
    }
}

pub(crate) fn read_token_from_env() -> color_eyre::eyre::Result<Option<String>> {
    let (token, _) = read_token_and_path_from_env()?;
    Ok(token)
}

fn read_token_and_path_from_env() -> color_eyre::eyre::Result<(Option<String>, Option<PathBuf>)> {
    if let Ok(path_str) = std::env::var("SINEX_GATEWAY_ADMIN_TOKEN_FILE") {
        let path = PathBuf::from(&path_str);
        let contents = std::fs::read_to_string(&path)
            .wrap_err("Failed to read SINEX_GATEWAY_ADMIN_TOKEN_FILE")?;
        return Ok((Some(contents.trim().to_string()), Some(path)));
    }

    if let Ok(path_str) = std::env::var("SINEX_RPC_TOKEN_FILE") {
        let path = PathBuf::from(&path_str);
        let contents =
            std::fs::read_to_string(&path).wrap_err("Failed to read SINEX_RPC_TOKEN_FILE")?;
        return Ok((Some(contents.trim().to_string()), Some(path)));
    }

    if let Ok(token) = std::env::var("SINEX_RPC_TOKEN") {
        return Ok((Some(token.trim().to_string()), None));
    }

    Ok((None, None))
}

pub(crate) fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(as_str) = value.to_str() {
            let trimmed = as_str.trim();
            if let Some(rest) = trimmed.strip_prefix("Bearer ") {
                return Some(rest.trim().to_string());
            }
        }
    }

    None
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
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
            AuthError::Missing => {
                "Authentication required. Provide SINEX_RPC_TOKEN via Authorization header."
            }
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
    auth: GatewayAuth,
}

/// Shared dispatch function for RPC methods (used by both rpc_server and native_messaging)
pub async fn dispatch_rpc_method(
    services: &ServiceContainer,
    method: &str,
    params: serde_json::Value,
) -> color_eyre::eyre::Result<serde_json::Value> {
    match method {
        "system.health" => handle_system_health(services, params).await,
        // Analytics methods
        "analytics.event_count_by_source" => {
            handle_event_count_by_source(services.analytics.as_ref(), params).await
        }

        "analytics.activity_heatmap" => {
            handle_activity_heatmap(services.analytics.as_ref(), params).await
        }

        "analytics.sources_statistics" => {
            handle_sources_statistics(services.analytics.as_ref(), params).await
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

        // Coordination methods
        "coordination.list_instances" => {
            let client = coordination_client(services)?;
            handle_coordination_list_instances(client, params).await
        }
        "coordination.get_leader" => {
            let client = coordination_client(services)?;
            handle_coordination_get_leader(client, params).await
        }
        "coordination.instance_health" => {
            let client = coordination_client(services)?;
            handle_coordination_instance_health(client, params).await
        }

        // DLQ management methods
        "dlq.list" => {
            let nats = nats_client_required(services)?;
            let env = services.environment();
            handle_dlq_list(nats, env, params).await
        }
        "dlq.peek" => {
            let nats = nats_client_required(services)?;
            let env = services.environment();
            handle_dlq_peek(nats, env, params).await
        }
        "dlq.requeue" => {
            let nats = nats_client_required(services)?;
            let env = services.environment();
            handle_dlq_requeue(nats, env, params).await
        }
        "dlq.purge" => {
            let nats = nats_client_required(services)?;
            let env = services.environment();
            handle_dlq_purge(nats, env, params).await
        }

        // Node operations methods
        "nodes.list" => {
            let nats = nats_client_required(services)?;
            let env = services.environment();
            handle_nodes_list(nats, env, params).await
        }
        "nodes.drain" => {
            let nats = nats_client_required(services)?;
            let env = services.environment();
            handle_nodes_drain(nats, env, params).await
        }
        "nodes.resume" => {
            let nats = nats_client_required(services)?;
            let env = services.environment();
            handle_nodes_resume(nats, env, params).await
        }
        "nodes.set_horizon" => {
            let nats = nats_client_required(services)?;
            let env = services.environment();
            handle_nodes_set_horizon(nats, env, params).await
        }

        // Operations log methods
        "ops.start" => {
            let pool = services.pool();
            handle_ops_start(pool, params).await
        }
        "ops.list" => {
            let pool = services.pool();
            handle_ops_list(pool, params).await
        }
        "ops.get" => {
            let pool = services.pool();
            handle_ops_get(pool, params).await
        }
        "ops.cancel" => {
            let pool = services.pool();
            handle_ops_cancel(pool, params).await
        }

        // Audit trail methods
        "audit.get" => {
            let pool = services.pool();
            handle_audit_get(pool, params).await
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
        .ok_or_else(|| eyre!("Replay control bus is not initialized"))
}

fn coordination_client<'a>(
    services: &'a ServiceContainer,
) -> color_eyre::eyre::Result<&'a CoordinationKvClient> {
    services
        .coordination
        .as_ref()
        .map(|arc| arc.as_ref())
        .ok_or_else(|| eyre!("Coordination client is not initialized (NATS connection required)"))
}

fn nats_client_required<'a>(
    services: &'a ServiceContainer,
) -> color_eyre::eyre::Result<&'a async_nats::Client> {
    services
        .nats_client()
        .ok_or_else(|| eyre!("NATS client is not available"))
}

/// Health check endpoint
///
/// Returns 200 OK if both database and NATS are reachable,
/// 503 Service Unavailable otherwise.
async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    // Check database connectivity
    let db_ok = sqlx::query("SELECT 1")
        .execute(state.services.pool())
        .await
        .is_ok();

    // Check NATS connectivity
    let nats_ok = state
        .services
        .nats_client()
        .map(|client| {
            matches!(
                client.connection_state(),
                async_nats::connection::State::Connected
            )
        })
        .unwrap_or(false);

    if db_ok && nats_ok {
        (StatusCode::OK, "OK").into_response()
    } else {
        let mut reasons = Vec::new();
        if !db_ok {
            reasons.push("database");
        }
        if !nats_ok {
            reasons.push("nats");
        }
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Service unhealthy: {}", reasons.join(", ")),
        )
            .into_response()
    }
}

/// Main RPC handler using dispatch table
async fn handle_rpc(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    if let Err(err) = state.auth.verify(&headers).await {
        return err.into_response();
    }

    if let Err(err) = validate_jsonrpc_request(&request) {
        let response = JsonRpcResponse::error(request.id, -32600, err.to_string());
        return (StatusCode::BAD_REQUEST, Json(response));
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

    let response = match result {
        Ok(value) => JsonRpcResponse::success(request.id, value),
        Err(err) if err.downcast_ref::<UnknownMethodError>().is_some() => {
            JsonRpcResponse::error(request.id, -32601, err.to_string())
        }
        Err(err) => {
            error!("RPC method {} failed: {}", method, err);
            JsonRpcResponse::error(request.id, -32603, format!("Internal error: {}", err))
        }
    };

    (StatusCode::OK, Json(response))
}

/// Server bind address configuration
#[derive(Debug)]
enum BindAddress {
    Tcp { host: String, port: u16 },
}

impl BindAddress {
    /// Create bind address from environment variables or defaults
    fn from_env_or_default(cli_tcp: Option<&str>) -> color_eyre::eyre::Result<Self> {
        if let Some(spec) = cli_tcp {
            let (host, port) = parse_tcp_listen(spec)?;
            return Ok(BindAddress::Tcp { host, port });
        }

        if let Ok(spec) = std::env::var("SINEX_GATEWAY_TCP_LISTEN") {
            let (host, port) = parse_tcp_listen(&spec)?;
            return Ok(BindAddress::Tcp { host, port });
        }

        let (host, port) = parse_tcp_listen(DEFAULT_TCP_LISTEN)?;
        Ok(BindAddress::Tcp { host, port })
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

fn tls_paths_from_env() -> color_eyre::eyre::Result<(String, String, Option<String>)> {
    let cert = std::env::var("SINEX_GATEWAY_TLS_CERT")
        .map_err(|_| eyre!("SINEX_GATEWAY_TLS_CERT is required for TCP bindings"))?;
    let key = std::env::var("SINEX_GATEWAY_TLS_KEY")
        .map_err(|_| eyre!("SINEX_GATEWAY_TLS_KEY is required for TCP bindings"))?;
    let client_ca = std::env::var("SINEX_GATEWAY_TLS_CLIENT_CA").ok();
    Ok((cert, key, client_ca))
}

fn load_rustls_config(
    cert_path: &str,
    key_path: &str,
    client_ca_path: Option<&str>,
) -> color_eyre::eyre::Result<rustls::ServerConfig> {
    let cert_file = &mut BufReader::new(File::open(cert_path)?);
    let key_file = &mut BufReader::new(File::open(key_path)?);

    let cert_chain: Vec<CertificateDer<'static>> = certs(cert_file)
        .map_err(|_| eyre!("Failed to read TLS certificate"))?
        .into_iter()
        .map(CertificateDer::from)
        .collect();

    let mut keys: Vec<PrivateKeyDer<'static>> = pkcs8_private_keys(key_file)
        .map_err(|_| eyre!("Failed to read TLS private key (pkcs8)"))?
        .into_iter()
        .map(|raw| {
            PrivateKeyDer::try_from(raw)
                .map_err(|_| eyre!("Failed to parse TLS private key (pkcs8): invalid DER"))
        })
        .collect::<Result<_, _>>()?;
    if keys.is_empty() {
        let mut key_file = BufReader::new(File::open(key_path)?);
        keys = rsa_private_keys(&mut key_file)
            .map_err(|_| eyre!("Failed to read TLS private key (rsa)"))?
            .into_iter()
            .map(|raw| {
                PrivateKeyDer::try_from(raw)
                    .map_err(|_| eyre!("Failed to parse TLS private key (rsa): invalid DER"))
            })
            .collect::<Result<_, _>>()?;
    }

    let key = keys
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("No private keys found in {}", key_path))?;

    if let Some(ca_path) = client_ca_path {
        let mut ca_reader = BufReader::new(File::open(ca_path)?);
        let client_certs: Vec<CertificateDer<'static>> = certs(&mut ca_reader)
            .map_err(|_| eyre!("Failed to read client CA bundle"))?
            .into_iter()
            .map(CertificateDer::from)
            .collect();
        let mut roots = rustls::RootCertStore::empty();
        let (added, _ignored) = roots.add_parsable_certificates(client_certs);
        if added == 0 {
            return Err(eyre!("No valid client CA certs found in {}", ca_path));
        }

        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| eyre!("Failed to build client verifier: {}", e))?;

        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, key)
            .map_err(|e| eyre!("Invalid TLS cert/key: {}", e))
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|e| eyre!("Invalid TLS cert/key: {}", e))
    }
}

fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    if let Ok(addr) = host.parse::<IpAddr>() {
        return addr.is_loopback();
    }
    false
}

fn client_tls_required_override() -> bool {
    matches!(
        std::env::var("SINEX_GATEWAY_REQUIRE_CLIENT_TLS")
            .unwrap_or_default()
            .to_lowercase()
            .as_str(),
        "1" | "true" | "yes"
    )
}

fn require_mtls_for_remote(
    bind_address: &BindAddress,
    client_ca: Option<&str>,
) -> color_eyre::eyre::Result<()> {
    let host_requires = match bind_address {
        BindAddress::Tcp { host, .. } => !is_loopback_host(host),
    };

    if (host_requires || client_tls_required_override()) && client_ca.is_none() {
        return Err(eyre!(
            "SINEX_GATEWAY_TLS_CLIENT_CA is required when mTLS is enforced (non-loopback or SINEX_GATEWAY_REQUIRE_CLIENT_TLS=1)"
        ));
    }
    Ok(())
}

fn warn_if_remote_bind(bind_address: &BindAddress) {
    if let BindAddress::Tcp { host, .. } = bind_address {
        if !is_loopback_host(host) {
            warn!(
                bind_host = %host,
                "Gateway RPC is exposed beyond loopback; ensure mTLS and firewalling are configured"
            );
        }
    }
}

fn apply_rpc_layers<S>(router: Router<S>, limits: &RpcServerLimits) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let request_id_header = HeaderName::from_static("x-request-id");

    router
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_layer_error))
                .layer(LoadShedLayer::new())
                .layer(ConcurrencyLimitLayer::new(limits.concurrency_limit))
                .layer(TimeoutLayer::new(limits.request_timeout))
                .layer(RequestBodyLimitLayer::new(limits.max_body_bytes.as_usize()))
                .layer(CorsLayer::permissive())
                .into_inner(),
        )
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request<_>| {
                let request_id = request
                    .headers()
                    .get("x-request-id")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("unknown");
                tracing::info_span!(
                    "rpc.request",
                    method = %request.method(),
                    uri = %request.uri(),
                    request_id = request_id
                )
            }),
        )
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(SetRequestIdLayer::new(
            request_id_header,
            MakeRequestUuid::default(),
        ))
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
    use sinex_test_utils::{sinex_test, TestResult};
    use std::net::SocketAddr;
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;
    static ENV_LOCK: once_cell::sync::Lazy<Mutex<()>> =
        once_cell::sync::Lazy::new(|| Mutex::new(()));

    fn clear_tcp_env() {
        std::env::remove_var("SINEX_GATEWAY_TCP_LISTEN");
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
    async fn concurrency_limit_returns_429() -> TestResult<()> {
        let limits =
            RpcServerLimits::test_limits(1, Duration::from_secs(5), Bytes::from_mebibytes(1));
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
    async fn timeout_layer_returns_504() -> TestResult<()> {
        let limits =
            RpcServerLimits::test_limits(8, Duration::from_millis(20), Bytes::from_mebibytes(1));
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
    async fn body_limit_returns_413() -> TestResult<()> {
        let limits = RpcServerLimits::test_limits(8, Duration::from_secs(5), Bytes::from_bytes(16));
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
    async fn rpc_responses_include_request_id_header() -> TestResult<()> {
        let limits =
            RpcServerLimits::test_limits(4, Duration::from_secs(1), Bytes::from_bytes(1024));
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
    async fn tcp_binding_defaults_to_loopback() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();

        let addr = BindAddress::from_env_or_default(None)?;
        match addr {
            BindAddress::Tcp { host, port } => {
                assert_eq!(&host, "127.0.0.1");
                assert_eq!(port, 9999);
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn mtls_configuration_is_loaded() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().await;

        std::env::set_var("SINEX_GATEWAY_TLS_CERT", "cert.pem");
        std::env::set_var("SINEX_GATEWAY_TLS_KEY", "key.pem");
        std::env::set_var("SINEX_GATEWAY_TLS_CLIENT_CA", "ca.pem");

        let (cert, key, ca) = tls_paths_from_env()?;
        assert_eq!(cert, "cert.pem");
        assert_eq!(key, "key.pem");
        assert_eq!(ca, Some("ca.pem".to_string()));

        std::env::remove_var("SINEX_GATEWAY_TLS_CLIENT_CA");
        let (_, _, ca) = tls_paths_from_env()?;
        assert!(ca.is_none());

        Ok(())
    }

    #[sinex_test]
    async fn tcp_binding_env_opt_in_respected() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();
        std::env::set_var("SINEX_GATEWAY_TCP_LISTEN", "127.0.0.1:7777");

        let addr = BindAddress::from_env_or_default(None)?;

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
    async fn tcp_binding_cli_override_wins() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();
        std::env::set_var("SINEX_GATEWAY_TCP_LISTEN", "127.0.0.1:7777");

        let addr = BindAddress::from_env_or_default(Some("127.0.0.1:8888"))?;

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
    async fn tcp_binding_invalid_cli_spec_rejected() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();

        let result = BindAddress::from_env_or_default(Some("not-a-valid-spec"));

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn mtls_required_for_non_loopback_bind() -> TestResult<()> {
        let remote = BindAddress::Tcp {
            host: "0.0.0.0".to_string(),
            port: 8080,
        };
        assert!(require_mtls_for_remote(&remote, None).is_err());
        assert!(require_mtls_for_remote(&remote, Some("ca.pem")).is_ok());

        let loopback = BindAddress::Tcp {
            host: "127.0.0.1".to_string(),
            port: 8080,
        };
        assert!(require_mtls_for_remote(&loopback, None).is_ok());
        Ok(())
    }

    #[test]
    fn mtls_override_requires_client_ca() -> TestResult<()> {
        let _guard = ENV_LOCK.blocking_lock();
        std::env::set_var("SINEX_GATEWAY_REQUIRE_CLIENT_TLS", "1");
        let loopback = BindAddress::Tcp {
            host: "127.0.0.1".to_string(),
            port: 8080,
        };
        assert!(require_mtls_for_remote(&loopback, None).is_err());
        assert!(require_mtls_for_remote(&loopback, Some("ca.pem")).is_ok());
        std::env::remove_var("SINEX_GATEWAY_REQUIRE_CLIENT_TLS");
        Ok(())
    }

    #[test]
    fn tls_paths_must_be_set_for_tcp() {
        // Ensure env is clean
        let _guard = ENV_LOCK.blocking_lock();
        std::env::remove_var("SINEX_GATEWAY_TLS_CERT");
        std::env::remove_var("SINEX_GATEWAY_TLS_KEY");

        assert!(
            tls_paths_from_env().is_err(),
            "TLS paths should be required when binding TCP"
        );
    }

    #[sinex_test]
    async fn gateway_auth_blocks_missing_token() -> TestResult<()> {
        let auth = GatewayAuth::with_test_token("secret");
        let headers = HeaderMap::new();
        assert!(matches!(auth.verify(&headers), Err(AuthError::Missing)));
        Ok(())
    }

    #[sinex_test]
    async fn gateway_auth_accepts_bearer_header() -> TestResult<()> {
        let auth = GatewayAuth::with_test_token("secret");
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );
        assert!(auth.verify(&headers).is_ok());
        Ok(())
    }
}

/// Run the RPC server with configurable binding
pub async fn run(
    tcp_listen: Option<&str>,
    services: ServiceContainer,
) -> color_eyre::eyre::Result<()> {
    let bind_address = BindAddress::from_env_or_default(tcp_listen)?;

    let auth = GatewayAuth::from_env()?.start_file_watcher()?;
    let limits = RpcServerLimits::from_env().apply_pool_limit(services.pool_max_connections());
    let state = AppState { services, auth };

    let base_router = Router::new()
        .route("/rpc", post(handle_rpc))
        .route("/", post(handle_rpc))
        .route("/health", get(health_check));

    let app = apply_rpc_layers(base_router, &limits).with_state(state);

    let (cert_path, key_path, client_ca) = tls_paths_from_env()?;
    require_mtls_for_remote(&bind_address, client_ca.as_deref())?;
    warn_if_remote_bind(&bind_address);

    let BindAddress::Tcp { host, port } = bind_address;
    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let tls_config = load_rustls_config(&cert_path, &key_path, client_ca.as_deref())?;
    let acceptor = TlsAcceptor::from(Arc::new(tls_config));
    info!("RPC server listening on TLS {}", addr);

    loop {
        let (stream, peer) = listener.accept().await?;
        let app_clone = app.clone();
        let acceptor = acceptor.clone();

        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let builder = HyperBuilder::new(TokioExecutor::new());
                    let service = TowerToHyperService::new(app_clone);
                    let io = TokioIo::new(tls_stream);
                    if let Err(err) = builder.serve_connection(io, service).await {
                        error!(?err, "TLS RPC connection from {:?} closed with error", peer);
                    }
                }
                Err(err) => {
                    error!(?err, "TLS handshake failed for {:?}", peer);
                }
            }
        });
    }

    #[allow(unreachable_code)]
    Ok(())
}
