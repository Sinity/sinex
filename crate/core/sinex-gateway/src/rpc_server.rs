#![doc = include_str!("../docs/rpc_server.md")]

// Local crate imports
use crate::{
    config::GatewayConfig,
    distributed_rate_limit::{DistributedRateLimitConfig, DistributedRateLimiter},
    gateway_metrics::GatewayMetrics,
    rate_limit::TokenRateLimiter,
    service_container::ServiceContainer,
};

// External crates
use axum::{
    BoxError, Json, Router,
    error_handling::HandleErrorLayer,
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use color_eyre::eyre::{WrapErr, eyre};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperBuilder;
use hyper_util::service::TowerToHyperService;
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::{Deserialize, Serialize};
use sinex_node_sdk::systemd_notify;
use serde_json::Value;
use sinex_primitives::Timestamp;
use sinex_primitives::rpc::JsonRpcError;
use sinex_primitives::{Bytes, Uuid};
use std::convert::TryFrom;
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::task::JoinHandle;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::{TlsAcceptor, rustls};
use tower::{
    ServiceBuilder,
    limit::ConcurrencyLimitLayer,
    load_shed::{LoadShedLayer, error::Overloaded},
    timeout::TimeoutLayer,
};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::TraceLayer;

// Standard library
use thiserror::Error;
use tracing::{debug, error, info, warn};

use std::time::Duration;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
};
pub const DEFAULT_TCP_LISTEN: &str = "127.0.0.1:9999";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    #[serde(default)]
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

#[derive(Debug, Error)]
#[error("Unknown method: {method}")]
struct UnknownMethodError {
    method: String,
}

/// Map `SinexError` variants to JSON-RPC error codes and client-safe messages.
///
/// Code ranges follow JSON-RPC 2.0 conventions:
/// - -32700 to -32600: Protocol errors (parse, invalid request, etc.)
/// - -32099 to -32000: Server errors (reserved)
/// - -32899 to -32800: Application errors (custom)
///
/// Messages are produced via `SinexError::client_message()` — client errors surface
/// their authored primary message; server-internal errors return generic category strings.
/// Context, source chains, and infrastructure details never reach the caller.
fn sinex_error_to_rpc_code(err: &sinex_primitives::error::SinexError) -> (i32, String) {
    use sinex_primitives::error::SinexError;

    let msg = err.client_message().to_string();
    match err {
        // ── Client errors ──
        SinexError::Validation(_) => (-32800, msg),
        SinexError::NotFound(_) => (-32801, msg),
        SinexError::AlreadyExists(_) => (-32802, msg),
        SinexError::InvalidState(_) => (-32803, msg),
        SinexError::PermissionDenied(_) => (-32804, msg),
        SinexError::Parse(_) => (-32805, msg),

        // ── Server-internal errors ──
        SinexError::Database(_) | SinexError::DbPersistenceFailed(_) => (-32810, msg),
        SinexError::Network(_) => (-32811, msg),
        SinexError::Timeout(_) => (-32812, msg),
        SinexError::ResourceExhausted(_) => (-32813, msg),

        SinexError::Service(_) => (-32820, msg),
        SinexError::Io(_) => (-32821, msg),
        SinexError::Configuration(_) => (-32822, msg),
        SinexError::Serialization(_) => (-32823, msg),

        SinexError::Cancelled(_) => (-32830, msg),
        SinexError::MaxRetriesExceeded(_) => (-32831, msg),

        SinexError::ChannelSend(_) | SinexError::ChannelReceive(_) => (-32840, msg),

        SinexError::Kv(_)
        | SinexError::Automaton(_)
        | SinexError::Checkpoint(_)
        | SinexError::Lifecycle(_)
        | SinexError::Processing(_) => (-32850, msg),

        SinexError::BlobStorage(_) => (-32860, msg),
        SinexError::Coordination(_) => (-32861, msg),

        // NATS-specific variants from sinex-primitives.
        SinexError::Nats(_)
        | SinexError::NatsAckFailed(_)
        | SinexError::NatsPublish(_)
        | SinexError::NatsSubscribe(_) => (-32870, msg),

        SinexError::Unknown(_) => (-32899, msg),

        // Required by #[non_exhaustive]. If you added a new SinexError variant and
        // reached this arm, add an explicit mapping above with a dedicated error code.
        _ => {
            tracing::warn!(
                variant = err.variant_name(),
                "Unmapped SinexError variant in RPC error code mapping"
            );
            (-32603, msg)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RpcServerLimits {
    pub(crate) concurrency_limit: usize,
    pub(crate) request_timeout: Duration,
    pub(crate) max_body_bytes: Bytes,
}

impl RpcServerLimits {
    pub(crate) fn from_config(config: &GatewayConfig) -> Self {
        Self {
            concurrency_limit: config.max_concurrency,
            request_timeout: config.request_timeout(),
            max_body_bytes: Bytes::from_bytes(config.max_body_bytes),
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

#[derive(Clone)]
pub(crate) struct GatewayAuth {
    token: Arc<RwLock<Option<String>>>,
    token_path: Option<PathBuf>,
}

impl GatewayAuth {
    fn store_token(token: &RwLock<Option<String>>, new_token: String) {
        let mut token_guard = match token.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("Gateway token lock poisoned during reload; continuing with inner state");
                poisoned.into_inner()
            }
        };
        *token_guard = Some(new_token);
    }

    fn reload_token_from_path(token: &RwLock<Option<String>>, path: &Path) {
        match std::fs::read_to_string(path) {
            Ok(new_token) => {
                let trimmed = new_token.trim().to_string();
                if trimmed.is_empty() {
                    warn!("Token file {:?} is empty after reload", path);
                } else {
                    Self::store_token(token, trimmed);
                    info!("RPC token reloaded from {:?}", path);
                }
            }
            Err(error) => {
                error!(
                    "Failed to read token file {:?} after modification: {}",
                    path, error
                );
            }
        }
    }

    fn from_config(config: &GatewayConfig) -> color_eyre::eyre::Result<Self> {
        let (token, token_path) = config
            .auth_token_from_config()
            .map_err(|err| eyre!(err.to_string()))?;

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

    fn start_file_watcher(
        self,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> color_eyre::eyre::Result<Self> {
        if let Some(ref path) = self.token_path {
            let token_clone = Arc::clone(&self.token);
            let path_clone = path.clone();
            let path_for_closure = path.clone();

            // Bridge the async shutdown watch into a sync channel so the OS-thread
            // watcher can block cleanly instead of polling with sleep().
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            {
                let mut shutdown_clone = shutdown.clone();
                tokio::spawn(async move {
                    // wait_for blocks until the predicate matches or the sender is dropped.
                    let _ = shutdown_clone.wait_for(|v| *v).await;
                    let _ = done_tx.send(());
                });
            }

            let (ready_tx, ready_rx) =
                std::sync::mpsc::sync_channel::<color_eyre::eyre::Result<()>>(1);

            std::thread::spawn(move || {
                use notify::{Event, EventKind, RecursiveMode, Watcher};
                let mut ready_tx = Some(ready_tx);

                let watcher = notify::recommended_watcher(
                    move |res: Result<Event, notify::Error>| {
                        match res {
                            Ok(event) => {
                                match event.kind {
                                    EventKind::Modify(_) | EventKind::Create(_) => {
                                        Self::reload_token_from_path(&token_clone, &path_for_closure);
                                    }
                                    EventKind::Remove(_) => {
                                        // File was deleted — keep last valid token (fail-closed).
                                        // Do NOT clear the token, as that would disable auth entirely,
                                        // allowing unauthenticated access. If the file is recreated,
                                        // the Create/Modify handler will reload it.
                                        error!(
                                            "RPC token file {:?} deleted! Keeping last valid token. \
                                               Re-create the file to update the token.",
                                            path_for_closure
                                        );
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
                    },
                );

                let mut watcher = match watcher {
                    Ok(w) => w,
                    Err(e) => {
                        if let Some(tx) = ready_tx.take() {
                            let _ = tx.send(Err(eyre!("Failed to create file watcher: {e}")));
                        }
                        error!("Failed to create file watcher: {}", e);
                        return;
                    }
                };

                if let Err(e) = watcher.watch(&path_clone, RecursiveMode::NonRecursive) {
                    if let Some(tx) = ready_tx.take() {
                        let _ = tx.send(Err(eyre!(
                            "Failed to watch token file {:?}: {e}",
                            path_clone
                        )));
                    }
                    error!("Failed to watch token file {:?}: {}", path_clone, e);
                    return;
                }

                if let Some(tx) = ready_tx.take() {
                    let _ = tx.send(Ok(()));
                }
                info!("Watching token file {:?} for changes", path_clone);

                // Block until the shutdown signal fires; no busy-polling.
                let _ = done_rx.recv();
                debug!("Token file watcher shutting down");
            });

            match ready_rx.recv_timeout(Duration::from_secs(2)) {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    return Err(eyre!(
                        "Timed out waiting for token file watcher to initialize for {:?}",
                        path
                    ));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(eyre!(
                        "Token file watcher thread exited before initialization for {:?}",
                        path
                    ));
                }
            }
        }

        Ok(self)
    }

    /// Verify the bearer token in the request headers.
    /// Returns the verified token string on success so callers need not re-extract it.
    pub(crate) fn verify(&self, headers: &HeaderMap) -> Result<String, AuthError> {
        let provided = extract_token(headers).ok_or(AuthError::Missing)?;

        let token_guard = match self.token.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("Gateway token lock poisoned during verify; continuing with inner state");
                poisoned.into_inner()
            }
        };
        if let Some(expected) = token_guard.as_ref() {
            if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
                Ok(provided)
            } else {
                Err(AuthError::Invalid)
            }
        } else {
            warn!("No token configured - rejecting request");
            Err(AuthError::Missing)
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
    if let Some(value) = headers.get(header::AUTHORIZATION)
        && let Ok(as_str) = value.to_str()
    {
        let trimmed = as_str.trim();
        if let Some(rest) = trimmed.strip_prefix("Bearer ") {
            return Some(rest.trim().to_string());
        }
    }

    None
}

// Issue 137: Use constant-time comparison from subtle crate
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    bool::from(a.ct_eq(b))
}

pub(crate) enum AuthError {
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

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::Missing => write!(
                f,
                "authentication required: provide SINEX_RPC_TOKEN via Authorization header"
            ),
            AuthError::Invalid => write!(f, "authentication failed: invalid token"),
        }
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

    fn error_with_data(id: Option<Value>, code: i32, message: String, data: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: Some(data),
            }),
            id,
        }
    }
}

/// Authorization context passed to RPC handlers
///
/// Contains actor information derived from the authenticated token,
/// allowing handlers to perform authorization checks and audit logging.
#[derive(Debug, Clone)]
pub struct RpcAuthContext {
    /// First 8 characters of the token for audit logging
    pub token_prefix: String,
    /// Stable actor identity for access audit records
    pub actor_id: String,
    /// Timestamp when authentication occurred
    pub authenticated_at: Timestamp,
    /// Role extracted from token (determines permissions)
    pub role: crate::auth::Role,
}

impl RpcAuthContext {
    /// Create an auth context from a validated token
    ///
    /// Parses the role from the token suffix (e.g., `sinex_xxx:readonly`)
    pub(crate) fn from_token(token: &str) -> Result<Self, crate::auth::TokenRoleError> {
        let (base, role) = crate::auth::Role::from_token(token)?;
        let token_prefix = base.chars().take(8).collect::<String>();
        Ok(Self {
            actor_id: format!("token:{token_prefix}"),
            token_prefix,
            authenticated_at: Timestamp::now(),
            role,
        })
    }

    /// Create a system auth context for native messaging or internal calls
    ///
    /// Native messaging uses stdin/stdout and doesn't go through HTTP auth,
    /// so we use a special "system" context to indicate trusted local calls.
    /// System context always has Admin role.
    #[must_use]
    pub fn system() -> Self {
        Self {
            token_prefix: "system".to_string(),
            actor_id: "system:local".to_string(),
            authenticated_at: Timestamp::now(),
            role: crate::auth::Role::Admin,
        }
    }

    /// Create an auth context for a native messaging extension
    ///
    /// Used when native messaging can attribute calls to specific browser extensions.
    /// The role is determined by the `SINEX_NATIVE_MESSAGING_EXTENSION_ROLES` env var.
    /// Unknown extensions default to `ReadOnly` for defense in depth.
    #[must_use]
    pub fn extension(extension_id: &str, role: crate::auth::Role) -> Self {
        Self {
            token_prefix: format!("ext:{}", &extension_id[..extension_id.len().min(8)]),
            actor_id: format!("extension:{extension_id}"),
            authenticated_at: Timestamp::now(),
            role,
        }
    }

    #[must_use]
    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }

    #[must_use]
    pub fn replay_actor(&self) -> String {
        if self.actor_id.starts_with("system:") {
            return self.actor_id.clone();
        }

        let replay_role = match self.role {
            crate::auth::Role::Admin => "admin",
            crate::auth::Role::Write => "operator",
            crate::auth::Role::ReadOnly => "user",
        };
        format!("{replay_role}:{}", self.actor_id)
    }

    /// Check if the token has at least the required role permission
    #[must_use]
    pub fn has_permission(&self, required: crate::auth::Role) -> bool {
        self.role.has_permission(required)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AccessOutcome {
    Success,
    Failed,
    Unauthenticated,
    Rejected,
    RateLimited,
    InvalidRequest,
    Forbidden,
    Unavailable,
}

impl AccessOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Unauthenticated => "unauthenticated",
            Self::Rejected => "rejected",
            Self::RateLimited => "rate_limited",
            Self::InvalidRequest => "invalid_request",
            Self::Forbidden => "forbidden",
            Self::Unavailable => "unavailable",
        }
    }
}

pub(crate) fn log_access_audit(
    surface: &'static str,
    operation: &str,
    outcome: AccessOutcome,
    auth: Option<&RpcAuthContext>,
    detail: Option<&str>,
) {
    let actor = auth.map_or("anonymous", RpcAuthContext::actor_id);
    let role = auth.map_or("none", |ctx| ctx.role.as_str());

    match (outcome, detail) {
        (AccessOutcome::Success, _) => info!(
            event = "gateway.access",
            surface,
            operation,
            outcome = outcome.as_str(),
            actor,
            role,
            "Gateway access allowed"
        ),
        (_, Some(detail)) => warn!(
            event = "gateway.access",
            surface,
            operation,
            outcome = outcome.as_str(),
            actor,
            role,
            detail,
            "Gateway access denied or failed"
        ),
        _ => warn!(
            event = "gateway.access",
            surface,
            operation,
            outcome = outcome.as_str(),
            actor,
            role,
            "Gateway access denied or failed"
        ),
    }
}

/// Rate limiter that can be either in-memory or distributed via NATS KV
#[derive(Clone)]
pub(crate) enum RateLimiter {
    /// In-memory rate limiter (fast, but state lost on restart)
    InMemory(Arc<TokenRateLimiter>),
    /// Distributed rate limiter via NATS KV (shared across instances, survives restarts)
    Distributed(Arc<DistributedRateLimiter>),
}

impl RateLimiter {
    /// Check if request is allowed for the given token
    async fn check(&self, token: &str) -> bool {
        match self {
            RateLimiter::InMemory(limiter) => limiter.check(token).is_ok(),
            RateLimiter::Distributed(limiter) => limiter.check_and_increment(token).await,
        }
    }
}

/// State shared between handlers
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) services: ServiceContainer,
    pub(crate) auth: GatewayAuth,
    pub(crate) rate_limiter: RateLimiter,
    pub(crate) metrics: Arc<GatewayMetrics>,
    pub(crate) sse_bus: Option<Arc<crate::sse_bus::SubscriptionBus>>,
}

/// Shared dispatch function for RPC methods (used by both `rpc_server` and `native_messaging`)
///
/// # Method Dispatch Pattern
///
/// This function uses a registry-based dispatch mechanism for method routing.
/// The registry is built once at startup via `build_registry()` and maps method
/// names to handler functions with required roles.
///
/// Benefits of registry-based dispatch:
/// - Centralized method registration (all methods visible in `build_registry()`)
/// - Type-safe handler signatures enforced at registration time
/// - Role requirements declared alongside method registration
/// - Easy to extend with middleware or instrumentation
///
/// # Authorization Context
///
/// The `auth` parameter contains authenticated actor information for audit logging
/// and authorization checks. Role-based access control (RBAC) is enforced:
///
/// - **`ReadOnly`**: Query operations (search, analytics, status)
/// - **Write**: `ReadOnly` + mutations (create entities, store blobs)
/// - **Admin**: Write + destructive operations (tombstone, DLQ, shadow delete)
#[tracing::instrument(skip(services, params, auth), fields(surface, method))]
pub async fn dispatch_rpc_method(
    surface: &'static str,
    services: &ServiceContainer,
    method: &str,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> color_eyre::eyre::Result<serde_json::Value> {
    // Use lazy static registry for zero-cost dispatch
    use std::sync::OnceLock;
    static REGISTRY: OnceLock<crate::rpc_registry::RpcRegistry> = OnceLock::new();
    let registry = REGISTRY.get_or_init(crate::rpc_registry::build_registry);

    let result = registry.dispatch(method, params, services, auth).await;
    match &result {
        Ok(_) => log_access_audit(surface, method, AccessOutcome::Success, Some(auth), None),
        Err(err) => {
            let detail = err.to_string();
            log_access_audit(
                surface,
                method,
                AccessOutcome::Failed,
                Some(auth),
                Some(&detail),
            );
        }
    }
    result
}

/// Health and readiness check endpoint
///
/// Returns 200 OK while the gateway can still serve DB-backed RPCs; the JSON
/// body distinguishes full health from degraded operation.
///
/// NATS or replay-control failures no longer present as "healthy". Operators
/// should read `status`, `healthy`, and `degradation_reasons` rather than
/// treating HTTP 200 as full readiness.
async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    let report = state.services.health_report().await;
    let crate::service_container::GatewayHealthReport {
        status: overall_status,
        db_ok,
        nats,
        replay,
        healthy,
        serving,
        degradation_reasons,
    } = report;

    let status = if serving {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        axum::Json(serde_json::json!({
            "status": overall_status,
            "healthy": healthy,
            "serving": serving,
            "degradation_reasons": degradation_reasons,
            "db": { "ok": db_ok },
            "nats": {
                "connected": nats.connected,
                "latency_ms": nats.latency_ms,
                "detail": nats.detail,
            },
            "replay_control": {
                "enabled": replay.enabled,
                "connected": replay.connected,
                "last_error": replay.last_error,
            },
        })),
    )
        .into_response()
}

/// Main RPC handler using dispatch table
///
/// # Issue 148 (LOW): Request IDs in JSON-RPC Responses
///
/// The gateway includes request IDs in HTTP response headers via `x-request-id`
/// (see middleware layers in `apply_rpc_layers`). This is sufficient for HTTP-level
/// tracing and correlation with load balancer/proxy logs.
///
/// JSON-RPC 2.0 spec strictly defines the response format:
/// - `jsonrpc`: "2.0"
/// - `result` or `error`: method result or error object
/// - `id`: echoes the request ID from the JSON-RPC request
///
/// Adding an HTTP request ID to the JSON-RPC response body would be non-standard.
/// Clients should use the `x-request-id` HTTP header for request correlation.
///
/// For applications requiring request IDs in the response payload, consider:
/// - Reading `x-request-id` from response headers
/// - Using JSON-RPC request `id` field for correlation
/// - Adding a custom middleware layer that wraps responses with metadata
async fn handle_rpc(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    // Record request start for metrics
    state.metrics.record_request_start();
    let start = std::time::Instant::now();

    let token = match state.auth.verify(&headers) {
        Ok(t) => t,
        Err(err) => {
            state.metrics.record_request_rejected();
            let detail = err.to_string();
            log_access_audit(
                "rpc",
                &request.method,
                AccessOutcome::Unauthenticated,
                None,
                Some(&detail),
            );
            return err.into_response();
        }
    };

    // Create auth context for handlers
    let auth_context = match RpcAuthContext::from_token(&token) {
        Ok(ctx) => ctx,
        Err(err) => {
            state.metrics.record_request_rejected();
            let detail = err.to_string();
            log_access_audit(
                "rpc",
                &request.method,
                AccessOutcome::Rejected,
                None,
                Some(&detail),
            );
            return (
                StatusCode::UNAUTHORIZED,
                Json(JsonRpcResponse::error(
                    request.id.clone(),
                    -32001,
                    format!("Invalid token role encoding: {err}"),
                )),
            );
        }
    };

    // Issue 143: Per-token rate limiting
    if !state.rate_limiter.check(&token).await {
        let token_prefix = &token[..8.min(token.len())];
        warn!(token_prefix, "Request rejected: rate limit exceeded");
        state.metrics.record_rate_limited();
        log_access_audit(
            "rpc",
            &request.method,
            AccessOutcome::RateLimited,
            Some(&auth_context),
            None,
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(JsonRpcResponse::error(
                request.id.clone(),
                -32029,
                "Rate limit exceeded for this token".to_string(),
            )),
        );
    }

    if let Err(err) = validate_jsonrpc_request(&request) {
        state.metrics.record_request_rejected();
        let detail = err.to_string();
        log_access_audit(
            "rpc",
            &request.method,
            AccessOutcome::InvalidRequest,
            Some(&auth_context),
            Some(&detail),
        );
        let response = JsonRpcResponse::error(request.id, -32600, err.to_string());
        return (StatusCode::BAD_REQUEST, Json(response));
    }

    debug!(
        "Received RPC request: method={}, params={:?}",
        request.method, request.params
    );

    let method = request.method.clone();

    // Use shared dispatch function with auth context
    let result = dispatch_rpc_method(
        "rpc",
        &state.services,
        &request.method,
        request.params,
        &auth_context,
    )
    .await;

    // Record latency on success
    let latency_us = start.elapsed().as_micros() as u64;

    let response = match result {
        Ok(value) => {
            state.metrics.record_request_success(latency_us);
            JsonRpcResponse::success(request.id, value)
        }
        Err(err) if err.downcast_ref::<UnknownMethodError>().is_some() => {
            state.metrics.record_request_rejected();
            JsonRpcResponse::error(request.id, -32601, err.to_string())
        }
        Err(err) => {
            let error_id = Uuid::now_v7();
            state.metrics.record_request_rejected();
            error!(
                error_id = %error_id,
                method = %method,
                error = %err,
                "RPC method failed"
            );

            // Try to extract structured error info from SinexError
            if let Some(sinex_err) = err.downcast_ref::<sinex_primitives::error::SinexError>() {
                let (code, message) = sinex_error_to_rpc_code(sinex_err);

                // Feature-gated error detail serialization (OPP-002)
                // In dev mode, include full error for debugging.
                // In production, only return error_id for log correlation.
                #[cfg(feature = "dev-errors")]
                let data = serde_json::json!({
                    "error_id": error_id.to_string(),
                    "error": sinex_err,  // Full error details in dev mode
                });

                #[cfg(not(feature = "dev-errors"))]
                let data = serde_json::json!({
                    "error_id": error_id.to_string(),
                    // No internal error details - check server logs with error_id
                });

                JsonRpcResponse::error_with_data(request.id, code, message, data)
            } else {
                JsonRpcResponse::error(
                    request.id,
                    -32603,
                    format!("Internal error (ref: {error_id})"),
                )
            }
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
    /// Create bind address from loaded gateway configuration.
    fn from_config(config: &GatewayConfig) -> color_eyre::eyre::Result<Self> {
        let (host, port) = parse_tcp_listen(&config.tcp_listen)?;
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

/// Read RPC token from environment variables.
/// Priority: `SINEX_GATEWAY_ADMIN_TOKEN_FILE` > `SINEX_RPC_TOKEN_FILE` > `SINEX_RPC_TOKEN`
///
/// Used by test support utilities and external consumers that need token access.
pub fn read_token_from_env() -> color_eyre::eyre::Result<Option<String>> {
    let (token, _) = read_token_and_path_from_env()?;
    Ok(token)
}

/// Backlog size for the TCP listener.
///
/// 128 matches the traditional `SOMAXCONN` default and is sufficient for gateway
/// workloads. The kernel may clamp this to the system-configured maximum.
const TCP_LISTEN_BACKLOG: i32 = 128;

/// Bind a TCP listener with `SO_REUSEPORT` for seamless hot reload
///
/// This allows multiple processes to bind to the same port simultaneously,
/// enabling zero-downtime upgrades:
/// - Old instance continues serving while new instance starts
/// - Both can accept connections (kernel load balances)
/// - Coordination/handoff mechanism ensures only one is the leader
/// - Old instance exits gracefully after handoff
async fn bind_with_reuseport(addr: &str) -> std::io::Result<tokio::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::SocketAddr;

    let socket_addr: SocketAddr = addr
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let domain = if socket_addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

    // Enable SO_REUSEADDR (standard practice)
    socket.set_reuse_address(true)?;

    // SO_REUSEPORT is intentionally skipped here: the current socket2 build in this
    // workspace does not expose `set_reuse_port` on `Socket`.

    socket.set_nonblocking(true)?;
    socket.bind(&socket_addr.into())?;
    socket.listen(TCP_LISTEN_BACKLOG)?;

    // Convert socket2::Socket to std::net::TcpListener then to tokio::net::TcpListener
    let std_listener: std::net::TcpListener = socket.into();
    std_listener.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(std_listener)
}

fn tls_paths_from_config(
    config: &GatewayConfig,
) -> color_eyre::eyre::Result<(String, String, Option<String>)> {
    let cert = config.tls_cert.clone().ok_or_else(|| {
        eyre!(
            "SINEX_GATEWAY_TLS_CERT is required for TCP bindings\n\n\
            For local development, run `xtask doctor --fix` to auto-generate certificates.\n\
            For production, provide proper certificates via environment variables."
        )
    })?;
    let key = config.tls_key.clone().ok_or_else(|| {
        eyre!(
            "SINEX_GATEWAY_TLS_KEY is required for TCP bindings\n\n\
            For local development, run `xtask doctor --fix` to auto-generate certificates.\n\
            For production, provide proper certificates via environment variables."
        )
    })?;
    let client_ca = config.tls_client_ca.clone();
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
        .map_err(|e| eyre!("Failed to read TLS certificate from {cert_path}: {e}"))?
        .into_iter()
        .map(CertificateDer::from)
        .collect();

    let mut keys: Vec<PrivateKeyDer<'static>> = pkcs8_private_keys(key_file)
        .map_err(|e| eyre!("Failed to read TLS private key (pkcs8) from {key_path}: {e}"))?
        .into_iter()
        .map(|raw| {
            PrivateKeyDer::try_from(raw)
                .map_err(|e| eyre!("Failed to parse TLS private key (pkcs8): {e}"))
        })
        .collect::<Result<_, _>>()?;
    if keys.is_empty() {
        let mut key_file = BufReader::new(File::open(key_path)?);
        keys = rsa_private_keys(&mut key_file)
            .map_err(|e| eyre!("Failed to read TLS private key (rsa) from {key_path}: {e}"))?
            .into_iter()
            .map(|raw| {
                PrivateKeyDer::try_from(raw)
                    .map_err(|e| eyre!("Failed to parse TLS private key (rsa): {e}"))
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
            .map_err(|e| eyre!("Failed to read client CA bundle from {ca_path}: {e}"))?
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

/// Enforce mTLS requirements based on bind address and configuration
///
/// # Security Note (Issue 151 - LOW)
///
/// The gateway currently requires mTLS for all TCP bindings. For deployments
/// behind a reverse proxy (nginx, `HAProxy`, Envoy), the proxy should handle
/// TLS termination and client authentication. In this configuration:
///
/// - Bind gateway to 127.0.0.1 (loopback only)
/// - Configure reverse proxy with TLS certificates
/// - Set up client certificate verification in the proxy
/// - Use `SINEX_GATEWAY_REQUIRE_CLIENT_TLS=0` if proxy handles mTLS
///
/// For direct TLS support without a proxy, native rustls integration is already
/// implemented in this file (see `load_rustls_config` and TLS acceptor logic).
fn require_mtls_for_remote(
    bind_address: &BindAddress,
    require_client_tls: bool,
    client_ca: Option<&str>,
) -> color_eyre::eyre::Result<()> {
    let host_requires = match bind_address {
        BindAddress::Tcp { host, .. } => !is_loopback_host(host),
    };

    if (host_requires || require_client_tls) && client_ca.is_none() {
        return Err(eyre!(
            "SINEX_GATEWAY_TLS_CLIENT_CA is required when mTLS is enforced (non-loopback or SINEX_GATEWAY_REQUIRE_CLIENT_TLS=1)"
        ));
    }
    Ok(())
}

fn warn_if_remote_bind(bind_address: &BindAddress) {
    let BindAddress::Tcp { host, .. } = bind_address;
    if !is_loopback_host(host) {
        warn!(
            bind_host = %host,
            "Gateway RPC is exposed beyond loopback; ensure mTLS and firewalling are configured"
        );
    }
}

fn apply_rpc_layers<S>(
    router: Router<S>,
    limits: &RpcServerLimits,
    cors_origins: &[String],
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let request_id_header = HeaderName::from_static("x-request-id");

    // Configure CORS: if no origins specified, allow localhost only
    let cors = if cors_origins.is_empty() {
        CorsLayer::new()
            .allow_origin(AllowOrigin::predicate(|origin, _| {
                origin.to_str().is_ok_and(|s| {
                    s.starts_with("http://localhost:") || s.starts_with("http://127.0.0.1:")
                })
            }))
            .allow_methods([Method::POST, Method::GET, Method::OPTIONS])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
    } else {
        let origins: Vec<HeaderValue> = cors_origins
            .iter()
            .filter_map(|o| HeaderValue::from_str(o).ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([Method::POST, Method::GET, Method::OPTIONS])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
    };

    // Note: TimeoutLayer is NOT applied here — it's applied per-route-group
    // in build_app() so that SSE (long-lived) routes are exempt from timeout.
    router
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_layer_error))
                .layer(LoadShedLayer::new())
                .layer(ConcurrencyLimitLayer::new(limits.concurrency_limit))
                .layer(RequestBodyLimitLayer::new(limits.max_body_bytes.as_usize()))
                .layer(cors)
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
        .layer(SetRequestIdLayer::new(request_id_header, MakeRequestUuid))
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

    let message = format!("Unhandled middleware error: {err}");
    rpc_layer_error_response(StatusCode::INTERNAL_SERVER_ERROR, -32099, message)
}

fn rpc_layer_error_response(status: StatusCode, code: i32, message: String) -> impl IntoResponse {
    (status, Json(JsonRpcResponse::error(None, code, message)))
}

/// Run the RPC server with configurable binding
///
/// Accepts a shutdown signal receiver that will trigger graceful shutdown when signaled.
///
/// # CORS Configuration
/// The `cors_origins` parameter controls allowed origins:
/// - Empty: Only localhost origins allowed (<http://localhost>:*, <http://127.0.0.1>:*)
/// - Non-empty: Only the specified origins allowed
/// Spawn the RPC server in a background task, returning the bound address and task handle.
///
/// This is used for integration testing (binding to port 0) and by the main `run` entry point.
pub async fn spawn(
    config: &GatewayConfig,
    services: ServiceContainer,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> color_eyre::eyre::Result<(
    std::net::SocketAddr,
    tokio::task::JoinHandle<color_eyre::eyre::Result<()>>,
)> {
    let bind_address = BindAddress::from_config(config)?;

    // Create shutdown channels for background tasks
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let auth = GatewayAuth::from_config(config)?.start_file_watcher(shutdown_rx.clone())?;
    let limits =
        RpcServerLimits::from_config(config).apply_pool_limit(services.pool_max_connections());

    // Read TLS config synchronously before any await points.
    // This prevents a race where concurrent tests overwrite the env vars during an async yield.
    let (addr_str, acceptor) = RpcServer::setup_tls_listener(config, &bind_address)?;

    let (rate_limiter, cleanup_task) =
        RpcServer::init_rate_limiter(config, &services, shutdown_rx.clone()).await?;
    let (metrics, metrics_task) = RpcServer::init_metrics(&services, shutdown_rx.clone());

    // SSE subscription bus — only if NATS is connected
    let (sse_bus, sse_bus_task) = if let Some(nats_client) = services.nats_client().cloned() {
        let bus = Arc::new(crate::sse_bus::SubscriptionBus::new());
        let pool = services.pool().clone();
        let env = services.environment().clone();
        let bus_shutdown = shutdown_rx.clone();
        let bus_ref = Arc::clone(&bus);
        let task = tokio::spawn(async move {
            bus_ref.run(nats_client, pool, env, bus_shutdown).await;
        });
        info!("SSE subscription bus spawned");
        (Some(bus), Some(task))
    } else {
        info!("NATS not connected — SSE event streaming disabled");
        (None, None)
    };

    let state = AppState {
        services,
        auth,
        rate_limiter,
        metrics,
        sse_bus,
    };

    let app = RpcServer::build_app(&limits, &config.cors_origins_list(), state);
    let listener = bind_with_reuseport(&addr_str)
        .await
        .wrap_err_with(|| format!("Failed to bind TCP listener to {addr_str}"))?;

    let local_addr = listener.local_addr()?;
    info!("RPC server listening on TLS {}", local_addr);

    systemd_notify::notify_ready("sinex-gateway");
    let watchdog_handle = systemd_notify::spawn_watchdog("sinex-gateway");

    let handle = tokio::spawn(async move {
        // Run accept loop until shutdown signal
        let accept_result = RpcServer::accept_loop(listener, acceptor, app, &mut shutdown).await;
        systemd_notify::stop_watchdog(watchdog_handle, "sinex-gateway").await;
        systemd_notify::notify_stopping("sinex-gateway");
        accept_result?;

        // Signal all background tasks to shut down
        info!("Shutting down background tasks...");
        let _ = shutdown_tx.send(true);

        RpcServer::wait_for_background_tasks(metrics_task, cleanup_task, sse_bus_task).await;

        info!("RPC server shutdown complete");
        Ok(())
    });

    Ok((local_addr, handle))
}

/// Run the RPC server with configurable binding (blocking until shutdown)
///
/// Accepts a shutdown signal receiver that will trigger graceful shutdown when signaled.
///
/// # CORS Configuration
/// The `cors_origins` parameter controls allowed origins:
/// - Empty: Only localhost origins allowed (<http://localhost>:*, <http://127.0.0.1>:*)
/// - Non-empty: Only the specified origins allowed
pub async fn run(
    config: &GatewayConfig,
    services: ServiceContainer,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> color_eyre::eyre::Result<()> {
    let (_, handle) = spawn(config, services, shutdown).await?;
    match handle.await {
        Ok(res) => res,
        Err(e) => Err(eyre!("RPC server task panicked: {}", e)),
    }
}

/// Helper struct for the server runner organization
struct RpcServer;

impl RpcServer {
    async fn init_rate_limiter(
        config: &GatewayConfig,
        services: &ServiceContainer,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> color_eyre::eyre::Result<(RateLimiter, Option<JoinHandle<()>>)> {
        // Issue 143: Per-token rate limiting
        // Use distributed rate limiting via NATS KV when available, fall back to in-memory
        if let Some(nats) = services.nats_client() {
            let jetstream = async_nats::jetstream::new(nats.clone());
            let distributed = DistributedRateLimitConfig::from_gateway_config(config);
            match DistributedRateLimiter::new(jetstream, distributed).await {
                Ok(limiter) => {
                    info!(
                        "Using distributed rate limiting via NATS KV (shared across instances, survives restarts)"
                    );
                    Ok((RateLimiter::Distributed(Arc::new(limiter)), None))
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "Failed to create distributed rate limiter, falling back to in-memory"
                    );
                    let in_memory = Arc::new(TokenRateLimiter::from_gateway_config(config));
                    let task = Arc::clone(&in_memory).spawn_cleanup_task(shutdown_rx);
                    Ok((RateLimiter::InMemory(in_memory), Some(task)))
                }
            }
        } else {
            info!("NATS not available - using in-memory rate limiting (state lost on restart)");
            let in_memory = Arc::new(TokenRateLimiter::from_gateway_config(config));
            let task = Arc::clone(&in_memory).spawn_cleanup_task(shutdown_rx);
            Ok((RateLimiter::InMemory(in_memory), Some(task)))
        }
    }

    fn init_metrics(
        services: &ServiceContainer,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> (Arc<GatewayMetrics>, Option<JoinHandle<()>>) {
        let metrics = Arc::new(if let Some(nats) = services.nats_client() {
            GatewayMetrics::new(nats.clone())
        } else {
            info!("NATS not available - gateway metrics emission disabled");
            GatewayMetrics::disabled()
        });

        let metrics_task = if metrics.is_enabled() {
            Some(Arc::clone(&metrics).spawn_emission_task(shutdown_rx))
        } else {
            None
        };

        (metrics, metrics_task)
    }

    fn setup_router() -> Router<AppState> {
        Router::new()
            .route("/rpc", post(handle_rpc))
            .route("/", post(handle_rpc))
            .route("/health", get(health_check))
            .route("/ready", get(health_check))
    }

    /// Build the complete app with split middleware:
    /// - RPC routes get `TimeoutLayer` + `HandleErrorLayer` (short-lived requests)
    /// - SSE route does NOT get `TimeoutLayer` (long-lived connections)
    /// - Both share outer layers: concurrency, CORS, trace, body limit
    fn build_app(limits: &RpcServerLimits, cors_origins: &[String], state: AppState) -> Router {
        use crate::sse_handler::handle_sse_stream;

        // RPC routes with timeout (HandleErrorLayer converts timeout to 504)
        let rpc_routes = Self::setup_router().layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_layer_error))
                .layer(TimeoutLayer::new(limits.request_timeout))
                .into_inner(),
        );

        // SSE route without timeout (long-lived connections)
        let sse_route = Router::new().route("/events/stream", get(handle_sse_stream));

        // Merge and apply shared outer layers (concurrency, CORS, trace, body limit)
        let merged = rpc_routes.merge(sse_route);
        apply_rpc_layers(merged, limits, cors_origins).with_state(state)
    }

    fn setup_tls_listener(
        config: &GatewayConfig,
        bind_address: &BindAddress,
    ) -> color_eyre::eyre::Result<(String, TlsAcceptor)> {
        let (cert_path, key_path, client_ca) = tls_paths_from_config(config)?;
        require_mtls_for_remote(
            bind_address,
            config.require_client_tls,
            client_ca.as_deref(),
        )?;
        warn_if_remote_bind(bind_address);

        let BindAddress::Tcp { host, port } = bind_address;
        let addr = format!("{host}:{port}");
        let tls_config = load_rustls_config(&cert_path, &key_path, client_ca.as_deref())?;
        let acceptor = TlsAcceptor::from(Arc::new(tls_config));

        Ok((addr, acceptor))
    }

    async fn accept_loop(
        listener: tokio::net::TcpListener,
        acceptor: TlsAcceptor,
        app: Router,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> color_eyre::eyre::Result<()> {
        let active_connections = Arc::new(AtomicUsize::new(0));
        let drain_notify = Arc::new(tokio::sync::Notify::new());

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, peer) = accept_result
                        .wrap_err("Failed to accept incoming TCP connection")?;
                    let app_clone = app.clone();
                    let acceptor = acceptor.clone();
                    let active_connections = Arc::clone(&active_connections);
                    let drain_notify = Arc::clone(&drain_notify);

                    tokio::spawn(async move {
                        struct ConnectionGuard {
                            active: Arc<AtomicUsize>,
                            notify: Arc<tokio::sync::Notify>,
                        }

                        impl Drop for ConnectionGuard {
                            fn drop(&mut self) {
                                if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
                                    self.notify.notify_waiters();
                                }
                            }
                        }

                        active_connections.fetch_add(1, Ordering::AcqRel);
                        let _guard = ConnectionGuard {
                            active: active_connections,
                            notify: drain_notify,
                        };

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
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("Shutdown signal received, stopping RPC server");
                        break;
                    }
                }
            }
        }

        // Drain in-flight connections for up to 30s before returning.
        let drain_timeout = Duration::from_secs(30);
        let drain_start = std::time::Instant::now();
        loop {
            let remaining = active_connections.load(Ordering::Acquire);
            if remaining == 0 {
                break;
            }

            let elapsed = drain_start.elapsed();
            if elapsed >= drain_timeout {
                warn!(
                    active_connections = remaining,
                    "Timed out waiting for active RPC connections to drain"
                );
                break;
            }

            let wait_budget = std::cmp::min(
                Duration::from_millis(250),
                drain_timeout.saturating_sub(elapsed),
            );
            tokio::select! {
                () = drain_notify.notified() => {}
                () = tokio::time::sleep(wait_budget) => {}
            }
        }

        Ok(())
    }

    async fn wait_for_background_tasks(
        metrics_task: Option<JoinHandle<()>>,
        cleanup_task: Option<JoinHandle<()>>,
        sse_bus_task: Option<JoinHandle<()>>,
    ) {
        let shutdown_timeout = std::time::Duration::from_secs(30);

        if let Some(task) = metrics_task {
            info!("Awaiting metrics emission task shutdown...");
            match tokio::time::timeout(shutdown_timeout, task).await {
                Ok(Ok(())) => info!("Metrics emission task shut down successfully"),
                Ok(Err(e)) => warn!(?e, "Metrics emission task exited with error"),
                Err(_) => warn!(
                    "Metrics emission task did not shut down within {:?}",
                    shutdown_timeout
                ),
            }
        }

        if let Some(task) = cleanup_task {
            info!("Awaiting rate limiter cleanup task shutdown...");
            match tokio::time::timeout(shutdown_timeout, task).await {
                Ok(Ok(())) => info!("Rate limiter cleanup task shut down successfully"),
                Ok(Err(e)) => warn!(?e, "Rate limiter cleanup task exited with error"),
                Err(_) => warn!(
                    "Rate limiter cleanup task did not shut down within {:?}",
                    shutdown_timeout
                ),
            }
        }

        if let Some(task) = sse_bus_task {
            info!("Awaiting SSE subscription bus shutdown...");
            match tokio::time::timeout(shutdown_timeout, task).await {
                Ok(Ok(())) => info!("SSE subscription bus shut down successfully"),
                Ok(Err(e)) => warn!(?e, "SSE subscription bus exited with error"),
                Err(_) => warn!(
                    "SSE subscription bus did not shut down within {:?}",
                    shutdown_timeout
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        http::{HeaderMap, HeaderValue},
        routing::post,
    };
    use reqwest::Client;
    use serde_json::json;
    use std::net::SocketAddr;
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;
    use xtask::sandbox::sinex_test;
    static ENV_LOCK: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));

    fn clear_tcp_env() {
        unsafe { std::env::remove_var("SINEX_GATEWAY_TCP_LISTEN") };
    }

    fn gateway_config_from_env() -> GatewayConfig {
        GatewayConfig::load()
    }

    fn clear_auth_env() {
        unsafe {
            std::env::remove_var("SINEX_RPC_TOKEN");
            std::env::remove_var("SINEX_RPC_TOKEN_FILE");
            std::env::remove_var("SINEX_GATEWAY_ADMIN_TOKEN_FILE");
        }
    }

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let value =
            HeaderValue::from_str(&format!("Bearer {token}")).expect("valid bearer header value");
        headers.insert(header::AUTHORIZATION, value);
        headers
    }

    fn build_test_router(limits: RpcServerLimits) -> Router {
        let base = Router::new()
            .route(
                "/",
                post(|| async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Json(json!({"status": "ok"}))
                }),
            )
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_layer_error))
                    .layer(TimeoutLayer::new(limits.request_timeout))
                    .into_inner(),
            );
        apply_rpc_layers(base, &limits, &[])
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

        let addr = BindAddress::from_config(&gateway_config_from_env())?;
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

        unsafe {
            std::env::set_var("SINEX_GATEWAY_TLS_CERT", "cert.pem");
            std::env::set_var("SINEX_GATEWAY_TLS_KEY", "key.pem");
            std::env::set_var("SINEX_GATEWAY_TLS_CLIENT_CA", "ca.pem");
        }

        let (cert, key, ca) = tls_paths_from_config(&gateway_config_from_env())?;
        assert_eq!(cert, "cert.pem");
        assert_eq!(key, "key.pem");
        assert_eq!(ca, Some("ca.pem".to_string()));

        unsafe { std::env::remove_var("SINEX_GATEWAY_TLS_CLIENT_CA") };
        let (_, _, ca) = tls_paths_from_config(&gateway_config_from_env())?;
        assert!(ca.is_none());

        Ok(())
    }

    #[sinex_test]
    async fn tcp_binding_env_opt_in_respected() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();
        unsafe { std::env::set_var("SINEX_GATEWAY_TCP_LISTEN", "127.0.0.1:7777") };

        let addr = BindAddress::from_config(&gateway_config_from_env())?;

        let BindAddress::Tcp { host, port } = addr;
        assert_eq!(&host, "127.0.0.1");
        assert_eq!(port, 7777);

        clear_tcp_env();
        Ok(())
    }

    #[sinex_test]
    async fn tcp_binding_cli_override_wins() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();
        unsafe { std::env::set_var("SINEX_GATEWAY_TCP_LISTEN", "127.0.0.1:7777") };

        let addr = BindAddress::from_config(&GatewayConfig {
            tcp_listen: "127.0.0.1:8888".to_string(),
            ..gateway_config_from_env()
        })?;

        let BindAddress::Tcp { host, port } = addr;
        assert_eq!(&host, "127.0.0.1");
        assert_eq!(port, 8888);

        clear_tcp_env();
        Ok(())
    }

    #[sinex_test]
    async fn tcp_binding_invalid_cli_spec_rejected() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_tcp_env();

        let result = BindAddress::from_config(&GatewayConfig {
            tcp_listen: "not-a-valid-spec".to_string(),
            ..gateway_config_from_env()
        });

        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn mtls_required_for_non_loopback_bind() -> TestResult<()> {
        let remote = BindAddress::Tcp {
            host: "0.0.0.0".to_string(),
            port: 8080,
        };
        assert!(require_mtls_for_remote(&remote, false, None).is_err());
        assert!(require_mtls_for_remote(&remote, false, Some("ca.pem")).is_ok());

        let loopback = BindAddress::Tcp {
            host: "127.0.0.1".to_string(),
            port: 8080,
        };
        assert!(require_mtls_for_remote(&loopback, false, None).is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn mtls_override_requires_client_ca() -> TestResult<()> {
        let loopback = BindAddress::Tcp {
            host: "127.0.0.1".to_string(),
            port: 8080,
        };
        assert!(require_mtls_for_remote(&loopback, true, None).is_err());
        assert!(require_mtls_for_remote(&loopback, true, Some("ca.pem")).is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn tls_paths_must_be_set_for_tcp() -> TestResult<()> {
        // Ensure env is clean
        let _guard = ENV_LOCK.lock().await;
        unsafe {
            std::env::remove_var("SINEX_GATEWAY_TLS_CERT");
            std::env::remove_var("SINEX_GATEWAY_TLS_KEY");
        }

        assert!(
            tls_paths_from_config(&gateway_config_from_env()).is_err(),
            "TLS paths should be required when binding TCP"
        );
        Ok(())
    }

    #[sinex_test]
    async fn gateway_auth_blocks_missing_token() -> TestResult<()> {
        let auth = GatewayAuth::with_test_token("secret");
        let headers = HeaderMap::new();
        assert!(matches!(
            auth.verify(&headers),
            Err(AuthError::Missing)
        ));
        Ok(())
    }

    #[sinex_test]
    async fn gateway_auth_accepts_bearer_header() -> TestResult<()> {
        let auth = GatewayAuth::with_test_token("secret");
        let headers = bearer_headers("secret");
        assert!(auth.verify(&headers).is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn gateway_auth_reloads_token_file_without_restart() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        clear_auth_env();

        let temp_dir = tempfile::tempdir()?;
        let token_file = temp_dir.path().join("gateway-token");
        std::fs::write(&token_file, "initial-token")?;
        unsafe {
            std::env::set_var(
                "SINEX_RPC_TOKEN_FILE",
                token_file
                    .to_str()
                    .expect("token path should be valid UTF-8"),
            );
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let auth = GatewayAuth::from_config(&gateway_config_from_env())?
            .start_file_watcher(shutdown_rx)?;

        assert!(auth.verify(&bearer_headers("initial-token")).is_ok());
        assert!(matches!(
            auth.verify(&bearer_headers("wrong-token")),
            Err(AuthError::Invalid)
        ));

        std::fs::write(&token_file, "rotated-token")?;

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let old_rejected = matches!(
                    auth.verify(&bearer_headers("initial-token")),
                    Err(AuthError::Invalid)
                );
                let new_accepted = auth.verify(&bearer_headers("rotated-token")).is_ok();

                if old_rejected && new_accepted {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("token watcher should reload updated token");

        let _ = shutdown_tx.send(true);
        clear_auth_env();
        Ok(())
    }

    #[sinex_test]
    async fn gateway_auth_keeps_last_token_when_reload_file_is_empty() -> TestResult<()> {
        let auth = GatewayAuth::with_test_token("initial-token");
        let temp_dir = tempfile::tempdir()?;
        let token_file = temp_dir.path().join("gateway-token");
        std::fs::write(&token_file, " \n\t")?;

        GatewayAuth::reload_token_from_path(&auth.token, &token_file);

        assert!(auth.verify(&bearer_headers("initial-token")).is_ok());
        assert!(matches!(
            auth.verify(&bearer_headers("wrong-token")),
            Err(AuthError::Invalid)
        ));
        Ok(())
    }
}
