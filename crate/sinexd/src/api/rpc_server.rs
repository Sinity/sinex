#![doc = include_str!("../../docs/api/rpc_server.md")]

// Local crate imports
use crate::api::{
    config::GatewayConfig,
    distributed_rate_limit::{DistributedRateLimitConfig, DistributedRateLimiter},
    gateway_metrics::GatewayMetrics,
    handlers::system::system_health_response,
    rate_limit::TokenRateLimiter,
    service_container::ServiceContainer,
};

use sinex_primitives::env as shared_env;

// External crates
use crate::runtime::systemd_notify;
use axum::{
    BoxError, Json, Router,
    error_handling::HandleErrorLayer,
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperBuilder;
use hyper_util::service::TowerToHyperService;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_primitives::Result as SinexResult;
use sinex_primitives::Timestamp;
use sinex_primitives::error::SinexError;
use sinex_primitives::rpc::JsonRpcError;
use sinex_primitives::{Bytes, Uuid};
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::task::JoinHandle;
use tokio_rustls::rustls::pki_types::pem::PemObject;
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
use tracing::{debug, error, info, warn};

use std::time::Duration;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
};

mod auth;
mod protocol;
mod transport;

pub use auth::RpcAuthContext;
pub(crate) use auth::{
    AccessOutcome, GatewayAuth, RateLimiter, constant_time_eq, extract_token, log_access_audit,
};
pub(crate) use protocol::{JsonRpcRequest, validate_jsonrpc_request};
use protocol::{JsonRpcResponse, rpc_error_data, sinex_error_to_rpc_code};
pub(crate) use transport::{
    BindAddress, RpcServerLimits, apply_rpc_layers, bind_tcp_listener, handle_layer_error,
    load_rustls_config, require_mtls_for_remote, tls_paths_from_config, warn_if_remote_bind,
};
pub use transport::{ensure_rustls_crypto_provider, read_token_from_env};

#[cfg(test)]
use auth::*;
#[cfg(test)]
use transport::*;
pub const DEFAULT_TCP_LISTEN: &str = "127.0.0.1:9999";

/// State shared between handlers
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) services: ServiceContainer,
    pub(crate) auth: GatewayAuth,
    pub(crate) rate_limiter: RateLimiter,
    pub(crate) metrics: Arc<GatewayMetrics>,
    pub(crate) sse_bus: Option<Arc<crate::api::sse_bus::SubscriptionBus>>,
    /// Shutdown signal receiver — `/ready` reports 503 once asserted so that
    /// upstream load balancers stop routing during graceful drain.
    pub(crate) shutdown_rx: tokio::sync::watch::Receiver<bool>,
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
) -> SinexResult<serde_json::Value> {
    // Use lazy static registry for zero-cost dispatch
    use std::sync::OnceLock;
    static REGISTRY: OnceLock<crate::api::rpc_registry::RpcRegistry> = OnceLock::new();
    let registry = REGISTRY.get_or_init(crate::api::rpc_registry::build_registry);

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

/// Detailed component health endpoint (`/health`).
///
/// Always returns the full `SystemHealthResponse` body and uses the HTTP
/// status code to distinguish serving (200) from non-serving (503). Operators
/// should read `status`, `healthy`, and `degradation_reasons` rather than
/// treating HTTP 200 as full readiness; this route is intentionally verbose
/// and is the destination for human-driven diagnostics.
async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    let response = system_health_response(state.services.health_report().await);
    let status = if response.serving {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, axum::Json(response)).into_response()
}

/// Load-balancer-oriented readiness probe (`/ready`).
///
/// Returns 200 only when:
/// - graceful drain has not been requested
/// - the database pool can be acquired and ping-checked in <100ms
/// - the NATS active probe reports connected
///
/// Returns 503 otherwise. The body is a minimal JSON object so probes can be
/// cheap; richer diagnostics belong on `/health`.
async fn ready_check(State(state): State<AppState>) -> impl IntoResponse {
    use serde_json::json;
    use std::time::Duration;

    // 1. Drain semantics: while the shutdown signal is asserted the gateway
    //    must report not-ready so external load balancers stop routing.
    let draining = *state.shutdown_rx.borrow();
    if draining {
        let body = json!({
            "ready": false,
            "reason": "draining",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    }

    // 2. DB pool acquirable in <100ms.
    let db_start = std::time::Instant::now();
    let db_ok = matches!(
        tokio::time::timeout(
            Duration::from_millis(100),
            sqlx::query_scalar!("SELECT 1").fetch_one(state.services.pool()),
        )
        .await,
        Ok(Ok(_))
    );
    let db_elapsed_ms = db_start.elapsed().as_millis() as u64;

    if !db_ok {
        let body = json!({
            "ready": false,
            "reason": "database_not_ready",
            "db_probe_ms": db_elapsed_ms,
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    }

    // 3. NATS connected (active probe).
    let nats = state.services.probe_nats_active().await;
    if !nats.connected {
        let body = json!({
            "ready": false,
            "reason": "nats_not_ready",
            "detail": nats.detail,
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    }

    let body = json!({
        "ready": true,
        "db_probe_ms": db_elapsed_ms,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
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
            // Audit `gateway.rpc.call` (#1172 AC-7): unauthenticated. We
            // cannot derive a token prefix from a rejected bearer; record
            // an empty prefix and `unknown` role.
            state.metrics.record_rpc_call(
                &request.method,
                "unknown",
                start.elapsed().as_millis() as u64,
                sinex_primitives::events::payloads::RpcStatus::Unauthenticated,
                "",
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
            let token_prefix = &token[..8.min(token.len())];
            state.metrics.record_rpc_call(
                &request.method,
                "unknown",
                start.elapsed().as_millis() as u64,
                sinex_primitives::events::payloads::RpcStatus::Rejected,
                token_prefix,
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
    if !state.rate_limiter.check(&token, auth_context.role).await {
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
        emit_rpc_call_audit(
            &state,
            &request.method,
            auth_context.role,
            start.elapsed(),
            sinex_primitives::events::payloads::RpcStatus::RateLimited,
            token_prefix,
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
        let detail = err.client_message();
        log_access_audit(
            "rpc",
            &request.method,
            AccessOutcome::InvalidRequest,
            Some(&auth_context),
            Some(detail),
        );
        emit_rpc_call_audit(
            &state,
            &request.method,
            auth_context.role,
            start.elapsed(),
            sinex_primitives::events::payloads::RpcStatus::InvalidRequest,
            &auth_context.token_prefix,
        );
        let response = JsonRpcResponse::error(request.id, -32600, detail.to_string());
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
    let elapsed = start.elapsed();

    let (response, audit_status) = match result {
        Ok(value) => {
            state.metrics.record_request_success(latency_us);
            (
                JsonRpcResponse::success(request.id, value),
                sinex_primitives::events::payloads::RpcStatus::Success,
            )
        }
        Err(err)
            if matches!(&err, SinexError::NotFound(_))
                && err.to_string().starts_with("Unknown method:") =>
        {
            state.metrics.record_request_rejected();
            (
                JsonRpcResponse::error(request.id, -32601, err.to_string()),
                sinex_primitives::events::payloads::RpcStatus::Failed,
            )
        }
        Err(err) => {
            let error_id = Uuid::now_v7();
            state.metrics.record_request_rejected();
            error!(
                target: "sinex_metrics",
                metric = "gateway.rpc_method_failures_total",
                error_id = %error_id,
                method = %method,
                error = %err,
                "RPC method failed"
            );

            let (code, public) = sinex_error_to_rpc_code(&err);
            let message = public.message.clone();
            let data = rpc_error_data(error_id, &public, &err);

            (
                JsonRpcResponse::error_with_data(request.id, code, message, data),
                sinex_primitives::events::payloads::RpcStatus::Failed,
            )
        }
    };

    emit_rpc_call_audit(
        &state,
        &method,
        auth_context.role,
        elapsed,
        audit_status,
        &auth_context.token_prefix,
    );

    (StatusCode::OK, Json(response))
}

/// Helper that wraps the gateway-metrics audit emission so the call sites
/// stay readable. No-op when metrics are disabled.
fn emit_rpc_call_audit(
    state: &AppState,
    method: &str,
    role: crate::api::auth::Role,
    elapsed: std::time::Duration,
    status: sinex_primitives::events::payloads::RpcStatus,
    token_prefix: &str,
) {
    state.metrics.record_rpc_call(
        method,
        role.as_str(),
        elapsed.as_millis() as u64,
        status,
        token_prefix,
    );
}

/// Maximum number of requests allowed in a single JSON-RPC batch
const MAX_BATCH_SIZE: usize = 10;

/// JSON-RPC 2.0 batch request handler
///
/// Accepts an array of JSON-RPC requests, processes each individually, and returns an
/// array of responses. Authentication is performed once for the entire batch; rate
/// limiting, validation, and dispatch are applied per-request so each batch item
/// consumes a rate-limit token independently.
async fn handle_rpc_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(requests): Json<Vec<JsonRpcRequest>>,
) -> axum::response::Response {
    state.metrics.record_request_start();
    let start = std::time::Instant::now();

    // Empty batch is invalid per JSON-RPC 2.0 spec
    if requests.is_empty() {
        state.metrics.record_request_rejected();
        log_access_audit(
            "rpc",
            "<batch>",
            AccessOutcome::InvalidRequest,
            None,
            Some("empty batch"),
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(JsonRpcResponse::error(
                None,
                -32600,
                "Batch request must not be empty".to_string(),
            )),
        )
            .into_response();
    }

    if requests.len() > MAX_BATCH_SIZE {
        state.metrics.record_request_rejected();
        log_access_audit(
            "rpc",
            "<batch>",
            AccessOutcome::InvalidRequest,
            None,
            Some("batch too large"),
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(JsonRpcResponse::error(
                None,
                -32600,
                format!(
                    "Batch size {} exceeds maximum of {MAX_BATCH_SIZE}",
                    requests.len()
                ),
            )),
        )
            .into_response();
    }

    // Authenticate once for the entire batch
    let token = match state.auth.verify(&headers) {
        Ok(t) => t,
        Err(err) => {
            state.metrics.record_request_rejected();
            let detail = err.to_string();
            log_access_audit(
                "rpc",
                "<batch>",
                AccessOutcome::Unauthenticated,
                None,
                Some(&detail),
            );
            return err.into_response().into_response();
        }
    };

    let auth_context = match RpcAuthContext::from_token(&token) {
        Ok(ctx) => ctx,
        Err(err) => {
            state.metrics.record_request_rejected();
            let detail = err.to_string();
            log_access_audit(
                "rpc",
                "<batch>",
                AccessOutcome::Rejected,
                None,
                Some(&detail),
            );
            return (
                StatusCode::UNAUTHORIZED,
                Json(JsonRpcResponse::error(
                    None,
                    -32001,
                    format!("Invalid token role encoding: {err}"),
                )),
            )
                .into_response();
        }
    };

    let mut responses = Vec::with_capacity(requests.len());
    for request in requests {
        // Each batch member gets its own audit/latency window so the
        // `gateway.rpc.call` stream covers per-dispatch outcomes (#1172 AC-7).
        let member_start = std::time::Instant::now();
        let method_for_audit = request.method.clone();

        // Rate limit each request individually
        if !state.rate_limiter.check(&token, auth_context.role).await {
            let token_prefix = &token[..8.min(token.len())];
            warn!(token_prefix, "Batch request rejected: rate limit exceeded");
            state.metrics.record_rate_limited();
            log_access_audit(
                "rpc",
                &request.method,
                AccessOutcome::RateLimited,
                Some(&auth_context),
                None,
            );
            emit_rpc_call_audit(
                &state,
                &method_for_audit,
                auth_context.role,
                member_start.elapsed(),
                sinex_primitives::events::payloads::RpcStatus::RateLimited,
                &auth_context.token_prefix,
            );
            responses.push(JsonRpcResponse::error(
                request.id,
                -32029,
                "Rate limit exceeded for this token".to_string(),
            ));
            continue;
        }

        if let Err(err) = validate_jsonrpc_request(&request) {
            state.metrics.record_request_rejected();
            let detail = err.client_message();
            log_access_audit(
                "rpc",
                &request.method,
                AccessOutcome::InvalidRequest,
                Some(&auth_context),
                Some(detail),
            );
            emit_rpc_call_audit(
                &state,
                &method_for_audit,
                auth_context.role,
                member_start.elapsed(),
                sinex_primitives::events::payloads::RpcStatus::InvalidRequest,
                &auth_context.token_prefix,
            );
            responses.push(JsonRpcResponse::error(
                request.id,
                -32600,
                detail.to_string(),
            ));
            continue;
        }

        let method = request.method.clone();

        let result = dispatch_rpc_method(
            "rpc",
            &state.services,
            &request.method,
            request.params,
            &auth_context,
        )
        .await;

        let (response, audit_status) = match result {
            Ok(value) => {
                state.metrics.record_request_success(0);
                (
                    JsonRpcResponse::success(request.id, value),
                    sinex_primitives::events::payloads::RpcStatus::Success,
                )
            }
            Err(err)
                if matches!(&err, SinexError::NotFound(_))
                    && err.to_string().starts_with("Unknown method:") =>
            {
                state.metrics.record_request_rejected();
                (
                    JsonRpcResponse::error(request.id, -32601, err.to_string()),
                    sinex_primitives::events::payloads::RpcStatus::Failed,
                )
            }
            Err(err) => {
                state.metrics.record_request_rejected();
                let error_id = Uuid::now_v7();
                error!(
                    target: "sinex_metrics",
                    metric = "gateway.rpc_method_failures_total",
                    error_id = %error_id,
                    method = %method,
                    error = %err,
                    "RPC method failed (batch)"
                );

                let (code, public) = sinex_error_to_rpc_code(&err);
                let message = public.message.clone();
                let data = rpc_error_data(error_id, &public, &err);

                (
                    JsonRpcResponse::error_with_data(request.id, code, message, data),
                    sinex_primitives::events::payloads::RpcStatus::Failed,
                )
            }
        };
        emit_rpc_call_audit(
            &state,
            &method_for_audit,
            auth_context.role,
            member_start.elapsed(),
            audit_status,
            &auth_context.token_prefix,
        );
        responses.push(response);
    }

    let latency_us = start.elapsed().as_micros() as u64;
    state.metrics.record_request_success(latency_us);

    let batch_result = serde_json::to_value(&responses).unwrap_or_else(|_| {
        serde_json::json!([{
            "jsonrpc": "2.0",
            "error": {"code": -32603, "message": "Internal error serializing batch response"},
            "id": null
        }])
    });

    (StatusCode::OK, Json(batch_result)).into_response()
}

/// Run the RPC server with configurable binding
///
/// Accepts a shutdown signal receiver that will trigger graceful shutdown when signaled.
///
/// # CORS Configuration
/// The `cors_origins` parameter controls allowed origins:
/// - Empty: Only localhost origins allowed (<http://localhost>:*, <http://127.0.0.1>:*)
/// - Non-empty: Only the specified origins allowed
///
/// Spawn the RPC server in a background task, returning the bound address and task handle.
///
/// This is used for integration testing (binding to port 0) and by the main `run` entry point.
pub async fn spawn(
    config: &GatewayConfig,
    services: ServiceContainer,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> SinexResult<(
    std::net::SocketAddr,
    tokio::task::JoinHandle<SinexResult<()>>,
)> {
    let bind_address = BindAddress::from_config(config)?;

    // Create shutdown channels for background tasks
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Eagerly build the static (source, event_type) registry from the
    // EventPayload inventory (#1172 schema-as-code). The first call lazily
    // initialises a `OnceLock`; touching it at startup turns any registry
    // panic / inventory drift into a startup failure rather than a per-RPC
    // surprise.
    let schema_registry = crate::api::schema_registry::registry();
    info!(
        registered_payloads = schema_registry.len(),
        "Schema-as-code: EventPayload inventory loaded into gateway registry"
    );

    let auth = GatewayAuth::from_config(config)?
        .start_file_watcher(shutdown_rx.clone())
        .await?;
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
        let bus = Arc::new(crate::api::sse_bus::SubscriptionBus::new());
        services.attach_sse_bus(Arc::clone(&bus));
        let pool = services.pool().clone();
        let env = services.environment().clone();
        let namespace = config.namespace.clone();
        let bus_shutdown = shutdown_rx.clone();
        let bus_ref = Arc::clone(&bus);
        let task = tokio::spawn(async move {
            bus_ref
                .run(nats_client, pool, env, namespace, bus_shutdown)
                .await;
        });
        info!("SSE subscription bus spawned");
        (Some(bus), Some(task))
    } else {
        info!("NATS not connected — SSE event streaming disabled");
        (None, None)
    };

    let metrics_task = metrics_task.map(|task| {
        RpcServer::monitor_background_task("Metrics emission task", task, shutdown_rx.clone())
    });
    let cleanup_task = cleanup_task.map(|task| {
        RpcServer::monitor_background_task("Rate limiter cleanup task", task, shutdown_rx.clone())
    });
    let sse_bus_task = sse_bus_task.map(|task| {
        RpcServer::monitor_background_task("SSE subscription bus", task, shutdown_rx.clone())
    });

    // TTL enforcement task (#1172 AC-5). Cheap: most ticks are no-ops because
    // very few schemas declare `retention_seconds`. Survives missing
    // coordination by self-skipping per tick.
    let host = sinex_primitives::events::builder::get_hostname();
    let ttl_instance_id = format!("gateway:{}:{}", host.as_str(), std::process::id());
    let _ttl_task = RpcServer::monitor_background_task(
        "TTL enforcement task",
        crate::api::lifecycle_ttl::spawn_ttl_task(
            services.clone(),
            ttl_instance_id,
            shutdown_rx.clone(),
        ),
        shutdown_rx.clone(),
    );

    let state = AppState {
        services,
        auth,
        rate_limiter,
        metrics,
        sse_bus,
        shutdown_rx: shutdown_rx.clone(),
    };

    let app = RpcServer::build_app(&limits, &config.cors_origins_list(), state);
    let listener = bind_tcp_listener(&addr_str).map_err(|error| {
        SinexError::network(format!("Failed to bind TCP listener to {addr_str}"))
            .with_std_error(&error)
    })?;

    let local_addr = listener.local_addr().map_err(|error| {
        SinexError::network("Failed to read local TCP listener address").with_std_error(&error)
    })?;
    info!("RPC server listening on TLS {}", local_addr);

    systemd_notify::notify_ready("sinexd");
    let watchdog_handle = systemd_notify::spawn_watchdog("sinexd");

    let handle = tokio::spawn(async move {
        // Run accept loop until shutdown signal
        let accept_result = RpcServer::accept_loop(listener, acceptor, app, &mut shutdown).await;
        systemd_notify::stop_watchdog(watchdog_handle, "sinexd").await;
        systemd_notify::notify_stopping("sinexd");
        accept_result?;

        // Signal all background tasks to shut down
        info!("Shutting down background tasks...");
        if shutdown_tx.send(true).is_err() {
            warn!("RPC server background-task shutdown receiver was already dropped");
        }

        RpcServer::wait_for_background_tasks(metrics_task, cleanup_task, sse_bus_task).await?;

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
) -> SinexResult<()> {
    let (_, handle) = spawn(config, services, shutdown).await?;
    match handle.await {
        Ok(res) => res,
        Err(error) => Err(SinexError::service("RPC server task panicked").with_std_error(&error)),
    }
}

/// Helper struct for the server runner organization
struct RpcServer;

impl RpcServer {
    async fn init_rate_limiter(
        config: &GatewayConfig,
        services: &ServiceContainer,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> SinexResult<(RateLimiter, Option<JoinHandle<()>>)> {
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
            .route("/rpc/batch", post(handle_rpc_batch))
            .route("/", post(handle_rpc))
            .route("/health", get(health_check))
            .route("/ready", get(ready_check))
    }

    /// Build the complete app with split middleware:
    /// - RPC routes get `TimeoutLayer` + `HandleErrorLayer` (short-lived requests)
    /// - SSE route does NOT get `TimeoutLayer` (long-lived connections)
    /// - Both share outer layers: concurrency, CORS, trace, body limit
    fn build_app(limits: &RpcServerLimits, cors_origins: &[String], state: AppState) -> Router {
        use crate::api::sse_handler::handle_sse_stream;

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
    ) -> SinexResult<(String, TlsAcceptor)> {
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
    ) -> SinexResult<()> {
        let active_connections = Arc::new(AtomicUsize::new(0));
        let drain_notify = Arc::new(tokio::sync::Notify::new());

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, peer) = accept_result.map_err(|error| {
                        SinexError::network("Failed to accept incoming TCP connection")
                            .with_std_error(&error)
                    })?;
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
                                    error!(
                                        target: "sinex_metrics",
                                        metric = "gateway.tls_failures_total",
                                        peer = ?peer,
                                        ?err,
                                        "TLS RPC connection closed with error"
                                    );
                                }
                            }
                            Err(err) => {
                                error!(
                                    target: "sinex_metrics",
                                    metric = "gateway.tls_failures_total",
                                    peer = ?peer,
                                    ?err,
                                    "TLS handshake failed"
                                );
                            }
                        }
                    });
                }
                shutdown_result = shutdown.changed() => {
                    if shutdown_result.is_err() {
                        warn!("RPC server shutdown channel dropped before explicit shutdown");
                    }
                    if shutdown_result.is_err() || *shutdown.borrow() {
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
        metrics_task: Option<JoinHandle<SinexResult<()>>>,
        cleanup_task: Option<JoinHandle<SinexResult<()>>>,
        sse_bus_task: Option<JoinHandle<SinexResult<()>>>,
    ) -> SinexResult<()> {
        Self::wait_for_background_tasks_with_timeout(
            metrics_task,
            cleanup_task,
            sse_bus_task,
            std::time::Duration::from_secs(30),
        )
        .await
    }

    fn monitor_background_task(
        task_name: &'static str,
        task: JoinHandle<()>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> JoinHandle<SinexResult<()>> {
        tokio::spawn(async move {
            let mut task = task;
            tokio::select! {
                task_result = &mut task => {
                    match task_result {
                        Ok(()) => {
                            if *shutdown.borrow() {
                                Ok(())
                            } else {
                                Err(SinexError::service(format!(
                                    "{task_name} exited before gateway shutdown"
                                )))
                            }
                        }
                        Err(error) => Err(SinexError::service(format!(
                            "{task_name} join failed"
                        ))
                        .with_std_error(&error)),
                    }
                }
                shutdown_result = shutdown.changed() => {
                    let shutdown_requested = *shutdown.borrow();
                    if shutdown_result.is_err() {
                        warn!(task = task_name, "Background task monitor shutdown channel dropped before explicit shutdown");
                    }
                    match task.await {
                        Ok(()) => {
                            if shutdown_requested {
                                Ok(())
                            } else {
                                Err(SinexError::service(format!(
                                    "{task_name} exited after shutdown channel closed without a shutdown signal"
                                )))
                            }
                        }
                        Err(error) => {
                            if shutdown_requested {
                                Err(SinexError::service(format!(
                                    "{task_name} join failed during shutdown"
                                ))
                                .with_std_error(&error))
                            } else {
                                Err(SinexError::service(format!(
                                    "{task_name} join failed after shutdown channel closed without a shutdown signal: {error}"
                                ))
                                .with_std_error(&error))
                            }
                        }
                    }
                }
            }
        })
    }

    async fn wait_for_background_tasks_with_timeout(
        metrics_task: Option<JoinHandle<SinexResult<()>>>,
        cleanup_task: Option<JoinHandle<SinexResult<()>>>,
        sse_bus_task: Option<JoinHandle<SinexResult<()>>>,
        shutdown_timeout: Duration,
    ) -> SinexResult<()> {
        let mut errors = Vec::new();

        async fn await_background_task(
            task: JoinHandle<SinexResult<()>>,
            task_name: &'static str,
            shutdown_timeout: Duration,
        ) -> SinexResult<()> {
            match tokio::time::timeout(shutdown_timeout, task).await {
                Ok(Ok(Ok(()))) => {
                    info!(task = task_name, "Background task shut down successfully");
                    Ok(())
                }
                Ok(Ok(Err(error))) => Err(error),
                Ok(Err(error)) => Err(SinexError::service(format!(
                    "{task_name} monitor join failed during shutdown: {error}"
                ))
                .with_source(error)),
                Err(_) => Err(SinexError::timeout(format!(
                    "{task_name} did not shut down within {shutdown_timeout:?}"
                ))),
            }
        }

        if let Some(task) = metrics_task {
            info!("Awaiting metrics emission task shutdown...");
            if let Err(error) =
                await_background_task(task, "Metrics emission task", shutdown_timeout).await
            {
                warn!(?error, "Metrics emission task shutdown failed");
                errors.push(error);
            }
        }

        if let Some(task) = cleanup_task {
            info!("Awaiting rate limiter cleanup task shutdown...");
            if let Err(error) =
                await_background_task(task, "Rate limiter cleanup task", shutdown_timeout).await
            {
                warn!(?error, "Rate limiter cleanup task shutdown failed");
                errors.push(error);
            }
        }

        if let Some(task) = sse_bus_task {
            info!("Awaiting SSE subscription bus shutdown...");
            if let Err(error) =
                await_background_task(task, "SSE subscription bus", shutdown_timeout).await
            {
                warn!(?error, "SSE subscription bus shutdown failed");
                errors.push(error);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            let combined = errors
                .into_iter()
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            Err(SinexError::service(format!(
                "Background task shutdown failed: {combined}"
            )))
        }
    }
}

#[cfg(test)]
#[path = "rpc_server_test.rs"]
mod tests;
