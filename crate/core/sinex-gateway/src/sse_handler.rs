//! SSE HTTP handler: `GET /events/stream` for real-time event push.
//!
//! Authenticates via bearer token, parses a [`SubscriptionFilter`] from query params,
//! registers with the [`SubscriptionBus`], and streams events as SSE frames.

use crate::auth::Role;
use crate::rpc_server::{AccessOutcome, RpcAuthContext, log_access_audit};
use crate::sse_bus::{
    HEARTBEAT_INTERVAL, SseEventPayload, SseGapPayload, SseHeartbeatPayload, SseMessage,
    SubscriptionBus,
};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use sinex_primitives::Timestamp;
use sinex_primitives::query::SubscriptionFilter;
use std::convert::Infallible;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use super::rpc_server::AppState;

/// Query parameters for the SSE endpoint.
#[derive(Debug, Deserialize)]
pub(crate) struct SseStreamParams {
    /// URL-encoded JSON filter (default: empty = all events).
    #[serde(default)]
    filter: Option<String>,
}

/// `GET /events/stream` — Server-Sent Events endpoint for real-time event push.
///
/// Requires bearer token authentication (minimum `ReadOnly` role).
/// Accepts an optional `filter` query parameter containing a URL-encoded JSON
/// [`SubscriptionFilter`].
pub(crate) async fn handle_sse_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<SseStreamParams>,
) -> Response {
    // ── Auth ──
    let token = match state.auth.verify(&headers) {
        Ok(t) => t,
        Err(_) => {
            log_access_audit(
                "sse",
                "events.stream",
                AccessOutcome::Unauthenticated,
                None,
                Some("missing or invalid bearer token"),
            );
            return (
                StatusCode::UNAUTHORIZED,
                "Authentication required. Provide SINEX_RPC_TOKEN via Authorization header.",
            )
                .into_response();
        }
    };

    let auth_ctx = match RpcAuthContext::from_token(&token) {
        Ok(ctx) => ctx,
        Err(_) => {
            log_access_audit(
                "sse",
                "events.stream",
                AccessOutcome::Rejected,
                None,
                Some("invalid token role encoding"),
            );
            return (StatusCode::UNAUTHORIZED, "Invalid token role encoding.").into_response();
        }
    };

    if !auth_ctx.has_permission(Role::ReadOnly) {
        log_access_audit(
            "sse",
            "events.stream",
            AccessOutcome::Forbidden,
            Some(&auth_ctx),
            Some("insufficient permissions"),
        );
        return (StatusCode::FORBIDDEN, "Insufficient permissions.").into_response();
    }

    // ── SSE bus required ──
    let bus = match state.sse_bus.as_ref() {
        Some(bus) => Arc::clone(bus),
        None => {
            log_access_audit(
                "sse",
                "events.stream",
                AccessOutcome::Unavailable,
                Some(&auth_ctx),
                Some("subscription bus unavailable"),
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Event streaming unavailable (NATS not connected).",
            )
                .into_response();
        }
    };

    // ── Parse filter ──
    let filter = if let Some(filter_json) = params.filter {
        match serde_json::from_str::<SubscriptionFilter>(&filter_json) {
            Ok(f) => {
                if let Err(e) = f.validate() {
                    let detail = e.to_string();
                    log_access_audit(
                        "sse",
                        "events.stream",
                        AccessOutcome::InvalidRequest,
                        Some(&auth_ctx),
                        Some(&detail),
                    );
                    return (StatusCode::BAD_REQUEST, format!("Invalid filter: {e}"))
                        .into_response();
                }
                f
            }
            Err(e) => {
                let detail = e.to_string();
                log_access_audit(
                    "sse",
                    "events.stream",
                    AccessOutcome::InvalidRequest,
                    Some(&auth_ctx),
                    Some(&detail),
                );
                return (StatusCode::BAD_REQUEST, format!("Invalid filter JSON: {e}"))
                    .into_response();
            }
        }
    } else {
        SubscriptionFilter::default()
    };

    // ── Register ──
    let Some((sub_id, rx)) = bus.register(filter) else {
        log_access_audit(
            "sse",
            "events.stream",
            AccessOutcome::Rejected,
            Some(&auth_ctx),
            Some("too many active subscriptions"),
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "Too many active event streams. Retry after existing subscriptions drain.",
        )
            .into_response();
    };
    log_access_audit(
        "sse",
        "events.stream",
        AccessOutcome::Success,
        Some(&auth_ctx),
        None,
    );

    // Build SSE stream from mpsc receiver + heartbeat
    let rx_stream = ReceiverStream::new(rx);

    let heartbeat_stream = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
        HEARTBEAT_INTERVAL,
    ))
    .map(|_| SseMessage::Heartbeat {
        ts: Timestamp::now(),
    });

    // Merge event stream and heartbeat stream
    let merged = StreamExt::merge(
        rx_stream.map(MergedItem::Msg),
        heartbeat_stream.map(MergedItem::Msg),
    );

    // Convert to SSE events
    let bus_for_cleanup = Arc::clone(&bus);
    let event_stream = merged.map(move |item| {
        let MergedItem::Msg(msg) = item;
        Ok::<_, Infallible>(format_sse_message(msg))
    });

    // Wrap in a stream that unregisters on drop
    let cleanup_stream = CleanupStream {
        inner: Box::pin(event_stream),
        bus: bus_for_cleanup,
        sub_id,
    };

    Sse::new(cleanup_stream)
        .keep_alive(KeepAlive::new())
        .into_response()
}

enum MergedItem {
    Msg(SseMessage),
}

/// Format an [`SseMessage`] into an axum SSE event.
fn format_sse_message(msg: SseMessage) -> SseEvent {
    match msg {
        SseMessage::Event { seq, event } => {
            let data = serde_json::to_string(&SseEventPayload { event: &event })
                .unwrap_or_else(|_| "{}".to_string());
            SseEvent::default()
                .event("event")
                .id(seq.to_string())
                .data(data)
        }
        SseMessage::Gap {
            from_seq,
            to_seq,
            dropped,
        } => {
            let data = serde_json::to_string(&SseGapPayload {
                from_seq,
                to_seq,
                dropped,
            })
            .unwrap_or_else(|_| "{}".to_string());
            SseEvent::default().event("gap").data(data)
        }
        SseMessage::Heartbeat { ts } => {
            let data = serde_json::to_string(&SseHeartbeatPayload { ts })
                .unwrap_or_else(|_| "{}".to_string());
            SseEvent::default().event("heartbeat").data(data)
        }
    }
}

// ─────────────────────────────────────────────────────────────────────
// Cleanup stream — unregisters subscription on drop
// ─────────────────────────────────────────────────────────────────────

use std::pin::Pin;
use std::task::{Context, Poll};

struct CleanupStream<S> {
    inner: Pin<Box<S>>,
    bus: Arc<SubscriptionBus>,
    sub_id: u64,
}

impl<S> futures::Stream for CleanupStream<S>
where
    S: futures::Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl<S> Drop for CleanupStream<S> {
    fn drop(&mut self) {
        self.bus.unregister(self.sub_id);
    }
}
