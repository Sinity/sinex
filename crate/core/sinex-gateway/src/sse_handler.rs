//! SSE HTTP handler: `GET /events/stream` for real-time event push.
//!
//! Authenticates via bearer token, parses a [`SubscriptionFilter`] from query params,
//! registers with the [`SubscriptionBus`], and streams events as SSE frames.

use crate::auth::Role;
use crate::rpc_server::{AccessOutcome, RpcAuthContext, log_access_audit};
use crate::sse_bus::{
    HEARTBEAT_INTERVAL, SseErrorPayload, SseEventPayload, SseGapPayload, SseHeartbeatPayload,
    SseMessage, SubscriptionBus,
};
use axum::extract::{Query, State};
use axum::http::header::HeaderName;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use sinex_primitives::Timestamp;
use sinex_primitives::events::Event;
use sinex_primitives::query::SubscriptionFilter;
use sinex_primitives::{Id, JsonValue};
use std::convert::Infallible;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use super::rpc_server::AppState;

const LAST_EVENT_ID_HEADER: HeaderName = HeaderName::from_static("last-event-id");

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
    let token = if let Ok(t) = state.auth.verify(&headers) {
        t
    } else {
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
    };

    let auth_ctx = if let Ok(ctx) = RpcAuthContext::from_token(&token) {
        ctx
    } else {
        log_access_audit(
            "sse",
            "events.stream",
            AccessOutcome::Rejected,
            None,
            Some("invalid token role encoding"),
        );
        return (StatusCode::UNAUTHORIZED, "Invalid token role encoding.").into_response();
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
    let bus = if let Some(bus) = state.sse_bus.as_ref() {
        Arc::clone(bus)
    } else {
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

    let resume_from = match parse_last_event_id(&headers) {
        Ok(last_event_id) => last_event_id,
        Err(error) => {
            let detail = error.clone();
            log_access_audit(
                "sse",
                "events.stream",
                AccessOutcome::InvalidRequest,
                Some(&auth_ctx),
                Some(&detail),
            );
            return (StatusCode::BAD_REQUEST, detail).into_response();
        }
    };

    // ── Register ──
    let Some((sub_id, rx)) = bus.register(filter, resume_from) else {
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

fn parse_last_event_id(headers: &HeaderMap) -> Result<Option<Id<Event<JsonValue>>>, String> {
    let Some(raw_value) = headers.get(&LAST_EVENT_ID_HEADER) else {
        return Ok(None);
    };
    let value = raw_value
        .to_str()
        .map_err(|_| "Last-Event-ID must be valid ASCII".to_string())?
        .trim();
    if value.is_empty() {
        return Ok(None);
    }

    value.parse().map(Some).map_err(|error| {
        format!("Last-Event-ID must be a persisted event UUID, got '{value}': {error}")
    })
}

/// Format an [`SseMessage`] into an axum SSE event.
fn format_sse_message(msg: SseMessage) -> SseEvent {
    match msg {
        SseMessage::Event { seq, event } => {
            let data = serialize_sse_payload("event", &SseEventPayload { event: &event });
            let mut frame = SseEvent::default().event("event").data(data);
            if let Some(event_id) = event.id {
                frame = frame.id(event_id.to_string());
            } else {
                frame = frame.id(seq.to_string());
            }
            frame
        }
        SseMessage::Gap {
            from_seq,
            to_seq,
            dropped,
        } => {
            let data = serialize_sse_payload(
                "gap",
                &SseGapPayload {
                    from_seq,
                    to_seq,
                    dropped,
                },
            );
            SseEvent::default().event("gap").data(data)
        }
        SseMessage::Heartbeat { ts } => {
            let data = serialize_sse_payload("heartbeat", &SseHeartbeatPayload { ts });
            SseEvent::default().event("heartbeat").data(data)
        }
    }
}

fn serialize_sse_payload<T: Serialize>(payload_kind: &str, payload: &T) -> String {
    match serde_json::to_string(payload) {
        Ok(data) => data,
        Err(error) => {
            tracing::error!(
                payload_kind,
                error = %error,
                "Failed to serialize SSE payload"
            );
            match serde_json::to_string(&SseErrorPayload {
                code: "serialization_error".to_string(),
                message: format!("failed to serialize SSE {payload_kind} payload: {error}"),
            }) {
                Ok(error_payload) => error_payload,
                Err(fallback_error) => {
                    tracing::error!(
                        payload_kind,
                        error = %fallback_error,
                        "Failed to serialize SSE fallback error payload"
                    );
                    r#"{"code":"serialization_error","message":"failed to serialize SSE fallback error payload"}"#.to_string()
                }
            }
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

#[cfg(test)]
mod tests {
    use super::{LAST_EVENT_ID_HEADER, parse_last_event_id, serialize_sse_payload};
    use axum::http::{HeaderMap, HeaderValue};
    use serde::Serialize;
    use sinex_primitives::events::Event;
    use sinex_primitives::{Id, JsonValue, Uuid};
    use xtask::sandbox::sinex_test;

    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("boom"))
        }
    }

    #[sinex_test]
    async fn serialize_sse_payload_surfaces_serialization_failures() -> TestResult<()> {
        let rendered = serialize_sse_payload("event", &FailingSerialize);
        let payload: serde_json::Value = serde_json::from_str(&rendered)?;

        assert_eq!(payload["code"], "serialization_error");
        assert!(
            payload["message"]
                .as_str()
                .is_some_and(|message| message.contains("event") && message.contains("boom"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn parse_last_event_id_accepts_persisted_event_uuid() -> TestResult<()> {
        let event_id = Id::<Event<JsonValue>>::from_uuid(Uuid::now_v7());
        let mut headers = HeaderMap::new();
        headers.insert(
            LAST_EVENT_ID_HEADER,
            HeaderValue::from_str(&event_id.to_string())?,
        );

        let parsed =
            parse_last_event_id(&headers).map_err(|error| color_eyre::eyre::eyre!(error))?;
        assert_eq!(parsed, Some(event_id));
        Ok(())
    }

    #[sinex_test]
    async fn parse_last_event_id_rejects_non_uuid_values() -> TestResult<()> {
        let mut headers = HeaderMap::new();
        headers.insert(LAST_EVENT_ID_HEADER, HeaderValue::from_static("42"));

        let error = parse_last_event_id(&headers).expect_err("sequence ids must be rejected");
        assert!(error.contains("persisted event UUID"));
        Ok(())
    }
}
