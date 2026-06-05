//! Full-stack SSE confirmation test (#1136).
//!
//! Proves the SSE HTTP endpoint receives events that flowed through the real
//! production path: NATS publish → event_engine admission → DB persist → confirmation
//! → `SubscriptionBus` → SSE frame.
//!
//! This complements `sse_stream_test.rs` (which exercises `SubscriptionBus`
//! fanout via direct DB inserts and synthetic confirmations) by closing the
//! loop end-to-end with no fakes.

use futures::StreamExt;
use sinex_primitives::DynamicPayload;
use std::time::Duration;
use xtask::sandbox::prelude::*;

/// Minimal SSE frame: `event: NAME\ndata: PAYLOAD\n\n`.
struct SseFrame {
    event: String,
    data: String,
}

/// Read the next SSE frame from a reqwest byte stream. Buffers across chunks
/// because frame boundaries can span TCP packets.
async fn next_frame(
    stream: &mut (impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin),
    buf: &mut String,
    timeout: Duration,
) -> TestResult<Option<SseFrame>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(idx) = buf.find("\n\n") {
            let raw = buf.drain(..idx + 2).collect::<String>();
            let mut event = String::from("message");
            let mut data = String::new();
            for line in raw.lines() {
                if let Some(rest) = line.strip_prefix("event:") {
                    event = rest.trim().to_string();
                } else if let Some(rest) = line.strip_prefix("data:") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(rest.trim_start());
                }
            }
            if data.is_empty() {
                continue; // comment/keepalive, keep reading
            }
            return Ok(Some(SseFrame { event, data }));
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(None);
        }
        let chunk = match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(c))) => c,
            Ok(Some(Err(e))) => return Err(eyre!("SSE chunk error: {e}")),
            Ok(None) => return Ok(None),
            Err(_) => return Ok(None),
        };
        buf.push_str(std::str::from_utf8(&chunk).map_err(|e| eyre!("SSE non-utf8: {e}"))?);
    }
}

/// Open an SSE subscription against the gateway and read until ready.
///
/// Returns (stream, buf). Caller drives `next_frame` against them.
async fn open_sse(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    filter_json: &str,
) -> TestResult<(
    impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
    String,
)> {
    let url = format!("{base_url}/events/stream");
    let response = client
        .get(&url)
        .query(&[("filter", filter_json)])
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .map_err(|e| eyre!("SSE connect failed: {e}"))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(eyre!("SSE HTTP {status}: {body}"));
    }
    Ok((Box::pin(response.bytes_stream()), String::new()))
}

/// A bearer-token client that trusts the test's self-signed gateway cert.
fn insecure_https_client() -> TestResult<reqwest::Client> {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .map_err(|e| eyre!("reqwest build: {e}"))
}

/// End-to-end: real publish → real event_engine → real confirmation → SSE delivery.
///
/// Replaces the gap that #1136 documented: previous SSE coverage used direct
/// DB inserts and synthetic confirmation messages, which validated the bus
/// but not the full HTTP-fed-by-real-event-engine path.
///
/// Previously `#[ignore]`d (#1626). The delivery path is core-NATS pub/sub
/// (`SubscriptionBus` subscribes to `events.confirmations.>`), independent of
/// the JetStream confirmations *stream* — so the fix is a subject-namespace
/// match, not a stream-naming one:
///   - `#1631` threaded the pipeline namespace into the gateway *process*
///     (`SINEX_NAMESPACE`), but the `SubscriptionBus` ignored it and subscribed
///     to the un-namespaced `{env}.events.confirmations.>`, while a namespaced
///     event_engine publishes to `{env}.{namespace}.events.confirmations.*`.
///   - Fixed here: `GatewayConfig::namespace` is now consumed by the bus, which
///     subscribes via `nats_subject_with_namespace`, matching the publisher.
///   - Separately, the source preflight stream-existence check was made
///     namespace-aware so it stops 404-ing on `..._CONFIRMATIONS` under a
///     per-test namespace.
#[sinex_test(timeout = 90)]
async fn test_sse_delivers_event_after_real_event_engine_confirmation(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let stack = TestCoreStack::new(&ctx).await?;
    let client = insecure_https_client()?;
    let base = stack.gateway_url();
    let token = stack.rpc_token().to_string();

    // Filter to only the source we publish, so we don't race on unrelated
    // events that other concurrent tests in the shared NATS namespace could
    // produce. (PipelineNamespace already isolates streams, but the bus
    // ingests every confirmation it sees on this namespace.)
    let unique_source = format!("sse.real.confirmations.{}", uuid::Uuid::new_v4());
    let filter_json = serde_json::json!({
        "sources": [unique_source.as_str()],
    })
    .to_string();

    let (mut stream, mut buf) = open_sse(&client, &base, &token, &filter_json).await?;

    // ── Bus-readiness handshake ──────────────────────────────────────────
    //
    // `start_test_gateway` only waits for TCP accept; it does not signal that
    // `SubscriptionBus::run` has finished `nats_client.subscribe(confirmations.>)`.
    // A heartbeat frame proves the per-connection HTTP task is alive but does
    // NOT prove the bus's NATS subscription is up — heartbeats originate from
    // the SSE handler's local timer.
    //
    // The only signal observable from a black-box test is: an event we publish
    // actually traverses event_engine → bus → SSE. So we publish a warmup event in
    // a retry loop until SSE delivers it. After that, the bus's NATS
    // subscription is provably active and we can run the real assertion.
    let warmup_event_type = "test.sse_warmup";
    let warmup_deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let mut warmup_id: Option<String> = None;
    while tokio::time::Instant::now() < warmup_deadline {
        let warmup = DynamicPayload::new(
            unique_source.as_str(),
            warmup_event_type,
            serde_json::json!({"marker": "warmup"}),
        );
        let id = stack.publish(warmup).await?.to_string();
        // Read frames for up to 5s; if our id appears, bus is live.
        let frame_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = frame_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let Some(frame) = next_frame(&mut stream, &mut buf, remaining).await? else {
                break;
            };
            if frame.event == "event" && frame.data.contains(&id) {
                warmup_id = Some(id.clone());
                break;
            }
        }
        if warmup_id.is_some() {
            break;
        }
        tracing::warn!("SSE warmup event {id} not yet delivered; republishing");
    }
    assert!(
        warmup_id.is_some(),
        "SSE never delivered warmup event after 45s — bus NATS subscription likely never came up"
    );

    // Publish through the real pipeline: NATS → event_engine → DB → confirmation
    // → SubscriptionBus → SSE.
    let payload = DynamicPayload::new(
        unique_source.as_str(),
        "test.sse_round_trip",
        serde_json::json!({"marker": "round-trip"}),
    );
    let event_id = stack.publish(payload).await?;

    // Drain SSE frames until we see our event id, an explicit error frame,
    // or hit the deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    let event_id_str = event_id.to_string();
    let mut saw_event = false;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = next_frame(&mut stream, &mut buf, remaining).await?;
        let Some(frame) = frame else { break };
        match frame.event.as_str() {
            "event" => {
                if frame.data.contains(&event_id_str) {
                    saw_event = true;
                    break;
                }
            }
            "error" => {
                return Err(eyre!("SSE delivered error frame: {}", frame.data));
            }
            "heartbeat" | "" | "message" => {}
            other => {
                tracing::debug!(kind = %other, "ignoring sse frame kind");
            }
        }
    }

    assert!(
        saw_event,
        "SSE never delivered event {event_id_str} after real event_engine confirmation; \
         check event_engine→confirmation→bus path"
    );

    stack.shutdown().await?;
    Ok(())
}

/// The auth gate is enforced before any subscription is opened.
///
/// Together with the happy path above, this proves the SSE endpoint isn't
/// just trusting the bus side-channel — it goes through the same auth path
/// as RPC.
#[sinex_test(timeout = 30)]
async fn test_sse_rejects_missing_bearer_at_http_layer(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let stack = TestCoreStack::new(&ctx).await?;
    let client = insecure_https_client()?;
    let url = format!("{}/events/stream", stack.gateway_url());

    let response = client
        .get(&url)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .map_err(|e| eyre!("SSE connect failed: {e}"))?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "SSE endpoint must reject unauthenticated requests"
    );

    stack.shutdown().await?;
    Ok(())
}
