//! Full-stack SSE confirmation test (#1136).
//!
//! Proves the SSE HTTP endpoint receives events that flowed through the real
//! production path: NATS publish → ingestd admission → DB persist → confirmation
//! → SubscriptionBus → SSE frame.
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

/// End-to-end: real publish → real ingestd → real confirmation → SSE delivery.
///
/// Replaces the gap that #1136 documented: previous SSE coverage used direct
/// DB inserts and synthetic confirmation messages, which validated the bus
/// but not the full HTTP-fed-by-real-ingestd path.
#[sinex_test(timeout = 90)]
async fn test_sse_delivers_event_after_real_ingestd_confirmation(
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

    // The bus subscribes asynchronously after the HTTP handler returns 200.
    // Wait for the first heartbeat or a subscribed marker so we don't race
    // the publish below ahead of the bus subscription.
    let prelude = next_frame(&mut stream, &mut buf, Duration::from_secs(15)).await?;
    assert!(
        prelude.is_some(),
        "SSE stream produced no frame within 15s of connect; bus may be unreachable"
    );

    // Publish through the real pipeline: NATS → ingestd → DB → confirmation
    // → SubscriptionBus → SSE.
    let payload = DynamicPayload::new(
        unique_source.as_str(),
        "test.sse_round_trip",
        serde_json::json!({"marker": "round-trip"}),
    );
    let event_id = stack.publish(payload).await?;

    // Drain SSE frames until we see our event id, an explicit error frame,
    // or hit the deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
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
        "SSE never delivered event {event_id_str} after real ingestd confirmation; \
         check ingestd→confirmation→bus path"
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
