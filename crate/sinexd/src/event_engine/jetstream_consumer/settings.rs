//! Bounded consumer, retry, and stream-capacity settings.

use tokio::time::Duration;

pub(super) const DEFAULT_BATCH_FETCH_MAX_MESSAGES: usize = 100;
pub(super) const DEFAULT_BATCH_FETCH_TIMEOUT: Duration = Duration::from_secs(1);
/// Cumulative payload-byte budget for a single event-engine fetch.
///
/// Bounds the in-flight decode high-watermark *independent of per-message size*.
/// Each fetched message is itself an event batch whose payloads can reach the
/// 10 MiB NATS limit, and every message is expanded into a fully-owned
/// `serde_json::Value` DOM (~5-10x the wire bytes) before persistence. With only
/// the 100-message count limit, a single fetch during a backlog drain can balloon
/// to multiple GiB of transient heap (heap-profiled as the dominant source of
/// sinexd's drain-time RSS). Capping the fetch at 64 MiB of raw payload keeps the
/// transient heap to a few hundred MiB regardless of backlog depth.
pub(super) const DEFAULT_BATCH_FETCH_MAX_BYTES: usize = 64 * 1024 * 1024;
pub(super) const DEFAULT_MAX_ACK_PENDING: i64 = 100;
/// NATS-side `max_deliver` on the events consumer. Must be >= the highest
/// application-side terminal threshold below so app-level DLQ routing fires
/// before NATS silently stops redelivery. Sized for the source-material
/// cross-stream-lag scenario (#1310/#1311).
pub(super) const MAIN_CONSUMER_JETSTREAM_MAX_DELIVER: i64 = 32;
pub(super) const MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD: i64 = 10;
/// Source-material-not-found is a soft cross-stream-lag condition, not a hard
/// error: the material's BEGIN message is being processed on a separate consumer
/// path. Give it generous retry budget. With `FK_VIOLATION_RETRY_DELAY` = 5s,
/// threshold = 30 means up to ~150s wall-clock for the BEGIN to catch up before
/// we give up and DLQ. The earlier value of 10 (50s) routed many events to DLQ
/// during normal backlog drains. See #1310 / #1311.
pub(super) const SOURCE_MATERIAL_READY_DLQ_THRESHOLD: i64 = 30;

/// Retry delay for deferred events whose source material isn't registered yet.
///
/// Each NAK with this delay counts toward `max_deliver` (10), so the total race
/// window the system tolerates is `delay * max_deliver` (= 50 s with 5 s delay).
///
/// The cross-stream race is the load-bearing case: events on
/// `PROD_SINEX_RAW_EVENTS` and material lifecycle frames on `SOURCE_MATERIAL`
/// flow through independent `JetStream` consumers with no cross-stream ordering.
/// Under backlog, the `SOURCE_MATERIAL` consumer can lag behind the events
/// consumer by tens of seconds; the previous 200 ms delay × 10 retries (= 2 s
/// total window) was insufficient and DLQ'd every fresh self-observation
/// material's first events (see issue #1241).
///
/// 5 s × 10 retries = 50 s is the practical upper bound a healthy assembler
/// should clear; longer delays mainly hurt liveness under transient races.
pub(super) const FK_VIOLATION_RETRY_DELAY: Duration = Duration::from_secs(5);
pub(super) const STREAM_CAPACITY_CHECK_INTERVAL: Duration = Duration::from_mins(5); // Check every 5 minutes
// Keep runtime-created stream caps aligned with the Nix bootstrap path. The current
// nats CLI rejects --max-bytes values above signed 32-bit range.
pub(super) const JETSTREAM_BOOTSTRAP_MAX_BYTES: i64 = 2_147_483_647;
