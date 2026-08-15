//! `desktop.activitywatch` source.
//!
//! Reads `ActivityWatch` events from its `SQLite` database by joining `events` and
//! `buckets` tables. The `bucket_id` prefix determines which payload type to emit:
//! - `aw-watcher-window_*` → `window.active`
//! - `aw-watcher-afk_*`    → `afk.changed`
//! - `aw-watcher-web_*`    → `browser.tab.active`
//!
//! Adapter: `SqliteRowAdapter` (`MutableSnapshot` checkpoint, ROWID cursor)
//! Anchor: `SqliteRow`
//! Checkpoint family: `MutableSnapshot { backing_store: "sqlite", anchor: "bucket_event_timestamp" }`
//! Privacy tier: `Secret` — title/URL fields are policy-scoped by payload path.
//!
//! ## Mutability (sinex-h3g)
//!
//! `aw-server-rust` extends the newest event row *for each bucket* in place
//! via heartbeat merging: as long as consecutive observations for a bucket
//! arrive within its `pulsetime`, the existing row's `endtime` (and derived
//! `duration`) grows instead of a new row being inserted. A plain
//! `WHERE rowid > cursor` scan therefore reads a still-growing row exactly
//! once, at whatever duration it had at that moment, and never learns it grew
//! — the `MutableSnapshot` contract's promise was previously undelivered.
//!
//! The fix is `SqliteRowConfig::mutable_trailing_rows` (see
//! `baseline_adapter_config` below): each poll re-reads the trailing N rows
//! *before* the cursor in addition to the new ones, so a growing row is
//! re-observed on every subsequent poll until it stops being the newest row
//! for its bucket. The window is sized in raw rowids (not per-bucket) because
//! the adapter has no bucket-aware cursor; it only needs to comfortably cover
//! the number of buckets that can be concurrently active (window/afk/web
//! watchers — a handful), not every row ActivityWatch has ever grown.
//!
//! Re-reads flow through the *normal* parser → admission path. Occurrence
//! identity is start-anchored (`bucket_id` + start timestamp — see
//! `occurrence_key` in `parse_record`, never `endtime`), and the AW payloads
//! opt into `RevisionPolicy::SupersedeOnChange`
//! (`sinex_primitives::events::payloads::desktop`): an unchanged re-read
//! content-hashes identically and is suppressed; a grown re-read archives the
//! stale short interpretation and admits the new one as the sole live row
//! for that occurrence (sinex-y8v).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::{ProcessingContext, SensitivityHint};
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape, SourceCriticality,
};
use sinex_primitives::temporal::Timestamp;

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};

// ---------------------------------------------------------------------------
// Parser config
// ---------------------------------------------------------------------------

/// Configuration for [`ActivityWatchParser`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActivityWatchParserConfig;

// ---------------------------------------------------------------------------
// Bucket kind classification
// ---------------------------------------------------------------------------

enum BucketKind {
    Window,
    Afk,
    Web,
    Unknown,
}

fn classify_bucket(bucket_id: &str) -> BucketKind {
    if bucket_id.starts_with("aw-watcher-window") {
        BucketKind::Window
    } else if bucket_id.starts_with("aw-watcher-afk") {
        BucketKind::Afk
    } else if bucket_id.starts_with("aw-watcher-web") {
        BucketKind::Web
    } else {
        BucketKind::Unknown
    }
}

// ---------------------------------------------------------------------------
// Timestamp parsing helpers
// ---------------------------------------------------------------------------

/// Parse an ActivityWatch timestamp into a `Timestamp`.
///
/// Older fixtures and exports may expose RFC3339 strings, but the live
/// `aw-server-rust` SQLite schema stores `events.starttime` as Unix
/// nanoseconds.
fn parse_aw_timestamp(value: &serde_json::Value) -> Option<Timestamp> {
    match value {
        serde_json::Value::Number(number) => number
            .as_i64()
            .and_then(|nanos| Timestamp::from_unix_timestamp_nanos(i128::from(nanos))),
        serde_json::Value::String(raw) => {
            time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
                .ok()
                .map(Timestamp::new)
                .or_else(|| {
                    raw.parse::<i64>()
                        .ok()
                        .and_then(|nanos| Timestamp::from_unix_timestamp_nanos(i128::from(nanos)))
                })
        }
        _ => None,
    }
}

fn activitywatch_data_object(row: &serde_json::Value) -> serde_json::Value {
    match row.get("data") {
        Some(serde_json::Value::Object(_)) => row["data"].clone(),
        Some(serde_json::Value::String(raw)) => {
            serde_json::from_str(raw).unwrap_or(serde_json::Value::Null)
        }
        Some(value) => value.clone(),
        None => serde_json::Value::Null,
    }
}

fn occurrence_timestamp_key(timestamp: Option<Timestamp>, anchor: &MaterialAnchor) -> String {
    timestamp.map_or_else(
        || match anchor {
            MaterialAnchor::SqliteRow { table, rowid } => format!("anchor:{table}:{rowid}"),
            other => format!("anchor:{other:?}"),
        },
        |timestamp| timestamp.format_rfc3339(),
    )
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parses `ActivityWatch` `SQLite` rows into typed window/afk/browser-tab events.
///
/// The `SqliteRowAdapter` is configured with a JOIN query that attaches the
/// `bucket_id` from the `buckets` table to each event row.  The parser reads
/// `bucket_id` from the row JSON and dispatches to one of three payload shapes.
///
/// Malformed or unknown bucket types produce a `skip_row` (empty intents)
/// rather than an error, so one bad bucket does not abort the whole batch.
#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "desktop.activitywatch",
    namespace = "desktop",
    event_source = "activitywatch",
    event_type = "window.active",
    event_types = "afk.changed, browser.tab.active",
    adapter = "SqliteRowAdapter",
    privacy_tier = PrivacyTier::Secret,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(source, bucket_id, event_timestamp)"),
    access_scope = AccessScope::TargetHome { path: "activitywatch_sqlite" },
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedStream,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::MutableSnapshot { backing_store_kind: "sqlite", occurrence_anchor: "bucket_event_timestamp" },
    runtime_shape = RuntimeShape::Continuous,
    recovery_policy = sinex_primitives::source_contracts::SourceRecoveryPolicy::MUTABLE_SNAPSHOT,
    // sinex-sn6s: ActivityWatch owns its own SQLite store; Sinex is a
    // downstream reader, never the sole copy.
    criticality = SourceCriticality::Reconstructable,
)]
pub struct ActivityWatchParser;

#[async_trait]
impl MaterialParser for ActivityWatchParser {
    type Config = ActivityWatchParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("activitywatch-sqlite"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::SqliteQuery],
            source_id: SourceId::from_static("desktop.activitywatch"),
            declared_event_types: vec![
                (
                    EventSource::from_static("activitywatch"),
                    EventType::from_static("window.active"),
                ),
                (
                    EventSource::from_static("activitywatch"),
                    EventType::from_static("afk.changed"),
                ),
                (
                    EventSource::from_static("activitywatch"),
                    EventType::from_static("browser.tab.active"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Document],
            // Window/browser titles and URLs are free-form user text that may
            // embed anything; exported for policy tooling, never auto-acted (#1611).
            sensitivity_hints: vec![
                SensitivityHint::FreeText,
                SensitivityHint::PotentiallySensitive,
            ],
            description: "Parses ActivityWatch SQLite events into typed window/afk/browser events."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        if record.bytes.is_empty() {
            return Ok(vec![]);
        }

        let row: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("failed to parse AW row JSON: {e}")))?;

        let bucket_id = row.get("bucket_id").and_then(|v| v.as_str()).unwrap_or("");

        // Silently skip rows with unknown bucket kinds — AW can have custom watchers.
        let kind = classify_bucket(bucket_id);
        if matches!(kind, BucketKind::Unknown) {
            return Ok(vec![]);
        }

        // Extract common fields. `parsed_started_at` tracks whether the row
        // actually carried a usable timestamp. Missing timestamps use the
        // material acquisition time only as an in-memory placeholder; the
        // Atemporal evidence below leaves persisted ts_orig for material-tier
        // resolution.
        let parsed_started_at = row.get("started_at").and_then(parse_aw_timestamp);
        let ts_orig = parsed_started_at.unwrap_or(ctx.acquisition_time);

        let data = activitywatch_data_object(&row);

        // Schema payloads (ActivityWatchWindowActivePayload, AfkChangedPayload,
        // BrowserTabActivePayload) require `duration_ms: u64` (not the
        // `duration_secs` we computed in the SQL query). Convert here. Also
        // BrowserTabActivePayload requires `browser` — extract from the
        // bucket name suffix (`aw-watcher-web-firefox` → "firefox").
        let duration_ms: u64 = row
            .get("duration")
            .and_then(sinex_primitives::JsonValue::as_f64)
            .map_or(0, |secs| (secs * 1000.0).max(0.0) as u64);

        let (event_type, payload) = match kind {
            BucketKind::Window => {
                let title = data.get("title").and_then(|v| v.as_str()).unwrap_or("");
                let app = data.get("app").and_then(|v| v.as_str()).unwrap_or("");
                (
                    "window.active",
                    serde_json::json!({
                        "bucket_id": bucket_id,
                        "app": app,
                        "title": title,
                        "duration_ms": duration_ms,
                    }),
                )
            }
            BucketKind::Afk => {
                let status = data.get("status").and_then(|v| v.as_str()).unwrap_or("");
                (
                    "afk.changed",
                    serde_json::json!({
                        "bucket_id": bucket_id,
                        "status": status,
                        "duration_ms": duration_ms,
                    }),
                )
            }
            BucketKind::Web => {
                let url = data.get("url").and_then(|v| v.as_str()).unwrap_or("");
                let title = data.get("title").and_then(|v| v.as_str()).unwrap_or("");
                // Bucket name pattern: `aw-watcher-web-<browser>` (e.g.
                // `aw-watcher-web-firefox`, `aw-watcher-web-chrome`).
                let browser = bucket_id
                    .strip_prefix("aw-watcher-web-")
                    .unwrap_or("")
                    .to_string();
                (
                    "browser.tab.active",
                    serde_json::json!({
                        "bucket_id": bucket_id,
                        "browser": browser,
                        "url": url,
                        "title": title,
                        "duration_ms": duration_ms,
                    }),
                )
            }
            BucketKind::Unknown => unreachable!("filtered above"),
        };

        // Start-anchored occurrence identity (sinex-y8v): keyed on bucket_id +
        // the START timestamp only, never `endtime`/`duration`. A grown
        // re-read of the same row (see module docs, sinex-h3g) carries the
        // SAME key here, so admission's SupersedeOnChange path treats it as a
        // revision of the same occurrence rather than a fresh interpretation.
        let occurrence_key = OccurrenceKey {
            source_id: ctx.source_id.clone(),
            fields: vec![
                ("bucket_id".into(), bucket_id.to_string()),
                (
                    "event_timestamp".into(),
                    occurrence_timestamp_key(parsed_started_at, &record.anchor),
                ),
            ],
        };

        let intent = ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
            .parser_id(ParserId::from_static("activitywatch-sqlite"))
            .parser_version("1.0.0")
            .event_type(EventType::new(event_type).map_err(|e| {
                ParserError::Parse(format!("invalid event type '{event_type}': {e}"))
            })?)
            .event_source(EventSource::from_static("activitywatch"))
            .payload(payload)
            .ts_orig(ts_orig)
            .timing(if parsed_started_at.is_some() {
                TimingEvidence::Intrinsic {
                    field: "started_at".into(),
                    confidence: TimingConfidence::Intrinsic,
                }
            } else {
                // `started_at` was missing/unparseable: `ts_orig` above is a
                // material-acquisition placeholder. `Atemporal` tells
                // `intent_to_event_with_anchor`
                // (via `TimingEvidence::resolved_quality() == None`) to leave
                // the event's real ts_orig unresolved, so persistence derives
                // it from the source material's own timing tier instead of
                // trusting this placeholder (sinex-dmz9).
                TimingEvidence::Atemporal
            })
            .anchor(record.anchor.clone())
            .occurrence_key(occurrence_key)
            .privacy_context(ProcessingContext::Document)
            .build();

        Ok(vec![intent])
    }

    fn required_input_keys(&self) -> Vec<String> {
        [
            "buckets.id",
            "buckets.name",
            "events.bucketrow",
            "events.data",
            "events.endtime",
            "events.id",
            "events.starttime",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    fn baseline_adapter_config() -> serde_json::Value {
        // Actual aw-server-rust schema:
        //   events:  id, bucketrow (FK → buckets.id), starttime, endtime, data
        //   buckets: id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL,
        //            type, client, hostname, created, data, metadata
        // The parser reads `bucket_id` (the *human name* like
        // `aw-watcher-window_<host>`), `started_at` (Unix nanoseconds from
        // `events.starttime`), `duration` (computed), and `data`. JOIN
        // buckets and expose `buckets.name AS bucket_id` —
        // not `buckets.id` (the integer primary key). The earlier shape
        // selected `buckets.id` so every row classified as
        // `BucketKind::Unknown` (the prefix `aw-watcher-*` never matched
        // integer "1","2",...) and silently dropped 4.8M events.
        // `mutable_trailing_rows`: re-read the trailing rows below the cursor
        // on every poll (see module docs, sinex-h3g) so a bucket's growing
        // tail row is re-observed after heartbeats extend its `endtime`. 32
        // comfortably covers the handful of watcher buckets
        // (window/afk/web-per-browser) that can be concurrently active
        // without materially widening each poll's row count against the
        // 10_000-row default batch size.
        serde_json::json!({
            "query": "SELECT events.id AS rowid, buckets.name AS bucket_id, events.starttime AS started_at, ((events.endtime - events.starttime) / 1000000000.0) AS duration, events.data AS data FROM events JOIN buckets ON events.bucketrow = buckets.id ORDER BY events.id",
            "table": "events",
            "mutable_trailing_rows": 32
        })
    }
}

#[cfg(test)]
#[path = "activitywatch_test.rs"]
mod tests;
