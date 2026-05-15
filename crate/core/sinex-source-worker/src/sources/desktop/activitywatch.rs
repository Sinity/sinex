//! `desktop.activitywatch` source unit.
//!
//! Reads ActivityWatch events from its SQLite database by joining `events` and
//! `buckets` tables. The bucket_id prefix determines which payload type to emit:
//! - `aw-watcher-window_*` → `window.active`
//! - `aw-watcher-afk_*`    → `afk.changed`
//! - `aw-watcher-web_*`    → `browser.tab.active`
//!
//! Adapter: `SqliteRowAdapter` (MutableSnapshot checkpoint, ROWID cursor)
//! Anchor: `SqliteRow`
//! Checkpoint family: `MutableSnapshot { backing_store: "sqlite", anchor: "bucket_event_timestamp" }`
//! Privacy tier: `Secret` — titles/URLs pass through `ProcessingContext::WindowTitle`

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceUnitId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::{self, ProcessingContext};
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

use sinex_node_sdk::parser::{MaterialParser, ParserError, ParserResult, SqliteRowAdapter};

use crate::register_adapter_ingestor;

// ---------------------------------------------------------------------------
// Source unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "desktop.activitywatch",
        namespace: "desktop",
        event_types: &[
            ("activitywatch", "window.active"),
            ("activitywatch", "afk.changed"),
            ("activitywatch", "browser.tab.active"),
        ],
        privacy_tier: PrivacyTier::Secret,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "anchor_sqlite_row",
            "timestamp_intrinsic",
            "window_title_redacted",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(source_unit, bucket_id, event_timestamp)",
        ),
        access_policy: "target_home_read:activitywatch_sqlite",
    }
}

// ---------------------------------------------------------------------------
// Binding
// ---------------------------------------------------------------------------

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:desktop.activitywatch"),
        "desktop.activitywatch",
        "desktop",
    )
    .implementation("sinex-source-worker")
    .adapter("SqliteRowAdapter")
    .output_event_type("window.active")
    .privacy_context("window_title")
    .material_policy("activitywatch_bucket_event")
    .checkpoint_policy("mutable_snapshot")
    .resource_shape("linear_rows_bounded_memory")
    .source_unit_id("desktop.activitywatch")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "bucket_event_timestamp",
    })
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("desktop_activitywatch")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

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

/// Parse an ISO8601 datetime string from ActivityWatch into a `Timestamp`.
///
/// ActivityWatch stores timestamps as `"2024-01-15T14:23:45.123456+00:00"`.
fn parse_aw_timestamp(s: &str) -> Option<Timestamp> {
    time::OffsetDateTime::parse(
        s,
        &time::format_description::well_known::Rfc3339,
    )
    .ok()
    .map(Timestamp::new)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parses ActivityWatch SQLite rows into typed window/afk/browser-tab events.
///
/// The `SqliteRowAdapter` is configured with a JOIN query that attaches the
/// `bucket_id` from the `buckets` table to each event row.  The parser reads
/// `bucket_id` from the row JSON and dispatches to one of three payload shapes.
///
/// Malformed or unknown bucket types produce a `skip_row` (empty intents)
/// rather than an error, so one bad bucket does not abort the whole batch.
#[derive(Debug, Clone, Default)]
pub struct ActivityWatchParser;

#[async_trait]
impl MaterialParser for ActivityWatchParser {
    type Config = ActivityWatchParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("activitywatch-sqlite"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::SqliteQuery],
            source_unit_id: SourceUnitId::from_static("desktop.activitywatch"),
            declared_event_types: vec![
                (EventSource::from_static("activitywatch"), EventType::from_static("window.active")),
                (EventSource::from_static("activitywatch"), EventType::from_static("afk.changed")),
                (EventSource::from_static("activitywatch"), EventType::from_static("browser.tab.active")),
            ],
            privacy_contexts: vec![ProcessingContext::WindowTitle],
            proof_obligations: vec![
                "anchor_sqlite_row".into(),
                "timestamp_intrinsic".into(),
                "window_title_redacted".into(),
            ],
            description: "Parses ActivityWatch SQLite events into typed window/afk/browser events.".into(),
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

        let bucket_id = row
            .get("bucket_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Silently skip rows with unknown bucket kinds — AW can have custom watchers.
        let kind = classify_bucket(bucket_id);
        if matches!(kind, BucketKind::Unknown) {
            return Ok(vec![]);
        }

        // Extract common fields.
        let started_at = row.get("started_at").and_then(|v| v.as_str());
        let ts_orig = started_at
            .and_then(parse_aw_timestamp)
            .unwrap_or_else(Timestamp::now);

        let data = row.get("data").cloned().unwrap_or(serde_json::Value::Null);

        let redact_title = |title: &str| -> String {
            privacy::engine()
                .ok()
                .map(|eng| eng.process(title, ProcessingContext::WindowTitle).text.into_owned())
                .unwrap_or_else(|| title.to_string())
        };

        // Schema payloads (ActivityWatchWindowActivePayload, AfkChangedPayload,
        // BrowserTabActivePayload) require `duration_ms: u64` (not the
        // `duration_secs` we computed in the SQL query). Convert here. Also
        // BrowserTabActivePayload requires `browser` — extract from the
        // bucket name suffix (`aw-watcher-web-firefox` → "firefox").
        let duration_ms: u64 = row
            .get("duration")
            .and_then(|v| v.as_f64())
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
                        "title": redact_title(title),
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
                // URLs are highly sensitive — redact via WindowTitle context (closest available).
                let redacted_url = redact_title(url);
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
                        "url": redacted_url,
                        "title": redact_title(title),
                        "duration_ms": duration_ms,
                    }),
                )
            }
            BucketKind::Unknown => unreachable!("filtered above"),
        };

        let intent = ParsedEventIntent {
            id: sinex_primitives::ids::Id::new(),
            source_unit_id: ctx.source_unit_id.clone(),
            parser_id: ParserId::from_static("activitywatch-sqlite"),
            parser_version: "1.0.0".into(),
            event_type: EventType::new(event_type).map_err(|e| {
                ParserError::Parse(format!("invalid event type '{event_type}': {e}"))
            })?,
            event_source: EventSource::from_static("activitywatch"),
            payload,
            ts_orig,
            timing: TimingEvidence::Intrinsic {
                field: "started_at".into(),
                confidence: TimingConfidence::Intrinsic,
            },
            anchor: record.anchor.clone(),
            occurrence_key: None,
            privacy_context: ProcessingContext::WindowTitle,
            field_privacy_log: None,
            synthesis_parents: None,
        };

        Ok(vec![intent])
    }

    fn baseline_adapter_config() -> serde_json::Value {
        // Actual aw-server-rust schema:
        //   events:  id, bucketrow (FK → buckets.id), starttime, endtime, data
        //   buckets: id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL,
        //            type, client, hostname, created, data, metadata
        // The parser reads `bucket_id` (the *human name* like
        // `aw-watcher-window_<host>`), `started_at`, `duration` (computed),
        // and `data`. JOIN buckets and expose `buckets.name AS bucket_id` —
        // not `buckets.id` (the integer primary key). The earlier shape
        // selected `buckets.id` so every row classified as
        // `BucketKind::Unknown` (the prefix `aw-watcher-*` never matched
        // integer "1","2",...) and silently dropped 4.8M events.
        serde_json::json!({
            "query": "SELECT events.id AS rowid, buckets.name AS bucket_id, events.starttime AS started_at, ((events.endtime - events.starttime) / 1000000000.0) AS duration, events.data AS data FROM events JOIN buckets ON events.bucketrow = buckets.id ORDER BY events.id",
            "table": "events"
        })
    }
}

// ---------------------------------------------------------------------------
// Node factory registration
// ---------------------------------------------------------------------------

register_adapter_ingestor!(
    source_unit_id: "desktop.activitywatch",
    adapter: SqliteRowAdapter,
    parser: ActivityWatchParser,
);
