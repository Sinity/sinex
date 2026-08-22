//! `browser.history` source — `SQLite` + dump-file browser history ingestion.
//!
//! Two input legs via [`ChainedAdapter`]:
//! - **Primary (`SQLite`)**: reads browser history DBs (qutebrowser `History` table,
//!   chromium `visits JOIN urls`). Format discrimination happens at parse time
//!   by inspecting which columns are present in each row's JSON.
//! - **Secondary (`AppendOnlyFile`)**: reads JSONL/NDJSON dump export lines appended
//!   to by polylogue or manual browser history exports.
//!
//! Privacy tier: `Secret` — URLs carry auth tokens. The parser emits privacy
//! context metadata; DB admission policy owns payload redaction/suppression.
//!
//! ## Mutability (sinex-h3g / sinex-audit-h3g-atuin-browser, Chromium leg)
//!
//! Chromium's `visits.visit_duration` column is finalized via an UPDATE to
//! the SAME row (`visits.id`) only once the NEXT navigation happens in that
//! tab, so a plain `WHERE rowid > cursor` scan permanently freezes
//! `visit_duration_ms` at `0`/absent for chromium-sourced visits. qutebrowser
//! has no equivalent in-place-mutated column and is unaffected. This is the
//! same `SqliteRowAdapter`/`MutableSnapshot` gap `desktop.activitywatch` had
//! before sinex-h3g (see that module's docs for the mechanism): the primary
//! leg's `mutable_trailing_rows` (set in `baseline_adapter_config` below)
//! re-reads a trailing window of already-cursored rows on every poll, and
//! `PageVisitedPayload` opts into `RevisionPolicy::SupersedeOnChange` so a
//! re-read carrying the finalized duration archives the stale interpretation
//! and admits the revision as the sole live row. Occurrence identity
//! (`visit_id`, from `visits.id`/`History.rowid`) is already stable across
//! the UPDATE, so no occurrence-key change was needed. The knob applies to
//! both legs (it's the shared primary-leg baseline) — qutebrowser rows are
//! never mutated in place, so a re-read there simply content-hashes
//! identically and is suppressed as a harmless duplicate.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_macros::SourceMeta;
use tracing::warn;

use crate::runtime::parser::{
    AppendOnlyFileAdapter, ChainedAdapter, MaterialParser, ParserError, ParserResult,
};
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape, SourceCriticality,
};
use sinex_primitives::{
    domain::{EventSource, EventType},
    parser::{
        InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
        ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
    },
    privacy::ProcessingContext,
    temporal::Timestamp,
};

// ---------------------------------------------------------------------------
// Timestamp heuristic (mirrors sinex-browser-source logic)
// ---------------------------------------------------------------------------

/// Chromium Windows FILETIME epoch offset (microseconds from 1601-01-01 to Unix epoch).
const CHROMIUM_EPOCH_OFFSET_MICROS: i64 = 11_644_473_600_i64 * 1_000_000_i64;

fn chromium_visit_timestamp(raw: i64) -> Option<Timestamp> {
    let unix_micros = raw.checked_sub(CHROMIUM_EPOCH_OFFSET_MICROS)?;
    Timestamp::from_unix_timestamp_nanos(i128::from(unix_micros) * 1_000)
}

/// Heuristic integer timestamp decoder: infers unit (ns/µs/ms/s) from digit count.
fn parse_integer_timestamp(value: i64) -> Option<Timestamp> {
    let digits = value.unsigned_abs().checked_ilog10().unwrap_or(0) + 1;
    let unit_nanos: i128 = if digits >= 18 {
        1
    } else if digits >= 15 {
        1_000
    } else if digits >= 12 {
        1_000_000
    } else {
        1_000_000_000
    };
    Timestamp::from_unix_timestamp_nanos(i128::from(value) * unit_nanos)
}

/// Extract the first recognisable timestamp from a JSON object.
fn extract_timestamp(obj: &serde_json::Map<String, serde_json::Value>) -> Option<Timestamp> {
    const FIELDS: &[&str] = &[
        "iso_time",
        "time",
        "visit_time",
        "visitTime",
        "lastVisitTime",
        "timestamp",
        "DateTime",
        "date",
    ];
    for field in FIELDS {
        let Some(v) = obj.get(*field) else { continue };
        match v {
            serde_json::Value::Number(n) => {
                if let Some(v) = n.as_i64()
                    && let Some(ts) = parse_integer_timestamp(v)
                {
                    return Some(ts);
                }
            }
            serde_json::Value::String(s) => {
                // Try RFC3339 via time crate (already a workspace dep).
                if let Ok(odt) =
                    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    && let Some(ts) =
                        Timestamp::from_unix_timestamp_nanos(odt.unix_timestamp_nanos())
                {
                    return Some(ts);
                }
                // Fallback: try parsing as integer string.
                if let Ok(n) = s.trim().parse::<i64>()
                    && let Some(ts) = parse_integer_timestamp(n)
                {
                    return Some(ts);
                }
            }
            _ => {}
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Browser inference from filename
// ---------------------------------------------------------------------------

fn infer_browser_from_path(path: &str) -> String {
    let lower = std::path::Path::new(path)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    for browser in [
        "chrome",
        "edge",
        "firefox",
        "floorp",
        "qutebrowser",
        "zen",
        "merged",
        "browser",
    ] {
        if lower.starts_with(browser) {
            return browser.to_string();
        }
    }
    "browser".to_string()
}

// ---------------------------------------------------------------------------
// Parser config
// ---------------------------------------------------------------------------

/// Configuration for [`BrowserHistoryParser`] (no fields required at runtime).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrowserHistoryParserConfig {}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Imperative parser for `browser.history`.
///
/// Dispatches on the `logical_path` prefix injected by [`ChainedAdapter`]:
/// - `"primary/"` → `SQLite` row JSON (columns from `SqliteRowAdapter`).
/// - `"secondary/"` → JSONL dump file line.
/// - No prefix → assume `SQLite` (direct test invocation).
#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "browser.history",
    namespace = "web",
    event_type = "page.visited",
    event_source = "webhistory",
    adapter = "ChainedAdapter<SqliteRowAdapter, AppendOnlyFileAdapter>",
    implementation = "sinexd",
    privacy_tier = PrivacyTier::Secret,
    horizons(Horizon::Continuous, Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(source, browser_profile, visit_id)"),
    access_scope = AccessScope::TargetHome {
        path: "browser_history"
    },
    capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:browser.web.check, operation:browser.web.reconnect, operation:browser.web.pause, operation:browser.web.resume, operation:browser.web.drain, operation:browser.web.inspect",
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::BoundedStream,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "visit_id",
    },
    runtime_shape = RuntimeShape::Continuous,
    recovery_policy = sinex_primitives::source_contracts::SourceRecoveryPolicy::MUTABLE_SNAPSHOT,
    factory_adapter = BrowserHistoryAdapter,
    // sinex-sn6s: qutebrowser/Chrome own their own history SQLite stores;
    // Sinex is a downstream reader, never the sole copy.
    criticality = SourceCriticality::Reconstructable,
)]
pub struct BrowserHistoryParser;

/// Configuration for [`TakeoutChromeHistoryParser`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TakeoutChromeHistoryConfig;

/// Parser for an extracted Google Takeout `Chrome/BrowserHistory.json` member.
///
/// The archive itself remains the operator-owned raw authority. This parser
/// deliberately consumes an extracted member through `StaticFileAdapter` so
/// the archive extraction step can be audited independently. Takeout rows do
/// not carry Chromium's SQLite visit id, so their occurrence key is scoped to
/// the Takeout client id and a deterministic row fingerprint. That makes
/// overlapping Takeout exports idempotent while keeping them distinct from
/// live or historical SQLite visits, which remain candidates for downstream
/// adjudication rather than silent merges.
#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "browser.takeout-history",
    namespace = "web",
    event_type = "page.visited",
    event_source = "webhistory",
    adapter = "StaticFileAdapter",
    implementation = "sinexd",
    privacy_tier = PrivacyTier::Secret,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(takeout_client_id, record_hash)"),
    access_scope = AccessScope::StagedExport,
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
    recovery_policy = sinex_primitives::source_contracts::SourceRecoveryPolicy::APPEND_STREAM,
    criticality = SourceCriticality::Reconstructable,
)]
pub struct TakeoutChromeHistoryParser;

const PARSER_ID: &str = "browser-history";
const PARSER_VERSION: &str = "1.0.1";

#[async_trait]
impl MaterialParser for BrowserHistoryParser {
    type Config = BrowserHistoryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static(PARSER_ID),
            parser_version: PARSER_VERSION.into(),
            accepted_input_shapes: vec![
                InputShapeKind::SqliteQuery,
                InputShapeKind::AppendOnlyFile,
                // ChainedAdapter reports Subprocess as a sentinel kind.
                InputShapeKind::Subprocess,
            ],
            source_id: SourceId::from_static("browser.history"),
            declared_event_types: vec![(
                EventSource::from_static("webhistory"),
                EventType::from_static("page.visited"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: "Parses browser history from SQLite DBs and JSONL dump files.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let logical_path = record
            .logical_path
            .as_deref()
            .map_or("", camino::Utf8Path::as_str);

        if logical_path.starts_with("secondary/") {
            parse_dump_record(&record, ctx)
        } else {
            parse_sqlite_record(&record, ctx)
        }
    }

    fn required_input_keys(&self) -> Vec<String> {
        [
            "History.url",
            "History.atime",
            "urls.url",
            "visits.visit_time",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    fn baseline_adapter_config() -> serde_json::Value {
        // Primary leg query: qutebrowser's `History` table. Chromium-only
        // deployments override via Nix to `SELECT rowid, * FROM visits JOIN
        // urls ON visits.url = urls.id`. The parser discriminates rows by
        // column presence (`atime` → qutebrowser, `visit_time` → chromium);
        // either way it gets the data it needs. Secondary leg defaults are
        // empty — `path` must come from Nix binding (the JSONL dump file).
        serde_json::json!({
            "primary": {
                "query": "SELECT rowid, * FROM History",
                "table": "History",
                // `mutable_trailing_rows` (sinex-h3g mechanism, see module
                // docs): re-reads a trailing window of already-cursored rows
                // on every poll so a visit's row is re-observed after
                // Chromium finalizes `visit_duration` on the next
                // navigation. 64 generously covers concurrently open tabs
                // across windows whose most recent visit hasn't yet been
                // superseded by a same-tab navigation.
                "mutable_trailing_rows": 64
            },
            "secondary": { "skip_empty": true }
        })
    }
}

#[async_trait]
impl MaterialParser for TakeoutChromeHistoryParser {
    type Config = TakeoutChromeHistoryConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("browser-takeout-history"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("browser.takeout-history"),
            declared_event_types: vec![(
                EventSource::from_static("webhistory"),
                EventType::from_static("page.visited"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: "Parses extracted Google Takeout Chrome BrowserHistory.json files.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let root: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("Takeout Chrome JSON parse failed: {e}")))?;
        let rows = root
            .get("Browser History")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                ParserError::Parse(
                    "Takeout Chrome JSON must contain a 'Browser History' array".into(),
                )
            })?;
        let source_file = record
            .logical_path
            .as_deref()
            .map_or_else(String::new, |path| path.as_str().to_owned());
        let mut intents = Vec::with_capacity(rows.len());

        for (index, row) in rows.iter().enumerate() {
            let object = row.as_object().ok_or_else(|| {
                ParserError::Parse(format!("Takeout Chrome row {index} is not an object"))
            })?;
            let url = object
                .get("url")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    ParserError::Parse(format!("Takeout Chrome row {index} has no url"))
                })?;
            let title = object
                .get("title")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let time_value = object.get("time_usec").ok_or_else(|| {
                ParserError::Parse(format!("Takeout Chrome row {index} has no time_usec"))
            })?;
            let (time_usec, visit_time) = parse_takeout_time_usec(time_value, index)?;
            let client_id = object
                .get("client_id")
                .and_then(|value| value.as_str())
                .map(str::to_owned);
            let page_transition = object
                .get("page_transition")
                .and_then(|value| value.as_str())
                .map(str::to_owned);
            let favicon_url = object
                .get("favicon_url")
                .and_then(|value| value.as_str())
                .map(str::to_owned);

            let record_hash = takeout_record_hash(
                client_id.as_deref(),
                &time_usec,
                url,
                title,
                page_transition.as_deref(),
            );
            let client_scope = client_id
                .clone()
                .unwrap_or_else(|| format!("path:{source_file}"));
            let mut payload = serde_json::Map::new();
            payload.insert("browser".into(), serde_json::json!("chromium"));
            payload.insert("title".into(), serde_json::json!(title));
            payload.insert("url".into(), serde_json::json!(url));
            payload.insert(
                "visit_time".into(),
                serde_json::json!(visit_time.format_rfc3339()),
            );
            payload.insert("time_usec".into(), serde_json::json!(time_usec));
            payload.insert("source_file".into(), serde_json::json!(source_file));
            payload.insert("takeout_record_hash".into(), serde_json::json!(record_hash));
            if let Some(client_id) = client_id {
                payload.insert("client_id".into(), serde_json::json!(client_id));
            }
            if let Some(page_transition) = page_transition {
                payload.insert("transition".into(), serde_json::json!(page_transition));
            }
            if let Some(favicon_url) = favicon_url {
                payload.insert("favicon_url".into(), serde_json::json!(favicon_url));
            }

            let intent = ParsedEventIntent::builder()
                .source_id(ctx.source_id.clone())
                .parser_id(ParserId::from_static("browser-takeout-history"))
                .parser_version("1.0.0")
                .event_type(EventType::from_static("page.visited"))
                .event_source(EventSource::from_static("webhistory"))
                .payload(serde_json::Value::Object(payload))
                .ts_orig(visit_time)
                .timing(TimingEvidence::Intrinsic {
                    field: "time_usec".into(),
                    confidence: TimingConfidence::Intrinsic,
                })
                // Static JSON-array imports use the stable provider-array
                // ordinal as their per-entry material anchor. The
                // record_hash above is the cross-export identity key.
                .anchor(MaterialAnchor::ByteRange {
                    start: index as u64,
                    len: 1,
                })
                .occurrence_key(OccurrenceKey {
                    source_id: SourceId::from_static("browser.takeout-history"),
                    fields: vec![
                        ("takeout_client_id".into(), client_scope),
                        ("record_hash".into(), record_hash),
                    ],
                })
                .privacy_context(ProcessingContext::Metadata)
                .build();
            intents.push(intent);
        }

        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec![
            "/Browser History/[]/time_usec".into(),
            "/Browser History/[]/url".into(),
        ]
    }
}

fn parse_takeout_time_usec(
    value: &serde_json::Value,
    row_index: usize,
) -> ParserResult<(String, Timestamp)> {
    let raw = match value {
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::String(string) => string.clone(),
        _ => {
            return Err(ParserError::Parse(format!(
                "Takeout Chrome row {row_index} time_usec is not numeric"
            )));
        }
    };
    let micros = raw.parse::<i64>().map_err(|error| {
        ParserError::Parse(format!(
            "Takeout Chrome row {row_index} invalid time_usec: {error}"
        ))
    })?;
    let timestamp =
        Timestamp::from_unix_timestamp_nanos(i128::from(micros) * 1_000).ok_or_else(|| {
            ParserError::Parse(format!(
                "Takeout Chrome row {row_index} timestamp out of range"
            ))
        })?;
    Ok((raw, timestamp))
}

fn takeout_record_hash(
    client_id: Option<&str>,
    time_usec: &str,
    url: &str,
    title: &str,
    page_transition: Option<&str>,
) -> String {
    let mut hasher = blake3::Hasher::new();
    for field in [
        client_id.unwrap_or(""),
        time_usec,
        url,
        title,
        page_transition.unwrap_or(""),
    ] {
        hasher.update(field.as_bytes());
        hasher.update(&[0]);
    }
    hasher.finalize().to_hex().to_string()
}

// ---------------------------------------------------------------------------
// SQLite leg
// ---------------------------------------------------------------------------

fn parse_sqlite_record(
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParserResult<Vec<ParsedEventIntent>> {
    let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(&record.bytes)
        .map_err(|e| ParserError::Parse(format!("browser SQLite row JSON parse failed: {e}")))?;

    // Carry the DB file path through to `source_file` — `PageVisitedPayload`
    // requires it (#1321). The row parsers leave it empty; we backfill from
    // the record's logical path here. Empty when path is missing for the
    // primary leg (e.g. test fixtures with raw bytes); `build_intent` skips
    // empty source_file but per #1321 we always populate when we have a path.
    let source_file = record
        .logical_path
        .as_deref()
        .map_or("", camino::Utf8Path::as_str)
        .to_string();

    let mut visit = if obj.contains_key("visit_time") {
        parse_chromium_row(&obj)?
    } else if obj.contains_key("atime") {
        parse_qutebrowser_row(&obj)?
    } else if let Some(visit_time) = extract_timestamp(&obj) {
        // Fallback: JSONL dump row arriving without the "secondary/" logical-path
        // prefix (e.g. test dispatch with logical_path = None). Parse generically.
        let url = obj
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let title = obj
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        VisitData {
            browser: infer_browser_from_path(&source_file),
            title,
            url,
            visit_time,
            referrer: obj
                .get("referrer")
                .or_else(|| obj.get("external_referrer_url"))
                .and_then(|v| v.as_str())
                .map(String::from),
            transition: obj
                .get("transition")
                .and_then(|v| v.as_str())
                .map(String::from),
            visit_id: obj
                .get("visitId")
                .or_else(|| obj.get("visit_id"))
                .or_else(|| obj.get("id"))
                .and_then(|v| v.as_str())
                .map(String::from),
            visit_duration_ms: None,
            source_file: source_file.clone(),
            line_number: None,
            db_row_id: None,
        }
    } else {
        return Ok(vec![]);
    };
    visit.source_file = source_file;
    build_intent(visit, record, ctx)
}

fn parse_qutebrowser_row(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> ParserResult<VisitData> {
    let row_id = obj
        .get("rowid")
        .and_then(sinex_primitives::JsonValue::as_i64);
    let url = obj
        .get("url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let title = obj
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let atime = obj
        .get("atime")
        .and_then(sinex_primitives::JsonValue::as_i64)
        .unwrap_or(0);
    let redirect = obj
        .get("redirect")
        .and_then(sinex_primitives::JsonValue::as_i64)
        .unwrap_or(0);
    let visit_time = parse_integer_timestamp(atime)
        .ok_or_else(|| ParserError::Parse(format!("invalid qutebrowser atime {atime}")))?;
    Ok(VisitData {
        browser: "qutebrowser".into(),
        title,
        url,
        visit_time,
        referrer: None,
        transition: (redirect != 0).then(|| "redirect".to_string()),
        visit_id: row_id.map(|id| id.to_string()),
        visit_duration_ms: None,
        source_file: String::new(),
        line_number: None,
        db_row_id: row_id.and_then(|id| u64::try_from(id).ok()),
    })
}

fn parse_chromium_row(obj: &serde_json::Map<String, serde_json::Value>) -> ParserResult<VisitData> {
    let row_id = obj
        .get("rowid")
        .and_then(sinex_primitives::JsonValue::as_i64);
    let url = obj
        .get("url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let title = obj
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let visit_time_raw = obj
        .get("visit_time")
        .and_then(sinex_primitives::JsonValue::as_i64)
        .unwrap_or(0);
    let referrer = obj
        .get("external_referrer_url")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from);
    let transition_raw = obj
        .get("transition")
        .and_then(sinex_primitives::JsonValue::as_i64)
        .unwrap_or(0);
    let visit_duration = obj
        .get("visit_duration")
        .and_then(sinex_primitives::JsonValue::as_i64)
        .unwrap_or(0);
    let visit_time = chromium_visit_timestamp(visit_time_raw).ok_or_else(|| {
        ParserError::Parse(format!("invalid chromium visit_time {visit_time_raw}"))
    })?;
    Ok(VisitData {
        browser: "chromium".into(),
        title,
        url,
        visit_time,
        referrer,
        transition: Some(transition_raw.to_string()),
        visit_id: row_id.map(|id| id.to_string()),
        visit_duration_ms: (visit_duration >= 0).then_some((visit_duration as u64) / 1_000),
        source_file: String::new(),
        line_number: None,
        db_row_id: row_id.and_then(|id| u64::try_from(id).ok()),
    })
}

// ---------------------------------------------------------------------------
// Dump file leg
// ---------------------------------------------------------------------------

fn parse_dump_record(
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParserResult<Vec<ParsedEventIntent>> {
    let line = std::str::from_utf8(&record.bytes)
        .map_err(|e| ParserError::Parse(format!("dump record UTF-8 decode: {e}")))?
        .trim();
    if line.is_empty() {
        return Ok(vec![]);
    }
    let json: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                error = %e,
                line = %line,
                "browser history dump: malformed JSON line; skipping record"
            );
            return Ok(vec![]);
        }
    };
    let Some(obj) = json.as_object() else {
        warn!("browser history dump: non-object JSON line; skipping record");
        return Ok(vec![]);
    };
    let Some(visit_time) = extract_timestamp(obj) else {
        return Ok(vec![]);
    };
    let url = obj
        .get("url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let title = obj
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let path_suffix = record
        .logical_path
        .as_deref()
        .and_then(|p| p.as_str().strip_prefix("secondary/"))
        .unwrap_or("");
    let visit = VisitData {
        browser: infer_browser_from_path(path_suffix),
        title,
        url,
        visit_time,
        referrer: obj
            .get("referrer")
            .or_else(|| obj.get("external_referrer_url"))
            .and_then(|v| v.as_str())
            .map(String::from),
        transition: obj
            .get("transition")
            .and_then(|v| v.as_str())
            .map(String::from),
        visit_id: obj
            .get("visitId")
            .or_else(|| obj.get("visit_id"))
            .or_else(|| obj.get("id"))
            .and_then(|v| v.as_str())
            .map(String::from),
        visit_duration_ms: None,
        source_file: path_suffix.to_string(),
        line_number: None,
        db_row_id: None,
    };
    build_intent(visit, record, ctx)
}

// ---------------------------------------------------------------------------
// Shared intermediate type + intent builder
// ---------------------------------------------------------------------------

struct VisitData {
    browser: String,
    title: String,
    url: String,
    visit_time: Timestamp,
    referrer: Option<String>,
    transition: Option<String>,
    visit_id: Option<String>,
    visit_duration_ms: Option<u64>,
    source_file: String,
    line_number: Option<u64>,
    db_row_id: Option<u64>,
}

fn build_intent(
    visit: VisitData,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParserResult<Vec<ParsedEventIntent>> {
    let mut payload = serde_json::Map::new();
    payload.insert("browser".into(), serde_json::json!(visit.browser));
    payload.insert("title".into(), serde_json::json!(visit.title));
    payload.insert("url".into(), serde_json::json!(visit.url));
    payload.insert(
        "visit_time".into(),
        serde_json::json!(visit.visit_time.format_rfc3339()),
    );
    if let Some(ref r) = visit.referrer {
        payload.insert("referrer".into(), serde_json::json!(r));
    }
    if let Some(ref t) = visit.transition {
        payload.insert("transition".into(), serde_json::json!(t));
    }
    if let Some(ref vid) = visit.visit_id {
        payload.insert("visit_id".into(), serde_json::json!(vid));
    }
    if let Some(ms) = visit.visit_duration_ms {
        payload.insert("visit_duration_ms".into(), serde_json::json!(ms));
    }
    // `PageVisitedPayload.source_file` is a required field — always insert,
    // even if empty (preserves the contract for schema validation). #1321.
    payload.insert("source_file".into(), serde_json::json!(visit.source_file));
    if let Some(ln) = visit.line_number {
        payload.insert("line_number".into(), serde_json::json!(ln));
    }
    if let Some(rid) = visit.db_row_id {
        payload.insert("db_row_id".into(), serde_json::json!(rid));
    }

    // Provider visit ids are stable across mutable SQLite re-reads. Keep the
    // profile coordinate separate from the payload's source_file so the
    // occurrence contract remains explicit. Records without a provider id
    // still receive a replay-stable, source-scoped key, but it includes the
    // physical record anchor and bytes. That prevents the old rowid=0
    // collision while avoiding a silent cross-artifact merge of id-less
    // Takeout rows with database visits.
    let browser_profile = if visit.source_file.is_empty() {
        visit.browser.clone()
    } else {
        visit.source_file.clone()
    };
    let visit_id = visit.visit_id.clone().unwrap_or_else(|| {
        let mut hasher = blake3::Hasher::new();
        hasher.update(browser_profile.as_bytes());
        hasher.update(format!("{:?}", record.anchor).as_bytes());
        hasher.update(&record.bytes);
        format!("anonymous:{}", hasher.finalize().to_hex())
    });
    let occurrence_key = Some(OccurrenceKey {
        source_id: ctx.source_id.clone(),
        fields: vec![
            ("browser_profile".to_string(), browser_profile),
            ("visit_id".to_string(), visit_id),
        ],
    });

    Ok(vec![
        ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
            .parser_id(ParserId::from_static(PARSER_ID))
            .parser_version(PARSER_VERSION)
            .event_type(EventType::from_static("page.visited"))
            .event_source(EventSource::from_static("webhistory"))
            .payload(serde_json::Value::Object(payload))
            .ts_orig(visit.visit_time)
            .timing(TimingEvidence::Intrinsic {
                field: "visit_time".into(),
                confidence: TimingConfidence::Intrinsic,
            })
            .anchor(record.anchor.clone())
            .maybe_occurrence_key(occurrence_key)
            .privacy_context(ProcessingContext::Metadata)
            .build(),
    ])
}

// ---------------------------------------------------------------------------
// Adapter type alias and registration
// ---------------------------------------------------------------------------

/// Chained adapter: primary = `SQLite` history DB rows, secondary = dump file lines.
pub type BrowserHistoryAdapter =
    ChainedAdapter<crate::runtime::parser::SqliteRowAdapter, AppendOnlyFileAdapter>;
