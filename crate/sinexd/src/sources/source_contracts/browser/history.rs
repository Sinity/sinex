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

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_macros::SourceMeta;
use tracing::warn;

use crate::runtime::parser::{
    AppendOnlyFileAdapter, ChainedAdapter, MaterialParser, ParserError, ParserResult,
};
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::{
    domain::{EventSource, EventType},
    parser::{
        InputShapeKind, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, ParserManifest,
        SourceId, SourceRecord, TimingConfidence, TimingEvidence,
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
    factory_adapter = BrowserHistoryAdapter
)]
pub struct BrowserHistoryParser;

const PARSER_ID: &str = "browser-history";
const PARSER_VERSION: &str = "1.0.0";

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
            privacy_contexts: vec![ProcessingContext::Clipboard, ProcessingContext::Metadata],
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
            "primary": { "query": "SELECT rowid, * FROM History", "table": "History" },
            "secondary": { "skip_empty": true }
        })
    }
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
        .and_then(sinex_primitives::JsonValue::as_i64)
        .unwrap_or(0);
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
        visit_id: Some(row_id.to_string()),
        visit_duration_ms: None,
        source_file: String::new(),
        line_number: None,
        db_row_id: Some(row_id as u64),
    })
}

fn parse_chromium_row(obj: &serde_json::Map<String, serde_json::Value>) -> ParserResult<VisitData> {
    let row_id = obj
        .get("rowid")
        .and_then(sinex_primitives::JsonValue::as_i64)
        .unwrap_or(0);
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
        visit_id: Some(row_id.to_string()),
        visit_duration_ms: (visit_duration >= 0).then_some((visit_duration as u64) / 1_000),
        source_file: String::new(),
        line_number: None,
        db_row_id: Some(row_id as u64),
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

    let occurrence_key = visit.visit_id.as_ref().map(|vid| OccurrenceKey {
        source_id: ctx.source_id.clone(),
        fields: vec![
            ("browser".to_string(), visit.browser.clone()),
            ("source_file".to_string(), visit.source_file.clone()),
            ("visit_id".to_string(), vid.clone()),
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
            .privacy_context(ProcessingContext::Clipboard)
            .build(),
    ])
}

// ---------------------------------------------------------------------------
// Adapter type alias and registration
// ---------------------------------------------------------------------------

/// Chained adapter: primary = `SQLite` history DB rows, secondary = dump file lines.
pub type BrowserHistoryAdapter =
    ChainedAdapter<crate::runtime::parser::SqliteRowAdapter, AppendOnlyFileAdapter>;
