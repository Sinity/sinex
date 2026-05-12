//! `browser.history` source unit — SQLite + dump-file browser history ingestion.
//!
//! Two input legs via [`ChainedAdapter`]:
//! - **Primary (SQLite)**: reads browser history DBs (qutebrowser `History` table,
//!   chromium `visits JOIN urls`). Format discrimination happens at parse time
//!   by inspecting which columns are present in each row's JSON.
//! - **Secondary (AppendOnlyFile)**: reads JSONL/NDJSON dump export lines appended
//!   to by polylogue or manual browser history exports.
//!
//! Privacy tier: `Secret` — URLs carry auth tokens.
//! `url` / `normalized_url` / `referrer` → `ProcessingContext::Clipboard`.
//! `title` → `ProcessingContext::WindowTitle`.
//! `source_file` → `ProcessingContext::Metadata`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sinex_node_sdk::parser::{
    AppendOnlyFileAdapter, ChainedAdapter, MaterialParser, ParserError, ParserResult,
};
use sinex_primitives::{
    domain::{EventSource, EventType},
    parser::{
        InputShapeKind, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, ParserManifest,
        SourceRecord, SourceUnitId, TimingConfidence, TimingEvidence,
    },
    privacy::{self, ProcessingContext},
    temporal::Timestamp,
};
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

// ---------------------------------------------------------------------------
// Source unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "browser.history",
        namespace: "web",
        event_types: &[
            ("webhistory", "page.visited"),
        ],
        privacy_tier: PrivacyTier::Secret,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "url_privacy_redaction",
            "sqlite_row_anchor",
            "dump_line_anchor",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(source_unit, browser_profile, visit_id)",
        ),
        access_policy: "target_home_read:browser_history",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:browser.history"),
        "browser.history",
        "web",
    )
    .implementation("sinex-source-worker")
    .adapter("ChainedAdapter<SqliteRowAdapter, AppendOnlyFileAdapter>")
    .output_event_type("page.visited")
    .privacy_context("url")
    .material_policy("browser_visit_id")
    .checkpoint_policy("mutable_snapshot")
    .resource_shape("linear_rows_bounded_memory")
    .source_unit_id("browser.history")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "visit_id",
    })
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("browser_history_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Timestamp heuristic (mirrors sinex-browser-ingestor logic)
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
    let unit_nanos: i128 = if digits >= 18 { 1 }
        else if digits >= 15 { 1_000 }
        else if digits >= 12 { 1_000_000 }
        else { 1_000_000_000 };
    Timestamp::from_unix_timestamp_nanos(i128::from(value) * unit_nanos)
}

/// Extract the first recognisable timestamp from a JSON object.
fn extract_timestamp(obj: &serde_json::Map<String, serde_json::Value>) -> Option<Timestamp> {
    const FIELDS: &[&str] = &[
        "iso_time", "time", "visit_time", "visitTime", "lastVisitTime",
        "timestamp", "DateTime", "date",
    ];
    for field in FIELDS {
        let Some(v) = obj.get(*field) else { continue };
        match v {
            serde_json::Value::Number(n) => {
                if let Some(v) = n.as_i64() {
                    if let Some(ts) = parse_integer_timestamp(v) {
                        return Some(ts);
                    }
                }
            }
            serde_json::Value::String(s) => {
                // Try RFC3339 via time crate (already a workspace dep).
                if let Ok(odt) = time::OffsetDateTime::parse(
                    s,
                    &time::format_description::well_known::Rfc3339,
                ) {
                    if let Some(ts) = Timestamp::from_unix_timestamp_nanos(
                        i128::from(odt.unix_timestamp_nanos()),
                    ) {
                        return Some(ts);
                    }
                }
                // Fallback: try parsing as integer string.
                if let Ok(n) = s.trim().parse::<i64>() {
                    if let Some(ts) = parse_integer_timestamp(n) {
                        return Some(ts);
                    }
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
    for browser in ["chrome", "edge", "firefox", "floorp", "qutebrowser", "zen", "merged", "browser"] {
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
/// - `"primary/"` → SQLite row JSON (columns from `SqliteRowAdapter`).
/// - `"secondary/"` → JSONL dump file line.
/// - No prefix → assume SQLite (direct test invocation).
#[derive(Debug, Clone, Default)]
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
            source_unit_id: SourceUnitId::from_static("browser.history"),
            declared_event_types: vec![(
                EventSource::from_static("webhistory"),
                EventType::from_static("page.visited"),
            )],
            privacy_contexts: vec![
                ProcessingContext::Clipboard,
                ProcessingContext::WindowTitle,
                ProcessingContext::Metadata,
            ],
            proof_obligations: vec![
                "url_privacy_redaction".into(),
                "sqlite_row_anchor".into(),
                "dump_line_anchor".into(),
            ],
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
            .map(camino::Utf8Path::as_str)
            .unwrap_or("");

        if logical_path.starts_with("secondary/") {
            parse_dump_record(&record, ctx)
        } else {
            parse_sqlite_record(&record, ctx)
        }
    }
}

// ---------------------------------------------------------------------------
// SQLite leg
// ---------------------------------------------------------------------------

fn parse_sqlite_record(
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParserResult<Vec<ParsedEventIntent>> {
    let obj: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("browser SQLite row JSON parse failed: {e}")))?;

    if obj.contains_key("visit_time") {
        build_intent(parse_chromium_row(&obj)?, record, ctx)
    } else if obj.contains_key("atime") {
        build_intent(parse_qutebrowser_row(&obj)?, record, ctx)
    } else {
        Ok(vec![])
    }
}

fn parse_qutebrowser_row(obj: &serde_json::Map<String, serde_json::Value>) -> ParserResult<VisitData> {
    let row_id = obj.get("rowid").and_then(|v| v.as_i64()).unwrap_or(0);
    let url = obj.get("url").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let title = obj.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let atime = obj.get("atime").and_then(|v| v.as_i64()).unwrap_or(0);
    let redirect = obj.get("redirect").and_then(|v| v.as_i64()).unwrap_or(0);
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
    let row_id = obj.get("rowid").and_then(|v| v.as_i64()).unwrap_or(0);
    let url = obj.get("url").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let title = obj.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let visit_time_raw = obj.get("visit_time").and_then(|v| v.as_i64()).unwrap_or(0);
    let referrer = obj
        .get("external_referrer_url")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from);
    let transition_raw = obj.get("transition").and_then(|v| v.as_i64()).unwrap_or(0);
    let visit_duration = obj.get("visit_duration").and_then(|v| v.as_i64()).unwrap_or(0);
    let visit_time = chromium_visit_timestamp(visit_time_raw)
        .ok_or_else(|| ParserError::Parse(format!("invalid chromium visit_time {visit_time_raw}")))?;
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
        Err(_) => return Ok(vec![]),
    };
    let obj = match json.as_object() {
        Some(o) => o,
        None => return Ok(vec![]),
    };
    let Some(visit_time) = extract_timestamp(obj) else {
        return Ok(vec![]);
    };
    let url = obj.get("url").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let title = obj.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
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
        transition: obj.get("transition").and_then(|v| v.as_str()).map(String::from),
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
    let url = redact(visit.url, ProcessingContext::Clipboard)?;
    let title = redact(visit.title, ProcessingContext::WindowTitle)?;
    let referrer = visit.referrer.map(|r| redact(r, ProcessingContext::Clipboard)).transpose()?;
    let source_file = redact(visit.source_file, ProcessingContext::Metadata)?;

    let mut payload = serde_json::Map::new();
    payload.insert("browser".into(), serde_json::json!(visit.browser));
    payload.insert("title".into(), serde_json::json!(title));
    payload.insert("url".into(), serde_json::json!(url));
    payload.insert("visit_time".into(), serde_json::json!(visit.visit_time.format_rfc3339()));
    if let Some(ref r) = referrer {
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
    if !source_file.is_empty() {
        payload.insert("source_file".into(), serde_json::json!(source_file));
    }
    if let Some(ln) = visit.line_number {
        payload.insert("line_number".into(), serde_json::json!(ln));
    }
    if let Some(rid) = visit.db_row_id {
        payload.insert("db_row_id".into(), serde_json::json!(rid));
    }

    let occurrence_key = visit.visit_id.map(|vid| OccurrenceKey {
        source_unit_id: ctx.source_unit_id.clone(),
        fields: vec![("visit_id".to_string(), vid)],
    });

    Ok(vec![ParsedEventIntent {
        id: sinex_primitives::ids::Id::new(),
        source_unit_id: ctx.source_unit_id.clone(),
        parser_id: ParserId::from_static(PARSER_ID),
        parser_version: PARSER_VERSION.into(),
        event_type: EventType::from_static("page.visited"),
        event_source: EventSource::from_static("webhistory"),
        payload: serde_json::Value::Object(payload),
        ts_orig: visit.visit_time,
        timing: TimingEvidence::Intrinsic {
            field: "visit_time".into(),
            confidence: TimingConfidence::Intrinsic,
        },
        anchor: record.anchor.clone(),
        occurrence_key,
        privacy_context: ProcessingContext::Clipboard,
        field_privacy_log: None,
        synthesis_parents: None,
    }])
}

// ---------------------------------------------------------------------------
// Privacy helper
// ---------------------------------------------------------------------------

fn redact(value: String, ctx: ProcessingContext) -> ParserResult<String> {
    privacy::process(&value, ctx)
        .map(|r| r.text.into_owned())
        .map_err(|e| ParserError::Privacy(e.to_string()))
}

// ---------------------------------------------------------------------------
// Adapter type alias and registration
// ---------------------------------------------------------------------------

/// Chained adapter: primary = SQLite history DB rows, secondary = dump file lines.
pub type BrowserHistoryAdapter =
    ChainedAdapter<sinex_node_sdk::parser::SqliteRowAdapter, AppendOnlyFileAdapter>;

crate::register_adapter_ingestor!(
    source_unit_id: "browser.history",
    adapter: BrowserHistoryAdapter,
    parser: BrowserHistoryParser,
    // Primary leg query: qutebrowser's `History` table. Chromium-only
    // deployments override via Nix to `SELECT rowid, * FROM visits JOIN
    // urls ON visits.url = urls.id`. The parser discriminates rows by
    // column presence (`atime` → qutebrowser, `visit_time` → chromium);
    // either way it gets the data it needs. Secondary leg defaults are
    // empty — `path` must come from Nix binding (the JSONL dump file).
    default_config: serde_json::json!({
        "primary": { "query": "SELECT rowid, * FROM History", "table": "History" },
        "secondary": { "skip_empty": true }
    }),
);
