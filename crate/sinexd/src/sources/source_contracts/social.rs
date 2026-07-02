//! Social platform export parsers (#1089).
//!
//! Three source contracts in one module:
//!
//! 1. **`reddit-gdpr-comments`** — parses `comments.csv` from the Reddit
//!    GDPR export into `reddit`/`social.comment.posted` events.
//!
//! 2. **`reddit-gdpr-posts`** — parses `posts.csv` from the Reddit GDPR
//!    export into `reddit`/`social.post.created` events.
//!
//! 3. **`wykop-entries`** — parses `wykop_entries_added.jsonl` (one JSON
//!    object per line) into `wykop`/`social.entry.created` events.
//!
//! 4. **`wykop-entry-comments`** — parses `wykop_entry_comments.jsonl` into
//!    `wykop`/`social.entry_comment.posted` events.
//!
//! All four use [`StaticFileAdapter`]: the operator stages the export file,
//! the adapter reads it once, and the parser emits one event per record.
//!
//! ## Reddit CSV schema
//!
//! `comments.csv`: `id,permalink,date,ip,subreddit,gildings,link,parent,body,media`
//!
//! `posts.csv`: `id,permalink,date,ip,subreddit,gildings,title,url,body`
//!
//! The `ip` and `gildings` columns are dropped; they carry no analytical
//! value and reduce the surface of personal-network metadata in the store.
//!
//! ## Wykop JSONL schema
//!
//! Each line is a self-contained JSON object. The `kind` field (`"entry"` /
//! `"entry_comment"`) identifies the record type. Parsers are registered on
//! separate source contracts so the operator can stage each JSONL separately.
//!
//! ## Anchoring
//!
//! CSV: `MaterialAnchor::Line { byte_start: 0, line: <1-based row index> }`.
//! JSONL: `MaterialAnchor::Line { byte_start: 0, line: <1-based line index> }`.
//!
//! ## Occurrence identity
//!
//! Reddit comments: `(reddit_id, subreddit)`.
//! Reddit posts: `(reddit_id, subreddit)`.
//! Wykop entries: `entry_id`.
//! Wykop entry comments: `comment_id`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;

// ===========================================================================
// Reddit — comments.csv
// ===========================================================================

// ---------------------------------------------------------------------------
// Raw CSV row
// ---------------------------------------------------------------------------

/// Raw row from Reddit's `comments.csv`.
///
/// Columns: `id,permalink,date,ip,subreddit,gildings,link,parent,body,media`
#[derive(Debug, Deserialize)]
struct RedditCommentCsvRow {
    id: String,
    #[serde(default)]
    permalink: String,
    date: String,
    #[serde(default, rename = "ip")]
    _ip: String,
    subreddit: String,
    #[serde(default, rename = "gildings")]
    _gildings: String,
    #[serde(default)]
    link: String,
    #[serde(default)]
    parent: String,
    #[serde(default)]
    body: String,
    #[serde(default, rename = "media")]
    _media: String,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RedditCommentParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "reddit-gdpr-comments",
    namespace = "social",
    event_source = "reddit",
    event_type = "social.comment.posted",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(reddit_id, subreddit)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct RedditCommentParser;

#[async_trait]
impl MaterialParser for RedditCommentParser {
    type Config = RedditCommentParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("reddit-gdpr-comments"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("reddit-gdpr-comments"),
            declared_event_types: vec![(
                EventSource::from_static("reddit"),
                EventType::from_static("social.comment.posted"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
            description: "Parses Reddit GDPR comments.csv into typed \
                social.comment.posted events. Drops ip, gildings, and media \
                columns."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(record.bytes.as_slice());

        let mut intents = Vec::new();
        for (row_index, row_result) in reader.deserialize::<RedditCommentCsvRow>().enumerate() {
            let row = row_result.map_err(|e| {
                ParserError::Parse(format!(
                    "Reddit comments.csv row {} parse error: {e}",
                    row_index + 1
                ))
            })?;
            intents.push(parse_reddit_comment_row(row, (row_index + 1) as u64, ctx)?);
        }
        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        ["id", "date", "subreddit"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}

fn parse_reddit_comment_row(
    row: RedditCommentCsvRow,
    line: u64,
    ctx: &ParserContext,
) -> ParserResult<ParsedEventIntent> {
    let created_at = parse_reddit_date(&row.date)?;

    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("reddit-gdpr-comments"),
        fields: vec![
            ("reddit_id".into(), row.id.clone()),
            ("subreddit".into(), row.subreddit.clone()),
        ],
    };

    let payload = serde_json::json!({
        "reddit_id": row.id,
        "subreddit": row.subreddit,
        "body": row.body,
        "created_at": created_at,
        "parent_id": non_empty(&row.parent),
        "link_id": non_empty(&row.link),
        "permalink": non_empty(&row.permalink),
    });

    Ok(ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("reddit-gdpr-comments"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("social.comment.posted"))
        .event_source(EventSource::from_static("reddit"))
        .payload(payload)
        .ts_orig(created_at)
        .timing(TimingEvidence::Intrinsic {
            field: "date".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::Line {
            byte_start: 0,
            line,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build())
}

// ===========================================================================
// Reddit — posts.csv
// ===========================================================================

// ---------------------------------------------------------------------------
// Raw CSV row
// ---------------------------------------------------------------------------

/// Raw row from Reddit's `posts.csv`.
///
/// Columns: `id,permalink,date,ip,subreddit,gildings,title,url,body`
#[derive(Debug, Deserialize)]
struct RedditPostCsvRow {
    id: String,
    #[serde(default)]
    permalink: String,
    date: String,
    #[serde(default, rename = "ip")]
    _ip: String,
    subreddit: String,
    #[serde(default, rename = "gildings")]
    _gildings: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    body: String,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RedditPostParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "reddit-gdpr-posts",
    namespace = "social",
    event_source = "reddit",
    event_type = "social.post.created",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(reddit_id, subreddit)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct RedditPostParser;

#[async_trait]
impl MaterialParser for RedditPostParser {
    type Config = RedditPostParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("reddit-gdpr-posts"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("reddit-gdpr-posts"),
            declared_event_types: vec![(
                EventSource::from_static("reddit"),
                EventType::from_static("social.post.created"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
            description: "Parses Reddit GDPR posts.csv into typed \
                social.post.created events. Drops ip and gildings columns."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(record.bytes.as_slice());

        let mut intents = Vec::new();
        for (row_index, row_result) in reader.deserialize::<RedditPostCsvRow>().enumerate() {
            let row = row_result.map_err(|e| {
                ParserError::Parse(format!(
                    "Reddit posts.csv row {} parse error: {e}",
                    row_index + 1
                ))
            })?;
            intents.push(parse_reddit_post_row(row, (row_index + 1) as u64, ctx)?);
        }
        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        ["id", "date", "subreddit"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}

fn parse_reddit_post_row(
    row: RedditPostCsvRow,
    line: u64,
    ctx: &ParserContext,
) -> ParserResult<ParsedEventIntent> {
    let created_at = parse_reddit_date(&row.date)?;

    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("reddit-gdpr-posts"),
        fields: vec![
            ("reddit_id".into(), row.id.clone()),
            ("subreddit".into(), row.subreddit.clone()),
        ],
    };

    let payload = serde_json::json!({
        "reddit_id": row.id,
        "subreddit": row.subreddit,
        "title": row.title,
        "created_at": created_at,
        "body": non_empty(&row.body),
        "url": non_empty(&row.url),
        "permalink": non_empty(&row.permalink),
    });

    Ok(ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("reddit-gdpr-posts"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("social.post.created"))
        .event_source(EventSource::from_static("reddit"))
        .payload(payload)
        .ts_orig(created_at)
        .timing(TimingEvidence::Intrinsic {
            field: "date".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::Line {
            byte_start: 0,
            line,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build())
}

// ===========================================================================
// Wykop — wykop_entries_added.jsonl
// ===========================================================================

// ---------------------------------------------------------------------------
// Raw JSONL row
// ---------------------------------------------------------------------------

/// Raw row from `wykop_entries_added.jsonl`.
#[derive(Debug, Deserialize)]
struct WykopEntryJsonRow {
    entry_id: u64,
    entry_url: String,
    entry_created_at: String,
    #[serde(default)]
    entry_content: String,
    #[serde(default)]
    entry_tags: Vec<String>,
    #[serde(default)]
    votes_score: i64,
    #[serde(default)]
    entry_photo_url: Option<String>,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WykopEntryParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "wykop-entries",
    namespace = "social",
    event_source = "wykop",
    event_type = "social.entry.created",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(entry_id)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct WykopEntryParser;

#[async_trait]
impl MaterialParser for WykopEntryParser {
    type Config = WykopEntryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("wykop-entries"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("wykop-entries"),
            declared_event_types: vec![(
                EventSource::from_static("wykop"),
                EventType::from_static("social.entry.created"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
            description: "Parses Wykop entries JSONL export into typed \
                social.entry.created events. One event per JSONL line."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let mut intents = Vec::new();
        for (line_index, line) in record.bytes.split(|&b| b == b'\n').enumerate() {
            let trimmed = line.to_vec();
            let trimmed_str = std::str::from_utf8(&trimmed)
                .map_err(|e| {
                    ParserError::Parse(format!("UTF-8 error on line {}: {e}", line_index + 1))
                })?
                .trim();
            if trimmed_str.is_empty() {
                continue;
            }
            let row: WykopEntryJsonRow = serde_json::from_str(trimmed_str).map_err(|e| {
                ParserError::Parse(format!(
                    "wykop_entries_added.jsonl line {} parse error: {e}",
                    line_index + 1
                ))
            })?;
            intents.push(parse_wykop_entry_row(row, (line_index + 1) as u64, ctx)?);
        }
        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["/[]/entry_id".into(), "/[]/entry_created_at".into()]
    }
}

fn parse_wykop_entry_row(
    row: WykopEntryJsonRow,
    line: u64,
    ctx: &ParserContext,
) -> ParserResult<ParsedEventIntent> {
    let created_at = parse_wykop_datetime(&row.entry_created_at)?;

    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("wykop-entries"),
        fields: vec![("entry_id".into(), row.entry_id.to_string())],
    };

    let payload = serde_json::json!({
        "entry_id": row.entry_id,
        "entry_url": row.entry_url,
        "created_at": created_at,
        "content": row.entry_content,
        "tags": row.entry_tags,
        "votes_score": row.votes_score,
        "photo_url": row.entry_photo_url,
    });

    Ok(ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("wykop-entries"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("social.entry.created"))
        .event_source(EventSource::from_static("wykop"))
        .payload(payload)
        .ts_orig(created_at)
        .timing(TimingEvidence::Intrinsic {
            field: "entry_created_at".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::Line {
            byte_start: 0,
            line,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build())
}

// ===========================================================================
// Wykop — wykop_entry_comments.jsonl
// ===========================================================================

// ---------------------------------------------------------------------------
// Raw JSONL row
// ---------------------------------------------------------------------------

/// Raw row from `wykop_entry_comments.jsonl`.
#[derive(Debug, Deserialize)]
struct WykopEntryCommentJsonRow {
    comment_id: u64,
    comment_created_at: String,
    #[serde(default)]
    comment_content: String,
    #[serde(default)]
    comment_photo_url: Option<String>,
    #[serde(default)]
    comment_rating: i64,
    entry_id: u64,
    #[serde(default)]
    entry_url: String,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WykopEntryCommentParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "wykop-entry-comments",
    namespace = "social",
    event_source = "wykop",
    event_type = "social.entry_comment.posted",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(comment_id)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct WykopEntryCommentParser;

#[async_trait]
impl MaterialParser for WykopEntryCommentParser {
    type Config = WykopEntryCommentParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("wykop-entry-comments"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("wykop-entry-comments"),
            declared_event_types: vec![(
                EventSource::from_static("wykop"),
                EventType::from_static("social.entry_comment.posted"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
            description: "Parses Wykop entry comments JSONL export into typed \
                social.entry_comment.posted events. One event per JSONL line."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let mut intents = Vec::new();
        for (line_index, line) in record.bytes.split(|&b| b == b'\n').enumerate() {
            let trimmed = line.to_vec();
            let trimmed_str = std::str::from_utf8(&trimmed)
                .map_err(|e| {
                    ParserError::Parse(format!("UTF-8 error on line {}: {e}", line_index + 1))
                })?
                .trim();
            if trimmed_str.is_empty() {
                continue;
            }
            let row: WykopEntryCommentJsonRow = serde_json::from_str(trimmed_str).map_err(|e| {
                ParserError::Parse(format!(
                    "wykop_entry_comments.jsonl line {} parse error: {e}",
                    line_index + 1
                ))
            })?;
            intents.push(parse_wykop_entry_comment_row(
                row,
                (line_index + 1) as u64,
                ctx,
            )?);
        }
        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["/[]/comment_id".into(), "/[]/comment_created_at".into()]
    }
}

fn parse_wykop_entry_comment_row(
    row: WykopEntryCommentJsonRow,
    line: u64,
    ctx: &ParserContext,
) -> ParserResult<ParsedEventIntent> {
    let created_at = parse_wykop_datetime(&row.comment_created_at)?;

    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("wykop-entry-comments"),
        fields: vec![("comment_id".into(), row.comment_id.to_string())],
    };

    let payload = serde_json::json!({
        "comment_id": row.comment_id,
        "entry_id": row.entry_id,
        "entry_url": row.entry_url,
        "created_at": created_at,
        "content": row.comment_content,
        "rating": row.comment_rating,
        "photo_url": row.comment_photo_url,
    });

    Ok(ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("wykop-entry-comments"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("social.entry_comment.posted"))
        .event_source(EventSource::from_static("wykop"))
        .payload(payload)
        .ts_orig(created_at)
        .timing(TimingEvidence::Intrinsic {
            field: "comment_created_at".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::Line {
            byte_start: 0,
            line,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build())
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// Parse Reddit's date column: `"YYYY-MM-DD HH:MM:SS UTC"`.
///
/// Reddit exports dates in this exact format with a literal ` UTC` suffix.
fn parse_reddit_date(raw: &str) -> ParserResult<Timestamp> {
    use time::PrimitiveDateTime;
    use time::format_description::FormatItem;
    use time::format_description::well_known::Rfc3339;
    use time::macros::format_description;

    // First try: strip " UTC" and parse as `YYYY-MM-DD HH:MM:SS`.
    let without_tz = raw.trim_end_matches(" UTC").trim();
    let fmt: &[FormatItem<'_>] =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    if let Ok(dt) = PrimitiveDateTime::parse(without_tz, fmt) {
        use time::UtcOffset;
        return Ok(Timestamp::new(dt.assume_offset(UtcOffset::UTC)));
    }
    // Fallback: try RFC 3339 in case a future export normalises the format.
    use time::OffsetDateTime;
    OffsetDateTime::parse(raw.trim(), &Rfc3339)
        .map(Timestamp::new)
        .map_err(|e| ParserError::Parse(format!("invalid Reddit timestamp '{raw}': {e}")))
}

/// Parse Wykop's datetime column: `"YYYY-MM-DD HH:MM:SS"` (no timezone; treat as UTC).
fn parse_wykop_datetime(raw: &str) -> ParserResult<Timestamp> {
    use time::PrimitiveDateTime;
    use time::UtcOffset;
    use time::macros::format_description;

    let fmt = format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    PrimitiveDateTime::parse(raw.trim(), fmt)
        .map(|dt| Timestamp::new(dt.assume_offset(UtcOffset::UTC)))
        .map_err(|e| ParserError::Parse(format!("invalid Wykop timestamp '{raw}': {e}")))
}

/// Return `Some(s)` when `s` is non-empty after trimming; otherwise `None`.
fn non_empty(s: &str) -> Option<&str> {
    let t = s.trim();
    (!t.is_empty()).then_some(t)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "social_test.rs"]
mod tests;
