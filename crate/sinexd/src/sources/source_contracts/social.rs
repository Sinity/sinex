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

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult, StaticFileAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceBuildImpact, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

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

#[derive(Debug, Clone, Default)]
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

// ---------------------------------------------------------------------------
// Source contract + binding + registration
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "reddit-gdpr-comments",
        namespace: "social",
        event_types: &[("reddit", "social.comment.posted")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(reddit_id, subreddit)"),
        access_policy: "personal_social_data",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:reddit-gdpr-comments"),
        "reddit-gdpr-comments",
        "social",
    )
    .implementation("sinexd")
    .adapter("StaticFileAdapter")
    .output_event_type("social.comment.posted")
    .privacy_context("Document")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_id("reddit-gdpr-comments")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("reddit_gdpr_comments_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

crate::register_source!(
    source_id: "reddit-gdpr-comments",
    adapter: StaticFileAdapter,
    parser: RedditCommentParser,
);

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

#[derive(Debug, Clone, Default)]
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

// ---------------------------------------------------------------------------
// Source contract + binding + registration
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "reddit-gdpr-posts",
        namespace: "social",
        event_types: &[("reddit", "social.post.created")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(reddit_id, subreddit)"),
        access_policy: "personal_social_data",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:reddit-gdpr-posts"),
        "reddit-gdpr-posts",
        "social",
    )
    .implementation("sinexd")
    .adapter("StaticFileAdapter")
    .output_event_type("social.post.created")
    .privacy_context("Document")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_id("reddit-gdpr-posts")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("reddit_gdpr_posts_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

crate::register_source!(
    source_id: "reddit-gdpr-posts",
    adapter: StaticFileAdapter,
    parser: RedditPostParser,
);

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

#[derive(Debug, Clone, Default)]
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

// ---------------------------------------------------------------------------
// Source contract + binding + registration
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "wykop-entries",
        namespace: "social",
        event_types: &[("wykop", "social.entry.created")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(entry_id)"),
        access_policy: "personal_social_data",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:wykop-entries"),
        "wykop-entries",
        "social",
    )
    .implementation("sinexd")
    .adapter("StaticFileAdapter")
    .output_event_type("social.entry.created")
    .privacy_context("Document")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_id("wykop-entries")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("wykop_entries_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

crate::register_source!(
    source_id: "wykop-entries",
    adapter: StaticFileAdapter,
    parser: WykopEntryParser,
);

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

#[derive(Debug, Clone, Default)]
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

// ---------------------------------------------------------------------------
// Source contract + binding + registration
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "wykop-entry-comments",
        namespace: "social",
        event_types: &[("wykop", "social.entry_comment.posted")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(comment_id)"),
        access_policy: "personal_social_data",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:wykop-entry-comments"),
        "wykop-entry-comments",
        "social",
    )
    .implementation("sinexd")
    .adapter("StaticFileAdapter")
    .output_event_type("social.entry_comment.posted")
    .privacy_context("Document")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_id("wykop-entry-comments")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("wykop_entry_comments_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

crate::register_source!(
    source_id: "wykop-entry-comments",
    adapter: StaticFileAdapter,
    parser: WykopEntryCommentParser,
);

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
mod tests {
    use super::*;
    use sinex_primitives::Uuid;
    use sinex_primitives::ids::Id;

    use xtask::sandbox::prelude::sinex_test;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn comment_ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("reddit-gdpr-comments"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn post_ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("reddit-gdpr-posts"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn wykop_entry_ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("wykop-entries"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn wykop_entry_comment_ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("wykop-entry-comments"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record_for(bytes: &[u8]) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: bytes.len() as u64,
            },
            bytes: bytes.to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    // -----------------------------------------------------------------------
    // Reddit comments
    // -----------------------------------------------------------------------

    const COMMENT_CSV: &str = "id,permalink,date,ip,subreddit,gildings,link,parent,body,media\n\
         ck1fsao,https://www.reddit.com/r/Futurology/comments/2em2io/elon_musk_warns_ais_could_exterminate_humanity/ck1fsao/,2014-08-27 00:59:46 UTC,,Futurology,0,https://www.reddit.com/r/Futurology/comments/2em2io/,ck1bai1,\"Great comment body.\",\n\
         ck1to2z,https://www.reddit.com/r/Futurology/comments/2em2io/elon_musk_warns_ais_could_exterminate_humanity/ck1to2z/,2014-08-27 13:36:36 UTC,,Futurology,0,https://www.reddit.com/r/Futurology/comments/2em2io/,ck1k0yu,Another comment.,\n";

    #[sinex_test]
    async fn reddit_comments_parses_two_rows() -> TestResult<()> {
        let mut parser = RedditCommentParser;
        let intents = parser
            .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
            .await
            .unwrap();
        assert_eq!(intents.len(), 2);
        for intent in &intents {
            assert_eq!(intent.event_source.as_str(), "reddit");
            assert_eq!(intent.event_type.as_str(), "social.comment.posted");
        }
        Ok(())
    }

    #[sinex_test]
    async fn reddit_comment_preserves_id_and_subreddit() -> TestResult<()> {
        let mut parser = RedditCommentParser;
        let intents = parser
            .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
            .await
            .unwrap();
        assert_eq!(intents[0].payload["reddit_id"], "ck1fsao");
        assert_eq!(intents[0].payload["subreddit"], "Futurology");
        Ok(())
    }

    #[sinex_test]
    async fn reddit_comment_anchor_is_one_based_line() -> TestResult<()> {
        let mut parser = RedditCommentParser;
        let intents = parser
            .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
            .await
            .unwrap();
        assert!(matches!(
            intents[0].anchor,
            MaterialAnchor::Line { line: 1, .. }
        ));
        assert!(matches!(
            intents[1].anchor,
            MaterialAnchor::Line { line: 2, .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn reddit_comment_occurrence_key_uses_id_and_subreddit() -> TestResult<()> {
        let mut parser = RedditCommentParser;
        let intents = parser
            .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(
            key.fields,
            vec![
                ("reddit_id".into(), "ck1fsao".into()),
                ("subreddit".into(), "Futurology".into()),
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn reddit_comment_ip_gildings_media_absent_from_payload() -> TestResult<()> {
        let mut parser = RedditCommentParser;
        let intents = parser
            .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
            .await
            .unwrap();
        let payload = &intents[0].payload;
        assert!(payload.get("ip").is_none());
        assert!(payload.get("gildings").is_none());
        assert!(payload.get("media").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn reddit_comment_timestamp_parses_utc_format() -> TestResult<()> {
        let mut parser = RedditCommentParser;
        let intents = parser
            .parse_record(record_for(COMMENT_CSV.as_bytes()), &comment_ctx())
            .await
            .unwrap();
        let ts = intents[0].ts_orig.inner();
        assert_eq!(ts.year(), 2014);
        assert_eq!(ts.month() as u8, 8);
        assert_eq!(ts.day(), 27);
        Ok(())
    }

    #[sinex_test]
    async fn reddit_comment_invalid_timestamp_errors() -> TestResult<()> {
        let bad = "id,permalink,date,ip,subreddit,gildings,link,parent,body,media\n\
            abc,,not-a-date,,Science,0,,,body,\n";
        let mut parser = RedditCommentParser;
        let err = parser
            .parse_record(record_for(bad.as_bytes()), &comment_ctx())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid Reddit timestamp"), "got: {err}");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Reddit posts
    // -----------------------------------------------------------------------

    const POST_CSV: &str = "id,permalink,date,ip,subreddit,gildings,title,url,body\n\
         38focg,https://www.reddit.com/r/kindle/comments/38focg/kindle_5621_rootjailbreak/,2015-06-03 22:18:00 UTC,,kindle,0,Kindle root/jailbreak,/r/kindle/comments/38focg/,\"Post body text.\"\n\
         3a1oqo,https://www.reddit.com/r/oculus/comments/3a1oqo/when_should_i_expect/,2015-06-16 15:17:27 UTC,,oculus,0,When should I expect CV1?,/r/oculus/comments/3a1oqo/,\n";

    #[sinex_test]
    async fn reddit_posts_parses_two_rows() -> TestResult<()> {
        let mut parser = RedditPostParser;
        let intents = parser
            .parse_record(record_for(POST_CSV.as_bytes()), &post_ctx())
            .await
            .unwrap();
        assert_eq!(intents.len(), 2);
        for intent in &intents {
            assert_eq!(intent.event_source.as_str(), "reddit");
            assert_eq!(intent.event_type.as_str(), "social.post.created");
        }
        Ok(())
    }

    #[sinex_test]
    async fn reddit_post_preserves_title() -> TestResult<()> {
        let mut parser = RedditPostParser;
        let intents = parser
            .parse_record(record_for(POST_CSV.as_bytes()), &post_ctx())
            .await
            .unwrap();
        assert_eq!(intents[0].payload["title"], "Kindle root/jailbreak");
        Ok(())
    }

    #[sinex_test]
    async fn reddit_post_empty_body_becomes_null() -> TestResult<()> {
        let mut parser = RedditPostParser;
        let intents = parser
            .parse_record(record_for(POST_CSV.as_bytes()), &post_ctx())
            .await
            .unwrap();
        // Second row has no body
        assert!(intents[1].payload["body"].is_null());
        Ok(())
    }

    #[sinex_test]
    async fn reddit_post_occurrence_key_uses_id_and_subreddit() -> TestResult<()> {
        let mut parser = RedditPostParser;
        let intents = parser
            .parse_record(record_for(POST_CSV.as_bytes()), &post_ctx())
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(
            key.fields,
            vec![
                ("reddit_id".into(), "38focg".into()),
                ("subreddit".into(), "kindle".into()),
            ]
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Wykop entries
    // -----------------------------------------------------------------------

    const WYKOP_ENTRIES_JSONL: &str = "{\"platform\":\"wykop\",\"kind\":\"entry\",\"username\":\"Sinity\",\"page\":1,\"entry_id\":76315507,\"entry_url\":\"https://wykop.pl/wpis/76315507/piosenka\",\"entry_created_at\":\"2024-05-18 06:53:25\",\"entry_author\":\"Sinity\",\"entry_content\":\"Piosenka o cenzopapie\",\"entry_tags\":[\"humor\",\"sztucznainteligencja\"],\"entry_photo_url\":null,\"votes_score\":0,\"votes_up\":0,\"votes_down\":0}\n\
         {\"platform\":\"wykop\",\"kind\":\"entry\",\"username\":\"Sinity\",\"page\":1,\"entry_id\":76315508,\"entry_url\":\"https://wykop.pl/wpis/76315508/test\",\"entry_created_at\":\"2024-05-19 10:00:00\",\"entry_author\":\"Sinity\",\"entry_content\":\"Test entry\",\"entry_tags\":[],\"entry_photo_url\":\"https://example.com/photo.jpg\",\"votes_score\":5,\"votes_up\":5,\"votes_down\":0}\n";

    #[sinex_test]
    async fn wykop_entries_parses_two_lines() -> TestResult<()> {
        let mut parser = WykopEntryParser;
        let intents = parser
            .parse_record(
                record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
                &wykop_entry_ctx(),
            )
            .await
            .unwrap();
        assert_eq!(intents.len(), 2);
        for intent in &intents {
            assert_eq!(intent.event_source.as_str(), "wykop");
            assert_eq!(intent.event_type.as_str(), "social.entry.created");
        }
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entry_preserves_id_content_tags() -> TestResult<()> {
        let mut parser = WykopEntryParser;
        let intents = parser
            .parse_record(
                record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
                &wykop_entry_ctx(),
            )
            .await
            .unwrap();
        assert_eq!(intents[0].payload["entry_id"], 76315507u64);
        assert_eq!(intents[0].payload["content"], "Piosenka o cenzopapie");
        assert_eq!(
            intents[0].payload["tags"],
            serde_json::json!(["humor", "sztucznainteligencja"])
        );
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entry_null_photo_url_becomes_null() -> TestResult<()> {
        let mut parser = WykopEntryParser;
        let intents = parser
            .parse_record(
                record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
                &wykop_entry_ctx(),
            )
            .await
            .unwrap();
        assert!(intents[0].payload["photo_url"].is_null());
        assert_eq!(
            intents[1].payload["photo_url"],
            "https://example.com/photo.jpg"
        );
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entry_occurrence_key_uses_entry_id() -> TestResult<()> {
        let mut parser = WykopEntryParser;
        let intents = parser
            .parse_record(
                record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
                &wykop_entry_ctx(),
            )
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(key.fields, vec![("entry_id".into(), "76315507".into())]);
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entry_timestamp_parses_datetime() -> TestResult<()> {
        let mut parser = WykopEntryParser;
        let intents = parser
            .parse_record(
                record_for(WYKOP_ENTRIES_JSONL.as_bytes()),
                &wykop_entry_ctx(),
            )
            .await
            .unwrap();
        let ts = intents[0].ts_orig.inner();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month() as u8, 5);
        assert_eq!(ts.day(), 18);
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entry_invalid_timestamp_errors() -> TestResult<()> {
        let bad = "{\"platform\":\"wykop\",\"kind\":\"entry\",\"username\":\"Sinity\",\"page\":1,\"entry_id\":1,\"entry_url\":\"https://wykop.pl/wpis/1/x\",\"entry_created_at\":\"not-a-time\",\"entry_author\":\"Sinity\",\"entry_content\":\"x\",\"entry_tags\":[],\"entry_photo_url\":null,\"votes_score\":0,\"votes_up\":0,\"votes_down\":0}\n";
        let mut parser = WykopEntryParser;
        let err = parser
            .parse_record(record_for(bad.as_bytes()), &wykop_entry_ctx())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid Wykop timestamp"), "got: {err}");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Wykop entry comments
    // -----------------------------------------------------------------------

    const WYKOP_COMMENTS_JSONL: &str = "{\"platform\":\"wykop\",\"kind\":\"entry_comment\",\"username\":\"Sinity\",\"page\":1,\"comment_id\":279391731,\"comment_created_at\":\"2025-02-16 08:21:58\",\"comment_content\":\"Nice entry!\",\"comment_photo_url\":null,\"comment_rating\":2,\"entry_id\":80205363,\"entry_url\":\"https://wykop.pl/wpis/80205363/x\"}\n\
         {\"platform\":\"wykop\",\"kind\":\"entry_comment\",\"username\":\"Sinity\",\"page\":1,\"comment_id\":279391732,\"comment_created_at\":\"2025-02-17 09:00:00\",\"comment_content\":\"Another reply\",\"comment_photo_url\":\"https://example.com/img.png\",\"comment_rating\":0,\"entry_id\":80205364,\"entry_url\":\"https://wykop.pl/wpis/80205364/y\"}\n";

    #[sinex_test]
    async fn wykop_entry_comments_parses_two_lines() -> TestResult<()> {
        let mut parser = WykopEntryCommentParser;
        let intents = parser
            .parse_record(
                record_for(WYKOP_COMMENTS_JSONL.as_bytes()),
                &wykop_entry_comment_ctx(),
            )
            .await
            .unwrap();
        assert_eq!(intents.len(), 2);
        for intent in &intents {
            assert_eq!(intent.event_source.as_str(), "wykop");
            assert_eq!(intent.event_type.as_str(), "social.entry_comment.posted");
        }
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entry_comment_preserves_ids_and_content() -> TestResult<()> {
        let mut parser = WykopEntryCommentParser;
        let intents = parser
            .parse_record(
                record_for(WYKOP_COMMENTS_JSONL.as_bytes()),
                &wykop_entry_comment_ctx(),
            )
            .await
            .unwrap();
        assert_eq!(intents[0].payload["comment_id"], 279391731u64);
        assert_eq!(intents[0].payload["entry_id"], 80205363u64);
        assert_eq!(intents[0].payload["content"], "Nice entry!");
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entry_comment_occurrence_key_uses_comment_id() -> TestResult<()> {
        let mut parser = WykopEntryCommentParser;
        let intents = parser
            .parse_record(
                record_for(WYKOP_COMMENTS_JSONL.as_bytes()),
                &wykop_entry_comment_ctx(),
            )
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(key.fields, vec![("comment_id".into(), "279391731".into())]);
        Ok(())
    }

    #[sinex_test]
    async fn wykop_entry_comment_photo_url_present_and_absent() -> TestResult<()> {
        let mut parser = WykopEntryCommentParser;
        let intents = parser
            .parse_record(
                record_for(WYKOP_COMMENTS_JSONL.as_bytes()),
                &wykop_entry_comment_ctx(),
            )
            .await
            .unwrap();
        assert!(intents[0].payload["photo_url"].is_null());
        assert_eq!(
            intents[1].payload["photo_url"],
            "https://example.com/img.png"
        );
        Ok(())
    }
}
