//! A public archive's copy of the operator's Reddit posts and comments
//! (Arctic Shift's `comments.jsonl` and `submissions.jsonl`, one Pushshift
//! object per line), on `ExportArchive`. The archive copied each item when
//! Reddit served it, so it holds items the operator later deleted and items
//! newer than the last GDPR export. Its items carry Reddit's own IDs, which
//! the GDPR export writes too; `post.fusion` joins the two.
use super::fields;
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::social::PostPublished;
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "social.reddit-archive",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "what the operator wrote at Reddit, including what the GDPR export lacks",
    class: Class::Open,
    parser: "reddit-archive",
    parser_version: 1,
    event_types: &["post.published"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        Framing::Lines,
    ),
};

/// Fields read into the event; the rest go to `extra`.
const KNOWN: &[&str] = &[
    "id",
    "name",
    "created_utc",
    "title",
    "body",
    "selftext",
    "permalink",
    "parent_id",
    "link_id",
    "subreddit",
    "score",
];

pub struct RedditArchiveParser;

impl Parser for RedditArchiveParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = PostPublished;

    fn parse(
        &self,
        item: &Map<String, Value>,
        _: &(),
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<PostPublished>, ()), RecordError> {
        // `t1_` names a comment and `t3_` a post (a submission).
        let name = fields::text(item, "name")?;
        let comment = match name.split_once('_') {
            Some(("t1", _)) => true,
            Some(("t3", _)) => false,
            _ => {
                return Err(RecordError::new(format!(
                    "{name:?} is not a comment or post"
                )));
            }
        };
        let created = match item.get("created_utc") {
            Some(Value::Number(n)) => n.as_i64(),
            Some(Value::String(s)) => s.parse().ok(),
            _ => None,
        }
        .ok_or_else(|| RecordError::new("created_utc is not a number"))?;
        let text = |name: &str| fields::optional_text(item, name).filter(|t| !t.is_empty());
        Ok((
            vec![PostPublished {
                service: "reddit".into(),
                kind: if comment { "comment" } else { "post" }.into(),
                native_id: fields::text(item, "id")?,
                created_at: Some(sinex_model::Timestamp::from_unix_secs(created).map_err(
                    |_| RecordError::new(format!("created_utc {created} out of range")),
                )?),
                title: text("title"),
                body: if comment {
                    text("body")
                } else {
                    text("selftext")
                },
                url: text("permalink").map(|p| format!("https://www.reddit.com{p}")),
                parent: if comment {
                    text("parent_id").or_else(|| text("link_id"))
                } else {
                    None
                },
                community: text("subreddit"),
                tags: vec![],
                score: item.get("score").and_then(Value::as_i64),
                extra: fields::extra(item, KNOWN),
            }],
            (),
        ))
    }
}

pub fn source() -> DeclaredSource<RedditArchiveParser> {
    DeclaredSource {
        spec: SPEC,
        parser: RedditArchiveParser,
    }
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.ends_with(".jsonl"),
        framing: Framing::Lines,
        part_bytes: PART_BYTES,
    }
}

/// The archive's files: `comments.jsonl` and `submissions.jsonl`.
fn archive_file(path: &std::path::Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n == "comments.jsonl" || n == "submissions.jsonl")
}

/// `--read reddit-archive=PATH`: an Arctic Shift pull of the operator's
/// items, a directory holding its two files.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "reddit-archive",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "input",
                path: "reddit-archive",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: Some(|input, settings| {
            Ok(sinex_source_core::binding::per_file(
                &SPEC,
                input,
                settings,
                false,
                archive_file,
            ))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::shared(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
