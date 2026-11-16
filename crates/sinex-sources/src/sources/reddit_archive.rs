//! A public archive's copy of the operator's Reddit posts and comments
//! (Arctic Shift's `comments.jsonl` and `submissions.jsonl`, one Pushshift
//! object per line), on `ExportArchive`. The archive copied each item when
//! Reddit served it, so it holds items the operator later deleted and items
//! newer than the last GDPR export. Its items carry Reddit's own IDs, which
//! the GDPR export writes too; the two stay separate records.
use super::fields;
use serde_json::{Map, Value};
use sinex_model::event_types::social::PostPublished;
use sinex_model::json::JsonExt;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES, frame_lines};
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Framer, RecordError, RecordResult, Stateless, StatelessParser, binding};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "social.reddit-archive",
    origin: Origin::Input(Shape::ExportArchive),
    expect: Expectation::Export,
    purpose: "what the operator wrote at Reddit, including what the GDPR export lacks",
    parser: "reddit-archive",
    event_types: &["post.published"],
    framer: Framer::export(frame_lines, Framing::Lines),
    ..SourceSpec::DEFAULT
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

impl StatelessParser for RedditArchiveParser {
    type Record<'a> = Map<String, Value>;
    type Payload = PostPublished;

    fn parse(
        &self,
        item: &Map<String, Value>,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<Vec<PostPublished>, RecordError> {
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
        .record("created_utc is not a number")?;
        let text = |name: &str| fields::optional_text(item, name).filter(|t| !t.is_empty());
        Ok(vec![PostPublished {
            service: "reddit".into(),
            kind: if comment { "comment" } else { "post" }.into(),
            native_id: fields::text(item, "id")?,
            created_at: Some(
                sinex_model::Timestamp::from_unix_secs(created)
                    .record_with(|| format!("created_utc {created} out of range"))?,
            ),
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
            score: item.i64("score"),
            extra: fields::extra(item, KNOWN),
        }])
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

super::input_format! {
    /// `stage --parser reddit-archive PATH`: an Arctic Shift pull of the operator's
    /// items, a directory holding its two files.
    pub FORMAT = "reddit-archive" {
    specs: [SPEC],
    parser: |_b| Stateless(RedditArchiveParser),
    discovery: DiscoverySpec::system(&[Location::new("input", "reddit-archive")]),
    plan: |input, settings| {
        Ok(binding::per_file(
            &SPEC,
            input,
            settings,
            false,
            archive_file,
        ))
    },
    reader: shared(driver),
} }

#[cfg(test)]
mod tests;
