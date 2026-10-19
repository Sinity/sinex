//! Wykop account scrape (`wykop_*.jsonl`, one object per line with a `kind`),
//! on `ExportArchive` (`spec/port-lynchpin-sources.md` §7.7). The operator's
//! entries, comments and links become `post.published`; entries plused and
//! links dug or buried become `vote.cast`. The observer feeds of other
//! people's content (`wykop_observed_*`) are not read.
//!
//! Every `*_created_at` is zoneless and is Warsaw local time, as Wykop's own
//! frontend reads it (the predecessor read it as UTC, an hour or two early).
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::kinds::social::{PostPublished, VoteCast};
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "social.wykop",
    driver: Driver::ExportArchive,
    consumer: "what the operator wrote and voted on at Wykop",
    class: Class::Open,
    parser: "wykop",
    parser_version: 1,
    event_types: &["post.published", "vote.cast"],
    framer: frame,
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum WykopEvent {
        Post(PostPublished),
        Vote(VoteCast),
    }
}

/// Fields read into the events; scrape bookkeeping (`page`, `platform`,
/// `username`) is dropped, anything else is kept.
const KNOWN: &[&str] = &[
    "kind",
    "page",
    "platform",
    "username",
    "entry_id",
    "entry_url",
    "entry_created_at",
    "entry_author",
    "entry_content",
    "entry_tags",
    "votes_score",
    "comment_id",
    "comment_content",
    "comment_created_at",
    "comment_rating",
    "comment_url",
    "link_id",
    "link_title",
    "link_url",
    "link_created_at",
    "link_tags",
    "link_slug",
];

pub struct WykopParser;

impl Parser for WykopParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = WykopEvent;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<WykopEvent>, ()), RecordError> {
        let kind = fields::text(row, "kind")?;
        let id = |name: &str| -> Result<String, RecordError> {
            match row.get(name) {
                Some(Value::Number(n)) => Ok(n.to_string()),
                Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
                other => Err(RecordError::new(format!("{name} missing: {other:?}"))),
            }
        };
        let at = |name: &str| {
            fields::optional_text(row, name)
                .map(|t| warsaw(&t))
                .transpose()
        };
        let tags = |name: &str| -> Vec<String> {
            row.get(name)
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|t| t.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        let text = |name: &str| fields::optional_text(row, name);
        let int = |name: &str| row.get(name).and_then(Value::as_i64);
        let extra = fields::extra(row, KNOWN);
        let post = |kind: &str, native_id, created_at, title, body, url, parent, tags, score| {
            WykopEvent::Post(PostPublished {
                service: "wykop".into(),
                kind: kind.into(),
                native_id,
                created_at,
                title,
                body,
                url,
                parent,
                community: None,
                tags,
                score,
                extra: extra.clone(),
            })
        };
        let vote = |target_kind: &str, target_id, direction: &str, created, title, url, author| {
            WykopEvent::Vote(VoteCast {
                service: "wykop".into(),
                target_kind: target_kind.into(),
                target_id,
                direction: direction.into(),
                cast_at: None,
                target_created_at: created,
                target_title: title,
                target_url: url,
                target_author: author,
                withdrawn_at: None,
                extra: extra.clone(),
            })
        };
        let event = match kind.as_str() {
            "entry" => post(
                "entry",
                id("entry_id")?,
                at("entry_created_at")?,
                None,
                text("entry_content"),
                text("entry_url"),
                None,
                tags("entry_tags"),
                int("votes_score"),
            ),
            "entry_comment" => post(
                "entry_comment",
                id("comment_id")?,
                at("comment_created_at")?,
                None,
                text("comment_content"),
                text("comment_url"),
                Some(format!("entry:{}", id("entry_id")?)),
                vec![],
                int("comment_rating"),
            ),
            "link_comment" => post(
                "link_comment",
                id("comment_id")?,
                at("comment_created_at")?,
                text("link_title"),
                text("comment_content"),
                text("comment_url"),
                Some(format!("link:{}", id("link_id")?)),
                tags("link_tags"),
                int("comment_rating"),
            ),
            "link_added" | "link_opublikowane" | "link_powiazane" => post(
                match kind.as_str() {
                    "link_powiazane" => "related_link",
                    _ => "link",
                },
                id("link_id")?,
                at("link_created_at")?,
                text("link_title"),
                None,
                text("link_url"),
                None,
                tags("link_tags"),
                None,
            ),
            "entry_plusowane" => vote(
                "entry",
                id("entry_id")?,
                "up",
                at("entry_created_at")?,
                None,
                text("entry_url"),
                text("entry_author").map(|a| a.split_whitespace().collect::<Vec<_>>().join(" ")),
            ),
            "link_wykopane" | "link_zakopane" => vote(
                "link",
                id("link_id")?,
                if kind == "link_wykopane" {
                    "up"
                } else {
                    "down"
                },
                at("link_created_at")?,
                text("link_title"),
                text("link_url"),
                None,
            ),
            other => return Err(RecordError::new(format!("unknown Wykop kind {other:?}"))),
        };
        Ok((vec![event], ()))
    }
}

/// `YYYY-MM-DD HH:MM:SS`, Warsaw local time.
fn warsaw(raw: &str) -> Result<Timestamp, RecordError> {
    let civil = jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S", raw.trim())
        .map_err(|e| RecordError::new(format!("invalid time {raw:?}: {e}")))?;
    let tz = jiff::tz::TimeZone::get("Europe/Warsaw")
        .map_err(|e| RecordError::new(format!("Europe/Warsaw: {e}")))?;
    let zoned = tz
        .to_ambiguous_zoned(civil)
        .compatible()
        .map_err(|e| RecordError::new(format!("{raw:?} in Warsaw: {e}")))?;
    Timestamp::from_unix_ns(zoned.timestamp().as_nanosecond())
        .map_err(|_| RecordError::new("time out of range"))
}

pub fn source() -> Declared<WykopParser> {
    Declared {
        spec: SPEC,
        parser: WykopParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Lines.frame(bytes, true)
}

/// The operator's own activity files; the observer feeds are other people's.
pub fn is_own(path: &std::path::Path) -> bool {
    path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
        n.starts_with("wykop_") && n.ends_with(".jsonl") && !n.contains("observed")
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn one(v: Value) -> WykopEvent {
        WykopParser
            .parse(v.as_object().unwrap(), &())
            .unwrap()
            .0
            .remove(0)
    }

    #[test]
    fn entries_are_warsaw_time_not_utc() {
        let WykopEvent::Post(p) = one(json!({"kind": "entry", "entry_id": 76315507,
            "entry_created_at": "2024-05-18 06:53:25", "entry_content": "x #tag",
            "entry_tags": ["tag"], "votes_score": 0, "page": 1, "platform": "wykop"}))
        else {
            panic!()
        };
        // 06:53:25 CEST is 04:53:25 UTC.
        assert_eq!(p.created_at, fields::rfc3339("2024-05-18T04:53:25Z").ok());
        assert_eq!(p.native_id, "76315507");
        assert_eq!(p.tags, vec!["tag"]);
        assert!(!p.extra.contains_key("page"));
    }

    #[test]
    fn votes_have_no_time_of_their_own() {
        let WykopEvent::Vote(v) = one(json!({"kind": "link_zakopane", "link_id": 7,
            "link_created_at": "2024-01-08 17:14:47", "link_title": "t"}))
        else {
            panic!()
        };
        assert_eq!(v.direction, "down");
        assert_eq!(v.cast_at, None);
        assert_eq!(
            v.target_created_at,
            fields::rfc3339("2024-01-08T16:14:47Z").ok(),
            "CET in winter"
        );
        assert!(
            WykopParser
                .parse(json!({"kind": "nope"}).as_object().unwrap(), &())
                .is_err()
        );
    }
}
