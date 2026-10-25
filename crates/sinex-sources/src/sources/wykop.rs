//! Wykop account scrape (`wykop_*.jsonl`, one object per line with a `kind`),
//! on `ExportArchive`. The operator's entries, comments and links become
//! `post.published`; entries plused and links dug or buried become `vote.cast`.
//! The activity feed (`wykop_actions`) repeats the operator's own posts and
//! votes under the same identities, adds down-votes on entries and favourites
//! (`bookmark.saved`), and lists items the operator only commented on, which
//! yield nothing here because the comment files carry the comments. Tag
//! notifications become `notification.received` (the grouped listing names the
//! same notifications), and blacklisted users, tags and domains become
//! `account.setting`. The observer feeds of other people's content
//! (`wykop_observed_*`) are not read.
//!
//! Every `*_created_at` is zoneless and is Warsaw local time, as Wykop's own
//! frontend reads it; read as UTC it would be an hour or two early.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::event_types::media::BookmarkSaved;
use sinex_model::event_types::social::{
    AccountSetting, NotificationReceived, PostPublished, VoteCast,
};
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "social.wykop",
    driver: Driver::ExportArchive,
    consumer: "what the operator wrote and voted on at Wykop",
    class: Class::Open,
    parser: "wykop",
    parser_version: 2,
    event_types: &[
        "post.published",
        "vote.cast",
        "bookmark.saved",
        "notification.received",
        "account.setting",
    ],
    framer: crate::Framer::export(frame, crate::drivers::export_archive::Framing::Lines),
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum WykopEvent {
        Post(PostPublished),
        Vote(VoteCast),
        Saved(BookmarkSaved),
        Notification(NotificationReceived),
        Setting(AccountSetting),
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
        _context: &crate::RecordContext<'_>,
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
        match kind.as_str() {
            "action" => return Ok((action(row)?, ())),
            "notification" | "notification_group_item" => {
                return Ok((vec![notification(row)?], ()));
            }
            "setting_item" => return Ok((vec![setting(row)?], ())),
            _ => {}
        }
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

/// The operator's account at Wykop; the activity feed lists other people's
/// items too, and only the operator's own are their posts.
const OPERATOR_ACCOUNTS: &[&str] = &["Sinity", "Ezoman"];

fn object<'a>(row: &'a Map<String, Value>, name: &str) -> Option<&'a Map<String, Value>> {
    row.get(name).and_then(Value::as_object)
}

fn id_text(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

fn item_url(resource: &str, id: &str, slug: Option<&str>) -> String {
    let path = if resource == "link" { "link" } else { "wpis" };
    match slug {
        Some(s) => format!("https://wykop.pl/{path}/{id}/{s}"),
        None => format!("https://wykop.pl/{path}/{id}"),
    }
}

fn optional_time(row: &Map<String, Value>, name: &str) -> Result<Option<Timestamp>, RecordError> {
    fields::optional_text(row, name)
        .map(|t| warsaw(&t))
        .transpose()
}

/// One activity-feed item: the operator's own post, a vote, a favourite, or an
/// item the operator commented on (nothing: the comment files carry it).
fn action(row: &Map<String, Value>) -> Result<Vec<WykopEvent>, RecordError> {
    let resource = fields::text(row, "resource")?;
    let id = id_text(row.get("id")).ok_or_else(|| RecordError::new("action without id"))?;
    let slug = fields::optional_text(row, "slug");
    let url = item_url(&resource, &id, slug.as_deref());
    let created = optional_time(row, "created_at")?;
    let author = object(row, "author")
        .and_then(|a| a.get("username"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let tags: Vec<String> = row
        .get("tags")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|t| t.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let title = fields::optional_text(row, "title");
    let votes = object(row, "votes");
    let up = votes.and_then(|v| v.get("up")).and_then(Value::as_i64);
    let down = votes.and_then(|v| v.get("down")).and_then(Value::as_i64);
    let mut extra = Map::new();
    for key in [
        "source",
        "media",
        "parent",
        "published_at",
        "status",
        "deleted",
        "archive",
    ] {
        if let Some(v) = row.get(key).filter(|v| !v.is_null()) {
            extra.insert(key.into(), v.clone());
        }
    }
    let mut out = Vec::new();
    if author
        .as_deref()
        .is_some_and(|a| OPERATOR_ACCOUNTS.contains(&a))
    {
        out.push(WykopEvent::Post(PostPublished {
            service: "wykop".into(),
            kind: if resource == "link" {
                "link".into()
            } else {
                "entry".into()
            },
            native_id: id.clone(),
            created_at: created,
            title: title.clone(),
            body: fields::optional_text(row, "content"),
            url: Some(url.clone()),
            parent: None,
            community: None,
            tags: tags.clone(),
            score: up.zip(down).map(|(u, d)| u - d),
            extra: extra.clone(),
        }));
    }
    match row.get("voted").and_then(Value::as_i64) {
        Some(v) if v != 0 => out.push(WykopEvent::Vote(VoteCast {
            service: "wykop".into(),
            target_kind: resource.clone(),
            target_id: id.clone(),
            direction: if v > 0 { "up" } else { "down" }.into(),
            cast_at: None,
            target_created_at: created,
            target_title: title.clone(),
            target_url: Some(url.clone()),
            target_author: author.clone(),
            withdrawn_at: None,
            extra: extra.clone(),
        })),
        _ => {}
    }
    if row.get("favourite").and_then(Value::as_bool) == Some(true) {
        out.push(WykopEvent::Saved(BookmarkSaved {
            service: "wykop".into(),
            native_id: format!("{resource}:{id}"),
            url,
            title,
            note: None,
            excerpt: fields::optional_text(row, "content"),
            folder: None,
            tags,
            created_at: None,
            updated_at: None,
            cover: None,
            highlights: None,
            favorite: Some(true),
            trashed_at: None,
            extra,
        }));
    }
    Ok(out)
}

/// A tag notification: a new entry or link under an observed tag.
fn notification(row: &Map<String, Value>) -> Result<WykopEvent, RecordError> {
    let id = id_text(row.get("id")).ok_or_else(|| RecordError::new("notification without id"))?;
    let (target, target_url) = ["entry", "link"]
        .iter()
        .find_map(|r| {
            let o = object(row, r)?;
            let tid = id_text(o.get("id"))?;
            let slug = o.get("slug").and_then(Value::as_str);
            Some((Some(format!("{r}:{tid}")), Some(item_url(r, &tid, slug))))
        })
        .unwrap_or((None, None));
    let mut extra = Map::new();
    for key in [
        "group_id",
        "group_count",
        "group_updated_at",
        "show_as_group",
        "scope",
    ] {
        if let Some(v) = row.get(key).filter(|v| !v.is_null()) {
            extra.insert(key.into(), v.clone());
        }
    }
    Ok(WykopEvent::Notification(NotificationReceived {
        service: "wykop".into(),
        native_id: id,
        kind: fields::text(row, "type")?,
        created_at: optional_time(row, "created_at")?,
        read: row.get("read").and_then(Value::as_i64).map(|r| r != 0),
        target,
        target_url,
        actor: object(row, "user")
            .and_then(|u| u.get("username"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        channel: object(row, "tag")
            .and_then(|t| t.get("name"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        extra,
    }))
}

/// One blacklisted user, tag or domain. The scrape stamps these with a
/// placeholder 1970 time, so no time is read.
fn setting(row: &Map<String, Value>) -> Result<WykopEvent, RecordError> {
    let endpoint = fields::text(row, "endpoint")?;
    let setting = endpoint
        .strip_prefix("settings/")
        .unwrap_or(&endpoint)
        .replace("blacklists", "blacklist");
    let value = ["username", "name", "domain", "url"]
        .iter()
        .find_map(|k| fields::optional_text(row, k))
        .ok_or_else(|| RecordError::new(format!("{endpoint} item without a value")))?;
    Ok(WykopEvent::Setting(AccountSetting {
        service: "wykop".into(),
        setting,
        value,
        extra: Map::new(),
    }))
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

pub fn source() -> DeclaredSource<WykopParser> {
    DeclaredSource {
        spec: SPEC,
        parser: WykopParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Lines.frame(bytes, true)
}

/// The operator's own activity files; the observer feeds are other people's.
fn is_own(path: &std::path::Path) -> bool {
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
        join: None,
    }
}

/// `--source wykop=PATH`: each of the account's own Wykop export files in PATH.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
    name: "wykop",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        Ok(crate::binding::per_file(
            &SPEC, input, settings, false, is_own,
        ))
    }),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::stateless(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
