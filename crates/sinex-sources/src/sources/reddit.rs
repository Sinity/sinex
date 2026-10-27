//! Reddit GDPR export (`export_<user>_<date>.zip` of CSVs), on `ExportArchive`:
//! comments and posts become `post.published`, votes `vote.cast`, saved items
//! `bookmark.saved`, messages and chats `conversation.message`. The `ip`
//! columns are dropped as read; account metadata files stay in the material,
//! unread. Exports are cumulative, so an older one adds only older readings of
//! the same items.
use crate::sources::fields;
use serde::{Deserialize, Serialize};
use sinex_model::Class;
use sinex_model::event_types::communication::ConversationMessage;
use sinex_model::event_types::media::BookmarkSaved;
use sinex_model::event_types::social::{PostPublished, VoteCast};
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordContext, RecordError};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "social.reddit",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "what the operator wrote, voted on, saved and sent at Reddit",
    class: Class::Open,
    parser: "reddit-gdpr",
    parser_version: 1,
    event_types: &[
        "post.published",
        "vote.cast",
        "bookmark.saved",
        "conversation.message",
    ],
    framer: sinex_source_core::Framer::export(
        frame,
        sinex_source_core::drivers::export_archive::Framing::Csv,
    ),
};

/// The members read; the rest (account metadata) are kept, not parsed.
const MEMBERS: &[&str] = &[
    "comments.csv",
    "posts.csv",
    "comment_votes.csv",
    "post_votes.csv",
    "saved_comments.csv",
    "saved_posts.csv",
    "messages_archive.csv",
    "chat_history.csv",
];

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum RedditEvent {
        Post(PostPublished),
        Vote(VoteCast),
        Saved(BookmarkSaved),
        Message(ConversationMessage),
    }
}

/// The member the last row came from and its header row.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Carry {
    pub member: String,
    pub header: Vec<String>,
}

pub struct RedditParser;

impl Parser for RedditParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = RedditEvent;

    fn parse(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<RedditEvent>, Carry), RecordError> {
        let path = context.path().unwrap_or_default();
        let member = path.rsplit('/').next().unwrap_or(path).to_owned();
        let cells = fields::csv_cells(record)?;
        if carry.member != member || carry.header.is_empty() {
            return Ok((
                vec![],
                Carry {
                    member,
                    header: cells,
                },
            ));
        }
        let get = |name: &str| -> Option<String> {
            carry
                .header
                .iter()
                .position(|h| h == name)
                .and_then(|i| cells.get(i))
                .filter(|v| !v.is_empty())
                .cloned()
        };
        let need =
            |name: &str| get(name).ok_or_else(|| RecordError::new(format!("{member}: no {name}")));
        let time = |name: &str| get(name).map(|t| utc(&t)).transpose();
        let service = || "reddit".to_owned();
        let event = match member.as_str() {
            "comments.csv" | "posts.csv" => {
                let comment = member == "comments.csv";
                RedditEvent::Post(PostPublished {
                    service: service(),
                    kind: if comment { "comment" } else { "post" }.into(),
                    native_id: need("id")?,
                    created_at: time("date")?,
                    title: get("title"),
                    body: get("body"),
                    url: get("permalink"),
                    parent: if comment {
                        get("parent").or_else(|| get("link"))
                    } else {
                        None
                    },
                    community: get("subreddit"),
                    tags: vec![],
                    score: None,
                    extra: [
                        ("gildings", get("gildings")),
                        ("link", get("link")),
                        ("url", get("url")),
                        ("media", get("media")),
                    ]
                    .into_iter()
                    .filter_map(|(k, v)| v.map(|v| (k.to_owned(), serde_json::Value::String(v))))
                    .collect(),
                })
            }
            "comment_votes.csv" | "post_votes.csv" => {
                let direction = need("direction")?;
                RedditEvent::Vote(VoteCast {
                    service: service(),
                    target_kind: if member.starts_with("comment") {
                        "comment"
                    } else {
                        "post"
                    }
                    .into(),
                    target_id: need("id")?,
                    // `none` is a vote taken back; the export gives no time.
                    withdrawn_at: (direction == "none")
                        .then(|| sinex_model::Timestamp::from_unix_secs(0).ok())
                        .flatten(),
                    direction,
                    cast_at: None,
                    target_created_at: None,
                    target_title: None,
                    target_url: get("permalink"),
                    target_author: None,
                    extra: Default::default(),
                })
            }
            "saved_comments.csv" | "saved_posts.csv" => {
                let kind = if member.starts_with("saved_comments") {
                    "comment"
                } else {
                    "post"
                };
                RedditEvent::Saved(BookmarkSaved {
                    service: "reddit".into(),
                    native_id: format!("{kind}:{}", need("id")?),
                    url: need("permalink")?,
                    title: None,
                    note: None,
                    excerpt: None,
                    folder: Some(format!("saved {kind}s")),
                    tags: vec![],
                    created_at: None,
                    updated_at: None,
                    cover: None,
                    highlights: None,
                    favorite: None,
                    trashed_at: None,
                    extra: Default::default(),
                })
            }
            "messages_archive.csv" => {
                let from = get("from").unwrap_or_default();
                let to = get("to").unwrap_or_default();
                let mut participants = vec![from.clone(), to];
                participants.sort();
                participants.dedup();
                RedditEvent::Message(ConversationMessage {
                    service: service(),
                    thread: get("thread_id").unwrap_or_else(|| participants.join("|")),
                    thread_name: get("subject"),
                    participants,
                    sender: from,
                    sent_at: time("date")?
                        .ok_or_else(|| RecordError::new("message without a date"))?,
                    text: get("body"),
                    kind: Some("message".into()),
                    media: vec![],
                    reactions: vec![],
                    message_id: get("id"),
                    unsent_at: None,
                })
            }
            "chat_history.csv" => RedditEvent::Message(ConversationMessage {
                service: service(),
                thread: get("channel_url")
                    .or_else(|| get("channel_name"))
                    .unwrap_or_else(|| "chat".into()),
                thread_name: get("channel_name"),
                participants: vec![],
                sender: get("username").unwrap_or_default(),
                sent_at: time("created_at")?
                    .ok_or_else(|| RecordError::new("chat without a time"))?,
                text: get("message"),
                kind: get("conversation_type"),
                media: vec![],
                reactions: vec![],
                message_id: get("message_id"),
                unsent_at: None,
            }),
            _ => return Ok((vec![], carry.clone())),
        };
        Ok((vec![event], carry.clone()))
    }
}

/// `2014-08-27 00:59:46 UTC`.
fn utc(raw: &str) -> Result<sinex_model::Timestamp, RecordError> {
    let body = raw.trim().trim_end_matches(" UTC");
    let civil = jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S", body)
        .map_err(|e| RecordError::new(format!("invalid time {raw:?}: {e}")))?;
    let ts = civil
        .to_zoned(jiff::tz::TimeZone::UTC)
        .map_err(|e| RecordError::new(e.to_string()))?;
    sinex_model::Timestamp::from_unix_ns(ts.timestamp().as_nanosecond())
        .map_err(|_| RecordError::new("time out of range"))
}

pub fn source() -> DeclaredSource<RedditParser> {
    DeclaredSource {
        spec: SPEC,
        parser: RedditParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Csv.frame(bytes, true)
}

pub fn select(member: &str) -> bool {
    let name = member.rsplit('/').next().unwrap_or(member);
    MEMBERS.contains(&name)
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select,
        framing: Framing::Csv,
        part_bytes: PART_BYTES,
    }
}

/// `--read reddit=PATH`: each Reddit export ZIP in PATH.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "reddit",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "input",
                path: "reddit",
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
                |p| p.extension().is_some_and(|e| e == "zip"),
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
