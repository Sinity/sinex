//! Facebook Messenger messages saved by `fbmessengerexport` into SQLite
//! (`messages` and `threads` tables), on `SqliteRows`. Unlike the GDPR export
//! it keeps Facebook's own IDs: each message has its ID, its thread's ID and
//! its author's numeric account ID, but no names. The same message in a GDPR
//! export is a separate record with the same text at the same millisecond.
use super::fields;
use serde_json::{Map, Value};
use sinex_model::event_types::communication::ConversationMessage;
use sinex_model::json::JsonExt;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::sqlite_rows::SqliteRows;
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Framer, RecordError, RecordResult, Stateless, StatelessParser};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "communication.messenger-sqlite",
    origin: Origin::Input(Shape::SqliteRows),
    expect: Expectation::Export,
    purpose: "conversations with people",
    parser: "fbmessengerexport",
    event_types: &["conversation.message"],
    framer: Framer::LINES,
    ..SourceSpec::DEFAULT
};

pub struct MessengerSqliteParser;

impl StatelessParser for MessengerSqliteParser {
    type Record<'a> = Map<String, Value>;
    type Payload = ConversationMessage;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<Vec<ConversationMessage>, RecordError> {
        // `data` is the message as the tool received it, as JSON text.
        let data: Map<String, Value> = match row.get("data") {
            Some(Value::String(text)) => {
                serde_json::from_str(text).record("data is not a JSON object")?
            }
            _ => Map::new(),
        };
        let sent_at = fields::from_unix_ms(fields::int(row, "timestamp")?)?;
        let unsent = data.bool("unsent") == Some(true);
        let sender = fields::text(row, "author")?;
        Ok(vec![ConversationMessage {
            service: "facebook-messenger".into(),
            thread: fields::text(row, "thread_id")?,
            thread_name: fields::optional_text(row, "thread_name"),
            participants: vec![],
            sender,
            sent_at,
            text: fields::optional_text(row, "text").filter(|t| !t.is_empty()),
            kind: None,
            media: vec![],
            reactions: vec![],
            message_id: Some(fields::text(row, "uid")?),
            unsent_at: unsent.then_some(sent_at),
        }])
    }
}

pub fn driver(path: PathBuf, host: String) -> SqliteRows {
    SqliteRows {
        path,
        host,
        table: "messages",
        from: Some(
            "(SELECT m.rowid AS rowid, m.uid, m.thread_id, m.timestamp, m.author, m.text, \
             m.data, t.name AS thread_name \
             FROM messages m LEFT JOIN threads t ON t.uid = m.thread_id)",
        ),
        required: &["uid", "thread_id", "timestamp", "author", "text", "data"],
        changes: sinex_source_core::drivers::sqlite_rows::Changes::InPlace {
            mutable: "0",
            full_pass_secs: 86_400,
        },
    }
}

super::input_format! {
    /// `stage --parser messenger-sqlite PATH`: an `fbmessengerexport` database.
    pub FORMAT = "messenger-sqlite" {
    specs: [SPEC],
    parser: |_b| Stateless(MessengerSqliteParser),
    discovery: DiscoverySpec::system(&[Location::new("input", "messenger-sqlite")]),
    plan: single,
    reader: rows(driver),
} }

#[cfg(test)]
mod tests;
