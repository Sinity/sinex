//! Facebook Messenger messages saved by `fbmessengerexport` into SQLite
//! (`messages` and `threads` tables), on `SqliteRows`. Unlike the GDPR export
//! it keeps Facebook's own IDs: each message has its ID, its thread's ID and
//! its author's numeric account ID, but no names. The same message in a GDPR
//! export is the same text at the same millisecond, which is how
//! `conversation.fusion` joins the two.
use super::fields;
use crate::drivers::sqlite_rows::SqliteRows;
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::communication::ConversationMessage;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "communication.messenger-sqlite",
    driver: Driver::SqliteRows,
    excisable: false,
    purpose: "conversations with people",
    class: Class::Open,
    parser: "fbmessengerexport",
    parser_version: 1,
    event_types: &["conversation.message"],
    framer: crate::Framer::incremental(crate::drivers::append_file::lines),
};

pub struct MessengerSqliteParser;

impl Parser for MessengerSqliteParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = ConversationMessage;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<ConversationMessage>, ()), RecordError> {
        // `data` is the message as the tool received it, as JSON text.
        let data: Map<String, Value> = match row.get("data") {
            Some(Value::String(text)) => serde_json::from_str(text)
                .map_err(|e| RecordError::new(format!("data is not a JSON object: {e}")))?,
            _ => Map::new(),
        };
        let sent_at = fields::from_unix_ms(fields::int(row, "timestamp")?)?;
        let unsent = data.get("unsent").and_then(Value::as_bool) == Some(true);
        let sender = fields::text(row, "author")?;
        Ok((
            vec![ConversationMessage {
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
            }],
            (),
        ))
    }
}

pub fn source() -> DeclaredSource<MessengerSqliteParser> {
    DeclaredSource {
        spec: SPEC,
        parser: MessengerSqliteParser,
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
        changes: crate::drivers::sqlite_rows::Changes::InPlace {
            mutable: "0",
            full_pass_secs: 86_400,
        },
    }
}

/// `--read messenger-sqlite=PATH`: an `fbmessengerexport` database.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "messenger-sqlite",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| Ok(crate::binding::single(&SPEC, input, settings))),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::rows(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
