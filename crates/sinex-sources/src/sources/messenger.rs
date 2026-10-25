//! Facebook Messenger exports (`*.json`, one thread per file), on
//! `ExportArchive`. Two shapes: the GDPR export (`participants: [name]`,
//! `threadName`, messages with `senderName`, `timestamp` ms, `text`,
//! `isUnsent`) and Facebook's older download (`participants: [{name}]`,
//! `title`, messages with `sender_name`, `timestamp_ms`, `content`), whose text
//! is UTF-8 written as Latin-1 escapes and is repaired here.
//!
//! File names carry an ordinal that changes between exports, so a thread is
//! its sorted participants and a message is the thread, time and sender.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::communication::ConversationMessage;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "facebook-messenger-thread",
    driver: Driver::ExportArchive,
    excisable: false,
    consumer: "conversations with people",
    class: Class::Open,
    parser: "facebook-messenger",
    parser_version: 1,
    event_types: &["conversation.message"],
    framer: crate::Framer::export(frame, crate::drivers::export_archive::Framing::Whole),
};

pub struct MessengerParser;

impl Parser for MessengerParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = ConversationMessage;

    fn parse(
        &self,
        doc: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<ConversationMessage>, ()), RecordError> {
        let messages = doc
            .get("messages")
            .and_then(Value::as_array)
            .ok_or_else(|| RecordError::new("no messages array: not a Messenger thread"))?;
        let older_export = messages
            .first()
            .is_some_and(|m| m.get("sender_name").is_some() || m.get("timestamp_ms").is_some())
            || doc.get("title").is_some();
        let fix = |s: &str| {
            if older_export {
                repair(s)
            } else {
                s.to_owned()
            }
        };
        let mut participants: Vec<String> = doc
            .get("participants")
            .and_then(Value::as_array)
            .map(|ps| {
                ps.iter()
                    .filter_map(|p| p.as_str().or_else(|| p.get("name").and_then(Value::as_str)))
                    .map(fix)
                    .collect()
            })
            .unwrap_or_default();
        participants.sort();
        participants.dedup();
        if participants.is_empty() {
            return Err(RecordError::new("thread without participants"));
        }
        let thread = participants.join("|");
        let thread_name = doc
            .get("threadName")
            .or_else(|| doc.get("title"))
            .and_then(Value::as_str)
            .map(fix);
        let mut out = Vec::with_capacity(messages.len());
        for m in messages {
            let m = m
                .as_object()
                .ok_or_else(|| RecordError::new("a message is not an object"))?;
            let text_of = |a: &str, b: &str| {
                fields::optional_text(m, a)
                    .or_else(|| fields::optional_text(m, b))
                    .map(|s| fix(&s))
            };
            let sender = text_of("senderName", "sender_name")
                .ok_or_else(|| RecordError::new("message without a sender"))?;
            let ms = m
                .get("timestamp")
                .or_else(|| m.get("timestamp_ms"))
                .and_then(Value::as_i64)
                .ok_or_else(|| RecordError::new("message without a time"))?;
            let sent_at = fields::from_unix_ms(ms)?;
            let unsent = m
                .get("isUnsent")
                .or_else(|| m.get("is_unsent"))
                .and_then(Value::as_bool)
                == Some(true);
            let mut media = Vec::new();
            for key in [
                "media",
                "photos",
                "videos",
                "audio_files",
                "files",
                "gifs",
                "sticker",
            ] {
                let items = match m.get(key) {
                    Some(Value::Array(a)) => a.clone(),
                    Some(one @ Value::Object(_)) => vec![one.clone()],
                    _ => continue,
                };
                media.extend(items.iter().filter_map(|i| {
                    i.as_str()
                        .or_else(|| i.get("uri").and_then(Value::as_str))
                        .map(str::to_owned)
                }));
            }
            out.push(ConversationMessage {
                service: "facebook-messenger".into(),
                thread: thread.clone(),
                thread_name: thread_name.clone(),
                participants: participants.clone(),
                sender,
                sent_at,
                text: text_of("text", "content"),
                kind: fields::optional_text(m, "type"),
                media,
                reactions: m
                    .get("reactions")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
                message_id: fields::optional_text(m, "messageId"),
                unsent_at: unsent.then_some(sent_at),
            });
        }
        Ok((out, ()))
    }
}

/// Facebook's older export writes each UTF-8 byte as its own `\u00XX` code
/// point. Text made only of such code points is re-read as UTF-8; anything
/// else is left as it is.
fn repair(s: &str) -> String {
    if s.chars().all(|c| (c as u32) < 0x100) {
        let bytes: Vec<u8> = s.chars().map(|c| c as u8).collect();
        if let Ok(fixed) = String::from_utf8(bytes) {
            return fixed;
        }
    }
    s.to_owned()
}

pub fn source() -> DeclaredSource<MessengerParser> {
    DeclaredSource {
        spec: SPEC,
        parser: MessengerParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Whole.frame(bytes, true)
}

pub fn select(member: &str) -> bool {
    member.ends_with(".json") && !member.contains("/media/") && !member.starts_with("media/")
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select,
        framing: Framing::Whole,
        part_bytes: PART_BYTES,
    }
}

/// `--source messenger=PATH`: each Messenger export in PATH.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
    name: "messenger",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        Ok(crate::binding::per_file(
            &SPEC,
            input,
            settings,
            false,
            crate::binding::any,
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
