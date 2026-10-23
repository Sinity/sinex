//! SMS from the Samsung account export (`SMS/SMS_*.csv`), on `ExportArchive`
//! (`spec/port-lynchpin-sources.md` §7.4). Each row's `VALUE` cell is a JSON
//! string holding the message object (`_id, thread_id, address, date (ms),
//! body, type`: 1 received, 2 sent, 3 draft). Drafts are kept, marked as
//! such.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::communication::ConversationMessage;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "phone.samsung-sms",
    driver: Driver::ExportArchive,
    consumer: "text messages sent and received",
    class: Class::Open,
    parser: "samsung-sms",
    parser_version: 1,
    event_types: &["conversation.message"],
    framer: crate::Framer::export(frame, crate::drivers::export_archive::Framing::Csv),
};

pub struct SmsParser;

impl Parser for SmsParser {
    type Record<'a> = &'a [u8];
    /// The header row.
    type Carry = Vec<String>;
    type Payload = ConversationMessage;

    fn parse(
        &self,
        record: &&[u8],
        header: &Vec<String>,
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<ConversationMessage>, Vec<String>), RecordError> {
        let cells = fields::csv_cells(record)?;
        if header.is_empty() || cells.first().map(String::as_str) == Some("ITEM_DATA") {
            return Ok((vec![], cells));
        }
        let value = header
            .iter()
            .position(|h| h == "VALUE")
            .and_then(|i| cells.get(i))
            .ok_or_else(|| RecordError::new("row without a VALUE"))?;
        let inner: String = serde_json::from_str(value)
            .map_err(|e| RecordError::new(format!("VALUE is not a JSON string: {e}")))?;
        let m: Map<String, Value> = serde_json::from_str(&inner)
            .map_err(|e| RecordError::new(format!("VALUE does not hold a JSON object: {e}")))?;
        let text = |name: &str| fields::optional_text(&m, name);
        let ms: i64 = text("date")
            .and_then(|d| d.parse().ok())
            .ok_or_else(|| RecordError::new("message without a date"))?;
        let address = text("address").unwrap_or_else(|| "unknown".into());
        let kind = match text("type").as_deref() {
            Some("1") => "received",
            Some("2") => "sent",
            Some("3") => "draft",
            Some("4") => "outbox",
            Some("5") => "failed",
            Some("6") => "queued",
            _ => "unknown",
        };
        Ok((
            vec![ConversationMessage {
                service: "sms".into(),
                thread: format!("sms:{address}"),
                thread_name: text("thread_id").map(|t| format!("thread {t}")),
                participants: vec![address.clone()],
                sender: if kind == "received" {
                    address
                } else {
                    "me".into()
                },
                sent_at: fields::from_unix_ms(ms)?,
                text: text("body"),
                kind: Some(kind.into()),
                media: vec![],
                reactions: vec![],
                message_id: text("_id"),
                unsent_at: None,
            }],
            header.clone(),
        ))
    }
}

pub fn source() -> DeclaredSource<SmsParser> {
    DeclaredSource {
        spec: SPEC,
        parser: SmsParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Csv.frame(bytes, true)
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| {
            let name = m.rsplit('/').next().unwrap_or(m);
            name.starts_with("SMS_") && name.ends_with(".csv")
        },
        framing: Framing::Csv,
        part_bytes: PART_BYTES,
    }
}

/// `--source samsung-sms=PATH`: each Samsung SMS backup (`SMS_*.csv`) under PATH.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
    name: "samsung-sms",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        Ok(crate::binding::per_file(
            &SPEC,
            input,
            settings,
            true,
            |p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("SMS_") && n.ends_with(".csv"))
            },
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
