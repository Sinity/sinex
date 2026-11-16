//! SMS from the Samsung account export (`SMS/SMS_*.csv`), on `ExportArchive`.
//! Each row's `VALUE` cell is a JSON string holding the message object (`_id,
//! thread_id, address, date (ms), body, type`: 1 received, 2 sent, 3 draft).
//! Drafts are kept, marked as such.
use crate::sources::fields;
use serde_json::{Map, Value};
use sinex_model::event_types::communication::ConversationMessage;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Framer, Parser, RecordError, RecordResult, binding};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "phone.samsung-sms",
    origin: Origin::Input(Shape::ExportArchive),
    expect: Expectation::Export,
    purpose: "text messages sent and received",
    parser: "samsung-sms",
    event_types: &["conversation.message"],
    framer: Framer::export(frame, Framing::Csv),
    ..SourceSpec::DEFAULT
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
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<ConversationMessage>, Vec<String>), RecordError> {
        let cells = fields::csv_cells(record)?;
        if header.is_empty() || cells.first().map(String::as_str) == Some("ITEM_DATA") {
            return Ok((vec![], cells));
        }
        let value = header
            .iter()
            .position(|h| h == "VALUE")
            .and_then(|i| cells.get(i))
            .record("row without a VALUE")?;
        let inner: String = serde_json::from_str(value).record("VALUE is not a JSON string")?;
        let m: Map<String, Value> =
            serde_json::from_str(&inner).record("VALUE does not hold a JSON object")?;
        let text = |name: &str| fields::optional_text(&m, name);
        let ms: i64 = text("date")
            .and_then(|d| d.parse().ok())
            .record("message without a date")?;
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

/// `stage --parser samsung-sms PATH`: each Samsung SMS backup (`SMS_*.csv`) under PATH.
super::input_format! { pub FORMAT = "samsung-sms" {
    specs: [SPEC],
    parser: |_b| SmsParser,
    discovery: DiscoverySpec::system(&[Location::new("input", "samsung-sms")]),
    plan: |input, settings| {
        Ok(binding::per_file(&SPEC, input, settings, true, |p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("SMS_") && n.ends_with(".csv"))
        }))
    },
    reader: shared(driver),
} }

#[cfg(test)]
mod tests;
