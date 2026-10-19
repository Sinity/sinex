//! SMS from the Samsung account export (`SMS/SMS_*.csv`), on `ExportArchive`
//! (`spec/port-lynchpin-sources.md` §7.4). Each row's `VALUE` cell is a JSON
//! string holding the message object (`_id, thread_id, address, date (ms),
//! body, type`: 1 received, 2 sent, 3 draft). Drafts are kept, marked as
//! such.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::kinds::communication::ConversationMessage;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "phone.samsung-sms",
    driver: Driver::ExportArchive,
    consumer: "text messages sent and received",
    class: Class::Open,
    parser: "samsung-sms",
    parser_version: 1,
    event_types: &["conversation.message"],
    framer: frame,
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

pub fn source() -> Declared<SmsParser> {
    Declared {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_encoded_values() {
        let (_, header) = SmsParser
            .parse(
                &&b"\xef\xbb\xbfITEM_DATA,FILE_LIST,TIMESTAMP,SIZE,VALUE,CREATETIME,KEY\n"[..],
                &vec![],
            )
            .unwrap();
        let row = br#""",,1659525740863,723,"""{\""_id\"":\""639\"",\""thread_id\"":\""59\"",\""address\"":\""4860\"",\""date\"":\""1659525740863\"",\""type\"":\""1\"",\""body\"":\""Hi\\n\""}""",1,k
"#;
        let (out, _) = SmsParser.parse(&&row[..], &header).unwrap();
        let m = &out[0];
        assert_eq!(m.message_id.as_deref(), Some("639"));
        assert_eq!(m.sender, "4860");
        assert_eq!(m.kind.as_deref(), Some("received"));
        assert_eq!(m.text.as_deref(), Some("Hi\n"));
        assert_eq!(m.sent_at, fields::from_unix_ms(1_659_525_740_863).unwrap());
    }
}
