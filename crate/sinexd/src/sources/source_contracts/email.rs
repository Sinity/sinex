//! Email capture source — `email.mailbox` (#1469).
//!
//! The first runnable mode is staged RFC822/`.eml` material. Live Gmail/IMAP
//! modes stay represented by issue scope, not by this parser.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sinex_macros::SourceMeta;
use sinex_primitives::{
    domain::{EventSource, EventType},
    events::{
        EventPayload,
        payloads::email::{EmailMessageReceivedPayload, EmailMessageSentPayload},
    },
    parser::{
        InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
        ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
    },
    privacy::{ProcessingContext, SensitivityHint},
    source_contracts::{
        AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
        RetentionPolicy, RunnerPack, RuntimeShape,
    },
    temporal::Timestamp,
};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmailMailboxParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "email.mailbox",
    namespace = "email",
    event_source = "email",
    event_type = "email.message.received",
    event_types = "email.message.sent",
    adapter = "FileContentDropAdapter",
    implementation = "staged-parser",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(message_id, folder)"),
    access_scope = AccessScope::StagedExport,
    capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.check, operation:email.mailbox.sync, operation:email.mailbox.pause, operation:email.mailbox.resume, operation:email.mailbox.inspect, operation:email.mailbox.replay",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Scheduled,
    binding(
        subject = "source:email.mailbox.sent",
        event_type = "email.message.sent",
        proposed = true
    )
)]
pub struct EmailMailboxParser;

#[async_trait]
impl MaterialParser for EmailMailboxParser {
    type Config = EmailMailboxParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("email-mailbox-rfc822"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::FileDrop],
            source_id: SourceId::from_static("email.mailbox"),
            declared_event_types: vec![
                (
                    EventSource::from_static("email"),
                    EventType::from_static("email.message.received"),
                ),
                (
                    EventSource::from_static("email"),
                    EventType::from_static("email.message.sent"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Document, ProcessingContext::Metadata],
            sensitivity_hints: vec![
                SensitivityHint::FreeText,
                SensitivityHint::PotentiallySensitive,
            ],
            description: "Parses staged RFC822/.eml material into email message observations."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let parsed = parse_rfc822(&record)?;
        let event_kind = EmailEventKind::from_record(&record);
        let ts_orig = parsed.date.unwrap_or(ctx.acquisition_time);
        let timing = parsed.date.map_or(TimingEvidence::StagedAtFallback, |_| {
            TimingEvidence::Intrinsic {
                field: "Date".into(),
                confidence: TimingConfidence::Intrinsic,
            }
        });
        let source_file = record
            .logical_path
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let folder = folder_from_record(&record);
        let raw_material_id = record.material_id.to_uuid().to_string();
        let occurrence_key = occurrence_key(
            parsed.message_id.as_deref(),
            folder.as_deref(),
            &source_file,
            &record.anchor,
            &raw_material_id,
        );

        let (event_type, payload) = match event_kind {
            EmailEventKind::Received => {
                let payload = EmailMessageReceivedPayload {
                    message_id: parsed.message_id,
                    date: parsed.date,
                    from: parsed.from,
                    to: parsed.to,
                    cc: parsed.cc,
                    bcc: parsed.bcc,
                    subject: parsed.subject,
                    in_reply_to: parsed.in_reply_to,
                    references: parsed.references,
                    list_id: parsed.list_id,
                    folder,
                    source_file,
                    raw_material_id,
                    size_bytes: record.bytes.len() as u64,
                    body_bytes: parsed.body_bytes,
                    attachment_count: parsed.attachment_count,
                };
                (
                    payload.event_type(),
                    serde_json::to_value(&payload).map_err(|error| {
                        ParserError::Parse(format!(
                            "failed to serialize EmailMessageReceivedPayload: {error}"
                        ))
                    })?,
                )
            }
            EmailEventKind::Sent => {
                let payload = EmailMessageSentPayload {
                    message_id: parsed.message_id,
                    date: parsed.date,
                    from: parsed.from,
                    to: parsed.to,
                    cc: parsed.cc,
                    bcc: parsed.bcc,
                    subject: parsed.subject,
                    in_reply_to: parsed.in_reply_to,
                    references: parsed.references,
                    list_id: parsed.list_id,
                    folder,
                    source_file,
                    raw_material_id,
                    size_bytes: record.bytes.len() as u64,
                    body_bytes: parsed.body_bytes,
                    attachment_count: parsed.attachment_count,
                };
                (
                    payload.event_type(),
                    serde_json::to_value(&payload).map_err(|error| {
                        ParserError::Parse(format!(
                            "failed to serialize EmailMessageSentPayload: {error}"
                        ))
                    })?,
                )
            }
        };

        Ok(vec![
            ParsedEventIntent::builder()
                .source_id(SourceId::from_static("email.mailbox"))
                .parser_id(ParserId::from_static("email-mailbox-rfc822"))
                .parser_version("1.0.0")
                .event_source(EventSource::from_static("email"))
                .event_type(event_type)
                .payload(payload)
                .ts_orig(ts_orig)
                .timing(timing)
                .anchor(record.anchor)
                .occurrence_key(occurrence_key)
                .privacy_context(ProcessingContext::Document)
                .build(),
        ])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmailEventKind {
    Received,
    Sent,
}

impl EmailEventKind {
    fn from_record(record: &SourceRecord) -> Self {
        if record
            .metadata
            .get("direction")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|direction| direction.eq_ignore_ascii_case("sent"))
        {
            return Self::Sent;
        }
        let Some(path) = record.logical_path.as_ref() else {
            return Self::Received;
        };
        let lower = path.as_str().to_ascii_lowercase();
        if lower.contains("/sent/") || lower.contains("/sent.") || lower.contains("/outbox/") {
            Self::Sent
        } else {
            Self::Received
        }
    }
}

#[derive(Debug)]
struct ParsedEmail {
    message_id: Option<String>,
    date: Option<Timestamp>,
    from: Vec<String>,
    to: Vec<String>,
    cc: Vec<String>,
    bcc: Vec<String>,
    subject: Option<String>,
    in_reply_to: Option<String>,
    references: Vec<String>,
    list_id: Option<String>,
    body_bytes: u64,
    attachment_count: u32,
}

fn parse_rfc822(record: &SourceRecord) -> ParserResult<ParsedEmail> {
    let text = std::str::from_utf8(&record.bytes).map_err(|error| {
        ParserError::Parse(format!("email RFC822 material is not UTF-8: {error}"))
    })?;
    let (headers, body) = split_headers_body(text);
    let headers = parse_headers(headers);

    Ok(ParsedEmail {
        message_id: header(&headers, "message-id").and_then(message_id_token),
        date: header(&headers, "date").and_then(parse_rfc822_date),
        from: header(&headers, "from").map_or_else(Vec::new, address_list),
        to: header(&headers, "to").map_or_else(Vec::new, address_list),
        cc: header(&headers, "cc").map_or_else(Vec::new, address_list),
        bcc: header(&headers, "bcc").map_or_else(Vec::new, address_list),
        subject: header(&headers, "subject").map(str::to_string),
        in_reply_to: header(&headers, "in-reply-to").and_then(message_id_token),
        references: header(&headers, "references").map_or_else(Vec::new, references_list),
        list_id: header(&headers, "list-id").map(str::to_string),
        body_bytes: body.as_bytes().len() as u64,
        attachment_count: attachment_count(text),
    })
}

fn split_headers_body(text: &str) -> (&str, &str) {
    if let Some((headers, body)) = text.split_once("\r\n\r\n") {
        (headers, body)
    } else if let Some((headers, body)) = text.split_once("\n\n") {
        (headers, body)
    } else {
        (text, "")
    }
}

fn parse_headers(headers: &str) -> Vec<(String, String)> {
    let mut parsed = Vec::new();
    let mut current_name: Option<String> = None;
    let mut current_value = String::new();

    for line in headers.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if !current_value.is_empty() {
                current_value.push(' ');
            }
            current_value.push_str(line.trim());
            continue;
        }

        if let Some(name) = current_name.take() {
            parsed.push((name, current_value.trim().to_string()));
            current_value.clear();
        }

        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        current_name = Some(name.trim().to_ascii_lowercase());
        current_value.push_str(value.trim());
    }

    if let Some(name) = current_name {
        parsed.push((name, current_value.trim().to_string()));
    }

    parsed
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name == name)
        .map(|(_, value)| value.as_str())
}

fn message_id_token(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(
            trimmed
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_string(),
        )
    }
}

fn address_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn references_list(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .filter_map(message_id_token)
        .collect()
}

fn parse_rfc822_date(value: &str) -> Option<Timestamp> {
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc2822).ok()?;
    Timestamp::from_unix_timestamp_nanos(parsed.unix_timestamp_nanos())
}

fn attachment_count(text: &str) -> u32 {
    text.lines()
        .filter(|line| {
            let lower = line.to_ascii_lowercase();
            lower.starts_with("content-disposition:") && lower.contains("attachment")
        })
        .count()
        .try_into()
        .unwrap_or(u32::MAX)
}

fn folder_from_record(record: &SourceRecord) -> Option<String> {
    record
        .metadata
        .get("folder")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| folder_from_path(record.logical_path.as_ref()))
}

fn folder_from_path(path: Option<&Utf8PathBuf>) -> Option<String> {
    let path = path?;
    path.parent()
        .and_then(|parent| parent.file_name())
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
}

fn occurrence_key(
    message_id: Option<&str>,
    folder: Option<&str>,
    source_file: &str,
    anchor: &MaterialAnchor,
    raw_material_id: &str,
) -> OccurrenceKey {
    let mut fields = vec![
        (
            "message_id_or_material".to_string(),
            message_id.unwrap_or(raw_material_id).to_string(),
        ),
        ("folder".to_string(), folder.unwrap_or("").to_string()),
        ("source_file".to_string(), source_file.to_string()),
    ];
    fields.push(("material_anchor".to_string(), format!("{anchor:?}")));
    OccurrenceKey {
        source_id: SourceId::from_static("email.mailbox"),
        fields,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::{Uuid, ids::Id};
    use xtask::sandbox::prelude::*;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("email.mailbox"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record_for(bytes: &[u8], logical_path: &str) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: bytes.len() as u64,
            },
            bytes: bytes.to_vec(),
            logical_path: Some(Utf8PathBuf::from(logical_path)),
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    #[sinex_test]
    async fn parses_received_rfc822_envelope_without_redacting_fields() -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let record = record_for(
            b"Message-ID: <m-1@example.com>\r\nDate: Tue, 14 Jan 2025 12:00:00 +0000\r\nFrom: Alice <alice@example.com>\r\nTo: Bob <bob@example.com>\r\nSubject: Quarterly plan\r\nBcc: Secret <secret@example.com>\r\nReferences: <root@example.com> <parent@example.com>\r\nList-Id: team.example.com\r\n\r\nHello Bob.\r\n",
            "inbox/message.eml",
        );

        let intents = parser.parse_record(record, &test_ctx()).await.unwrap();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type.as_str(), "email.message.received");
        assert_eq!(intents[0].payload["message_id"], "m-1@example.com");
        assert_eq!(intents[0].payload["subject"], "Quarterly plan");
        assert_eq!(intents[0].payload["bcc"][0], "Secret <secret@example.com>");
        assert_eq!(intents[0].payload["references"][1], "parent@example.com");
        assert_eq!(intents[0].payload["folder"], "inbox");
        assert!(intents[0].occurrence_key.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn sent_path_emits_sent_event() -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let record = record_for(
            b"Message-ID: <sent-1@example.com>\nFrom: Bob <bob@example.com>\nTo: Alice <alice@example.com>\nSubject: Re: Quarterly plan\n\nSent body.\n",
            "mail/Sent/message.eml",
        );

        let intents = parser.parse_record(record, &test_ctx()).await.unwrap();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type.as_str(), "email.message.sent");
        assert_eq!(intents[0].payload["message_id"], "sent-1@example.com");
        assert_eq!(intents[0].payload["folder"], "Sent");
        Ok(())
    }

    #[sinex_test]
    async fn malformed_utf8_is_rejected() -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let record = record_for(&[0xff, 0xfe], "inbox/bad.eml");

        let err = parser
            .parse_record(record, &test_ctx())
            .await
            .expect_err("invalid UTF-8 should be rejected");

        assert!(err.to_string().contains("not UTF-8"));
        Ok(())
    }
}
