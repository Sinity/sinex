//! Email capture source — `email.mailbox` (#1469).
//!
//! The accepted staged modes cover RFC822/`.eml`, Maildir entries, and MBOX
//! message slices. Proposed Gmail/IMAP modes publish package-mode, cursor, and
//! runtime contracts for coverage/debt/deployment inventory without claiming
//! that this staged parser runs those provider clients.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sinex_macros::SourceMeta;
use sinex_primitives::{
    domain::{EventSource, EventType},
    events::{
        EventPayload,
        payloads::email::{
            EmailAttachmentObservedPayload, EmailMailboxFormat, EmailMessageReceivedPayload,
            EmailMessageSentPayload, EmailThreadObservedPayload,
        },
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
    event_types = "email.message.sent, email.attachment.observed, email.thread.observed, email.sync_cursor.observed, email.capture_runtime.observed",
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
        subject = "source:email.mailbox.maildir-staged",
        event_type = "email.message.received",
        implementation = "staged-maildir-parser",
        adapter = "FileContentDropAdapter",
        resource_profile = ResourceProfile::BoundedFile,
        runner_pack = RunnerPack::Staged,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::Scheduled,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.sync, operation:email.mailbox.inspect, operation:email.mailbox.replay"
    ),
    binding(
        subject = "source:email.mailbox.mbox-staged",
        event_type = "email.message.received",
        implementation = "staged-mbox-parser",
        adapter = "EmailMboxFileAdapter",
        resource_profile = ResourceProfile::BoundedFile,
        runner_pack = RunnerPack::Staged,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::Scheduled,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.sync, operation:email.mailbox.inspect, operation:email.mailbox.replay"
    ),
    binding(
        subject = "source:email.mailbox.sent",
        event_type = "email.message.sent",
        proposed = true
    ),
    binding(
        subject = "source:email.mailbox.gmail-api-scheduled-sync",
        event_type = "email.sync_cursor.observed",
        implementation = "gmail-api-scheduled-sync",
        adapter = "GmailApiCursorAdapter",
        resource_profile = ResourceProfile::BoundedStream,
        runner_pack = RunnerPack::SinexdSource,
        checkpoint_family = CheckpointFamily::Journal,
        runtime_shape = RuntimeShape::Scheduled,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.authorize, operation:email.mailbox.check, operation:email.mailbox.sync, operation:email.mailbox.pause, operation:email.mailbox.resume, operation:email.mailbox.inspect, operation:email.mailbox.replay",
        proposed = true
    ),
    binding(
        subject = "source:email.mailbox.imap-scheduled-sync",
        event_type = "email.sync_cursor.observed",
        implementation = "imap-scheduled-sync",
        adapter = "ImapCursorAdapter",
        resource_profile = ResourceProfile::BoundedStream,
        runner_pack = RunnerPack::SinexdSource,
        checkpoint_family = CheckpointFamily::Polling,
        runtime_shape = RuntimeShape::Scheduled,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.authorize, operation:email.mailbox.check, operation:email.mailbox.sync, operation:email.mailbox.pause, operation:email.mailbox.resume, operation:email.mailbox.inspect, operation:email.mailbox.replay",
        proposed = true
    ),
    binding(
        subject = "source:email.mailbox.imap-idle-live",
        event_type = "email.capture_runtime.observed",
        implementation = "imap-idle-live",
        adapter = "ImapIdleAdapter",
        resource_profile = ResourceProfile::LiveWatcher,
        runner_pack = RunnerPack::Live,
        checkpoint_family = CheckpointFamily::LiveObservation,
        runtime_shape = RuntimeShape::Continuous,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.authorize, operation:email.mailbox.check, operation:email.mailbox.pause, operation:email.mailbox.resume, operation:email.mailbox.inspect",
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
            accepted_input_shapes: vec![InputShapeKind::FileDrop, InputShapeKind::Archive],
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
                (
                    EventSource::from_static("email"),
                    EventType::from_static("email.attachment.observed"),
                ),
                (
                    EventSource::from_static("email"),
                    EventType::from_static("email.thread.observed"),
                ),
                (
                    EventSource::from_static("email"),
                    EventType::from_static("email.sync_cursor.observed"),
                ),
                (
                    EventSource::from_static("email"),
                    EventType::from_static("email.capture_runtime.observed"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Document, ProcessingContext::Metadata],
            sensitivity_hints: vec![
                SensitivityHint::FreeText,
                SensitivityHint::PotentiallySensitive,
            ],
            description:
                "Parses staged RFC822/.eml material and MBOX slices into email message observations."
                    .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        if let Some(records) = split_mbox_container_record(&record) {
            let mut intents = Vec::new();
            for record in records {
                intents.extend(parse_email_message_record(record, ctx)?);
            }
            return Ok(intents);
        }
        parse_email_message_record(record, ctx)
    }
}

fn parse_email_message_record(
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
    let material = EmailMaterialIdentity::from_record(&record);
    let raw_material_id = record.material_id.to_uuid().to_string();
    let occurrence_key = occurrence_key(parsed.message_id.as_deref(), &material, &raw_material_id);
    let attachment_occurrence_prefix = material_fallback_identity(&material, &raw_material_id);
    let thread_key = email_thread_key(
        parsed.message_id.as_deref(),
        parsed.in_reply_to.as_deref(),
        &parsed.references,
        &material,
        &raw_material_id,
    );
    let thread_root_message_id = parsed
        .references
        .first()
        .cloned()
        .or_else(|| parsed.in_reply_to.clone())
        .or_else(|| parsed.message_id.clone());

    let (event_type, payload) = match event_kind {
        EmailEventKind::Received => {
            let payload = EmailMessageReceivedPayload {
                message_id: parsed.message_id.clone(),
                date: parsed.date,
                from: parsed.from.clone(),
                to: parsed.to.clone(),
                cc: parsed.cc.clone(),
                bcc: parsed.bcc.clone(),
                subject: parsed.subject.clone(),
                in_reply_to: parsed.in_reply_to.clone(),
                references: parsed.references.clone(),
                list_id: parsed.list_id.clone(),
                folder: material.folder.clone(),
                source_file: source_file.clone(),
                raw_material_id: raw_material_id.clone(),
                mailbox_format: material.mailbox_format,
                maildir_subdir: material.maildir_subdir.clone(),
                maildir_flags: material.maildir_flags.clone(),
                maildir_stable_filename: material.maildir_stable_filename.clone(),
                mbox_file: material.mbox_file.clone(),
                mbox_byte_start: material.mbox_byte_start,
                mbox_byte_end: material.mbox_byte_end,
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
                message_id: parsed.message_id.clone(),
                date: parsed.date,
                from: parsed.from.clone(),
                to: parsed.to.clone(),
                cc: parsed.cc.clone(),
                bcc: parsed.bcc.clone(),
                subject: parsed.subject.clone(),
                in_reply_to: parsed.in_reply_to.clone(),
                references: parsed.references.clone(),
                list_id: parsed.list_id.clone(),
                folder: material.folder.clone(),
                source_file: source_file.clone(),
                raw_material_id: raw_material_id.clone(),
                mailbox_format: material.mailbox_format,
                maildir_subdir: material.maildir_subdir.clone(),
                maildir_flags: material.maildir_flags.clone(),
                maildir_stable_filename: material.maildir_stable_filename.clone(),
                mbox_file: material.mbox_file.clone(),
                mbox_byte_start: material.mbox_byte_start,
                mbox_byte_end: material.mbox_byte_end,
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

    let mut intents = vec![
        ParsedEventIntent::builder()
            .source_id(SourceId::from_static("email.mailbox"))
            .parser_id(ParserId::from_static("email-mailbox-rfc822"))
            .parser_version("1.0.0")
            .event_source(EventSource::from_static("email"))
            .event_type(event_type)
            .payload(payload)
            .ts_orig(ts_orig)
            .timing(timing.clone())
            .anchor(record.anchor.clone())
            .occurrence_key(occurrence_key)
            .privacy_context(ProcessingContext::Document)
            .build(),
    ];

    let thread_payload = EmailThreadObservedPayload {
        thread_key: thread_key.clone(),
        thread_root_message_id,
        message_id: parsed.message_id.clone(),
        in_reply_to: parsed.in_reply_to.clone(),
        references: parsed.references.clone(),
        date: parsed.date,
        subject: parsed.subject.clone(),
        from: parsed.from.clone(),
        to: parsed.to.clone(),
        cc: parsed.cc.clone(),
        bcc: parsed.bcc.clone(),
        folder: material.folder.clone(),
        source_file: source_file.clone(),
        raw_material_id: raw_material_id.clone(),
        mailbox_format: material.mailbox_format,
    };
    let thread_payload = serde_json::to_value(&thread_payload).map_err(|error| {
        ParserError::Parse(format!(
            "failed to serialize EmailThreadObservedPayload: {error}"
        ))
    })?;
    intents.push(
        ParsedEventIntent::builder()
            .source_id(SourceId::from_static("email.mailbox"))
            .parser_id(ParserId::from_static("email-mailbox-rfc822"))
            .parser_version("1.0.0")
            .event_source(EventSource::from_static("email"))
            .event_type(EventType::from_static("email.thread.observed"))
            .payload(thread_payload)
            .ts_orig(ts_orig)
            .timing(timing.clone())
            .anchor(record.anchor.clone())
            .occurrence_key(thread_occurrence_key(
                &thread_key,
                parsed.message_id.as_deref(),
                &attachment_occurrence_prefix,
            ))
            .privacy_context(ProcessingContext::Document)
            .build(),
    );

    for (index, attachment) in parsed.attachments.iter().enumerate() {
        let attachment_index = u32::try_from(index).unwrap_or(u32::MAX);
        let payload = EmailAttachmentObservedPayload {
            message_id: parsed.message_id.clone(),
            folder: material.folder.clone(),
            source_file: source_file.clone(),
            raw_material_id: raw_material_id.clone(),
            mailbox_format: material.mailbox_format,
            attachment_index,
            disposition: attachment.disposition.clone(),
            filename: attachment.filename.clone(),
            content_type: attachment.content_type.clone(),
            content_id: attachment.content_id.clone(),
            material_policy_ref: "operator.email-mailbox.attachment-deferred".to_string(),
        };
        let payload = serde_json::to_value(&payload).map_err(|error| {
            ParserError::Parse(format!(
                "failed to serialize EmailAttachmentObservedPayload: {error}"
            ))
        })?;
        intents.push(
            ParsedEventIntent::builder()
                .source_id(SourceId::from_static("email.mailbox"))
                .parser_id(ParserId::from_static("email-mailbox-rfc822"))
                .parser_version("1.0.0")
                .event_source(EventSource::from_static("email"))
                .event_type(EventType::from_static("email.attachment.observed"))
                .payload(payload)
                .ts_orig(ts_orig)
                .timing(timing.clone())
                .anchor(record.anchor.clone())
                .occurrence_key(attachment_occurrence_key(
                    parsed.message_id.as_deref(),
                    &attachment_occurrence_prefix,
                    attachment,
                    attachment_index,
                ))
                .privacy_context(ProcessingContext::Document)
                .build(),
        );
    }

    Ok(intents)
}

#[derive(Debug, Clone)]
struct EmailMaterialIdentity {
    mailbox_format: EmailMailboxFormat,
    folder: Option<String>,
    source_file: String,
    material_anchor: String,
    maildir_subdir: Option<String>,
    maildir_flags: Vec<String>,
    maildir_stable_filename: Option<String>,
    mbox_file: Option<String>,
    mbox_byte_start: Option<u64>,
    mbox_byte_end: Option<u64>,
}

impl EmailMaterialIdentity {
    fn from_record(record: &SourceRecord) -> Self {
        let source_file = record
            .logical_path
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let material_anchor = format!("{:?}", record.anchor);
        let metadata_folder = record
            .metadata
            .get("folder")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);

        if let Some(maildir) = MaildirPathIdentity::from_record(record) {
            return Self {
                mailbox_format: EmailMailboxFormat::MaildirStaged,
                folder: metadata_folder.or(maildir.folder),
                source_file,
                material_anchor,
                maildir_subdir: Some(maildir.subdir),
                maildir_flags: maildir.flags,
                maildir_stable_filename: maildir.stable_filename,
                mbox_file: None,
                mbox_byte_start: None,
                mbox_byte_end: None,
            };
        }

        if is_mbox_record(record) {
            let (mbox_byte_start, mbox_byte_end) = match record.anchor {
                MaterialAnchor::ByteRange { start, len } => (Some(start), Some(start + len)),
                _ => (None, None),
            };
            return Self {
                mailbox_format: EmailMailboxFormat::MboxStaged,
                folder: metadata_folder
                    .or_else(|| mbox_folder_from_path(record.logical_path.as_ref()))
                    .or_else(|| folder_from_path(record.logical_path.as_ref())),
                source_file: source_file.clone(),
                material_anchor,
                maildir_subdir: None,
                maildir_flags: Vec::new(),
                maildir_stable_filename: None,
                mbox_file: if source_file.is_empty() {
                    None
                } else {
                    Some(source_file)
                },
                mbox_byte_start,
                mbox_byte_end,
            };
        }

        Self {
            mailbox_format: EmailMailboxFormat::Rfc822DropStaged,
            folder: metadata_folder.or_else(|| folder_from_path(record.logical_path.as_ref())),
            source_file,
            material_anchor,
            maildir_subdir: None,
            maildir_flags: Vec::new(),
            maildir_stable_filename: None,
            mbox_file: None,
            mbox_byte_start: None,
            mbox_byte_end: None,
        }
    }
}

#[derive(Debug, Clone)]
struct MaildirPathIdentity {
    folder: Option<String>,
    subdir: String,
    stable_filename: Option<String>,
    flags: Vec<String>,
}

impl MaildirPathIdentity {
    fn from_record(record: &SourceRecord) -> Option<Self> {
        let path = record.logical_path.as_ref()?;
        let parts: Vec<&str> = path
            .as_str()
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        let subdir_index = parts
            .iter()
            .position(|part| matches!(*part, "cur" | "new" | "tmp"))?;
        let subdir = parts[subdir_index].to_string();
        let file_name = parts.get(subdir_index + 1).copied();
        let stable_filename = file_name.map(stable_maildir_name);
        let folder = if subdir_index == 0 {
            None
        } else {
            Some(parts[..subdir_index].join("/"))
        };
        let flags = file_name.map(maildir_flags).unwrap_or_default();
        Some(Self {
            folder,
            subdir,
            stable_filename,
            flags,
        })
    }
}

fn stable_maildir_name(name: &str) -> String {
    name.split_once(":2,")
        .map_or(name, |(stable, _)| stable)
        .to_string()
}

fn maildir_flags(name: &str) -> Vec<String> {
    let Some((_, flags)) = name.split_once(":2,") else {
        return Vec::new();
    };
    flags.chars().map(|flag| flag.to_string()).collect()
}

fn is_mbox_record(record: &SourceRecord) -> bool {
    if record
        .metadata
        .get("mailbox_format")
        .and_then(serde_json::Value::as_str)
        .and_then(email_mailbox_format_token)
        .is_some_and(|format| format == EmailMailboxFormat::MboxStaged)
    {
        return true;
    }
    record
        .logical_path
        .as_ref()
        .and_then(|path| path.file_name())
        .is_some_and(|name| name.ends_with(".mbox") || name == "mbox")
}

fn split_mbox_container_record(record: &SourceRecord) -> Option<Vec<SourceRecord>> {
    if !is_mbox_record(record) {
        return None;
    }
    if record
        .metadata
        .get("mbox_message_index")
        .and_then(serde_json::Value::as_u64)
        .is_some()
    {
        return None;
    }
    let ranges = mbox_message_ranges(&record.bytes);
    if ranges.len() <= 1 {
        return None;
    }

    let base_start = match record.anchor {
        MaterialAnchor::ByteRange { start, .. } => start,
        _ => 0,
    };
    let source_file = record
        .logical_path
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let folder = record
        .metadata
        .get("folder")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| mbox_folder_from_path(record.logical_path.as_ref()));

    Some(
        ranges
            .into_iter()
            .enumerate()
            .map(|(index, range)| {
                let start = base_start + range.start as u64;
                let len = (range.end - range.start) as u64;
                let mut metadata = record.metadata.clone();
                if !metadata.is_object() {
                    metadata = serde_json::json!({});
                }
                if let Some(object) = metadata.as_object_mut() {
                    object.insert(
                        "mailbox_format".to_string(),
                        serde_json::json!(EmailMailboxFormat::MboxStaged.as_str()),
                    );
                    object.insert("mbox_message_index".to_string(), serde_json::json!(index));
                    object.insert("mbox_file".to_string(), serde_json::json!(source_file));
                    if let Some(folder) = &folder {
                        object.insert("folder".to_string(), serde_json::json!(folder));
                    }
                }
                SourceRecord {
                    material_id: record.material_id,
                    anchor: MaterialAnchor::ByteRange { start, len },
                    bytes: record.bytes[range.start..range.end].to_vec(),
                    logical_path: record.logical_path.clone(),
                    source_ts_hint: record.source_ts_hint.clone(),
                    metadata,
                }
            })
            .collect(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MboxMessageRange {
    start: usize,
    end: usize,
}

fn mbox_message_ranges(bytes: &[u8]) -> Vec<MboxMessageRange> {
    let delimiter_starts = mbox_delimiter_line_starts(bytes);
    if delimiter_starts.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::with_capacity(delimiter_starts.len());
    for (index, delimiter_start) in delimiter_starts.iter().copied().enumerate() {
        let message_start = line_end_after(bytes, delimiter_start).unwrap_or(bytes.len());
        let message_end = delimiter_starts
            .get(index + 1)
            .copied()
            .unwrap_or(bytes.len());
        if message_start < message_end {
            ranges.push(MboxMessageRange {
                start: message_start,
                end: trim_mbox_message_end(bytes, message_start, message_end),
            });
        }
    }
    ranges
}

fn mbox_delimiter_line_starts(bytes: &[u8]) -> Vec<usize> {
    let mut starts = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        if bytes[offset..].starts_with(b"From ") {
            starts.push(offset);
        }
        let Some(next_line) = bytes[offset..].iter().position(|byte| *byte == b'\n') else {
            break;
        };
        offset += next_line + 1;
    }
    starts
}

fn line_end_after(bytes: &[u8], start: usize) -> Option<usize> {
    bytes[start..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|offset| start + offset + 1)
}

fn trim_mbox_message_end(bytes: &[u8], start: usize, end: usize) -> usize {
    let mut trimmed = end;
    while trimmed > start && matches!(bytes[trimmed - 1], b'\n' | b'\r') {
        trimmed -= 1;
    }
    trimmed
}

fn mbox_folder_from_path(path: Option<&Utf8PathBuf>) -> Option<String> {
    let path = path?;
    let file_name = path.file_stem().or_else(|| path.file_name())?;
    if file_name.is_empty() {
        None
    } else {
        Some(file_name.to_string())
    }
}

fn email_mailbox_format_token(value: &str) -> Option<EmailMailboxFormat> {
    match value.trim().to_ascii_lowercase().as_str() {
        "rfc822-drop" | "rfc822-drop-staged" | "rfc822_drop_staged" => {
            Some(EmailMailboxFormat::Rfc822DropStaged)
        }
        "maildir" | "maildir-staged" | "maildir_staged" => Some(EmailMailboxFormat::MaildirStaged),
        "mbox" | "mbox-staged" | "mbox_staged" => Some(EmailMailboxFormat::MboxStaged),
        _ => None,
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
    attachments: Vec<ParsedEmailAttachment>,
}

#[derive(Debug, Clone)]
struct ParsedEmailAttachment {
    disposition: String,
    filename: Option<String>,
    content_type: Option<String>,
    content_id: Option<String>,
}

fn parse_rfc822(record: &SourceRecord) -> ParserResult<ParsedEmail> {
    let text = std::str::from_utf8(&record.bytes).map_err(|error| {
        ParserError::Parse(format!("email RFC822 material is not UTF-8: {error}"))
    })?;
    let (headers, body) = split_headers_body(text);
    let headers = parse_headers(headers);

    let attachments = attachment_headers(text);
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
        attachment_count: attachments.len().try_into().unwrap_or(u32::MAX),
        attachments,
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

fn attachment_headers(text: &str) -> Vec<ParsedEmailAttachment> {
    let mut attachments = Vec::new();
    let mut current_content_type: Option<String> = None;
    let mut current_content_id: Option<String> = None;

    for line in text.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match name.as_str() {
            "content-type" => {
                current_content_type = Some(value.to_string());
            }
            "content-id" => {
                current_content_id = message_id_token(value);
            }
            "content-disposition" => {
                if !value.to_ascii_lowercase().contains("attachment") {
                    continue;
                }
                attachments.push(ParsedEmailAttachment {
                    disposition: disposition_token(value).unwrap_or("attachment").to_string(),
                    filename: disposition_parameter(value, "filename")
                        .or_else(|| disposition_parameter(value, "filename*")),
                    content_type: current_content_type.clone(),
                    content_id: current_content_id.clone(),
                });
                current_content_type = None;
                current_content_id = None;
            }
            _ => {}
        }
    }

    attachments
}

fn disposition_token(value: &str) -> Option<&str> {
    value
        .split(';')
        .next()
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

fn disposition_parameter(value: &str, key: &str) -> Option<String> {
    value.split(';').skip(1).find_map(|part| {
        let (name, raw_value) = part.trim().split_once('=')?;
        if !name.trim().eq_ignore_ascii_case(key) {
            return None;
        }
        let value = raw_value.trim().trim_matches('"');
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    })
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
    material: &EmailMaterialIdentity,
    raw_material_id: &str,
) -> OccurrenceKey {
    let fallback_identity = material_fallback_identity(material, raw_material_id);
    let mut fields = vec![
        (
            "message_id_or_material".to_string(),
            message_id.unwrap_or(&fallback_identity).to_string(),
        ),
        (
            "mailbox_format".to_string(),
            material.mailbox_format.as_str().to_string(),
        ),
        (
            "folder".to_string(),
            material.folder.as_deref().unwrap_or("").to_string(),
        ),
    ];
    match material.mailbox_format {
        EmailMailboxFormat::MaildirStaged => {
            fields.push((
                "maildir_stable_filename".to_string(),
                material
                    .maildir_stable_filename
                    .as_deref()
                    .unwrap_or("")
                    .to_string(),
            ));
        }
        EmailMailboxFormat::MboxStaged => {
            fields.push((
                "mbox_file".to_string(),
                material.mbox_file.as_deref().unwrap_or("").to_string(),
            ));
            fields.push((
                "mbox_byte_start".to_string(),
                material
                    .mbox_byte_start
                    .map(|start| start.to_string())
                    .unwrap_or_default(),
            ));
            fields.push((
                "mbox_byte_end".to_string(),
                material
                    .mbox_byte_end
                    .map(|end| end.to_string())
                    .unwrap_or_default(),
            ));
        }
        EmailMailboxFormat::Rfc822DropStaged => {
            fields.push(("source_file".to_string(), material.source_file.clone()));
        }
    }
    fields.push((
        "material_anchor".to_string(),
        material.material_anchor.clone(),
    ));
    OccurrenceKey {
        source_id: SourceId::from_static("email.mailbox"),
        fields,
    }
}

fn attachment_occurrence_key(
    message_id: Option<&str>,
    fallback_message_identity: &str,
    attachment: &ParsedEmailAttachment,
    attachment_index: u32,
) -> OccurrenceKey {
    OccurrenceKey {
        source_id: SourceId::from_static("email.mailbox"),
        fields: vec![
            (
                "message_id_or_material".to_string(),
                message_id.unwrap_or(fallback_message_identity).to_string(),
            ),
            ("attachment_index".to_string(), attachment_index.to_string()),
            (
                "filename".to_string(),
                attachment.filename.as_deref().unwrap_or("").to_string(),
            ),
            (
                "content_id".to_string(),
                attachment.content_id.as_deref().unwrap_or("").to_string(),
            ),
        ],
    }
}

fn email_thread_key(
    message_id: Option<&str>,
    in_reply_to: Option<&str>,
    references: &[String],
    material: &EmailMaterialIdentity,
    raw_material_id: &str,
) -> String {
    references
        .first()
        .map(String::as_str)
        .or(in_reply_to)
        .or(message_id)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| material_fallback_identity(material, raw_material_id))
}

fn thread_occurrence_key(
    thread_key: &str,
    message_id: Option<&str>,
    fallback_message_identity: &str,
) -> OccurrenceKey {
    OccurrenceKey {
        source_id: SourceId::from_static("email.mailbox"),
        fields: vec![
            ("thread_key".to_string(), thread_key.to_string()),
            (
                "message_id_or_material".to_string(),
                message_id.unwrap_or(fallback_message_identity).to_string(),
            ),
        ],
    }
}

fn material_fallback_identity(material: &EmailMaterialIdentity, raw_material_id: &str) -> String {
    match material.mailbox_format {
        EmailMailboxFormat::MaildirStaged => material
            .maildir_stable_filename
            .clone()
            .unwrap_or_else(|| raw_material_id.to_string()),
        EmailMailboxFormat::MboxStaged => {
            let file = material.mbox_file.as_deref().unwrap_or("");
            let start = material
                .mbox_byte_start
                .map(|value| value.to_string())
                .unwrap_or_default();
            let end = material
                .mbox_byte_end
                .map(|value| value.to_string())
                .unwrap_or_default();
            format!("{file}:{start}:{end}")
        }
        EmailMailboxFormat::Rfc822DropStaged => {
            if material.source_file.is_empty() {
                raw_material_id.to_string()
            } else {
                material.source_file.clone()
            }
        }
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

    fn record_with_anchor(
        bytes: &[u8],
        logical_path: &str,
        anchor: MaterialAnchor,
        metadata: serde_json::Value,
    ) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor,
            bytes: bytes.to_vec(),
            logical_path: Some(Utf8PathBuf::from(logical_path)),
            source_ts_hint: None,
            metadata,
        }
    }

    fn occurrence_field<'a>(intent: &'a ParsedEventIntent, name: &str) -> Option<&'a str> {
        intent
            .occurrence_key
            .as_ref()?
            .fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }

    #[sinex_test]
    async fn parses_received_rfc822_envelope_without_redacting_fields() -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let record = record_for(
            b"Message-ID: <m-1@example.com>\r\nDate: Tue, 14 Jan 2025 12:00:00 +0000\r\nFrom: Alice <alice@example.com>\r\nTo: Bob <bob@example.com>\r\nSubject: Quarterly plan\r\nBcc: Secret <secret@example.com>\r\nReferences: <root@example.com> <parent@example.com>\r\nList-Id: team.example.com\r\n\r\nHello Bob.\r\n",
            "inbox/message.eml",
        );

        let intents = parser.parse_record(record, &test_ctx()).await.unwrap();

        assert_eq!(intents.len(), 2);
        assert_eq!(intents[0].event_type.as_str(), "email.message.received");
        assert_eq!(intents[0].payload["message_id"], "m-1@example.com");
        assert_eq!(intents[0].payload["subject"], "Quarterly plan");
        assert_eq!(intents[0].payload["bcc"][0], "Secret <secret@example.com>");
        assert_eq!(intents[0].payload["references"][1], "parent@example.com");
        assert_eq!(intents[0].payload["folder"], "inbox");
        assert_eq!(intents[0].payload["mailbox_format"], "rfc822-drop-staged");
        assert!(intents[0].occurrence_key.is_some());
        assert_eq!(intents[1].event_type.as_str(), "email.thread.observed");
        assert_eq!(intents[1].payload["thread_key"], "root@example.com");
        assert_eq!(
            intents[1].payload["thread_root_message_id"],
            "root@example.com"
        );
        assert_eq!(intents[1].payload["message_id"], "m-1@example.com");
        assert_eq!(intents[1].payload["references"][1], "parent@example.com");
        assert_eq!(
            occurrence_field(&intents[1], "thread_key"),
            Some("root@example.com")
        );
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

        assert_eq!(intents.len(), 2);
        assert_eq!(intents[0].event_type.as_str(), "email.message.sent");
        assert_eq!(intents[0].payload["message_id"], "sent-1@example.com");
        assert_eq!(intents[0].payload["folder"], "Sent");
        assert_eq!(intents[1].event_type.as_str(), "email.thread.observed");
        assert_eq!(intents[1].payload["thread_key"], "sent-1@example.com");
        Ok(())
    }

    #[sinex_test]
    async fn attachment_headers_emit_attachment_observation_without_fetching_bytes()
    -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let record = record_for(
            b"Message-ID: <attach-1@example.com>\n\
From: Alice <alice@example.com>\n\
To: Bob <bob@example.com>\n\
Subject: Attachment\n\
Content-Type: application/pdf; name=\"report.pdf\"\n\
Content-ID: <part-1@example.com>\n\
Content-Disposition: attachment; filename=\"report.pdf\"\n\
\n\
binary bytes are not decoded by this staged envelope parser\n",
            "inbox/with-attachment.eml",
        );

        let intents = parser.parse_record(record, &test_ctx()).await?;

        assert_eq!(intents.len(), 3);
        assert_eq!(intents[0].event_type.as_str(), "email.message.received");
        assert_eq!(intents[0].payload["attachment_count"], 1);
        assert_eq!(intents[1].event_type.as_str(), "email.thread.observed");
        assert_eq!(intents[1].payload["thread_key"], "attach-1@example.com");
        assert_eq!(intents[2].event_type.as_str(), "email.attachment.observed");
        assert_eq!(intents[2].payload["message_id"], "attach-1@example.com");
        assert_eq!(intents[2].payload["filename"], "report.pdf");
        assert_eq!(
            intents[2].payload["content_type"],
            "application/pdf; name=\"report.pdf\""
        );
        assert_eq!(intents[2].payload["content_id"], "part-1@example.com");
        assert_eq!(
            intents[2].payload["material_policy_ref"],
            "operator.email-mailbox.attachment-deferred"
        );
        assert_eq!(
            occurrence_field(&intents[2], "message_id_or_material"),
            Some("attach-1@example.com")
        );
        assert_eq!(
            occurrence_field(&intents[2], "filename"),
            Some("report.pdf")
        );
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

    #[sinex_test]
    async fn maildir_entry_preserves_folder_flags_and_move_identity() -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let bytes = b"Message-ID: <move-1@example.com>\nFrom: Alice <alice@example.com>\nTo: Bob <bob@example.com>\nSubject: Maildir move\n\nBody.\n";
        let new_record = record_for(bytes, "Maildir/INBOX/new/1710000000.M1P1.host");
        let cur_record = record_for(bytes, "Maildir/INBOX/cur/1710000000.M1P1.host:2,RS");

        let new_intent = parser
            .parse_record(new_record, &test_ctx())
            .await?
            .remove(0);
        let cur_intent = parser
            .parse_record(cur_record, &test_ctx())
            .await?
            .remove(0);

        assert_eq!(cur_intent.payload["mailbox_format"], "maildir-staged");
        assert_eq!(cur_intent.payload["folder"], "Maildir/INBOX");
        assert_eq!(cur_intent.payload["maildir_subdir"], "cur");
        assert_eq!(cur_intent.payload["maildir_flags"][0], "R");
        assert_eq!(cur_intent.payload["maildir_flags"][1], "S");
        assert_eq!(
            cur_intent.payload["maildir_stable_filename"],
            "1710000000.M1P1.host"
        );
        assert_eq!(
            occurrence_field(&new_intent, "maildir_stable_filename"),
            occurrence_field(&cur_intent, "maildir_stable_filename"),
            "Maildir cur/new moves should keep the stable filename identity"
        );
        assert_eq!(
            occurrence_field(&new_intent, "folder"),
            occurrence_field(&cur_intent, "folder"),
            "Maildir cur/new moves within one folder should keep occurrence folder identity"
        );
        assert_eq!(
            occurrence_field(&new_intent, "message_id_or_material"),
            occurrence_field(&cur_intent, "message_id_or_material"),
            "Maildir cur/new moves should not mint a new message occurrence"
        );
        assert!(
            occurrence_field(&cur_intent, "source_file").is_none(),
            "Maildir occurrence identity should not depend on cur/new source path"
        );
        Ok(())
    }

    #[sinex_test]
    async fn mbox_slice_exposes_byte_range_identity() -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let record = record_with_anchor(
            b"Message-ID: <mbox-1@example.com>\nDate: Tue, 14 Jan 2025 12:00:00 +0000\nFrom: Alice <alice@example.com>\nTo: Bob <bob@example.com>\nSubject: MBOX slice\n\nBody.\n",
            "exports/inbox.mbox",
            MaterialAnchor::ByteRange { start: 1024, len: 162 },
            serde_json::json!({"mailbox_format": "mbox", "folder": "archive/inbox"}),
        );

        let intent = parser.parse_record(record, &test_ctx()).await?.remove(0);

        assert_eq!(intent.payload["mailbox_format"], "mbox-staged");
        assert_eq!(intent.payload["folder"], "archive/inbox");
        assert_eq!(intent.payload["mbox_file"], "exports/inbox.mbox");
        assert_eq!(intent.payload["mbox_byte_start"], 1024);
        assert_eq!(intent.payload["mbox_byte_end"], 1186);
        assert_eq!(
            occurrence_field(&intent, "mbox_file"),
            Some("exports/inbox.mbox")
        );
        assert_eq!(occurrence_field(&intent, "mbox_byte_start"), Some("1024"));
        assert_eq!(occurrence_field(&intent, "mbox_byte_end"), Some("1186"));
        Ok(())
    }

    #[sinex_test]
    async fn canonical_mbox_staged_metadata_selects_mbox_identity() -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let record = record_with_anchor(
            b"Message-ID: <mbox-2@example.com>\nFrom: Alice <alice@example.com>\nTo: Bob <bob@example.com>\nSubject: Canonical MBOX metadata\n\nBody.\n",
            "exports/message.slice",
            MaterialAnchor::ByteRange { start: 10, len: 128 },
            serde_json::json!({"mailbox_format": "mbox-staged", "folder": "archive/sent"}),
        );

        let intent = parser.parse_record(record, &test_ctx()).await?.remove(0);

        assert_eq!(intent.payload["mailbox_format"], "mbox-staged");
        assert_eq!(intent.payload["folder"], "archive/sent");
        assert_eq!(intent.payload["mbox_file"], "exports/message.slice");
        assert_eq!(occurrence_field(&intent, "mbox_byte_start"), Some("10"));
        assert_eq!(occurrence_field(&intent, "mbox_byte_end"), Some("138"));
        Ok(())
    }

    #[sinex_test]
    async fn missing_message_id_falls_back_to_material_identity() -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let first = record_for(
            b"From: Alice <alice@example.com>\nTo: Bob <bob@example.com>\nSubject: No id\n\nBody.\n",
            "Maildir/INBOX/cur/1710000001.M2P1.host:2,S",
        );
        let second = record_for(
            b"From: Alice <alice@example.com>\nTo: Bob <bob@example.com>\nSubject: No id\n\nBody.\n",
            "Maildir/INBOX/cur/1710000002.M3P1.host:2,S",
        );

        let first_intent = parser.parse_record(first, &test_ctx()).await?.remove(0);
        let second_intent = parser.parse_record(second, &test_ctx()).await?.remove(0);

        assert!(
            first_intent.payload["message_id"].is_null(),
            "fixture should exercise missing Message-ID fallback"
        );
        assert_ne!(
            occurrence_field(&first_intent, "message_id_or_material"),
            occurrence_field(&second_intent, "message_id_or_material"),
            "missing Message-ID fallback should remain material-specific"
        );
        assert_ne!(
            occurrence_field(&first_intent, "maildir_stable_filename"),
            occurrence_field(&second_intent, "maildir_stable_filename"),
            "Maildir stable filenames should distinguish no-Message-ID messages"
        );
        Ok(())
    }

    #[sinex_test]
    async fn missing_message_id_maildir_replay_keeps_occurrence_identity() -> TestResult<()> {
        let mut parser = EmailMailboxParser;
        let bytes =
            b"From: Alice <alice@example.com>\nTo: Bob <bob@example.com>\nSubject: Replay\n\nBody.\n";
        let first = record_for(bytes, "Maildir/INBOX/cur/1710000004.M4P1.host:2,S");
        let replay = record_for(bytes, "Maildir/INBOX/cur/1710000004.M4P1.host:2,S");

        let first_intent = parser.parse_record(first, &test_ctx()).await?.remove(0);
        let replay_intent = parser.parse_record(replay, &test_ctx()).await?.remove(0);

        assert_eq!(
            occurrence_field(&first_intent, "message_id_or_material"),
            occurrence_field(&replay_intent, "message_id_or_material"),
            "replaying the same no-Message-ID Maildir entry should not depend on raw material UUID"
        );
        Ok(())
    }
}
