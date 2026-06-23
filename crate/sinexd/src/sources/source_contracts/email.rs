//! Email capture source — `email.mailbox` (#1469).
//!
//! The accepted staged modes cover RFC822/`.eml`, Maildir entries, and MBOX
//! message slices. Accepted Gmail/IMAP modes publish provider cursor and runtime
//! contracts for coverage/debt/deployment inventory; provider executors emit
//! typed cursor material that this parser turns into sync observations.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use mail_parser::{ContentType, MessageParser, MessagePart, MimeHeaders};
use serde::{Deserialize, Serialize};
use sinex_macros::SourceMeta;
use sinex_primitives::{
    domain::{EventSource, EventType},
    events::{
        EventPayload,
        payloads::email::{
            EmailAttachmentObservedPayload, EmailContinuityState, EmailMailboxFormat,
            EmailMessageReceivedPayload, EmailMessageSentPayload, EmailProviderKind,
            EmailSyncCursorKind, EmailSyncCursorObservedPayload, EmailThreadObservedPayload,
        },
    },
    parser::{
        InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
        ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
    },
    privacy::{ProcessingContext, SensitivityHint},
    source_contracts::{
        AccessScope, CheckpointFamily, Horizon, MaterialLifecyclePolicy, OccurrenceIdentity,
        PrivacyTier, ResourceProfile, RetentionPolicy, RunnerPack, RuntimeShape,
        TransportSemantics,
    },
    temporal::Timestamp,
};

use crate::runtime::parser::{
    GmailApiRecord, GmailApiRecordKind, ImapSyncMode, ImapSyncRecord, ImapSyncRecordKind,
    MaterialParser, ParserError, ParserResult,
};

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
    capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.check, operation:email.mailbox.import-rfc822, operation:email.mailbox.inspect, operation:email.mailbox.replay",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Scheduled,
    material_lifecycle = MaterialLifecyclePolicy::RetainRaw,
    transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
    binding(
        subject = "source:email.mailbox.maildir-staged",
        event_type = "email.message.received",
        implementation = "staged-maildir-parser",
        adapter = "FileContentDropAdapter",
        resource_profile = ResourceProfile::BoundedFile,
        runner_pack = RunnerPack::Staged,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::Scheduled,
        material_lifecycle = MaterialLifecyclePolicy::RetainRaw,
        transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.import-maildir, operation:email.mailbox.inspect, operation:email.mailbox.replay"
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
        material_lifecycle = MaterialLifecyclePolicy::RetainRaw,
        transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.import-mbox, operation:email.mailbox.inspect, operation:email.mailbox.replay"
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
        material_lifecycle = MaterialLifecyclePolicy::ExternalReferenceOnly,
        transport_semantics = TransportSemantics::EXTERNAL_API_CURSOR,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.authorize, operation:email.mailbox.check, operation:email.mailbox.sync, operation:email.mailbox.pause, operation:email.mailbox.resume, operation:email.mailbox.inspect, operation:email.mailbox.replay"
    ),
    binding(
        subject = "source:email.mailbox.imap-scheduled-sync",
        event_type = "email.sync_cursor.observed",
        implementation = "imap-scheduled-sync",
        adapter = "ImapSyncAdapter",
        resource_profile = ResourceProfile::BoundedStream,
        runner_pack = RunnerPack::SinexdSource,
        checkpoint_family = CheckpointFamily::Polling,
        runtime_shape = RuntimeShape::Scheduled,
        material_lifecycle = MaterialLifecyclePolicy::ExternalReferenceOnly,
        transport_semantics = TransportSemantics::EXTERNAL_API_CURSOR,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.authorize, operation:email.mailbox.check, operation:email.mailbox.sync, operation:email.mailbox.pause, operation:email.mailbox.resume, operation:email.mailbox.inspect, operation:email.mailbox.replay"
    ),
    binding(
        subject = "source:email.mailbox.imap-idle-live",
        event_type = "email.capture_runtime.observed",
        implementation = "imap-idle-live",
        adapter = "ImapSyncAdapter",
        resource_profile = ResourceProfile::LiveWatcher,
        runner_pack = RunnerPack::Live,
        checkpoint_family = CheckpointFamily::LiveObservation,
        runtime_shape = RuntimeShape::Continuous,
        material_lifecycle = MaterialLifecyclePolicy::ExternalReferenceOnly,
        transport_semantics = TransportSemantics::EXTERNAL_API_CURSOR,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:email.mailbox.authorize, operation:email.mailbox.check, operation:email.mailbox.pause, operation:email.mailbox.resume, operation:email.mailbox.inspect"
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
        if is_gmail_provider_record(&record) {
            return parse_gmail_provider_record(record, ctx);
        }
        if is_imap_provider_record(&record) {
            return parse_imap_provider_record(record, ctx);
        }
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

fn is_gmail_provider_record(record: &SourceRecord) -> bool {
    metadata_str(record, "provider").is_some_and(|provider| provider == "gmail")
        && metadata_str(record, "gmail_record_kind").is_some()
}

fn is_imap_provider_record(record: &SourceRecord) -> bool {
    metadata_str(record, "provider").is_some_and(|provider| provider == "imap")
        && metadata_str(record, "imap_record_kind").is_some()
}

fn parse_gmail_provider_record(
    record: SourceRecord,
    ctx: &ParserContext,
) -> ParserResult<Vec<ParsedEventIntent>> {
    let provider_record: GmailApiRecord =
        serde_json::from_slice(&record.bytes).map_err(|error| {
            ParserError::Parse(format!("failed to parse Gmail provider record: {error}"))
        })?;
    let account_binding_ref = required_metadata_string(&record, "account_binding_ref")?;
    let mailbox_scope = metadata_string(&record, "mailbox_scope");
    let gmail_history_id =
        metadata_string(&record, "gmail_history_id").or_else(|| provider_record.history_id.clone());
    let page_token = metadata_string(&record, "gmail_page_token_next");
    let cursor_kind = if page_token.is_some() {
        EmailSyncCursorKind::GmailPageToken
    } else {
        EmailSyncCursorKind::GmailHistoryId
    };
    let cursor_value = match cursor_kind {
        EmailSyncCursorKind::GmailPageToken => page_token.clone(),
        EmailSyncCursorKind::GmailHistoryId => gmail_history_id.clone(),
        EmailSyncCursorKind::ImapUidvalidityUid | EmailSyncCursorKind::ImapModseq => None,
    };
    let continuity_state = provider_record_continuity_state(&provider_record.payload);
    let required_action = provider_record_required_action(&provider_record.payload);
    let caveats = provider_cursor_caveats(
        gmail_cursor_caveats(provider_record.kind),
        &provider_record.payload,
    );
    let payload = EmailSyncCursorObservedPayload {
        provider: EmailProviderKind::Gmail,
        account_binding_ref: account_binding_ref.clone(),
        mailbox_scope: mailbox_scope.clone(),
        cursor_kind,
        cursor_value,
        uidvalidity: None,
        uid: None,
        gmail_history_id: gmail_history_id.clone(),
        page_token: page_token.clone(),
        observed_at: ctx.acquisition_time,
        continuity_state,
        required_action,
        caveats,
    };
    Ok(vec![provider_cursor_intent(
        record,
        ctx,
        serde_json::to_value(&payload).map_err(|error| {
            ParserError::Parse(format!("failed to serialize Gmail cursor payload: {error}"))
        })?,
        provider_cursor_occurrence_key(
            EmailProviderKind::Gmail,
            &account_binding_ref,
            mailbox_scope.as_deref(),
            cursor_kind,
            payload.gmail_history_id.as_deref(),
            payload.page_token.as_deref(),
            None,
            None,
            None,
        ),
    )])
}

fn parse_imap_provider_record(
    record: SourceRecord,
    ctx: &ParserContext,
) -> ParserResult<Vec<ParsedEventIntent>> {
    let provider_record: ImapSyncRecord =
        serde_json::from_slice(&record.bytes).map_err(|error| {
            ParserError::Parse(format!("failed to parse IMAP provider record: {error}"))
        })?;
    let account_binding_ref = required_metadata_string(&record, "account_binding_ref")?;
    let mailbox_scope = metadata_string(&record, "mailbox");
    let uidvalidity = metadata_string(&record, "imap_uid_validity");
    let uid = metadata_string(&record, "imap_uid_next").or_else(|| {
        provider_record
            .uid
            .map(|uid| uid.saturating_add(1).to_string())
    });
    let highest_modseq = metadata_string(&record, "imap_highest_modseq");
    let cursor_kind =
        if highest_modseq.is_some() && provider_record.kind == ImapSyncRecordKind::Flags {
            EmailSyncCursorKind::ImapModseq
        } else {
            EmailSyncCursorKind::ImapUidvalidityUid
        };
    let cursor_value = match cursor_kind {
        EmailSyncCursorKind::ImapUidvalidityUid => uidvalidity
            .as_ref()
            .zip(uid.as_ref())
            .map(|(uidvalidity, uid)| format!("{uidvalidity}:{uid}")),
        EmailSyncCursorKind::ImapModseq => highest_modseq.clone(),
        EmailSyncCursorKind::GmailHistoryId | EmailSyncCursorKind::GmailPageToken => None,
    };
    let continuity_state = provider_record_continuity_state(&provider_record.payload);
    let required_action = provider_record_required_action(&provider_record.payload);
    let caveats = provider_cursor_caveats(
        imap_cursor_caveats(provider_record.kind, imap_mode(&record)),
        &provider_record.payload,
    );
    let payload = EmailSyncCursorObservedPayload {
        provider: EmailProviderKind::Imap,
        account_binding_ref: account_binding_ref.clone(),
        mailbox_scope: mailbox_scope.clone(),
        cursor_kind,
        cursor_value,
        uidvalidity: uidvalidity.clone(),
        uid: uid.clone(),
        gmail_history_id: None,
        page_token: None,
        observed_at: ctx.acquisition_time,
        continuity_state,
        required_action,
        caveats,
    };
    Ok(vec![provider_cursor_intent(
        record,
        ctx,
        serde_json::to_value(&payload).map_err(|error| {
            ParserError::Parse(format!("failed to serialize IMAP cursor payload: {error}"))
        })?,
        provider_cursor_occurrence_key(
            EmailProviderKind::Imap,
            &account_binding_ref,
            mailbox_scope.as_deref(),
            cursor_kind,
            None,
            None,
            uidvalidity.as_deref(),
            uid.as_deref(),
            highest_modseq.as_deref(),
        ),
    )])
}

fn provider_cursor_intent(
    record: SourceRecord,
    ctx: &ParserContext,
    payload: serde_json::Value,
    occurrence_key: OccurrenceKey,
) -> ParsedEventIntent {
    ParsedEventIntent::builder()
        .source_id(SourceId::from_static("email.mailbox"))
        .parser_id(ParserId::from_static("email-mailbox-rfc822"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("email"))
        .event_type(EventType::from_static("email.sync_cursor.observed"))
        .payload(payload)
        .ts_orig(ctx.acquisition_time)
        .timing(TimingEvidence::StagedAtFallback)
        .anchor(record.anchor)
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Metadata)
        .build()
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
    let text = std::str::from_utf8(&record.bytes)
        .map_err(|e| ParserError::Parse(format!("RFC822 email material is not UTF-8: {e}")))?;
    let (headers, body) = split_headers_body(text);
    let headers = parse_headers(headers);
    let header_attachments = attachment_headers(text);
    let parsed_message = MessageParser::default().parse(&record.bytes);

    let attachments = parsed_message
        .as_ref()
        .map(parsed_message_attachments)
        .map(|mut attachments| {
            apply_attachment_header_fallbacks(&mut attachments, &header_attachments);
            attachments
        })
        .filter(|attachments| !attachments.is_empty())
        .unwrap_or(header_attachments);
    Ok(ParsedEmail {
        message_id: header(&headers, "message-id").and_then(message_id_token),
        date: parsed_message
            .as_ref()
            .and_then(|message| message.date())
            .and_then(|date| parse_mail_parser_date(date.to_rfc3339().as_str()))
            .or_else(|| header(&headers, "date").and_then(parse_rfc822_date)),
        from: header(&headers, "from").map_or_else(Vec::new, address_list),
        to: header(&headers, "to").map_or_else(Vec::new, address_list),
        cc: header(&headers, "cc").map_or_else(Vec::new, address_list),
        bcc: header(&headers, "bcc").map_or_else(Vec::new, address_list),
        subject: parsed_message
            .as_ref()
            .and_then(|message| message.subject())
            .map(str::to_string)
            .or_else(|| header(&headers, "subject").map(str::to_string)),
        in_reply_to: header(&headers, "in-reply-to").and_then(message_id_token),
        references: header(&headers, "references").map_or_else(Vec::new, references_list),
        list_id: header(&headers, "list-id").map(str::to_string),
        body_bytes: parsed_message
            .as_ref()
            .and_then(|message| message.body_text(0))
            .map_or_else(
                || body.as_bytes().len() as u64,
                |body| body.as_bytes().len() as u64,
            ),
        attachment_count: attachments.len().try_into().unwrap_or(u32::MAX),
        attachments,
    })
}

fn parse_mail_parser_date(value: &str) -> Option<Timestamp> {
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()?;
    Timestamp::from_unix_timestamp_nanos(parsed.unix_timestamp_nanos())
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

fn parsed_message_attachments(message: &mail_parser::Message<'_>) -> Vec<ParsedEmailAttachment> {
    message.attachments().map(mime_part_attachment).collect()
}

fn apply_attachment_header_fallbacks(
    attachments: &mut [ParsedEmailAttachment],
    header_attachments: &[ParsedEmailAttachment],
) {
    for (attachment, header_attachment) in attachments.iter_mut().zip(header_attachments) {
        attachment.content_type = enriched_attachment_value(
            attachment.content_type.take(),
            header_attachment.content_type.as_deref(),
        );
        if attachment.filename.is_none() {
            attachment.filename = header_attachment.filename.clone();
        }
        if attachment.content_id.is_none() {
            attachment.content_id = header_attachment.content_id.clone();
        }
    }
}

fn enriched_attachment_value(parsed: Option<String>, header: Option<&str>) -> Option<String> {
    match (parsed, header) {
        (Some(parsed), Some(header))
            if header.starts_with(&parsed) && header.len() > parsed.len() =>
        {
            Some(header.to_string())
        }
        (parsed @ Some(_), _) => parsed,
        (None, Some(header)) => Some(header.to_string()),
        (None, None) => None,
    }
}

fn mime_part_attachment(part: &MessagePart<'_>) -> ParsedEmailAttachment {
    ParsedEmailAttachment {
        disposition: part
            .content_disposition()
            .map(|disposition| disposition.ctype().to_string())
            .unwrap_or_else(|| "attachment".to_string()),
        filename: part.attachment_name().map(str::to_string),
        content_type: part.content_type().map(render_content_type),
        content_id: part.content_id().and_then(message_id_token),
    }
}

fn render_content_type(content_type: &ContentType<'_>) -> String {
    match content_type.subtype() {
        Some(subtype) => format!("{}/{}", content_type.ctype(), subtype),
        None => content_type.ctype().to_string(),
    }
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

fn metadata_str<'a>(record: &'a SourceRecord, key: &str) -> Option<&'a str> {
    record.metadata.get(key).and_then(serde_json::Value::as_str)
}

fn metadata_string(record: &SourceRecord, key: &str) -> Option<String> {
    metadata_str(record, key).map(str::to_string).or_else(|| {
        record
            .metadata
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .map(|value| value.to_string())
    })
}

fn required_metadata_string(record: &SourceRecord, key: &str) -> ParserResult<String> {
    metadata_string(record, key).ok_or_else(|| {
        ParserError::Parse(format!(
            "email provider record missing metadata field `{key}`"
        ))
    })
}

fn provider_cursor_occurrence_key(
    provider: EmailProviderKind,
    account_binding_ref: &str,
    mailbox_scope: Option<&str>,
    cursor_kind: EmailSyncCursorKind,
    gmail_history_id: Option<&str>,
    page_token: Option<&str>,
    uidvalidity: Option<&str>,
    uid: Option<&str>,
    highest_modseq: Option<&str>,
) -> OccurrenceKey {
    OccurrenceKey {
        source_id: SourceId::from_static("email.mailbox"),
        fields: vec![
            ("provider".to_string(), provider.as_str().to_string()),
            (
                "account_binding_ref".to_string(),
                account_binding_ref.to_string(),
            ),
            (
                "mailbox_scope".to_string(),
                mailbox_scope.unwrap_or("").to_string(),
            ),
            ("cursor_kind".to_string(), cursor_kind.as_str().to_string()),
            (
                "gmail_history_id".to_string(),
                gmail_history_id.unwrap_or("").to_string(),
            ),
            (
                "page_token".to_string(),
                page_token.unwrap_or("").to_string(),
            ),
            (
                "uidvalidity".to_string(),
                uidvalidity.unwrap_or("").to_string(),
            ),
            ("uid".to_string(), uid.unwrap_or("").to_string()),
            (
                "highest_modseq".to_string(),
                highest_modseq.unwrap_or("").to_string(),
            ),
        ],
    }
}

fn gmail_cursor_caveats(kind: GmailApiRecordKind) -> &'static [&'static str] {
    match kind {
        GmailApiRecordKind::Message | GmailApiRecordKind::History => &[
            "provider cursor is only committed after the adapter record is consumed",
            "Gmail message/body materialization is owned by the runtime client",
        ],
        GmailApiRecordKind::Cursor => &[
            "cursor checkpoint page contained no message/history records",
            "provider cursor is only committed after the adapter record is consumed",
        ],
        GmailApiRecordKind::Continuity => &[
            "Gmail history cursor is discontinuous and requires mailbox resync",
            "provider cursor discontinuity must appear as email sync debt",
        ],
    }
}

fn imap_cursor_caveats(
    kind: ImapSyncRecordKind,
    mode: Option<ImapSyncMode>,
) -> &'static [&'static str] {
    match (kind, mode) {
        (ImapSyncRecordKind::Cursor, _) => &[
            "cursor checkpoint batch contained no mailbox records",
            "UIDVALIDITY changes must be handled as continuity debt",
        ],
        (ImapSyncRecordKind::Continuity, _) => &[
            "IMAP UIDVALIDITY changed and old UID coordinates cannot be reused",
            "provider cursor discontinuity must appear as email sync debt",
        ],
        (ImapSyncRecordKind::IdleHeartbeat, Some(ImapSyncMode::Idle)) => &[
            "IDLE heartbeat updates runtime freshness without implying new messages",
            "UIDVALIDITY changes must be handled as continuity debt",
        ],
        _ => &[
            "provider cursor is only committed after the adapter record is consumed",
            "UIDVALIDITY changes must be handled as continuity debt",
        ],
    }
}

fn provider_record_continuity_state(payload: &serde_json::Value) -> EmailContinuityState {
    payload
        .get("continuity_state")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or(EmailContinuityState::Current)
}

fn provider_record_required_action(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("required_action")
        .and_then(serde_json::Value::as_str)
        .filter(|action| !action.trim().is_empty())
        .map(str::to_string)
}

fn provider_cursor_caveats(
    base: &'static [&'static str],
    payload: &serde_json::Value,
) -> Vec<String> {
    let mut caveats = base
        .iter()
        .map(|caveat| (*caveat).to_string())
        .collect::<Vec<_>>();
    if let Some(reason) = payload
        .get("continuity_reason")
        .and_then(serde_json::Value::as_str)
        .filter(|reason| !reason.trim().is_empty())
    {
        caveats.push(format!("provider continuity reason: {reason}"));
    }
    if let Some(action) = payload
        .get("required_action")
        .and_then(serde_json::Value::as_str)
        .filter(|action| !action.trim().is_empty())
    {
        caveats.push(format!("required provider action: {action}"));
    }
    caveats
}

fn imap_mode(record: &SourceRecord) -> Option<ImapSyncMode> {
    match metadata_str(record, "imap_mode")? {
        "scheduled" => Some(ImapSyncMode::Scheduled),
        "idle" => Some(ImapSyncMode::Idle),
        _ => None,
    }
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
mod tests;
