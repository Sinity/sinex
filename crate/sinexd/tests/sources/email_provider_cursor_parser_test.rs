use camino::Utf8PathBuf;
use sinex_primitives::{
    Uuid,
    events::payloads::email::{
        EMAIL_REQUIRED_ACTION_RESYNC_MAILBOX, EmailAttachmentObservedPayload, EmailContinuityState,
        EmailProviderKind, EmailSyncCursorKind, EmailSyncCursorObservedPayload,
    },
    ids::Id,
    parser::{MaterialAnchor, OccurrenceKey, ParserContext, SourceId, SourceRecord},
    temporal::Timestamp,
};
use sinexd::{
    runtime::parser::{
        GmailApiRecord, GmailApiRecordKind, ImapSyncRecord, ImapSyncRecordKind, MaterialParser,
    },
    sources::source_contracts::email::EmailMailboxParser,
};
use xtask::sandbox::prelude::sinex_test;

fn test_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("email.mailbox"),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn occurrence_field<'a>(
    intent: &'a sinex_primitives::parser::ParsedEventIntent,
    key: &str,
) -> Option<&'a str> {
    let OccurrenceKey { fields, .. } = intent.occurrence_key.as_ref()?;
    fields
        .iter()
        .find_map(|(field, value)| (field == key).then_some(value.as_str()))
}

fn provider_record(
    bytes: Vec<u8>,
    logical_path: &str,
    metadata: serde_json::Value,
) -> SourceRecord {
    SourceRecord {
        material_id: Id::new(),
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        bytes,
        logical_path: Some(Utf8PathBuf::from(logical_path)),
        source_ts_hint: None,
        metadata,
    }
}

#[sinex_test]
async fn gmail_provider_record_emits_sync_cursor_observation() -> xtask::sandbox::TestResult<()> {
    let mut parser = EmailMailboxParser;
    let record = GmailApiRecord {
        kind: GmailApiRecordKind::History,
        message_id: Some("gmail-msg-1".to_string()),
        thread_id: Some("thread-1".to_string()),
        history_id: Some("101".to_string()),
        label_ids: vec!["INBOX".to_string()],
        payload: serde_json::json!({"id": "101"}),
    };
    let source_record = provider_record(
        serde_json::to_vec(&record)?,
        "gmail/operator-mailbox:primary/0",
        serde_json::json!({
            "provider": "gmail",
            "account_binding_ref": "operator-mailbox:primary",
            "mailbox_scope": "INBOX",
            "gmail_record_kind": "history",
            "gmail_history_id": "101",
        }),
    );

    let intents = parser.parse_record(source_record, &test_ctx()).await?;
    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(intent.event_type.as_str(), "email.sync_cursor.observed");
    assert_eq!(intent.payload["provider"], "gmail");
    assert_eq!(
        intent.payload["account_binding_ref"],
        "operator-mailbox:primary"
    );
    assert_eq!(intent.payload["mailbox_scope"], "INBOX");
    assert_eq!(intent.payload["cursor_kind"], "gmail-history-id");
    assert_eq!(intent.payload["gmail_history_id"], "101");
    assert_eq!(intent.payload["continuity_state"], "current");
    assert_eq!(occurrence_field(intent, "provider"), Some("gmail"));
    assert_eq!(
        occurrence_field(intent, "account_binding_ref"),
        Some("operator-mailbox:primary")
    );
    assert_eq!(
        occurrence_field(intent, "cursor_kind"),
        Some("gmail-history-id")
    );

    let payload: EmailSyncCursorObservedPayload = serde_json::from_value(intent.payload.clone())?;
    assert_eq!(payload.provider, EmailProviderKind::Gmail);
    assert_eq!(payload.cursor_kind, EmailSyncCursorKind::GmailHistoryId);
    assert_eq!(payload.gmail_history_id.as_deref(), Some("101"));
    Ok(())
}

#[sinex_test]
async fn rfc822_parser_decodes_mime_encoded_subjects() -> xtask::sandbox::TestResult<()> {
    let mut parser = EmailMailboxParser;
    let source_record = provider_record(
        b"Message-ID: <encoded-subject@example.com>\r\nDate: Tue, 14 Jan 2025 12:00:00 +0000\r\nFrom: Alice <alice@example.com>\r\nTo: Bob <bob@example.com>\r\nSubject: =?UTF-8?B?UXVhcnRlcmx5IHBsYW4=?=\r\n\r\nHello Bob.\r\n".to_vec(),
        "maildir/inbox/encoded-subject.eml",
        serde_json::json!({}),
    );

    let intents = parser.parse_record(source_record, &test_ctx()).await?;

    assert_eq!(intents[0].event_type.as_str(), "email.message.received");
    assert_eq!(intents[0].payload["subject"], "Quarterly plan");
    Ok(())
}

#[sinex_test]
async fn rfc822_parser_emits_mime_attachment_metadata() -> xtask::sandbox::TestResult<()> {
    let mut parser = EmailMailboxParser;
    let source_record = provider_record(
        b"Message-ID: <attachment@example.com>\r\n\
Date: Tue, 14 Jan 2025 12:00:00 +0000\r\n\
From: Alice <alice@example.com>\r\n\
To: Bob <bob@example.com>\r\n\
Subject: Attachment\r\n\
MIME-Version: 1.0\r\n\
Content-Type: multipart/mixed; boundary=\"sinex-boundary\"\r\n\
\r\n\
--sinex-boundary\r\n\
Content-Type: text/plain; charset=utf-8\r\n\
\r\n\
See attachment.\r\n\
--sinex-boundary\r\n\
Content-Type: application/pdf; name=\"quarterly-plan.pdf\"\r\n\
Content-Disposition: attachment; filename=\"quarterly-plan.pdf\"\r\n\
Content-ID: <plan-attachment@example.com>\r\n\
Content-Transfer-Encoding: base64\r\n\
\r\n\
JVBERi0xLjQK\r\n\
--sinex-boundary--\r\n"
            .to_vec(),
        "maildir/inbox/with-mime-attachment.eml",
        serde_json::json!({}),
    );

    let intents = parser.parse_record(source_record, &test_ctx()).await?;
    let message = intents
        .iter()
        .find(|intent| intent.event_type.as_str() == "email.message.received")
        .expect("message event should be emitted");
    let attachment = intents
        .iter()
        .find(|intent| intent.event_type.as_str() == "email.attachment.observed")
        .expect("attachment event should be emitted");

    assert_eq!(message.payload["attachment_count"], 1);
    assert_eq!(attachment.payload["filename"], "quarterly-plan.pdf");
    assert_eq!(attachment.payload["content_type"], "application/pdf");
    assert_eq!(
        attachment.payload["content_id"],
        "plan-attachment@example.com"
    );
    assert_eq!(attachment.payload["disposition"], "attachment");
    assert_eq!(
        occurrence_field(attachment, "content_id"),
        Some("plan-attachment@example.com")
    );

    let payload: EmailAttachmentObservedPayload =
        serde_json::from_value(attachment.payload.clone())?;
    assert_eq!(payload.filename.as_deref(), Some("quarterly-plan.pdf"));
    assert_eq!(payload.content_type.as_deref(), Some("application/pdf"));
    assert_eq!(
        payload.content_id.as_deref(),
        Some("plan-attachment@example.com")
    );
    Ok(())
}

#[sinex_test]
async fn rfc822_parser_accepts_non_utf8_message_bytes() -> xtask::sandbox::TestResult<()> {
    let mut parser = EmailMailboxParser;
    let mut bytes = b"Message-ID: <binary-body@example.com>\r\n\
Date: Tue, 14 Jan 2025 12:00:00 +0000\r\n\
From: Alice <alice@example.com>\r\n\
To: Bob <bob@example.com>\r\n\
Subject: Binary body\r\n\
\r\n\
plain prefix "
        .to_vec();
    bytes.extend_from_slice(&[0xff, 0xfe, 0xfd]);

    let source_record = provider_record(
        bytes,
        "maildir/inbox/binary-body.eml",
        serde_json::json!({}),
    );

    let intents = parser.parse_record(source_record, &test_ctx()).await?;

    let message = intents
        .iter()
        .find(|intent| intent.event_type.as_str() == "email.message.received")
        .expect("message event should be emitted");
    assert_eq!(message.payload["subject"], "Binary body");
    assert_eq!(message.payload["message_id"], "binary-body@example.com");
    Ok(())
}

#[sinex_test]
async fn imap_provider_record_emits_uidvalidity_cursor_observation()
-> xtask::sandbox::TestResult<()> {
    let mut parser = EmailMailboxParser;
    let record = ImapSyncRecord {
        kind: ImapSyncRecordKind::Message,
        uid: Some(41),
        message_id: Some("imap-message-41@example.com".to_string()),
        flags: vec!["\\Seen".to_string()],
        payload: serde_json::json!({"uid": 41}),
    };
    let source_record = provider_record(
        serde_json::to_vec(&record)?,
        "imap/operator-mailbox:imap-primary/INBOX/0",
        serde_json::json!({
            "provider": "imap",
            "account_binding_ref": "operator-mailbox:imap-primary",
            "mailbox": "INBOX",
            "imap_mode": "scheduled",
            "imap_record_kind": "message",
            "imap_uid_validity": 700,
            "imap_uid_next": 42,
            "imap_highest_modseq": 1200,
        }),
    );

    let intents = parser.parse_record(source_record, &test_ctx()).await?;
    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(intent.event_type.as_str(), "email.sync_cursor.observed");
    assert_eq!(intent.payload["provider"], "imap");
    assert_eq!(
        intent.payload["account_binding_ref"],
        "operator-mailbox:imap-primary"
    );
    assert_eq!(intent.payload["mailbox_scope"], "INBOX");
    assert_eq!(intent.payload["cursor_kind"], "imap-uidvalidity-uid");
    assert_eq!(intent.payload["cursor_value"], "700:42");
    assert_eq!(intent.payload["uidvalidity"], "700");
    assert_eq!(intent.payload["uid"], "42");
    assert_eq!(occurrence_field(intent, "provider"), Some("imap"));
    assert_eq!(
        occurrence_field(intent, "account_binding_ref"),
        Some("operator-mailbox:imap-primary")
    );
    assert_eq!(
        occurrence_field(intent, "cursor_kind"),
        Some("imap-uidvalidity-uid")
    );

    let payload: EmailSyncCursorObservedPayload = serde_json::from_value(intent.payload.clone())?;
    assert_eq!(payload.provider, EmailProviderKind::Imap);
    assert_eq!(payload.cursor_kind, EmailSyncCursorKind::ImapUidvalidityUid);
    assert_eq!(payload.uidvalidity.as_deref(), Some("700"));
    assert_eq!(payload.uid.as_deref(), Some("42"));
    Ok(())
}

#[sinex_test]
async fn gmail_history_gap_emits_gap_cursor_observation() -> xtask::sandbox::TestResult<()> {
    let mut parser = EmailMailboxParser;
    let record = GmailApiRecord::continuity_gap(
        Some("90".to_string()),
        "gmail-history-id-expired-or-unavailable",
    );
    let source_record = provider_record(
        serde_json::to_vec(&record)?,
        "gmail/operator-mailbox:primary/0",
        serde_json::json!({
            "provider": "gmail",
            "account_binding_ref": "operator-mailbox:primary",
            "mailbox_scope": "INBOX",
            "gmail_record_kind": "continuity",
            "gmail_history_id": "90",
        }),
    );

    let intents = parser.parse_record(source_record, &test_ctx()).await?;
    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(intent.event_type.as_str(), "email.sync_cursor.observed");
    assert_eq!(intent.payload["continuity_state"], "gap");
    assert_eq!(
        intent.payload["required_action"],
        EMAIL_REQUIRED_ACTION_RESYNC_MAILBOX
    );
    assert!(
        intent.payload["caveats"]
            .as_array()
            .expect("caveats array")
            .iter()
            .any(|caveat| caveat
                .as_str()
                .is_some_and(|text| text.contains("Gmail history cursor is discontinuous")))
    );

    let payload: EmailSyncCursorObservedPayload = serde_json::from_value(intent.payload.clone())?;
    assert_eq!(payload.provider, EmailProviderKind::Gmail);
    assert_eq!(payload.cursor_kind, EmailSyncCursorKind::GmailHistoryId);
    assert_eq!(payload.continuity_state, EmailContinuityState::Gap);
    assert_eq!(payload.gmail_history_id.as_deref(), Some("90"));
    Ok(())
}

#[sinex_test]
async fn imap_uidvalidity_reset_emits_gap_cursor_observation() -> xtask::sandbox::TestResult<()> {
    let mut parser = EmailMailboxParser;
    let record = ImapSyncRecord::uidvalidity_gap(700, 701, Some(2), Some(2000));
    let source_record = provider_record(
        serde_json::to_vec(&record)?,
        "imap/operator-mailbox:imap-primary/INBOX/0",
        serde_json::json!({
            "provider": "imap",
            "account_binding_ref": "operator-mailbox:imap-primary",
            "mailbox": "INBOX",
            "imap_mode": "scheduled",
            "imap_record_kind": "continuity",
            "imap_uid_validity": 701,
            "imap_uid_next": 2,
            "imap_highest_modseq": 2000,
        }),
    );

    let intents = parser.parse_record(source_record, &test_ctx()).await?;
    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(intent.event_type.as_str(), "email.sync_cursor.observed");
    assert_eq!(intent.payload["continuity_state"], "gap");
    assert_eq!(
        intent.payload["required_action"],
        EMAIL_REQUIRED_ACTION_RESYNC_MAILBOX
    );
    assert_eq!(intent.payload["cursor_value"], "701:2");
    assert!(
        intent.payload["caveats"]
            .as_array()
            .expect("caveats array")
            .iter()
            .any(|caveat| caveat
                .as_str()
                .is_some_and(|text| text.contains("old UID coordinates cannot be reused")))
    );

    let payload: EmailSyncCursorObservedPayload = serde_json::from_value(intent.payload.clone())?;
    assert_eq!(payload.provider, EmailProviderKind::Imap);
    assert_eq!(payload.cursor_kind, EmailSyncCursorKind::ImapUidvalidityUid);
    assert_eq!(payload.continuity_state, EmailContinuityState::Gap);
    assert_eq!(payload.uidvalidity.as_deref(), Some("701"));
    assert_eq!(payload.uid.as_deref(), Some("2"));
    Ok(())
}
