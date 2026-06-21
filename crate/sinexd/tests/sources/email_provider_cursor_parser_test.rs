use camino::Utf8PathBuf;
use sinex_primitives::{
    Uuid,
    events::payloads::email::{
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
