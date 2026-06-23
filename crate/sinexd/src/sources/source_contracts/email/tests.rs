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
async fn attachment_headers_emit_attachment_observation_without_fetching_bytes() -> TestResult<()> {
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
