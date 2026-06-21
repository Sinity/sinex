//! Email mailbox parser regression tests.
//!
//! These live with the crate's source parser tests because they exercise the
//! public parser contract for common staged mailbox file formats.

use camino::Utf8PathBuf;
use sinex_primitives::{
    Uuid,
    ids::Id,
    parser::{MaterialAnchor, OccurrenceKey, ParserContext, SourceId, SourceRecord},
    temporal::Timestamp,
};
use sinexd::runtime::parser::MaterialParser;
use sinexd::sources::source_contracts::email::EmailMailboxParser;

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

fn occurrence_field<'a>(
    intent: &'a sinex_primitives::parser::ParsedEventIntent,
    key: &str,
) -> Option<&'a str> {
    let OccurrenceKey { fields, .. } = intent.occurrence_key.as_ref()?;
    fields
        .iter()
        .find_map(|(field, value)| (field == key).then_some(value.as_str()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn takeout_mbox_container_splits_messages_with_byte_range_identity() {
    let mut parser = EmailMailboxParser;
    let mbox = b"From sender@example.com Sat Jan 01 00:00:00 2022\n\
Message-ID: <takeout-1@example.com>\n\
Date: Sat, 01 Jan 2022 00:00:00 +0000\n\
From: Sender <sender@example.com>\n\
To: Receiver <receiver@example.com>\n\
Subject: First\n\
\n\
First body.\n\
From sender@example.com Sun Jan 02 00:00:00 2022\n\
Message-ID: <takeout-2@example.com>\n\
Date: Sun, 02 Jan 2022 00:00:00 +0000\n\
From: Sender <sender@example.com>\n\
To: Receiver <receiver@example.com>\n\
Subject: Second\n\
\n\
Second body.\n";
    let record = record_for(mbox, "Takeout/Mail/All mail Including Spam and Trash.mbox");

    let intents = parser
        .parse_record(record, &test_ctx())
        .await
        .expect("Takeout MBOX should parse");
    let messages: Vec<_> = intents
        .iter()
        .filter(|intent| intent.event_type.as_str() == "email.message.received")
        .collect();

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].payload["message_id"], "takeout-1@example.com");
    assert_eq!(messages[0].payload["mailbox_format"], "mbox-staged");
    assert_eq!(
        messages[0].payload["folder"],
        "All mail Including Spam and Trash"
    );
    assert_eq!(
        messages[0].payload["mbox_file"],
        "Takeout/Mail/All mail Including Spam and Trash.mbox"
    );
    assert_eq!(messages[0].payload["mbox_byte_start"], 49);
    assert_eq!(messages[1].payload["message_id"], "takeout-2@example.com");
    assert!(
        messages[1].payload["mbox_byte_start"]
            .as_u64()
            .expect("second mbox byte start should be numeric")
            > messages[0].payload["mbox_byte_start"]
                .as_u64()
                .expect("first mbox byte start should be numeric")
    );
    assert_eq!(
        occurrence_field(messages[0], "mbox_file"),
        Some("Takeout/Mail/All mail Including Spam and Trash.mbox")
    );
    assert_ne!(
        occurrence_field(messages[0], "mbox_byte_start"),
        occurrence_field(messages[1], "mbox_byte_start")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mboxrd_escaped_from_lines_do_not_split_messages() {
    let mut parser = EmailMailboxParser;
    let mbox = b"From sender@example.com Sat Jan 01 00:00:00 2022\n\
Message-ID: <mboxrd-1@example.com>\n\
From: Sender <sender@example.com>\n\
To: Receiver <receiver@example.com>\n\
Subject: Body from line\n\
\n\
>From this line is escaped body content, not a message delimiter.\n";
    let record = record_for(mbox, "Takeout/Mail/Inbox.mbox");

    let intents = parser
        .parse_record(record, &test_ctx())
        .await
        .expect("mboxrd body escaping should parse");
    let messages: Vec<_> = intents
        .iter()
        .filter(|intent| intent.event_type.as_str() == "email.message.received")
        .collect();

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload["message_id"], "mboxrd-1@example.com");
    assert_eq!(messages[0].payload["folder"], "Inbox");
}
