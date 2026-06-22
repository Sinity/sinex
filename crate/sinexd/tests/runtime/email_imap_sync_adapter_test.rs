use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeAdapter, MaterialAnchor};
use sinexd::runtime::parser::{
    ImapSyncAdapter, ImapSyncBatch, ImapSyncClient, ImapSyncConfig, ImapSyncMode, ImapSyncRecord,
    ImapSyncRecordKind, ImapSyncRequest, all_adapter_schemas,
};
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

#[derive(Debug)]
struct FakeImapError;

impl fmt::Display for FakeImapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fake IMAP error")
    }
}

impl Error for FakeImapError {}

#[derive(Clone)]
struct FakeImapClient {
    batches: Arc<Vec<ImapSyncBatch>>,
    requests: Arc<Mutex<Vec<ImapSyncRequest>>>,
}

impl FakeImapClient {
    fn new(batches: Vec<ImapSyncBatch>) -> Self {
        Self {
            batches: Arc::new(batches),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> Vec<ImapSyncRequest> {
        self.requests.lock().expect("request mutex").clone()
    }
}

impl ImapSyncClient for FakeImapClient {
    type Error = FakeImapError;

    async fn fetch_batch(&self, request: ImapSyncRequest) -> Result<ImapSyncBatch, Self::Error> {
        let batch_index = self.requests.lock().expect("request mutex").len();
        self.requests.lock().expect("request mutex").push(request);
        Ok(self
            .batches
            .get(batch_index)
            .cloned()
            .unwrap_or(ImapSyncBatch {
                records: Vec::new(),
                uid_validity: None,
                uid_next: None,
                highest_modseq: None,
                has_more: false,
            }))
    }
}

fn config(mode: ImapSyncMode) -> ImapSyncConfig {
    ImapSyncConfig {
        account_binding_ref: "operator-mailbox:imap-primary".to_string(),
        mailbox: "INBOX".to_string(),
        mode,
        initial_uid_next: Some(40),
        initial_uid_validity: Some(700),
        initial_highest_modseq: Some(1000),
        batch_size: 25,
        fetch_bodies: true,
        fetch_attachments: false,
    }
}

fn message_record(uid: u32, message_id: &str) -> ImapSyncRecord {
    ImapSyncRecord {
        kind: ImapSyncRecordKind::Message,
        uid: Some(uid),
        message_id: Some(message_id.to_string()),
        flags: vec!["\\Seen".to_string()],
        payload: serde_json::json!({
            "uid": uid,
            "message_id": message_id,
        }),
    }
}

fn flags_record(uid: u32, flags: &[&str]) -> ImapSyncRecord {
    ImapSyncRecord {
        kind: ImapSyncRecordKind::Flags,
        uid: Some(uid),
        message_id: None,
        flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
        payload: serde_json::json!({
            "uid": uid,
            "flags": flags,
        }),
    }
}

#[sinex_test]
async fn imap_scheduled_sync_advances_uid_and_modseq() -> xtask::sandbox::TestResult<()> {
    let client = FakeImapClient::new(vec![
        ImapSyncBatch {
            records: vec![
                message_record(40, "<imap-40@example.com>"),
                message_record(41, "<imap-41@example.com>"),
            ],
            uid_validity: Some(700),
            uid_next: Some(42),
            highest_modseq: Some(1100),
            has_more: true,
        },
        ImapSyncBatch {
            records: vec![flags_record(41, &["\\Seen", "\\Flagged"])],
            uid_validity: Some(700),
            uid_next: Some(43),
            highest_modseq: Some(1200),
            has_more: false,
        },
    ]);
    let request_log = client.clone();
    let adapter = ImapSyncAdapter::new(client);

    let mut stream = adapter
        .open(dummy_material_id(), &config(ImapSyncMode::Scheduled), None)
        .await?;
    let first = stream.next().await.expect("first IMAP record")?;
    let second = stream.next().await.expect("second IMAP record")?;
    let third = stream.next().await.expect("third IMAP record")?;
    assert!(stream.next().await.is_none());

    assert_eq!(first.metadata["provider"], "imap");
    assert_eq!(
        first.metadata["account_binding_ref"],
        "operator-mailbox:imap-primary"
    );
    assert_eq!(first.metadata["mailbox"], "INBOX");
    assert_eq!(first.metadata["imap_mode"], "scheduled");
    assert_eq!(first.metadata["imap_record_kind"], "message");
    assert!(matches!(
        first.anchor,
        MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0
        }
    ));

    let first_cursor = adapter.cursor_after(&first)?;
    assert_eq!(first_cursor.uid_validity, Some(700));
    assert_eq!(first_cursor.uid_next, Some(40));
    assert_eq!(first_cursor.highest_modseq, Some(1000));

    let second_cursor = adapter.cursor_after(&second)?;
    assert_eq!(second_cursor.uid_validity, Some(700));
    assert_eq!(second_cursor.uid_next, Some(42));
    assert_eq!(second_cursor.highest_modseq, Some(1100));

    let third_cursor = adapter.cursor_after(&third)?;
    assert_eq!(third_cursor.uid_validity, Some(700));
    assert_eq!(third_cursor.uid_next, Some(43));
    assert_eq!(third_cursor.highest_modseq, Some(1200));

    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].uid_validity, Some(700));
    assert_eq!(requests[0].uid_next, Some(40));
    assert_eq!(requests[0].highest_modseq, Some(1000));
    assert_eq!(requests[0].batch_size, 25);
    assert!(requests[0].fetch_bodies);
    assert!(!requests[0].fetch_attachments);
    assert_eq!(requests[1].uid_next, Some(42));
    assert_eq!(requests[1].highest_modseq, Some(1100));
    Ok(())
}

#[sinex_test]
async fn imap_idle_empty_batch_emits_cursor_checkpoint() -> xtask::sandbox::TestResult<()> {
    let client = FakeImapClient::new(vec![ImapSyncBatch {
        records: Vec::new(),
        uid_validity: Some(701),
        uid_next: Some(99),
        highest_modseq: Some(2000),
        has_more: false,
    }]);
    let adapter = ImapSyncAdapter::new(client);

    let mut stream = adapter
        .open(dummy_material_id(), &config(ImapSyncMode::Idle), None)
        .await?;
    let checkpoint = stream.next().await.expect("cursor checkpoint")?;
    assert!(stream.next().await.is_none());

    assert_eq!(checkpoint.metadata["imap_mode"], "idle");
    assert_eq!(checkpoint.metadata["imap_record_kind"], "cursor");
    assert_eq!(checkpoint.metadata["imap_uid_validity"], 701);
    assert_eq!(checkpoint.metadata["imap_uid_next"], 99);
    assert_eq!(checkpoint.metadata["imap_highest_modseq"], 2000);
    let record: ImapSyncRecord = serde_json::from_slice(&checkpoint.bytes)?;
    assert_eq!(record.kind, ImapSyncRecordKind::Cursor);

    let cursor = adapter.cursor_after(&checkpoint)?;
    assert_eq!(cursor.uid_validity, Some(701));
    assert_eq!(cursor.uid_next, Some(99));
    assert_eq!(cursor.highest_modseq, Some(2000));
    Ok(())
}

#[sinex_test]
async fn imap_sync_adapter_schema_exposes_mailbox_mode_and_fetch_policy()
-> xtask::sandbox::TestResult<()> {
    let schemas = all_adapter_schemas();
    let schema = schemas
        .get("ImapSyncAdapter")
        .expect("IMAP adapter schema should be registered");

    assert!(
        schema
            .required
            .iter()
            .any(|field| field == "account_binding_ref")
    );
    assert!(schema.required.iter().any(|field| field == "mailbox"));
    assert!(schema.schema.pointer("/properties/mode").is_some());
    assert!(
        schema
            .schema
            .pointer("/properties/initial_uid_next")
            .is_some()
    );
    assert!(schema.schema.pointer("/properties/fetch_bodies").is_some());
    assert!(
        schema
            .schema
            .pointer("/properties/fetch_attachments")
            .is_some()
    );
    Ok(())
}
