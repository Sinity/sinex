use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::payloads::email::EMAIL_REQUIRED_ACTION_RESYNC_MAILBOX;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeAdapter, MaterialAnchor};
use sinexd::runtime::parser::{
    GmailApiClient, GmailApiCursorAdapter, GmailApiCursorConfig, GmailApiPage, GmailApiPageRequest,
    GmailApiRecord, GmailApiRecordKind, GmailHttpClient, all_adapter_schemas,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

#[derive(Debug)]
struct FakeGmailError;

impl fmt::Display for FakeGmailError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fake Gmail error")
    }
}

impl Error for FakeGmailError {}

#[derive(Clone)]
struct FakeGmailClient {
    pages: Arc<Vec<GmailApiPage>>,
    requests: Arc<Mutex<Vec<GmailApiPageRequest>>>,
}

impl FakeGmailClient {
    fn new(pages: Vec<GmailApiPage>) -> Self {
        Self {
            pages: Arc::new(pages),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> Vec<GmailApiPageRequest> {
        self.requests.lock().expect("request mutex").clone()
    }
}

impl GmailApiClient for FakeGmailClient {
    type Error = FakeGmailError;

    async fn fetch_page(&self, request: GmailApiPageRequest) -> Result<GmailApiPage, Self::Error> {
        let page_index = request
            .page_token
            .as_deref()
            .and_then(|token| token.strip_prefix("page-"))
            .and_then(|token| token.parse::<usize>().ok())
            .map_or(0, |page| page.saturating_sub(1));
        self.requests.lock().expect("request mutex").push(request);
        Ok(self.pages.get(page_index).cloned().unwrap_or(GmailApiPage {
            records: Vec::new(),
            next_page_token: None,
            history_id: None,
        }))
    }
}

fn message_record(message_id: &str, thread_id: &str, history_id: &str) -> GmailApiRecord {
    GmailApiRecord {
        kind: GmailApiRecordKind::Message,
        message_id: Some(message_id.to_string()),
        thread_id: Some(thread_id.to_string()),
        history_id: Some(history_id.to_string()),
        label_ids: vec!["INBOX".to_string()],
        payload: serde_json::json!({
            "id": message_id,
            "threadId": thread_id,
            "historyId": history_id,
        }),
    }
}

fn history_record(history_id: &str, message_id: &str) -> GmailApiRecord {
    GmailApiRecord {
        kind: GmailApiRecordKind::History,
        message_id: Some(message_id.to_string()),
        thread_id: None,
        history_id: Some(history_id.to_string()),
        label_ids: Vec::new(),
        payload: serde_json::json!({
            "id": history_id,
            "messages": [{"id": message_id}],
        }),
    }
}

fn config() -> GmailApiCursorConfig {
    GmailApiCursorConfig {
        account_binding_ref: "operator-mailbox:primary".to_string(),
        mailbox_scope: Some("INBOX".to_string()),
        initial_page_token: None,
        initial_history_id: Some("90".to_string()),
        page_size: 50,
        label_ids: vec!["INBOX".to_string()],
        include_spam_trash: false,
    }
}

#[sinex_test]
async fn gmail_api_cursor_advances_page_token_and_history_id() -> xtask::sandbox::TestResult<()> {
    let client = FakeGmailClient::new(vec![
        GmailApiPage {
            records: vec![
                message_record("gmail-msg-1", "thread-1", "91"),
                message_record("gmail-msg-2", "thread-2", "92"),
            ],
            next_page_token: Some("page-2".to_string()),
            history_id: Some("100".to_string()),
        },
        GmailApiPage {
            records: vec![history_record("101", "gmail-msg-3")],
            next_page_token: None,
            history_id: Some("101".to_string()),
        },
    ]);
    let request_log = client.clone();
    let adapter = GmailApiCursorAdapter::new(client);

    let mut stream = adapter.open(dummy_material_id(), &config(), None).await?;
    let first = stream.next().await.expect("first Gmail record")?;
    let second = stream.next().await.expect("second Gmail record")?;
    let third = stream.next().await.expect("third Gmail record")?;
    assert!(stream.next().await.is_none());

    assert_eq!(first.metadata["provider"], "gmail");
    assert_eq!(
        first.metadata["account_binding_ref"],
        "operator-mailbox:primary"
    );
    assert_eq!(first.metadata["mailbox_scope"], "INBOX");
    assert_eq!(first.metadata["gmail_record_kind"], "message");
    assert_eq!(first.metadata["gmail_message_id"], "gmail-msg-1");
    assert!(matches!(
        first.anchor,
        MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0
        }
    ));

    let first_cursor = adapter.cursor_after(&first)?;
    assert_eq!(first_cursor.page_token, None);
    assert_eq!(first_cursor.history_id.as_deref(), Some("100"));

    let second_cursor = adapter.cursor_after(&second)?;
    assert_eq!(second_cursor.page_token.as_deref(), Some("page-2"));
    assert_eq!(second_cursor.history_id.as_deref(), Some("100"));

    let third_cursor = adapter.cursor_after(&third)?;
    assert_eq!(third_cursor.page_token, None);
    assert_eq!(third_cursor.history_id.as_deref(), Some("101"));

    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].page_token, None);
    assert_eq!(requests[0].history_id.as_deref(), Some("90"));
    assert_eq!(requests[0].page_size, 50);
    assert_eq!(requests[1].page_token.as_deref(), Some("page-2"));
    assert_eq!(requests[1].history_id.as_deref(), Some("100"));
    Ok(())
}

#[sinex_test]
async fn gmail_api_empty_history_page_emits_cursor_checkpoint() -> xtask::sandbox::TestResult<()> {
    let client = FakeGmailClient::new(vec![GmailApiPage {
        records: Vec::new(),
        next_page_token: None,
        history_id: Some("200".to_string()),
    }]);
    let adapter = GmailApiCursorAdapter::new(client);

    let mut stream = adapter.open(dummy_material_id(), &config(), None).await?;
    let checkpoint = stream.next().await.expect("cursor checkpoint")?;
    assert!(stream.next().await.is_none());

    assert_eq!(checkpoint.metadata["gmail_record_kind"], "cursor");
    assert_eq!(checkpoint.metadata["gmail_history_id"], "200");
    let record: GmailApiRecord = serde_json::from_slice(&checkpoint.bytes)?;
    assert_eq!(record.kind, GmailApiRecordKind::Cursor);

    let cursor = adapter.cursor_after(&checkpoint)?;
    assert_eq!(cursor.page_token, None);
    assert_eq!(cursor.history_id.as_deref(), Some("200"));
    Ok(())
}

#[sinex_test]
async fn gmail_api_history_gap_emits_continuity_record() -> xtask::sandbox::TestResult<()> {
    let client = FakeGmailClient::new(vec![GmailApiPage {
        records: vec![GmailApiRecord::continuity_gap(
            Some("90".to_string()),
            "gmail-history-id-expired-or-unavailable",
        )],
        next_page_token: None,
        history_id: Some("90".to_string()),
    }]);
    let adapter = GmailApiCursorAdapter::new(client);

    let mut stream = adapter.open(dummy_material_id(), &config(), None).await?;
    let continuity = stream.next().await.expect("continuity record")?;
    assert!(stream.next().await.is_none());

    assert_eq!(continuity.metadata["gmail_record_kind"], "continuity");
    assert_eq!(continuity.metadata["gmail_history_id"], "90");
    let record: GmailApiRecord = serde_json::from_slice(&continuity.bytes)?;
    assert_eq!(record.kind, GmailApiRecordKind::Continuity);
    assert_eq!(record.payload["continuity_state"], "gap");
    assert_eq!(
        record.payload["continuity_reason"],
        "gmail-history-id-expired-or-unavailable"
    );
    assert_eq!(
        record.payload["required_action"],
        EMAIL_REQUIRED_ACTION_RESYNC_MAILBOX
    );

    let cursor = adapter.cursor_after(&continuity)?;
    assert_eq!(cursor.page_token, None);
    assert_eq!(cursor.history_id.as_deref(), Some("90"));
    Ok(())
}

#[sinex_test]
async fn gmail_http_history_404_maps_to_continuity_record() -> xtask::sandbox::TestResult<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = [0_u8; 2048];
        let _ = stream.read(&mut request).await?;
        stream
            .write_all(
                b"HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: 22\r\n\r\n{\"error\":\"not found\"}\n",
            )
            .await?;
        stream.shutdown().await
    });
    let client = GmailHttpClient::with_endpoint(
        reqwest::Client::new(),
        endpoint,
        "me".to_string(),
        "test-token".to_string(),
    );

    let page = client
        .fetch_page(GmailApiPageRequest {
            account_binding_ref: "operator-mailbox:gmail-primary".to_string(),
            mailbox_scope: Some("INBOX".to_string()),
            page_token: None,
            history_id: Some("90".to_string()),
            page_size: 10,
            label_ids: vec!["INBOX".to_string()],
            include_spam_trash: false,
        })
        .await?;
    server.await??;

    assert_eq!(page.next_page_token, None);
    assert_eq!(page.history_id.as_deref(), Some("90"));
    assert_eq!(page.records.len(), 1);
    let record = &page.records[0];
    assert_eq!(record.kind, GmailApiRecordKind::Continuity);
    assert_eq!(record.history_id.as_deref(), Some("90"));
    assert_eq!(record.payload["continuity_state"], "gap");
    assert_eq!(
        record.payload["continuity_reason"],
        "gmail-history-id-expired-or-unavailable"
    );
    assert_eq!(
        record.payload["required_action"],
        EMAIL_REQUIRED_ACTION_RESYNC_MAILBOX
    );
    Ok(())
}

#[sinex_test]
async fn gmail_api_adapter_schema_exposes_provider_scope() -> xtask::sandbox::TestResult<()> {
    let schemas = all_adapter_schemas();
    let schema = schemas
        .get("GmailApiCursorAdapter")
        .expect("Gmail adapter schema should be registered");

    assert!(
        schema
            .required
            .iter()
            .any(|field| field == "account_binding_ref")
    );
    assert!(schema.schema.pointer("/properties/mailbox_scope").is_some());
    assert!(
        schema
            .schema
            .pointer("/properties/initial_history_id")
            .is_some()
    );
    assert!(schema.schema.pointer("/properties/page_size").is_some());
    Ok(())
}
