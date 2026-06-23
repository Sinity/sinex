//! Gmail API cursor adapter for scheduled email sync.
//!
//! This adapter owns Gmail page/cursor mechanics for `email.mailbox` without
//! owning OAuth secret lookup. Runtime executors provide a [`GmailApiClient`]
//! implementation; the adapter turns provider pages into bounded
//! [`SourceRecord`] streams with typed cursor metadata.

use std::{error::Error, fmt, future::Future, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::payloads::email::EMAIL_REQUIRED_ACTION_RESYNC_MAILBOX;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::runtime::parser::{InputShapeAdapter, ParserError, ParserResult};

const META_PROVIDER: &str = "provider";
const META_ACCOUNT_BINDING_REF: &str = "account_binding_ref";
const META_MAILBOX_SCOPE: &str = "mailbox_scope";
const META_GMAIL_RECORD_KIND: &str = "gmail_record_kind";
const META_GMAIL_MESSAGE_ID: &str = "gmail_message_id";
const META_GMAIL_THREAD_ID: &str = "gmail_thread_id";
const META_GMAIL_HISTORY_ID: &str = "gmail_history_id";
const META_GMAIL_PAGE_TOKEN_NEXT: &str = "gmail_page_token_next";
const META_GMAIL_PAGE_INDEX: &str = "gmail_page_index";
const META_GMAIL_RECORD_INDEX: &str = "gmail_record_index";
const DEFAULT_GMAIL_PAGE_SIZE: u32 = 100;

/// Configuration for [`GmailApiCursorAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GmailApiCursorConfig {
    /// Operator-owned provider/account binding. This is not a secret value.
    pub account_binding_ref: String,
    /// Gmail label, folder, or operator scope for this sync lane.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mailbox_scope: Option<String>,
    /// Optional first page token for a brand-new list walk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_page_token: Option<String>,
    /// Optional Gmail history id for delta sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_history_id: Option<String>,
    /// Gmail list/history page size requested by the runtime client.
    #[serde(default = "default_gmail_page_size")]
    pub page_size: u32,
    /// Gmail labels included in this sync lane.
    #[serde(default)]
    pub label_ids: Vec<String>,
    /// Whether this sync lane includes Spam and Trash.
    #[serde(default)]
    pub include_spam_trash: bool,
}

fn default_gmail_page_size() -> u32 {
    DEFAULT_GMAIL_PAGE_SIZE
}

/// Gmail cursor persisted after a consumed provider record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GmailApiCursor {
    pub page_token: Option<String>,
    pub history_id: Option<String>,
}

/// Request passed to the runtime-provided Gmail client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GmailApiPageRequest {
    pub account_binding_ref: String,
    pub mailbox_scope: Option<String>,
    pub page_token: Option<String>,
    pub history_id: Option<String>,
    pub page_size: u32,
    pub label_ids: Vec<String>,
    pub include_spam_trash: bool,
}

/// One page returned by a Gmail client implementation.
#[derive(Debug, Clone, PartialEq)]
pub struct GmailApiPage {
    pub records: Vec<GmailApiRecord>,
    pub next_page_token: Option<String>,
    pub history_id: Option<String>,
}

/// Gmail provider record kind emitted by the adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum GmailApiRecordKind {
    Message,
    History,
    Cursor,
    Continuity,
}

impl GmailApiRecordKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::History => "history",
            Self::Cursor => "cursor",
            Self::Continuity => "continuity",
        }
    }
}

/// Provider record serialized into `SourceRecord.bytes`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GmailApiRecord {
    pub kind: GmailApiRecordKind,
    pub message_id: Option<String>,
    pub thread_id: Option<String>,
    pub history_id: Option<String>,
    pub label_ids: Vec<String>,
    pub payload: JsonValue,
}

impl GmailApiRecord {
    #[must_use]
    pub fn cursor(history_id: Option<String>, page_token: Option<String>) -> Self {
        Self {
            kind: GmailApiRecordKind::Cursor,
            message_id: None,
            thread_id: None,
            history_id,
            label_ids: Vec::new(),
            payload: serde_json::json!({ "page_token": page_token }),
        }
    }

    #[must_use]
    pub fn continuity_gap(history_id: Option<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: GmailApiRecordKind::Continuity,
            message_id: None,
            thread_id: None,
            history_id,
            label_ids: Vec::new(),
            payload: serde_json::json!({
                "continuity_state": "gap",
                "continuity_reason": reason.into(),
                "required_action": EMAIL_REQUIRED_ACTION_RESYNC_MAILBOX,
            }),
        }
    }
}

/// Runtime-provided Gmail page client.
pub trait GmailApiClient: Send + Sync {
    type Error: Error + Send + Sync + 'static;

    fn fetch_page(
        &self,
        request: GmailApiPageRequest,
    ) -> impl Future<Output = Result<GmailApiPage, Self::Error>> + Send;
}

/// Reqwest-backed Gmail REST client for scheduled sync operations.
///
/// OAuth refresh and secret lookup stay outside the adapter. Callers pass a
/// bearer token that was read from an operator-owned secret file; the token is
/// never stored in emitted provider records.
#[derive(Clone)]
pub struct GmailHttpClient {
    http: reqwest::Client,
    api_base_url: String,
    user_id: String,
    bearer_token: String,
}

impl GmailHttpClient {
    #[must_use]
    pub fn new(bearer_token: String) -> Self {
        Self::with_endpoint(
            reqwest::Client::new(),
            "https://gmail.googleapis.com/gmail/v1".to_string(),
            "me".to_string(),
            bearer_token,
        )
    }

    #[must_use]
    pub fn with_endpoint(
        http: reqwest::Client,
        api_base_url: String,
        user_id: String,
        bearer_token: String,
    ) -> Self {
        Self {
            http,
            api_base_url: api_base_url.trim_end_matches('/').to_string(),
            user_id,
            bearer_token,
        }
    }

    fn user_url(&self, suffix: &str) -> String {
        format!(
            "{}/users/{}/{}",
            self.api_base_url,
            urlencoding::encode(&self.user_id),
            suffix.trim_start_matches('/')
        )
    }

    fn user_url_with_query(&self, suffix: &str, query: &[(&str, &str)]) -> String {
        if query.is_empty() {
            return self.user_url(suffix);
        }
        let query = query
            .iter()
            .map(|(key, value)| {
                format!(
                    "{}={}",
                    urlencoding::encode(key),
                    urlencoding::encode(value)
                )
            })
            .collect::<Vec<_>>()
            .join("&");
        format!("{}?{query}", self.user_url(suffix))
    }

    async fn fetch_json<T>(&self, request: reqwest::RequestBuilder) -> Result<T, GmailHttpError>
    where
        T: serde::de::DeserializeOwned,
    {
        let response = request
            .bearer_auth(&self.bearer_token)
            .send()
            .await
            .map_err(GmailHttpError::Transport)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GmailHttpError::Status { status, body });
        }
        response.json::<T>().await.map_err(GmailHttpError::Decode)
    }

    async fn fetch_message(&self, message_id: &str) -> Result<GmailRestMessage, GmailHttpError> {
        self.fetch_json(self.http.get(self.user_url_with_query(
            &format!("messages/{message_id}"),
            &[
                ("format", "metadata"),
                ("metadataHeaders", "Message-ID"),
                ("metadataHeaders", "Date"),
                ("metadataHeaders", "From"),
                ("metadataHeaders", "To"),
                ("metadataHeaders", "Cc"),
                ("metadataHeaders", "Bcc"),
                ("metadataHeaders", "Subject"),
                ("metadataHeaders", "In-Reply-To"),
                ("metadataHeaders", "References"),
                ("metadataHeaders", "List-Id"),
            ],
        )))
        .await
    }
}

#[derive(Debug)]
pub enum GmailHttpError {
    Transport(reqwest::Error),
    Decode(reqwest::Error),
    Status {
        status: reqwest::StatusCode,
        body: String,
    },
}

impl fmt::Display for GmailHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "Gmail transport error: {error}"),
            Self::Decode(error) => write!(f, "Gmail response decode error: {error}"),
            Self::Status { status, body } => {
                if body.trim().is_empty() {
                    write!(f, "Gmail API returned HTTP {status}")
                } else {
                    write!(f, "Gmail API returned HTTP {status}: {body}")
                }
            }
        }
    }
}

impl Error for GmailHttpError {}

impl GmailHttpError {
    fn is_history_gap(&self) -> bool {
        matches!(self, Self::Status { status, .. } if status.as_u16() == 404)
    }
}

impl GmailApiClient for GmailHttpClient {
    type Error = GmailHttpError;

    async fn fetch_page(&self, request: GmailApiPageRequest) -> Result<GmailApiPage, Self::Error> {
        if request.history_id.is_some() {
            let history_id = request.history_id.clone();
            return match self.fetch_history_page(request).await {
                Ok(page) => Ok(page),
                Err(error) if error.is_history_gap() => Ok(GmailApiPage {
                    records: vec![GmailApiRecord::continuity_gap(
                        history_id.clone(),
                        "gmail-history-id-expired-or-unavailable",
                    )],
                    next_page_token: None,
                    history_id,
                }),
                Err(error) => Err(error),
            };
        }
        self.fetch_message_page(request).await
    }
}

impl GmailHttpClient {
    async fn fetch_message_page(
        &self,
        request: GmailApiPageRequest,
    ) -> Result<GmailApiPage, GmailHttpError> {
        let max_results = request.page_size.to_string();
        let mut query = vec![("maxResults", max_results.as_str())];
        if let Some(page_token) = request.page_token.as_deref() {
            query.push(("pageToken", page_token));
        }
        for label in &request.label_ids {
            query.push(("labelIds", label));
        }
        if request.include_spam_trash {
            query.push(("includeSpamTrash", "true"));
        }
        let page: GmailRestMessageList = self
            .fetch_json(self.http.get(self.user_url_with_query("messages", &query)))
            .await?;
        let mut records = Vec::new();
        for listed in page.messages {
            let detail = self.fetch_message(&listed.id).await?;
            records.push(gmail_rest_message_record(detail));
        }
        Ok(GmailApiPage {
            records,
            next_page_token: page.next_page_token,
            history_id: page.history_id,
        })
    }

    async fn fetch_history_page(
        &self,
        request: GmailApiPageRequest,
    ) -> Result<GmailApiPage, GmailHttpError> {
        let history_id = request
            .history_id
            .as_deref()
            .expect("history page request should carry history id");
        let max_results = request.page_size.to_string();
        let mut query = vec![
            ("startHistoryId", history_id),
            ("maxResults", max_results.as_str()),
        ];
        if let Some(page_token) = request.page_token.as_deref() {
            query.push(("pageToken", page_token));
        }
        for label in &request.label_ids {
            query.push(("labelId", label));
        }
        let page: GmailRestHistoryList = self
            .fetch_json(self.http.get(self.user_url_with_query("history", &query)))
            .await?;
        let mut records = Vec::new();
        for history in page.history {
            let history_id = history.id.clone();
            for message in history.messages_added {
                records.push(gmail_history_record(
                    "message-added",
                    history_id.clone(),
                    message,
                ));
            }
            for message in history.messages_deleted {
                records.push(gmail_history_record(
                    "message-deleted",
                    history_id.clone(),
                    message,
                ));
            }
            for message in history.labels_added {
                records.push(gmail_history_record(
                    "labels-added",
                    history_id.clone(),
                    message,
                ));
            }
            for message in history.labels_removed {
                records.push(gmail_history_record(
                    "labels-removed",
                    history_id.clone(),
                    message,
                ));
            }
        }
        Ok(GmailApiPage {
            records,
            next_page_token: page.next_page_token,
            history_id: page.history_id,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GmailRestMessageList {
    #[serde(default)]
    messages: Vec<GmailRestMessageRef>,
    next_page_token: Option<String>,
    #[allow(dead_code)]
    result_size_estimate: Option<u64>,
    history_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GmailRestMessageRef {
    id: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GmailRestMessage {
    id: String,
    thread_id: Option<String>,
    #[serde(default)]
    label_ids: Vec<String>,
    snippet: Option<String>,
    history_id: Option<String>,
    internal_date: Option<String>,
    size_estimate: Option<u64>,
    payload: Option<JsonValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GmailRestHistoryList {
    #[serde(default)]
    history: Vec<GmailRestHistory>,
    next_page_token: Option<String>,
    history_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GmailRestHistory {
    id: Option<String>,
    #[serde(default)]
    messages_added: Vec<GmailRestHistoryMessage>,
    #[serde(default)]
    messages_deleted: Vec<GmailRestHistoryMessage>,
    #[serde(default)]
    labels_added: Vec<GmailRestHistoryMessage>,
    #[serde(default)]
    labels_removed: Vec<GmailRestHistoryMessage>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GmailRestHistoryMessage {
    message: GmailRestMessage,
}

fn gmail_rest_message_record(message: GmailRestMessage) -> GmailApiRecord {
    GmailApiRecord {
        kind: GmailApiRecordKind::Message,
        message_id: Some(message.id.clone()),
        thread_id: message.thread_id.clone(),
        history_id: message.history_id.clone(),
        label_ids: message.label_ids.clone(),
        payload: serde_json::to_value(message).unwrap_or(JsonValue::Null),
    }
}

fn gmail_history_record(
    change_kind: &'static str,
    history_id: Option<String>,
    message: GmailRestHistoryMessage,
) -> GmailApiRecord {
    let mut payload = serde_json::to_value(&message).unwrap_or(JsonValue::Null);
    if let JsonValue::Object(object) = &mut payload {
        object.insert(
            "history_change_kind".to_string(),
            serde_json::json!(change_kind),
        );
    }
    GmailApiRecord {
        kind: GmailApiRecordKind::History,
        message_id: Some(message.message.id),
        thread_id: message.message.thread_id,
        history_id,
        label_ids: message.message.label_ids,
        payload,
    }
}

/// Scheduled Gmail API adapter.
pub struct GmailApiCursorAdapter<C: GmailApiClient> {
    client: Arc<C>,
}

impl<C: GmailApiClient> GmailApiCursorAdapter<C> {
    #[must_use]
    pub fn new(client: C) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
}

#[async_trait]
impl<C> InputShapeAdapter for GmailApiCursorAdapter<C>
where
    C: GmailApiClient + 'static,
{
    type Config = GmailApiCursorConfig;
    type Cursor = GmailApiCursor;
    const KIND: InputShapeKind = InputShapeKind::ApiCursor;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let start_page_token = cursor
            .as_ref()
            .and_then(|cursor| cursor.page_token.clone())
            .or_else(|| config.initial_page_token.clone());
        let start_history_id = cursor
            .as_ref()
            .and_then(|cursor| cursor.history_id.clone())
            .or_else(|| config.initial_history_id.clone());
        let request_seed = GmailRequestSeed::from_config(config);
        let client = Arc::clone(&self.client);

        let pages = stream::unfold(
            Some((start_page_token, start_history_id, 0_u64)),
            move |state| {
                let client = Arc::clone(&client);
                let request_seed = request_seed.clone();
                async move {
                    let (page_token, history_id, page_index) = state?;
                    let request = request_seed.to_request(page_token.clone(), history_id.clone());
                    let page = match client.fetch_page(request).await {
                        Ok(page) => page,
                        Err(error) => {
                            return Some((
                                vec![Err(ParserError::Adapter(format!(
                                    "Gmail API fetch failed: {error}"
                                )))],
                                None,
                            ));
                        }
                    };

                    let next_page_token = page.next_page_token.clone();
                    let page_history_id = page.history_id.clone().or(history_id);
                    let mut records = page.records;
                    if records.is_empty()
                        && (page_history_id.is_some() || next_page_token.is_some())
                    {
                        records.push(GmailApiRecord::cursor(
                            page_history_id.clone(),
                            next_page_token.clone(),
                        ));
                    }
                    let total = records.len();
                    let emitted = records
                        .into_iter()
                        .enumerate()
                        .map(|(record_index, record)| {
                            let cursor_after = if record_index + 1 == total {
                                GmailApiCursor {
                                    page_token: next_page_token.clone(),
                                    history_id: page_history_id.clone(),
                                }
                            } else {
                                GmailApiCursor {
                                    page_token: page_token.clone(),
                                    history_id: page_history_id.clone(),
                                }
                            };
                            build_gmail_record(
                                material_id,
                                &request_seed,
                                page_index,
                                record_index as u64,
                                record,
                                &cursor_after,
                            )
                        })
                        .collect::<Vec<_>>();

                    let next_state = next_page_token
                        .map(|token| (Some(token), page_history_id, page_index.saturating_add(1)));

                    Some((emitted, next_state))
                }
            },
        )
        .flat_map(stream::iter);

        Ok(pages.boxed())
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(GmailApiCursor {
            page_token: record
                .metadata
                .get(META_GMAIL_PAGE_TOKEN_NEXT)
                .and_then(JsonValue::as_str)
                .map(str::to_owned),
            history_id: record
                .metadata
                .get(META_GMAIL_HISTORY_ID)
                .and_then(JsonValue::as_str)
                .map(str::to_owned),
        })
    }
}

#[derive(Debug, Clone)]
struct GmailRequestSeed {
    account_binding_ref: String,
    mailbox_scope: Option<String>,
    page_size: u32,
    label_ids: Vec<String>,
    include_spam_trash: bool,
}

impl GmailRequestSeed {
    fn from_config(config: &GmailApiCursorConfig) -> Self {
        Self {
            account_binding_ref: config.account_binding_ref.clone(),
            mailbox_scope: config.mailbox_scope.clone(),
            page_size: config.page_size,
            label_ids: config.label_ids.clone(),
            include_spam_trash: config.include_spam_trash,
        }
    }

    fn to_request(
        &self,
        page_token: Option<String>,
        history_id: Option<String>,
    ) -> GmailApiPageRequest {
        GmailApiPageRequest {
            account_binding_ref: self.account_binding_ref.clone(),
            mailbox_scope: self.mailbox_scope.clone(),
            page_token,
            history_id,
            page_size: self.page_size,
            label_ids: self.label_ids.clone(),
            include_spam_trash: self.include_spam_trash,
        }
    }
}

fn build_gmail_record(
    material_id: Id<SourceMaterial>,
    request_seed: &GmailRequestSeed,
    page_index: u64,
    record_index: u64,
    record: GmailApiRecord,
    cursor_after: &GmailApiCursor,
) -> ParserResult<SourceRecord> {
    let mut metadata = Map::new();
    metadata.insert(META_PROVIDER.to_string(), serde_json::json!("gmail"));
    metadata.insert(
        META_ACCOUNT_BINDING_REF.to_string(),
        serde_json::json!(&request_seed.account_binding_ref),
    );
    if let Some(scope) = &request_seed.mailbox_scope {
        metadata.insert(META_MAILBOX_SCOPE.to_string(), serde_json::json!(scope));
    }
    metadata.insert(
        META_GMAIL_RECORD_KIND.to_string(),
        serde_json::json!(record.kind.as_str()),
    );
    if let Some(message_id) = &record.message_id {
        metadata.insert(
            META_GMAIL_MESSAGE_ID.to_string(),
            serde_json::json!(message_id),
        );
    }
    if let Some(thread_id) = &record.thread_id {
        metadata.insert(
            META_GMAIL_THREAD_ID.to_string(),
            serde_json::json!(thread_id),
        );
    }
    if let Some(history_id) = &cursor_after.history_id {
        metadata.insert(
            META_GMAIL_HISTORY_ID.to_string(),
            serde_json::json!(history_id),
        );
    }
    if let Some(page_token) = &cursor_after.page_token {
        metadata.insert(
            META_GMAIL_PAGE_TOKEN_NEXT.to_string(),
            serde_json::json!(page_token),
        );
    }
    metadata.insert(
        META_GMAIL_PAGE_INDEX.to_string(),
        serde_json::json!(page_index),
    );
    metadata.insert(
        META_GMAIL_RECORD_INDEX.to_string(),
        serde_json::json!(record_index),
    );

    let bytes = serde_json::to_vec(&record).map_err(|error| {
        ParserError::Adapter(format!("failed to serialize Gmail API record: {error}"))
    })?;

    Ok(SourceRecord {
        material_id,
        anchor: MaterialAnchor::StreamFrame {
            material_offset: page_index,
            frame_index: record_index,
        },
        bytes,
        logical_path: Some(
            format!("gmail/{}/{}", request_seed.account_binding_ref, page_index).into(),
        ),
        source_ts_hint: None,
        metadata: JsonValue::Object(metadata),
    })
}
