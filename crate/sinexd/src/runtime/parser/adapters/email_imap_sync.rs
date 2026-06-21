//! IMAP sync adapter for scheduled and IDLE-backed email capture.
//!
//! This adapter owns IMAP mailbox cursor mechanics for `email.mailbox` without
//! owning TLS, credential lookup, or socket lifecycle. Runtime executors provide
//! an [`ImapSyncClient`] implementation; the adapter turns mailbox batches into
//! bounded [`SourceRecord`] streams with typed UID/UIDVALIDITY/MODSEQ cursors.

use std::{error::Error, future::Future, sync::Arc};

use async_trait::async_trait;
use camino::Utf8PathBuf;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::runtime::parser::{InputShapeAdapter, ParserError, ParserResult};

const META_PROVIDER: &str = "provider";
const META_ACCOUNT_BINDING_REF: &str = "account_binding_ref";
const META_MAILBOX: &str = "mailbox";
const META_IMAP_MODE: &str = "imap_mode";
const META_IMAP_RECORD_KIND: &str = "imap_record_kind";
const META_IMAP_UID_VALIDITY: &str = "imap_uid_validity";
const META_IMAP_UID_NEXT: &str = "imap_uid_next";
const META_IMAP_HIGHEST_MODSEQ: &str = "imap_highest_modseq";
const META_IMAP_BATCH_INDEX: &str = "imap_batch_index";
const META_IMAP_RECORD_INDEX: &str = "imap_record_index";
const DEFAULT_IMAP_BATCH_SIZE: u32 = 100;

/// IMAP acquisition mode represented by this adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ImapSyncMode {
    Scheduled,
    Idle,
}

impl ImapSyncMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Idle => "idle",
        }
    }
}

/// Configuration for [`ImapSyncAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ImapSyncConfig {
    /// Operator-owned provider/account binding. This is not a secret value.
    pub account_binding_ref: String,
    /// Mailbox selected by this sync lane, for example `INBOX`.
    pub mailbox: String,
    /// Scheduled polling or IDLE-backed mailbox update mode.
    #[serde(default = "default_imap_sync_mode")]
    pub mode: ImapSyncMode,
    /// First UID requested when no stored cursor exists.
    #[serde(default)]
    pub initial_uid_next: Option<u32>,
    /// Known UIDVALIDITY for the mailbox, if the operator has already stored it.
    #[serde(default)]
    pub initial_uid_validity: Option<u32>,
    /// Known HIGHESTMODSEQ for CONDSTORE/QRESYNC-aware clients.
    #[serde(default)]
    pub initial_highest_modseq: Option<u64>,
    /// Maximum records requested per mailbox batch.
    #[serde(default = "default_imap_batch_size")]
    pub batch_size: u32,
    /// Whether raw RFC822 bodies are fetched by the runtime client.
    #[serde(default)]
    pub fetch_bodies: bool,
    /// Whether attachment body parts may be materialized by the runtime client.
    #[serde(default)]
    pub fetch_attachments: bool,
}

fn default_imap_sync_mode() -> ImapSyncMode {
    ImapSyncMode::Scheduled
}

fn default_imap_batch_size() -> u32 {
    DEFAULT_IMAP_BATCH_SIZE
}

/// IMAP cursor persisted after a consumed mailbox record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImapSyncCursor {
    pub uid_validity: Option<u32>,
    pub uid_next: Option<u32>,
    pub highest_modseq: Option<u64>,
}

/// Request passed to the runtime-provided IMAP client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImapSyncRequest {
    pub account_binding_ref: String,
    pub mailbox: String,
    pub mode: ImapSyncMode,
    pub uid_validity: Option<u32>,
    pub uid_next: Option<u32>,
    pub highest_modseq: Option<u64>,
    pub batch_size: u32,
    pub fetch_bodies: bool,
    pub fetch_attachments: bool,
}

/// One mailbox batch returned by an IMAP client implementation.
#[derive(Debug, Clone, PartialEq)]
pub struct ImapSyncBatch {
    pub records: Vec<ImapSyncRecord>,
    pub uid_validity: Option<u32>,
    pub uid_next: Option<u32>,
    pub highest_modseq: Option<u64>,
    pub has_more: bool,
}

/// IMAP provider record kind emitted by the adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ImapSyncRecordKind {
    Message,
    Flags,
    Expunge,
    IdleHeartbeat,
    Cursor,
}

impl ImapSyncRecordKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Flags => "flags",
            Self::Expunge => "expunge",
            Self::IdleHeartbeat => "idle-heartbeat",
            Self::Cursor => "cursor",
        }
    }
}

/// Provider record serialized into `SourceRecord.bytes`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImapSyncRecord {
    pub kind: ImapSyncRecordKind,
    pub uid: Option<u32>,
    pub message_id: Option<String>,
    pub flags: Vec<String>,
    pub payload: JsonValue,
}

impl ImapSyncRecord {
    #[must_use]
    pub fn cursor(uid_validity: Option<u32>, uid_next: Option<u32>, modseq: Option<u64>) -> Self {
        Self {
            kind: ImapSyncRecordKind::Cursor,
            uid: uid_next,
            message_id: None,
            flags: Vec::new(),
            payload: serde_json::json!({
                "uid_validity": uid_validity,
                "uid_next": uid_next,
                "highest_modseq": modseq,
            }),
        }
    }
}

/// Runtime-provided IMAP batch client.
pub trait ImapSyncClient: Send + Sync {
    type Error: Error + Send + Sync + 'static;

    fn fetch_batch(
        &self,
        request: ImapSyncRequest,
    ) -> impl Future<Output = Result<ImapSyncBatch, Self::Error>> + Send;
}

/// Scheduled/IDLE IMAP adapter.
pub struct ImapSyncAdapter<C: ImapSyncClient> {
    client: Arc<C>,
}

impl<C: ImapSyncClient> ImapSyncAdapter<C> {
    #[must_use]
    pub fn new(client: C) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
}

#[async_trait]
impl<C> InputShapeAdapter for ImapSyncAdapter<C>
where
    C: ImapSyncClient + 'static,
{
    type Config = ImapSyncConfig;
    type Cursor = ImapSyncCursor;
    const KIND: InputShapeKind = InputShapeKind::ApiCursor;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let start_cursor = cursor.unwrap_or_else(|| ImapSyncCursor {
            uid_validity: config.initial_uid_validity,
            uid_next: config.initial_uid_next,
            highest_modseq: config.initial_highest_modseq,
        });
        let request_seed = ImapRequestSeed::from_config(config);
        let client = Arc::clone(&self.client);

        let batches = stream::unfold(Some((start_cursor, 0_u64)), move |state| {
            let client = Arc::clone(&client);
            let request_seed = request_seed.clone();
            async move {
                let (cursor, batch_index) = state?;
                let request = request_seed.to_request(&cursor);
                let batch = match client.fetch_batch(request).await {
                    Ok(batch) => batch,
                    Err(error) => {
                        return Some((
                            vec![Err(ParserError::Adapter(format!(
                                "IMAP sync fetch failed: {error}"
                            )))],
                            None,
                        ));
                    }
                };

                let batch_cursor = ImapSyncCursor {
                    uid_validity: batch.uid_validity.or(cursor.uid_validity),
                    uid_next: batch.uid_next.or(cursor.uid_next),
                    highest_modseq: batch.highest_modseq.or(cursor.highest_modseq),
                };
                let mut records = batch.records;
                if records.is_empty()
                    && (batch_cursor.uid_validity.is_some()
                        || batch_cursor.uid_next.is_some()
                        || batch_cursor.highest_modseq.is_some())
                {
                    records.push(ImapSyncRecord::cursor(
                        batch_cursor.uid_validity,
                        batch_cursor.uid_next,
                        batch_cursor.highest_modseq,
                    ));
                }
                let total = records.len();
                let emitted = records
                    .into_iter()
                    .enumerate()
                    .map(|(record_index, record)| {
                        let cursor_after = if record_index + 1 == total {
                            batch_cursor.clone()
                        } else {
                            cursor.clone()
                        };
                        build_imap_record(
                            material_id,
                            &request_seed,
                            batch_index,
                            record_index as u64,
                            record,
                            &cursor_after,
                        )
                    })
                    .collect::<Vec<_>>();

                let next_state = batch
                    .has_more
                    .then(|| (batch_cursor, batch_index.saturating_add(1)));

                Some((emitted, next_state))
            }
        })
        .flat_map(stream::iter);

        Ok(batches.boxed())
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(ImapSyncCursor {
            uid_validity: record
                .metadata
                .get(META_IMAP_UID_VALIDITY)
                .and_then(JsonValue::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            uid_next: record
                .metadata
                .get(META_IMAP_UID_NEXT)
                .and_then(JsonValue::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            highest_modseq: record
                .metadata
                .get(META_IMAP_HIGHEST_MODSEQ)
                .and_then(JsonValue::as_u64),
        })
    }
}

#[derive(Debug, Clone)]
struct ImapRequestSeed {
    account_binding_ref: String,
    mailbox: String,
    mode: ImapSyncMode,
    batch_size: u32,
    fetch_bodies: bool,
    fetch_attachments: bool,
}

impl ImapRequestSeed {
    fn from_config(config: &ImapSyncConfig) -> Self {
        Self {
            account_binding_ref: config.account_binding_ref.clone(),
            mailbox: config.mailbox.clone(),
            mode: config.mode,
            batch_size: config.batch_size,
            fetch_bodies: config.fetch_bodies,
            fetch_attachments: config.fetch_attachments,
        }
    }

    fn to_request(&self, cursor: &ImapSyncCursor) -> ImapSyncRequest {
        ImapSyncRequest {
            account_binding_ref: self.account_binding_ref.clone(),
            mailbox: self.mailbox.clone(),
            mode: self.mode,
            uid_validity: cursor.uid_validity,
            uid_next: cursor.uid_next,
            highest_modseq: cursor.highest_modseq,
            batch_size: self.batch_size,
            fetch_bodies: self.fetch_bodies,
            fetch_attachments: self.fetch_attachments,
        }
    }
}

fn build_imap_record(
    material_id: Id<SourceMaterial>,
    request_seed: &ImapRequestSeed,
    batch_index: u64,
    record_index: u64,
    record: ImapSyncRecord,
    cursor_after: &ImapSyncCursor,
) -> ParserResult<SourceRecord> {
    let mut metadata = Map::new();
    metadata.insert(META_PROVIDER.to_string(), serde_json::json!("imap"));
    metadata.insert(
        META_ACCOUNT_BINDING_REF.to_string(),
        serde_json::json!(&request_seed.account_binding_ref),
    );
    metadata.insert(META_MAILBOX.to_string(), serde_json::json!(&request_seed.mailbox));
    metadata.insert(
        META_IMAP_MODE.to_string(),
        serde_json::json!(request_seed.mode.as_str()),
    );
    metadata.insert(
        META_IMAP_RECORD_KIND.to_string(),
        serde_json::json!(record.kind.as_str()),
    );
    if let Some(uid_validity) = cursor_after.uid_validity {
        metadata.insert(
            META_IMAP_UID_VALIDITY.to_string(),
            serde_json::json!(uid_validity),
        );
    }
    if let Some(uid_next) = cursor_after.uid_next {
        metadata.insert(META_IMAP_UID_NEXT.to_string(), serde_json::json!(uid_next));
    }
    if let Some(highest_modseq) = cursor_after.highest_modseq {
        metadata.insert(
            META_IMAP_HIGHEST_MODSEQ.to_string(),
            serde_json::json!(highest_modseq),
        );
    }
    metadata.insert(
        META_IMAP_BATCH_INDEX.to_string(),
        serde_json::json!(batch_index),
    );
    metadata.insert(
        META_IMAP_RECORD_INDEX.to_string(),
        serde_json::json!(record_index),
    );

    let bytes = serde_json::to_vec(&record).map_err(|error| {
        ParserError::Adapter(format!("failed to serialize IMAP sync record: {error}"))
    })?;

    Ok(SourceRecord {
        material_id,
        anchor: MaterialAnchor::StreamFrame {
            material_offset: batch_index,
            frame_index: record_index,
        },
        bytes,
        logical_path: Some(Utf8PathBuf::from(format!(
            "imap/{}/{}/{}",
            request_seed.account_binding_ref, request_seed.mailbox, batch_index
        ))),
        source_ts_hint: None,
        metadata: JsonValue::Object(metadata),
    })
}
