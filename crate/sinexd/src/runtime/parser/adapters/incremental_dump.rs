//! Incremental-dump adapter — periodically re-exported full dumps.
//!
//! Many providers ship the same data as a *full* export each time (GDPR /
//! Takeout archives: Spotify streaming history, Goodreads, Reddit). Each export
//! is a superset of the previous one. Re-importing the whole dump every time
//! would re-create every record; this adapter emits only the records that are
//! new since the last run.
//!
//! A [`DumpLoader`] loads the current full export parsed into JSON records. Each
//! record carries a configured *order key* (e.g. an ISO-8601 timestamp). The
//! adapter sorts records by that key and emits only those strictly greater than
//! the high-water mark from the prior run.
//!
//! # Cursor semantics
//!
//! The cursor ([`IncrementalDumpCursor`]) is the highest **(order key, content
//! hash)** position consumed so far. The content hash is a BLAKE3 digest of the
//! record's canonical JSON, used purely as a tie-breaker so that records sharing
//! an order key (non-unique timestamps are common in GDPR/Takeout exports) are
//! still fully ordered and *none are dropped*. The position is reconstructable
//! from a single record's metadata, so the per-record
//! [`cursor_after`][InputShapeAdapter::cursor_after] contract stays O(1) (a
//! growing "seen-set" cursor would not).
//!
//! - `cursor = None` or `high_water = None` → brand-new source: emit everything.
//! - `cursor.high_water = Some(pos)` → emit only records whose `(order_key,
//!   content_hash)` is strictly greater than `pos`.
//!
//! The order key SHOULD be monotonic across exports (timestamps, ULIDs,
//! zero-padded sequence ids, or a `timestamp+id` composite) so the high-water
//! mark advances in append order. It need **not** be unique — the content-hash
//! tie-breaker disambiguates ties, which is the fix for the original data-loss
//! bug where records sharing a timestamp were dropped (Codex review, PR #1776).
//! Comparison is string-wise, so unpadded decimal ids (`"9" > "10"`) still
//! misorder — pad them.
//!
//! The only residual ambiguity: two records that are byte-identical *and* share
//! an order key map to the same composite position, so if a run is interrupted
//! after consuming one of them, an identical sibling is skipped on resume. That
//! is correct under the object-level dedup model — identical content is the same
//! occurrence, and cross-record dedup is a downstream concern, not the adapter's.
//!
//! [`ApiCursorPosition`]: super::ApiCursorPosition

use std::{error::Error, fmt, future::Future, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::runtime::parser::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// DumpLoader
// =============================================================================

/// Loads the current full export as a flat list of JSON records.
///
/// Implementors own the acquisition + parse of the provider's archive (read a
/// file, unzip, parse CSV/JSON, etc.) and return one [`JsonValue`] per record.
/// The adapter handles dedup against the prior high-water mark.
pub trait DumpLoader: Send + Sync {
    /// Error type returned by [`load`](DumpLoader::load).
    type Error: Error + Send + Sync + 'static;

    /// Load the current full export.
    ///
    /// Declared as `-> impl Future + Send` (rather than `async fn`) so the
    /// returned future is `Send` — required because the adapter awaits it inside
    /// an `#[async_trait]` (boxed-`Send`) `open`. Impls may still use `async fn`.
    fn load(&self) -> impl Future<Output = Result<Vec<JsonValue>, Self::Error>> + Send;
}

// =============================================================================
// Errors
// =============================================================================

/// Errors raised while walking an incremental dump.
#[derive(Debug)]
pub enum IncrementalDumpError {
    /// The [`DumpLoader`] failed to produce the export.
    Load(String),
    /// A record was missing the configured order-key field.
    MissingOrderKey {
        /// The configured field name that was absent.
        field: String,
    },
}

impl fmt::Display for IncrementalDumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load(msg) => write!(f, "incremental dump load failed: {msg}"),
            Self::MissingOrderKey { field } => {
                write!(f, "record missing order-key field `{field}`")
            }
        }
    }
}

impl Error for IncrementalDumpError {}

// =============================================================================
// Config and cursor
// =============================================================================

/// Configuration for [`IncrementalDumpAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IncrementalDumpConfig {
    /// Name of the top-level field in each record that holds a lexicographically
    /// monotonic order key (e.g. a ULID or a timestamp at sufficient
    /// resolution). Records are emitted in ascending `(order_key, content_hash)`
    /// order; only positions strictly greater than the prior high-water mark are
    /// emitted. The key need not be unique — a BLAKE3 content-hash tie-breaker
    /// disambiguates records that share a key (see the module docs). Required —
    /// there is no sensible default.
    pub order_key_field: String,
}

/// A consumed position: an order key plus a content-hash tie-breaker.
///
/// Ordering is the field-declaration tuple order — `order_key` first, then
/// `content_hash` — so two records with the same order key are still totally
/// ordered by content.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IncrementalDumpPosition {
    /// The record's order key (value of the configured `order_key_field`).
    pub order_key: String,
    /// BLAKE3 hex digest of the record's canonical JSON — the tie-breaker that
    /// keeps records sharing an `order_key` distinct and ordered.
    pub content_hash: String,
}

/// Cursor for [`IncrementalDumpAdapter`] — the highest position consumed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalDumpCursor {
    /// The highest `(order_key, content_hash)` position emitted so far.
    ///
    /// `None` means nothing has been consumed yet (brand-new source); the next
    /// `open` emits the entire dump.
    pub high_water: Option<IncrementalDumpPosition>,
}

// =============================================================================
// Metadata keys embedded in SourceRecord
// =============================================================================

const META_ORDER_KEY: &str = "incremental_dump_order_key";
const META_CONTENT_HASH: &str = "incremental_dump_content_hash";
const META_DUMP_INDEX: &str = "incremental_dump_index";

fn build_record_metadata(order_key: &str, content_hash: &str, dump_index: u64) -> JsonValue {
    let mut map = Map::new();
    map.insert(
        META_ORDER_KEY.to_owned(),
        JsonValue::String(order_key.to_owned()),
    );
    map.insert(
        META_CONTENT_HASH.to_owned(),
        JsonValue::String(content_hash.to_owned()),
    );
    map.insert(
        META_DUMP_INDEX.to_owned(),
        JsonValue::Number(dump_index.into()),
    );
    JsonValue::Object(map)
}

/// Extract the order key from a record as a string. JSON strings are used
/// verbatim; other scalars are stringified (callers should prefer string
/// timestamps/ids — see the module-level lexical-ordering caveat).
fn extract_order_key(record: &JsonValue, field: &str) -> Option<String> {
    record.get(field).map(|value| match value {
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    })
}

// =============================================================================
// IncrementalDumpAdapter
// =============================================================================

/// Input-shape adapter for periodically re-exported full dumps.
///
/// Loads the current full export via the injected [`DumpLoader`], sorts records
/// by the configured order key, and emits only the records new since the prior
/// run's high-water mark.
///
/// # Anchor
///
/// Each record's anchor is [`MaterialAnchor::StreamFrame`] with
/// `material_offset = 0` and `frame_index = position in the sorted dump`. The
/// logical occurrence identity is the order key, carried in metadata under
/// `"incremental_dump_order_key"`.
///
/// # Usage
///
/// ```rust,ignore
/// let adapter = IncrementalDumpAdapter::new(MySpotifyDumpLoader::new(path));
/// let config = IncrementalDumpConfig { order_key_field: "ts".to_owned() };
/// let stream = adapter.open(material_id, &config, prior_cursor).await?;
/// ```
pub struct IncrementalDumpAdapter<L: DumpLoader> {
    loader: Arc<L>,
}

impl<L: DumpLoader> IncrementalDumpAdapter<L> {
    /// Construct an adapter over the given dump loader.
    pub fn new(loader: L) -> Self {
        Self {
            loader: Arc::new(loader),
        }
    }
}

#[async_trait]
impl<L> InputShapeAdapter for IncrementalDumpAdapter<L>
where
    L: DumpLoader + 'static,
{
    type Config = IncrementalDumpConfig;
    type Cursor = IncrementalDumpCursor;
    const KIND: InputShapeKind = InputShapeKind::IncrementalDump;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let high_water: Option<IncrementalDumpPosition> = cursor.and_then(|c| c.high_water);

        let raw = self.loader.load().await.map_err(|e| {
            ParserError::Adapter(IncrementalDumpError::Load(e.to_string()).to_string())
        })?;

        // Pair each record with its composite position + serialized bytes,
        // failing closed if any record is missing the order-key field (a silent
        // skip would lose data). The content hash is a tie-breaker so records
        // sharing an order key stay distinct and ordered.
        let mut keyed: Vec<(IncrementalDumpPosition, Vec<u8>)> = Vec::with_capacity(raw.len());
        for record in raw {
            let Some(order_key) = extract_order_key(&record, &config.order_key_field) else {
                return Err(ParserError::Adapter(
                    IncrementalDumpError::MissingOrderKey {
                        field: config.order_key_field.clone(),
                    }
                    .to_string(),
                ));
            };
            // `serde_json::Map` is BTreeMap-backed (no `preserve_order` feature),
            // so serialization is canonical (sorted keys, recursively) and the
            // hash is stable even if the provider reorders fields between dumps.
            let bytes = serde_json::to_vec(&record).map_err(|e| {
                ParserError::Adapter(format!("failed to serialize incremental dump record: {e}"))
            })?;
            let content_hash = blake3::hash(&bytes).to_hex().to_string();
            keyed.push((
                IncrementalDumpPosition {
                    order_key,
                    content_hash,
                },
                bytes,
            ));
        }

        // Emit in ascending (order_key, content_hash) order so the high-water
        // mark advances monotonically and a mid-stream checkpoint is resumable.
        keyed.sort_by(|a, b| a.0.cmp(&b.0));

        let mut records: Vec<ParserResult<SourceRecord>> = Vec::new();
        for (dump_index, (position, bytes)) in keyed.into_iter().enumerate() {
            if let Some(hw) = high_water.as_ref() {
                if position <= *hw {
                    continue;
                }
            }

            let metadata = build_record_metadata(
                &position.order_key,
                &position.content_hash,
                dump_index as u64,
            );

            records.push(Ok(SourceRecord {
                material_id,
                anchor: MaterialAnchor::StreamFrame {
                    material_offset: 0,
                    frame_index: dump_index as u64,
                },
                bytes,
                logical_path: None,
                source_ts_hint: None,
                metadata,
            }));
        }

        Ok(stream::iter(records).boxed())
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        let order_key = record.metadata.get(META_ORDER_KEY).and_then(|v| v.as_str());
        let content_hash = record
            .metadata
            .get(META_CONTENT_HASH)
            .and_then(|v| v.as_str());
        // Both halves of the composite position must be present; a record we
        // emitted always carries both. If either is missing the record did not
        // come from this adapter — leave the cursor empty rather than checkpoint
        // a half-position that would mis-filter the next run.
        let high_water = match (order_key, content_hash) {
            (Some(order_key), Some(content_hash)) => Some(IncrementalDumpPosition {
                order_key: order_key.to_owned(),
                content_hash: content_hash.to_owned(),
            }),
            _ => None,
        };
        Ok(IncrementalDumpCursor { high_water })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
#[path = "incremental_dump_test.rs"]
mod tests;
