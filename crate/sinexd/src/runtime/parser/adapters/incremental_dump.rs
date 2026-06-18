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
mod tests {
    use futures::StreamExt;
    use xtask::sandbox::prelude::sinex_test;

    use super::*;

    #[derive(Debug)]
    struct MockError(String);

    impl fmt::Display for MockError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "mock error: {}", self.0)
        }
    }

    impl Error for MockError {}

    /// A loader backed by a fixed record list (optionally failing).
    struct MockLoader {
        records: Vec<JsonValue>,
        fail: bool,
    }

    impl MockLoader {
        fn new(records: Vec<JsonValue>) -> Self {
            Self {
                records,
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                records: Vec::new(),
                fail: true,
            }
        }
    }

    impl DumpLoader for MockLoader {
        type Error = MockError;

        async fn load(&self) -> Result<Vec<JsonValue>, Self::Error> {
            if self.fail {
                return Err(MockError("load boom".to_owned()));
            }
            Ok(self.records.clone())
        }
    }

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::<SourceMaterial>::from_uuid(sinex_primitives::Uuid::nil())
    }

    fn config() -> IncrementalDumpConfig {
        IncrementalDumpConfig {
            order_key_field: "ts".to_owned(),
        }
    }

    async fn collect(
        adapter: &IncrementalDumpAdapter<MockLoader>,
        cursor: Option<IncrementalDumpCursor>,
    ) -> Vec<JsonValue> {
        let stream = adapter
            .open(dummy_material_id(), &config(), cursor)
            .await
            .unwrap();
        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| serde_json::from_slice(&r.unwrap().bytes).unwrap())
            .collect()
    }

    /// Open and return the raw [`SourceRecord`]s (needed to derive cursors via
    /// `cursor_after`, which a plain payload collect discards).
    async fn open_records(
        adapter: &IncrementalDumpAdapter<MockLoader>,
        cursor: Option<IncrementalDumpCursor>,
    ) -> Vec<SourceRecord> {
        let stream = adapter
            .open(dummy_material_id(), &config(), cursor)
            .await
            .unwrap();
        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    fn v_of(record: &SourceRecord) -> i64 {
        let json: JsonValue = serde_json::from_slice(&record.bytes).unwrap();
        json["v"].as_i64().unwrap()
    }

    #[sinex_test]
    async fn first_import_emits_all_in_order() -> xtask::sandbox::TestResult<()> {
        let loader = MockLoader::new(vec![
            serde_json::json!({"ts": "2026-01-03", "v": 3}),
            serde_json::json!({"ts": "2026-01-01", "v": 1}),
            serde_json::json!({"ts": "2026-01-02", "v": 2}),
        ]);
        let adapter = IncrementalDumpAdapter::new(loader);
        let out = collect(&adapter, None).await;

        let vs: Vec<i64> = out.iter().map(|r| r["v"].as_i64().unwrap()).collect();
        assert_eq!(vs, vec![1, 2, 3], "emitted in ascending order-key order");
        Ok(())
    }

    #[sinex_test]
    async fn superset_reimport_emits_only_new() -> xtask::sandbox::TestResult<()> {
        // Second export supersets the first; only records past the high-water
        // mark are emitted. The cursor is derived from a prior record (the real
        // checkpoint path), not hand-built.
        let records = vec![
            serde_json::json!({"ts": "2026-01-01", "v": 1}),
            serde_json::json!({"ts": "2026-01-02", "v": 2}),
            serde_json::json!({"ts": "2026-01-03", "v": 3}),
        ];
        let adapter = IncrementalDumpAdapter::new(MockLoader::new(records));
        let first = open_records(&adapter, None).await;
        let at_02 = first.iter().find(|r| v_of(r) == 2).unwrap();
        let cursor = adapter.cursor_after(at_02).unwrap();
        let out = open_records(&adapter, Some(cursor)).await;

        let vs: Vec<i64> = out.iter().map(v_of).collect();
        assert_eq!(vs, vec![3], "only the record past the high-water mark");
        Ok(())
    }

    #[sinex_test]
    async fn non_unique_order_keys_all_emit() -> xtask::sandbox::TestResult<()> {
        // GDPR/Takeout timestamps are not unique. Distinct records that share an
        // order key must ALL be emitted — the content-hash tie-breaker keeps them
        // ordered instead of dropping siblings (the original Codex P1, PR #1776).
        let adapter = IncrementalDumpAdapter::new(MockLoader::new(vec![
            serde_json::json!({"ts": "2026-01-01", "v": 1}),
            serde_json::json!({"ts": "2026-01-01", "v": 2}),
            serde_json::json!({"ts": "2026-01-02", "v": 3}),
        ]));
        let out = open_records(&adapter, None).await;
        let mut vs: Vec<i64> = out.iter().map(v_of).collect();
        vs.sort();
        assert_eq!(
            vs,
            vec![1, 2, 3],
            "no record sharing a timestamp is dropped"
        );
        Ok(())
    }

    #[sinex_test]
    async fn resume_across_shared_order_key_keeps_siblings() -> xtask::sandbox::TestResult<()> {
        // The exact data-loss scenario from the Codex P1 review (PR #1776): a run
        // is interrupted after consuming one of two records that share an order
        // key. On resume the sibling at the same timestamp must NOT be dropped.
        let adapter = IncrementalDumpAdapter::new(MockLoader::new(vec![
            serde_json::json!({"ts": "2026-01-01", "v": 1}),
            serde_json::json!({"ts": "2026-01-01", "v": 2}),
            serde_json::json!({"ts": "2026-01-02", "v": 3}),
        ]));
        let first = open_records(&adapter, None).await;
        // Checkpoint right after the first emitted record (lowest position).
        let consumed = v_of(&first[0]);
        let cursor = adapter.cursor_after(&first[0]).unwrap();
        let out = open_records(&adapter, Some(cursor)).await;

        let mut vs: Vec<i64> = out.iter().map(v_of).collect();
        vs.sort();
        let mut expected: Vec<i64> = vec![1, 2, 3]
            .into_iter()
            .filter(|v| *v != consumed)
            .collect();
        expected.sort();
        // Everything except the already-consumed record survives — crucially the
        // other record sharing ts=2026-01-01 is still present.
        assert_eq!(vs, expected, "sibling sharing the order key is not dropped");
        Ok(())
    }

    #[sinex_test]
    async fn cursor_after_reports_composite_position() -> xtask::sandbox::TestResult<()> {
        let adapter = IncrementalDumpAdapter::new(MockLoader::new(vec![
            serde_json::json!({"ts": "2026-05-05", "v": 9}),
        ]));
        let records = open_records(&adapter, None).await;
        let position = adapter
            .cursor_after(&records[0])
            .unwrap()
            .high_water
            .expect("a consumed record yields a position");
        assert_eq!(position.order_key, "2026-05-05");
        assert_eq!(
            position.content_hash.len(),
            64,
            "BLAKE3 hex digest is 64 chars"
        );
        Ok(())
    }

    #[sinex_test]
    async fn missing_order_key_field_fails_closed() -> xtask::sandbox::TestResult<()> {
        let loader = MockLoader::new(vec![serde_json::json!({"no_ts": "x"})]);
        let adapter = IncrementalDumpAdapter::new(loader);
        let result = adapter.open(dummy_material_id(), &config(), None).await;
        assert!(
            result.is_err(),
            "a record missing the order-key field must fail, not silently drop"
        );
        Ok(())
    }

    #[sinex_test]
    async fn empty_dump_yields_no_records() -> xtask::sandbox::TestResult<()> {
        let adapter = IncrementalDumpAdapter::new(MockLoader::new(vec![]));
        let out = collect(&adapter, None).await;
        assert!(out.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn load_failure_surfaces_typed_error() -> xtask::sandbox::TestResult<()> {
        let adapter = IncrementalDumpAdapter::new(MockLoader::failing());
        let result = adapter.open(dummy_material_id(), &config(), None).await;
        assert!(
            result.is_err(),
            "loader failure must surface, not yield empty"
        );
        Ok(())
    }

    #[sinex_test]
    async fn input_shape_kind_is_incremental_dump() -> xtask::sandbox::TestResult<()> {
        assert_eq!(
            <IncrementalDumpAdapter<MockLoader> as InputShapeAdapter>::KIND,
            InputShapeKind::IncrementalDump
        );
        Ok(())
    }
}
