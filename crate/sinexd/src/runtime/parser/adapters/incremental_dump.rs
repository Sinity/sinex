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
//! The cursor ([`IncrementalDumpCursor`]) is the highest order key consumed so
//! far — exactly the high-water-mark shape of [`ApiCursorPosition`], which fits
//! the per-record [`cursor_after`][InputShapeAdapter::cursor_after] contract in
//! O(1) (a growing "seen-set" cursor would not — it cannot be reconstructed
//! from a single record without O(n) per-record metadata).
//!
//! - `cursor = None` or `high_water = None` → brand-new source: emit everything.
//! - `cursor.high_water = Some(k)` → emit only records whose order key `> k`.
//!
//! **The order key MUST be lexicographically monotonic across exports**
//! (ISO-8601 timestamps, zero-padded ids, ULIDs). Comparison is string-wise, so
//! unpadded decimal ids (`"9" > "10"`) will misorder — pad them or use a
//! timestamp. This is the documented contract narrowing that makes the cursor
//! record-local; sources whose new records can appear at arbitrary positions
//! (no monotonic key) are not a fit for this adapter.
//!
//! [`ApiCursorPosition`]: super::ApiCursorPosition

use std::{error::Error, fmt, future::Future, sync::Arc};

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
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
    /// monotonic order key (e.g. an ISO-8601 timestamp). Records are emitted in
    /// ascending order-key order; only keys strictly greater than the prior
    /// high-water mark are emitted. Required — there is no sensible default.
    pub order_key_field: String,
}

/// Cursor for [`IncrementalDumpAdapter`] — the highest order key consumed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalDumpCursor {
    /// The highest order key emitted so far.
    ///
    /// `None` means nothing has been consumed yet (brand-new source); the next
    /// `open` emits the entire dump.
    pub high_water: Option<String>,
}

// =============================================================================
// Metadata keys embedded in SourceRecord
// =============================================================================

const META_ORDER_KEY: &str = "incremental_dump_order_key";
const META_DUMP_INDEX: &str = "incremental_dump_index";

fn build_record_metadata(order_key: &str, dump_index: u64) -> JsonValue {
    let mut map = Map::new();
    map.insert(
        META_ORDER_KEY.to_owned(),
        JsonValue::String(order_key.to_owned()),
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
        let high_water: Option<String> = cursor.and_then(|c| c.high_water);

        let raw = self
            .loader
            .load()
            .await
            .map_err(|e| ParserError::Adapter(IncrementalDumpError::Load(e.to_string()).to_string()))?;

        // Pair each record with its order key, failing closed if any record is
        // missing the configured field (a silent skip would lose data).
        let mut keyed: Vec<(String, JsonValue)> = Vec::with_capacity(raw.len());
        for record in raw {
            let Some(key) = extract_order_key(&record, &config.order_key_field) else {
                return Err(ParserError::Adapter(
                    IncrementalDumpError::MissingOrderKey {
                        field: config.order_key_field.clone(),
                    }
                    .to_string(),
                ));
            };
            keyed.push((key, record));
        }

        // Emit in ascending order-key order so the high-water mark advances
        // monotonically and a mid-stream checkpoint is always resumable.
        keyed.sort_by(|a, b| a.0.cmp(&b.0));

        let mut records: Vec<ParserResult<SourceRecord>> = Vec::new();
        for (dump_index, (key, record)) in keyed.into_iter().enumerate() {
            if let Some(hw) = high_water.as_deref() {
                if key.as_str() <= hw {
                    continue;
                }
            }

            let bytes = serde_json::to_vec(&record).map_err(|e| {
                ParserError::Adapter(format!("failed to serialize incremental dump record: {e}"))
            })?;

            let metadata = build_record_metadata(&key, dump_index as u64);

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
        let high_water = record
            .metadata
            .get(META_ORDER_KEY)
            .and_then(|v| v.as_str())
            .map(str::to_owned);
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
        // mark are emitted.
        let loader = MockLoader::new(vec![
            serde_json::json!({"ts": "2026-01-01", "v": 1}),
            serde_json::json!({"ts": "2026-01-02", "v": 2}),
            serde_json::json!({"ts": "2026-01-03", "v": 3}),
        ]);
        let adapter = IncrementalDumpAdapter::new(loader);
        let cursor = Some(IncrementalDumpCursor {
            high_water: Some("2026-01-02".to_owned()),
        });
        let out = collect(&adapter, cursor).await;

        let vs: Vec<i64> = out.iter().map(|r| r["v"].as_i64().unwrap()).collect();
        assert_eq!(vs, vec![3], "only the record past the high-water mark");
        Ok(())
    }

    #[sinex_test]
    async fn cursor_after_reports_record_order_key() -> xtask::sandbox::TestResult<()> {
        let loader = MockLoader::new(vec![serde_json::json!({"ts": "2026-05-05", "v": 9})]);
        let adapter = IncrementalDumpAdapter::new(loader);
        let stream = adapter
            .open(dummy_material_id(), &config(), None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;
        let cursor = adapter.cursor_after(records[0].as_ref().unwrap()).unwrap();
        assert_eq!(cursor.high_water.as_deref(), Some("2026-05-05"));
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
        assert!(result.is_err(), "loader failure must surface, not yield empty");
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
