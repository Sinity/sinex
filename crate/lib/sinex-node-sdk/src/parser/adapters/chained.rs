//! Adapter composition: merge two [`InputShapeAdapter`]s into one logical stream.
//!
//! [`ChainedAdapter<A, B>`] drains adapter `A` first, then adapter `B`.
//! Both adapters share the same [`Id<SourceMaterial>`] but use independent
//! configs and cursors. Records from each adapter carry a `logical_path`
//! prefix (`"primary/"` or `"secondary/"`) so downstream parsers can route
//! by origin without an additional field on [`SourceRecord`].
//!
//! # Sequential vs interleaved
//!
//! The default merge is **sequential**: exhaust `A`, then exhaust `B`.
//! This matches `browser.history` which needs `SQLite` rows (primary) fully
//! drained before dump files (secondary). Set `interleaved: true` in
//! [`ChainedConfig`] to interleave via `futures::stream::select` — useful
//! when both adapters are live streams with no natural end.
//!
//! # Cursor advancement
//!
//! [`ChainedCursor`] wraps independent cursors for each leg.
//! `cursor_after()` inspects the `logical_path` prefix to identify which
//! leg produced the record and updates only that cursor.

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, SourceRecord};

use crate::parser::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// Logical-path prefixes — the tagging mechanism
// =============================================================================

/// Prefix prepended to `SourceRecord::logical_path` for records from the
/// primary adapter leg.
pub const PRIMARY_PREFIX: &str = "primary/";

/// Prefix prepended to `SourceRecord::logical_path` for records from the
/// secondary adapter leg.
pub const SECONDARY_PREFIX: &str = "secondary/";

// =============================================================================
// ChainedAdapter
// =============================================================================

/// Compose two [`InputShapeAdapter`]s into a single logical stream.
///
/// Records are tagged with a `logical_path` prefix (`"primary/"` or
/// `"secondary/"`) so that parsers and `cursor_after()` can identify which
/// leg produced each record.
pub struct ChainedAdapter<A, B>(pub A, pub B);

impl<A: Default, B: Default> Default for ChainedAdapter<A, B> {
    fn default() -> Self {
        Self(A::default(), B::default())
    }
}

/// Configuration for [`ChainedAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ChainedConfig<A, B> {
    /// Configuration for the primary (first) adapter.
    pub primary: A,

    /// Configuration for the secondary (second) adapter.
    pub secondary: B,

    /// If `true`, interleave records from both adapters concurrently using
    /// `futures::stream::select` (fair interleaving, not biased toward
    /// either leg). If `false` (default), drain primary entirely before
    /// starting secondary.
    #[serde(default)]
    pub interleaved: bool,
}

/// Cursor for [`ChainedAdapter`] — independent cursors per leg.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainedCursor<A, B> {
    /// Cursor for the primary leg.
    pub primary: Option<A>,

    /// Cursor for the secondary leg.
    pub secondary: Option<B>,
}

impl<A: Default, B: Default> Default for ChainedCursor<A, B> {
    fn default() -> Self {
        Self {
            primary: None,
            secondary: None,
        }
    }
}

// =============================================================================
// Tag helpers
// =============================================================================

/// Prepend a prefix to the logical path of a record, marking which leg it
/// came from.
fn tag_record(mut record: SourceRecord, prefix: &'static str) -> SourceRecord {
    use camino::Utf8PathBuf;
    let base = record
        .logical_path
        .as_deref()
        .map_or("", camino::Utf8Path::as_str);
    record.logical_path = Some(Utf8PathBuf::from(format!("{prefix}{base}")));
    record
}

/// Determine which leg produced a record based on its `logical_path` prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainedLeg {
    Primary,
    Secondary,
}

/// Strip the leg prefix from a record's `logical_path` and return which leg
/// owned it. Returns an error if the prefix is unrecognised.
pub fn classify_record(record: &SourceRecord) -> ParserResult<ChainedLeg> {
    match &record.logical_path {
        None => Err(ParserError::Cursor(
            "ChainedAdapter: record has no logical_path — cannot identify originating leg".into(),
        )),
        Some(p) => {
            let s = p.as_str();
            if s.starts_with(PRIMARY_PREFIX) {
                Ok(ChainedLeg::Primary)
            } else if s.starts_with(SECONDARY_PREFIX) {
                Ok(ChainedLeg::Secondary)
            } else {
                Err(ParserError::Cursor(format!(
                    "ChainedAdapter: logical_path '{s}' has neither 'primary/' nor 'secondary/' prefix"
                )))
            }
        }
    }
}

// =============================================================================
// InputShapeAdapter impl
// =============================================================================

#[async_trait]
impl<A, B> InputShapeAdapter for ChainedAdapter<A, B>
where
    A: InputShapeAdapter + Send + Sync + 'static,
    B: InputShapeAdapter + Send + Sync + 'static,
{
    type Config = ChainedConfig<A::Config, B::Config>;
    type Cursor = ChainedCursor<A::Cursor, B::Cursor>;

    /// The chained adapter reports `InputShapeKind::Subprocess` as a
    /// catch-all sentinel — neither leg's kind dominates the composed
    /// shape. Callers that need the per-leg kinds can inspect
    /// `A::KIND` / `B::KIND` directly.
    ///
    /// If this proves awkward, `InputShapeKind::Chained` can be added to
    /// `sinex-primitives` — deferred per the scope constraint.
    const KIND: InputShapeKind = InputShapeKind::Subprocess;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let primary_cursor = cursor.as_ref().and_then(|c| c.primary.clone());
        let secondary_cursor = cursor.as_ref().and_then(|c| c.secondary.clone());

        let primary_stream = self
            .0
            .open(material_id, &config.primary, primary_cursor)
            .await?
            .map(|r| r.map(|rec| tag_record(rec, PRIMARY_PREFIX)));

        let secondary_stream = self
            .1
            .open(material_id, &config.secondary, secondary_cursor)
            .await?
            .map(|r| r.map(|rec| tag_record(rec, SECONDARY_PREFIX)));

        let merged: BoxStream<'static, ParserResult<SourceRecord>> = if config.interleaved {
            // Fair interleaving — poll both streams concurrently.
            Box::pin(futures::stream::select(primary_stream, secondary_stream))
        } else {
            // Sequential — drain primary then secondary.
            Box::pin(primary_stream.chain(secondary_stream))
        };

        Ok(merged)
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        // We need the current cursor to carry forward the unchanged leg.
        // The runtime calls this without access to the previous cursor, so we
        // reconstruct just the updated leg and leave the other as None.
        // The runtime is responsible for merging this update with the stored
        // checkpoint via `ChainedCursor { primary, secondary }`.
        //
        // Contract: the caller (source-worker runtime) must merge the returned
        // partial cursor with the persisted one. Here we return a cursor where
        // only the producing leg is `Some` and the other is `None`.
        let leg = classify_record(record)?;

        // Build a stripped record (without the prefix) so the underlying
        // adapter's `cursor_after` sees its original logical_path.
        let stripped = strip_prefix(record);

        match leg {
            ChainedLeg::Primary => {
                let cur = self.0.cursor_after(&stripped)?;
                Ok(ChainedCursor {
                    primary: Some(cur),
                    secondary: None,
                })
            }
            ChainedLeg::Secondary => {
                let cur = self.1.cursor_after(&stripped)?;
                Ok(ChainedCursor {
                    primary: None,
                    secondary: Some(cur),
                })
            }
        }
    }
}

/// Return a clone of `record` with the leg prefix stripped from `logical_path`.
fn strip_prefix(record: &SourceRecord) -> SourceRecord {
    use camino::Utf8PathBuf;
    let mut stripped = record.clone();
    if let Some(ref p) = record.logical_path {
        let s = p.as_str();
        if let Some(rest) = s.strip_prefix(PRIMARY_PREFIX) {
            stripped.logical_path = if rest.is_empty() {
                None
            } else {
                Some(Utf8PathBuf::from(rest))
            };
        } else if let Some(rest) = s.strip_prefix(SECONDARY_PREFIX) {
            stripped.logical_path = if rest.is_empty() {
                None
            } else {
                Some(Utf8PathBuf::from(rest))
            };
        }
    }
    stripped
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use sinex_primitives::parser::{MaterialAnchor, SourceRecord};

    use xtask::sandbox::prelude::sinex_test;

    // -------------------------------------------------------------------------
    // Fixture adapter — yields a fixed list of records.
    // -------------------------------------------------------------------------

    #[derive(Clone, Default)]
    struct FixtureAdapter {
        records: Vec<SourceRecord>,
    }

    impl FixtureAdapter {
        fn with_records(records: Vec<SourceRecord>) -> Self {
            Self { records }
        }
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct FixtureConfig;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct FixtureCursor {
        next_frame: u64,
    }

    fn make_record(material_id: Id<SourceMaterial>, frame_index: u64, label: &str) -> SourceRecord {
        SourceRecord {
            material_id,
            anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index,
            },
            bytes: label.as_bytes().to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    #[async_trait]
    impl InputShapeAdapter for FixtureAdapter {
        type Config = FixtureConfig;
        type Cursor = FixtureCursor;
        const KIND: InputShapeKind = InputShapeKind::StaticFile;

        async fn open(
            &self,
            _material_id: Id<SourceMaterial>,
            _config: &Self::Config,
            cursor: Option<Self::Cursor>,
        ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
            let start = cursor.map_or(0, |c| c.next_frame as usize);
            let records: Vec<_> = self.records[start..].to_vec();
            let s = stream::iter(records.into_iter().map(Ok));
            Ok(Box::pin(s))
        }

        fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
            match &record.anchor {
                MaterialAnchor::StreamFrame { frame_index, .. } => Ok(FixtureCursor {
                    next_frame: frame_index + 1,
                }),
                _ => Err(ParserError::Cursor("unexpected anchor".into())),
            }
        }
    }

    // -------------------------------------------------------------------------
    // Test: sequential merge drains primary then secondary
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn test_sequential_merge_drains_primary_first() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let primary = FixtureAdapter::with_records(vec![
            make_record(mid, 0, "p0"),
            make_record(mid, 1, "p1"),
        ]);
        let secondary = FixtureAdapter::with_records(vec![make_record(mid, 0, "s0")]);

        let adapter = ChainedAdapter(primary, secondary);
        let config = ChainedConfig {
            primary: FixtureConfig,
            secondary: FixtureConfig,
            interleaved: false,
        };

        let stream = adapter.open(mid, &config, None).await.unwrap();
        let records: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(records.len(), 3);

        // First two come from primary.
        let lp0 = records[0].logical_path.as_ref().unwrap().as_str();
        let lp1 = records[1].logical_path.as_ref().unwrap().as_str();
        let lp2 = records[2].logical_path.as_ref().unwrap().as_str();

        assert!(
            lp0.starts_with(PRIMARY_PREFIX),
            "first record must be primary: {lp0}"
        );
        assert!(
            lp1.starts_with(PRIMARY_PREFIX),
            "second record must be primary: {lp1}"
        );
        assert!(
            lp2.starts_with(SECONDARY_PREFIX),
            "third record must be secondary: {lp2}"
        );

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Test: classify_record distinguishes legs
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn test_classify_record_primary() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let mut rec = make_record(mid, 0, "x");
        rec.logical_path = Some("primary/subpath".into());
        assert_eq!(classify_record(&rec).unwrap(), ChainedLeg::Primary);
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_record_secondary() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let mut rec = make_record(mid, 0, "x");
        rec.logical_path = Some("secondary/subpath".into());
        assert_eq!(classify_record(&rec).unwrap(), ChainedLeg::Secondary);
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_record_missing_prefix_errors() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let mut rec = make_record(mid, 0, "x");
        rec.logical_path = Some("unknown/subpath".into());
        assert!(classify_record(&rec).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_record_no_path_errors() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let rec = make_record(mid, 0, "x");
        assert!(classify_record(&rec).is_err());
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Test: cursor_after updates only the producing leg
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn test_cursor_after_primary_leg() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let adapter = ChainedAdapter(FixtureAdapter::default(), FixtureAdapter::default());

        let mut rec = make_record(mid, 5, "x");
        rec.logical_path = Some("primary/".into());

        let cursor = adapter.cursor_after(&rec).unwrap();
        assert!(cursor.primary.is_some());
        assert!(cursor.secondary.is_none());
        assert_eq!(cursor.primary.unwrap().next_frame, 6);
        Ok(())
    }

    #[sinex_test]
    async fn test_cursor_after_secondary_leg() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let adapter = ChainedAdapter(FixtureAdapter::default(), FixtureAdapter::default());

        let mut rec = make_record(mid, 3, "x");
        rec.logical_path = Some("secondary/".into());

        let cursor = adapter.cursor_after(&rec).unwrap();
        assert!(cursor.primary.is_none());
        assert!(cursor.secondary.is_some());
        assert_eq!(cursor.secondary.unwrap().next_frame, 4);
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Test: empty adapter on one leg is harmless
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn test_empty_primary_leg_yields_only_secondary() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let primary = FixtureAdapter::with_records(vec![]);
        let secondary = FixtureAdapter::with_records(vec![make_record(mid, 0, "s0")]);

        let adapter = ChainedAdapter(primary, secondary);
        let config = ChainedConfig {
            primary: FixtureConfig,
            secondary: FixtureConfig,
            interleaved: false,
        };

        let stream = adapter.open(mid, &config, None).await.unwrap();
        let records: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(records.len(), 1);
        let lp = records[0].logical_path.as_ref().unwrap().as_str();
        assert!(lp.starts_with(SECONDARY_PREFIX));
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Test: strip_prefix restores original logical_path
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn test_strip_prefix_restores_path() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let mut rec = make_record(mid, 0, "x");
        rec.logical_path = Some("primary/foo/bar.csv".into());

        let stripped = strip_prefix(&rec);
        assert_eq!(
            stripped
                .logical_path
                .as_deref()
                .map(camino::Utf8Path::as_str),
            Some("foo/bar.csv")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_strip_prefix_bare_primary_gives_none() -> xtask::sandbox::TestResult<()> {
        let mid = dummy_material_id();
        let mut rec = make_record(mid, 0, "x");
        rec.logical_path = Some("primary/".into());

        let stripped = strip_prefix(&rec);
        assert!(stripped.logical_path.is_none());
        Ok(())
    }
}
