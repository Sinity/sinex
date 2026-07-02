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

use crate::runtime::parser::{
    InputShapeAdapter, ParserError, ParserResult, SourceRecordFingerprint,
};

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

    fn input_fingerprint(
        &self,
        config: &Self::Config,
    ) -> ParserResult<Option<SourceRecordFingerprint>> {
        if let Some(primary) = self.0.input_fingerprint(&config.primary)? {
            return Ok(Some(primary));
        }
        self.1.input_fingerprint(&config.secondary)
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        // We need the current cursor to carry forward the unchanged leg.
        // The runtime calls this without access to the previous cursor, so we
        // reconstruct just the updated leg and leave the other as None.
        // The runtime is responsible for merging this update with the stored
        // checkpoint via `ChainedCursor { primary, secondary }`.
        //
        // Contract: the caller (source runtime) must merge the returned
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
#[path = "chained_test.rs"]
mod tests;
