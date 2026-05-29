//! In-memory occurrence-key dedup filter for source migrations (#1050).
//!
//! [`OccurrenceFilter`] is a lightweight `HashSet<String>` wrapper that
//! source migration jobs can consult before admitting a parsed event.
//! The filter is built by querying existing occurrence keys from the
//! database once before a historical parser job starts, and then
//! updated in-memory as new keys are admitted.
//!
//! # Relationship to other dedup
//!
//! - [`ContentHashWindow`](crate::node_sdk_parser_dedup) is a bounded ring
//!   buffer for append-only-file rotation overlap dedup — ephemeral,
//!   byte-hash-based, tied to inode rotations. This is NOT the
//!   occurrence-filtered model; it only answers "did I emit this record
//!   recently."
//! - `OccurrenceFilter` is a *semantic* filter — it answers "has this
//!   logical occurrence (same user, same track, same time) ever been
//!   admitted before." It is built from the database, not from recent
//!   in-memory hashes.
//!
//! # Key format
//!
//! The canonical string key is derived from [`OccurrenceKey`] via
//! [`occurrence_key_string`]: each `(field_name, value)` pair is
//! rendered as `name=value`, joined by `|`, prefixed by the source unit
//! id. This format is stable, human-readable, and avoids collision
//! across source units.

use std::collections::HashSet;

use super::OccurrenceKey;

/// In-memory filter for occurrence-based dedup during source migrations.
///
/// Build before starting a parser job, check each parsed event's
/// occurrence key against the filter before admission, and insert
/// newly-admitted keys so the running import also filters self-duplicates.
#[derive(Debug, Clone)]
pub struct OccurrenceFilter {
    keys: HashSet<String>,
}

impl OccurrenceFilter {
    /// Create an empty filter (no keys yet).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            keys: HashSet::new(),
        }
    }

    /// Create a filter pre-populated with the given keys.
    #[must_use]
    pub fn from_keys(keys: impl IntoIterator<Item = String>) -> Self {
        Self {
            keys: keys.into_iter().collect(),
        }
    }

    /// Returns `true` if the key already exists — the event should be
    /// **skipped** (already imported).
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.keys.contains(key)
    }

    /// Record a new occurrence key after successful admission.
    pub fn insert(&mut self, key: String) {
        self.keys.insert(key);
    }

    /// Number of distinct keys currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the filter holds any keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// Derive a canonical string key from an [`OccurrenceKey`].
///
/// Format: `source_unit_id|name1=val1|name2=val2|...`
///
/// This is the key stored in the in-memory filter and also what the DB
/// builder function ([`build_occurrence_filter`]) extracts from the event
/// payload.
#[must_use]
pub fn occurrence_key_string(key: &OccurrenceKey) -> String {
    let mut s = String::with_capacity(128);
    s.push_str(key.source_unit_id.as_str());
    for (name, value) in &key.fields {
        s.push('|');
        s.push_str(name);
        s.push('=');
        s.push_str(value);
    }
    s
}

/// Derive a canonical string key from an optional reference.
///
/// Returns `None` if the occurrence key is absent, otherwise
/// `Some(occurrence_key_string(key))`.
#[must_use]
pub fn maybe_occurrence_key_string(key: Option<&OccurrenceKey>) -> Option<String> {
    key.map(occurrence_key_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SourceUnitId;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn empty_filter_contains_nothing() -> xtask::sandbox::TestResult<()> {
        let f = OccurrenceFilter::empty();
        assert!(!f.contains("anything"));
        Ok(())
    }

    #[sinex_test]
    async fn insert_then_contains_returns_true() -> xtask::sandbox::TestResult<()> {
        let mut f = OccurrenceFilter::empty();
        f.insert("key-a".to_string());
        assert!(f.contains("key-a"));
        assert!(!f.contains("key-b"));
        assert_eq!(f.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn from_keys_builds_correctly() -> xtask::sandbox::TestResult<()> {
        let f = OccurrenceFilter::from_keys([
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);
        assert_eq!(f.len(), 3);
        assert!(f.contains("a"));
        assert!(f.contains("b"));
        assert!(f.contains("c"));
        assert!(!f.contains("d"));
        Ok(())
    }

    #[sinex_test]
    async fn duplicate_insert_is_idempotent() -> xtask::sandbox::TestResult<()> {
        let mut f = OccurrenceFilter::empty();
        f.insert("dup".to_string());
        f.insert("dup".to_string());
        assert_eq!(f.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_string_format() -> xtask::sandbox::TestResult<()> {
        let key = OccurrenceKey {
            source_unit_id: SourceUnitId::from_static("test.unit"),
            fields: vec![
                ("a".into(), "1".into()),
                ("b".into(), "hello".into()),
            ],
        };
        let s = occurrence_key_string(&key);
        assert_eq!(s, "test.unit|a=1|b=hello");
        Ok(())
    }

    #[sinex_test]
    async fn maybe_occurrence_key_string_some_and_none() -> xtask::sandbox::TestResult<()> {
        let key = OccurrenceKey {
            source_unit_id: SourceUnitId::from_static("test.unit"),
            fields: vec![("x".into(), "y".into())],
        };
        assert_eq!(
            maybe_occurrence_key_string(Some(&key)),
            Some("test.unit|x=y".to_string())
        );
        assert_eq!(maybe_occurrence_key_string(None), None);
        Ok(())
    }
}
