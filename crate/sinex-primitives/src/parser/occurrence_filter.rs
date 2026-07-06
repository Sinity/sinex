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
//! - `ContentHashWindow` (in `sinexd::sources::source_contracts::dedup`) is a bounded
//!   ring buffer for append-only-file rotation overlap dedup — ephemeral,
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
//! rendered as `name=value`, joined by `|`, prefixed by the source
//! id. Backslash, `|`, and `=` characters inside names and values are
//! escaped (`\\`, `\|`, `\=`) so adversarial track titles like
//! `Foo|bar=baz` cannot collide with the encoding of a different
//! `(name, value)` pair. This format is stable, human-readable, and
//! avoids collision across source contracts.

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
/// Format: `source_id|name1=val1|name2=val2|...`
///
/// Backslash (`\\`), pipe (`|`), and equals (`=`) characters inside the
/// `source_id`, field names, and values are escaped (`\\\\`, `\\|`,
/// `\\=`) so two distinct keys never collapse to the same string. For
/// example, `(foo, "bar|baz")` and `(foo|bar, "baz")` produce different
/// outputs, which they would not under bare concatenation.
///
/// This is the key stored in the in-memory filter and also what the DB
/// builder function ([`build_occurrence_filter`]) extracts from the event
/// payload.
#[must_use]
pub fn occurrence_key_string(key: &OccurrenceKey) -> String {
    let mut s = String::with_capacity(128);
    push_escaped(&mut s, key.source_id.as_str());
    for (name, value) in &key.fields {
        s.push('|');
        push_escaped(&mut s, name);
        s.push('=');
        push_escaped(&mut s, value);
    }
    s
}

/// Escape `\\`, `|`, and `=` in `input`, appending the result to `out`.
///
/// The escape character is backslash; the inverse decoding is unambiguous
/// because the escape character itself is escaped first.
fn push_escaped(out: &mut String, input: &str) {
    for ch in input.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '|' => out.push_str("\\|"),
            '=' => out.push_str("\\="),
            other => out.push(other),
        }
    }
}

const MAX_EQUIVALENCE_KEY_BYTES: usize = 512;

fn bounded_occurrence_key_string(key: &OccurrenceKey) -> String {
    let exact = occurrence_key_string(key);
    if exact.len() <= MAX_EQUIVALENCE_KEY_BYTES {
        return exact;
    }

    format!(
        "{}|occurrence_hash={}",
        key.source_id.as_str(),
        blake3::hash(exact.as_bytes()).to_hex()
    )
}

/// Derive a canonical DB-safe string key from an optional reference.
///
/// Returns `None` if the occurrence key is absent, otherwise
/// `Some(key)`. Short keys retain the exact escaped occurrence string.
/// Longer keys are represented by a stable BLAKE3 digest so the projected
/// `equivalence_key` always satisfies the database's 512-byte bound.
#[must_use]
pub fn maybe_occurrence_key_string(key: Option<&OccurrenceKey>) -> Option<String> {
    key.map(bounded_occurrence_key_string)
}

#[cfg(test)]
#[path = "occurrence_filter_test.rs"]
mod tests;
