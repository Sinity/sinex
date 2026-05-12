//! Bounded content-hash dedup window for append-only parsers.
//!
//! Append-only sources (bash/zsh/text histories, generic line-tail files) can
//! be rotated out from under us: a new inode replaces the old, and the new
//! file may share a tail with the old one (a typical pattern is "copy + truncate"
//! during log rotation, leaving the most recent N lines duplicated). The
//! [`AppendOnlyFileAdapter`](crate::parser::AppendOnlyFileAdapter) detects the
//! rotation and resets offsets to 0, but cannot suppress a re-emit of records
//! that appear in both inodes — that requires content-level memory.
//!
//! [`ContentHashWindow`] is a small ring buffer of BLAKE3 hashes that parsers
//! can consult to drop records they've already emitted within the trailing
//! window. It is intentionally bounded so it does not grow unbounded across a
//! long-running session; the size should be picked so the window comfortably
//! covers the largest plausible rotation overlap.

use blake3::{Hash, Hasher};
use std::collections::VecDeque;

/// Default window size: 10_000 records is enough for the longest plausible
/// rotation overlap on the terminal history files we ingest (bash/zsh/text)
/// while staying small enough to keep the per-source-unit memory footprint
/// trivial (~320 KiB).
pub const DEFAULT_WINDOW_CAPACITY: usize = 10_000;

/// Rolling window of content hashes for record-level deduplication.
///
/// Push every emitted record's hash via [`ContentHashWindow::observe`]. Before
/// emitting a candidate record, call [`ContentHashWindow::contains`] — if it
/// returns `true`, the record was emitted recently (within the window) and
/// should be dropped. The window evicts the oldest entries once `capacity`
/// is reached.
#[derive(Debug, Clone)]
pub struct ContentHashWindow {
    capacity: usize,
    order: VecDeque<Hash>,
    seen: std::collections::HashSet<Hash>,
}

impl Default for ContentHashWindow {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_WINDOW_CAPACITY)
    }
}

impl ContentHashWindow {
    /// Construct a window holding up to `capacity` recent hashes. A capacity
    /// of 0 disables the window (every `contains` call returns `false` and
    /// `observe` is a no-op).
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            seen: std::collections::HashSet::with_capacity(capacity),
        }
    }

    /// Hash `bytes` and check whether the window has seen the same content
    /// recently. Does not mutate the window.
    #[must_use]
    pub fn contains(&self, bytes: &[u8]) -> bool {
        if self.capacity == 0 {
            return false;
        }
        self.seen.contains(&hash_bytes(bytes))
    }

    /// Hash `bytes` and record it in the window, evicting the oldest entry if
    /// the window is at capacity. Returns the hash for callers who want to
    /// surface it elsewhere (e.g. attach to event metadata).
    pub fn observe(&mut self, bytes: &[u8]) -> Hash {
        let hash = hash_bytes(bytes);
        if self.capacity == 0 {
            return hash;
        }
        if self.seen.insert(hash) {
            if self.order.len() >= self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.seen.remove(&evicted);
                }
            }
            self.order.push_back(hash);
        }
        hash
    }

    /// Clear the window — appropriate after a confirmed rotation event when
    /// the parser wants a fresh dedup horizon.
    pub fn clear(&mut self) {
        self.order.clear();
        self.seen.clear();
    }

    /// Number of distinct hashes currently retained.
    #[must_use]
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Whether the window holds any hashes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

fn hash_bytes(bytes: &[u8]) -> Hash {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_returns_false_before_observe() {
        let window = ContentHashWindow::default();
        assert!(!window.contains(b"line one"));
    }

    #[test]
    fn observe_then_contains_returns_true() {
        let mut window = ContentHashWindow::with_capacity(4);
        window.observe(b"line one");
        assert!(window.contains(b"line one"));
        assert!(!window.contains(b"line two"));
    }

    #[test]
    fn observe_evicts_oldest_when_at_capacity() {
        let mut window = ContentHashWindow::with_capacity(2);
        window.observe(b"a");
        window.observe(b"b");
        window.observe(b"c"); // evicts "a"
        assert!(!window.contains(b"a"));
        assert!(window.contains(b"b"));
        assert!(window.contains(b"c"));
        assert_eq!(window.len(), 2);
    }

    #[test]
    fn capacity_zero_disables_dedup() {
        let mut window = ContentHashWindow::with_capacity(0);
        window.observe(b"x");
        assert!(!window.contains(b"x"));
        assert!(window.is_empty());
    }

    #[test]
    fn observe_is_idempotent_for_duplicates() {
        let mut window = ContentHashWindow::with_capacity(4);
        window.observe(b"dup");
        window.observe(b"dup");
        window.observe(b"dup");
        assert_eq!(window.len(), 1, "duplicate observations should not grow the window");
    }

    #[test]
    fn clear_drops_all_entries() {
        let mut window = ContentHashWindow::with_capacity(4);
        window.observe(b"a");
        window.observe(b"b");
        window.clear();
        assert!(window.is_empty());
        assert!(!window.contains(b"a"));
    }
}
