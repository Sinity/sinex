//! Adapter for clipboard content polling.
//!
//! Polls the system clipboard at a configurable interval and emits a record
//! only when the content changes (detected by BLAKE3 hash comparison).
//! Suitable for `desktop.clipboard.changed` events.
//!
//! Cursor is `()` — clipboard history has no stable addresses; anchor only.
//! Anchor is [`MaterialAnchor::StreamFrame`] with a monotonic change counter.
//!
//! # Testability
//!
//! The adapter is backed by a [`ClipboardBackend`] trait so tests can inject a
//! mock queue of clipboard strings without requiring a display server. The
//! default impl uses `arboard::Clipboard`.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::parser::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// ClipboardBackend trait
// =============================================================================

/// Abstracts clipboard access so tests can inject fake content.
pub trait ClipboardBackend: Send + 'static {
    /// Read the current text content of the clipboard.
    ///
    /// Returns `Ok(None)` if the clipboard is empty or non-text.
    /// Returns `Err` only on hard backend failures.
    fn get_text(&mut self) -> ParserResult<Option<String>>;
}

// =============================================================================
// ArboardBackend — the real backend
// =============================================================================

/// Default clipboard backend using `arboard`.
pub struct ArboardBackend {
    inner: arboard::Clipboard,
}

impl ArboardBackend {
    pub fn new() -> ParserResult<Self> {
        arboard::Clipboard::new()
            .map(|inner| Self { inner })
            .map_err(|e| ParserError::Adapter(format!("failed to open clipboard: {e}")))
    }
}

impl ClipboardBackend for ArboardBackend {
    fn get_text(&mut self) -> ParserResult<Option<String>> {
        match self.inner.get_text() {
            Ok(text) => Ok(Some(text)),
            Err(arboard::Error::ContentNotAvailable) => Ok(None),
            Err(e) => Err(ParserError::Adapter(format!("clipboard read error: {e}"))),
        }
    }
}

// =============================================================================
// MockClipboardBackend
// =============================================================================

/// A mock [`ClipboardBackend`] that yields a pre-configured sequence of
/// clipboard snapshots.
///
/// When the queue is exhausted, returns `Ok(None)` indefinitely.
pub struct MockClipboardBackend {
    snapshots: std::collections::VecDeque<Option<String>>,
}

impl MockClipboardBackend {
    pub fn new(snapshots: impl IntoIterator<Item = Option<String>>) -> Self {
        Self {
            snapshots: snapshots.into_iter().collect(),
        }
    }
}

impl ClipboardBackend for MockClipboardBackend {
    fn get_text(&mut self) -> ParserResult<Option<String>> {
        Ok(self.snapshots.pop_front().flatten())
    }
}

// =============================================================================
// ClipboardPollingConfig
// =============================================================================

/// Configuration for [`ClipboardPollingAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClipboardPollingConfig {
    /// Poll interval in milliseconds.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// Maximum content size to emit (bytes). Larger content is silently dropped.
    #[serde(default = "default_max_content_bytes")]
    pub max_content_bytes: usize,
}

fn default_poll_interval_ms() -> u64 {
    500
}

fn default_max_content_bytes() -> usize {
    1024 * 1024 // 1 MiB
}

impl Default for ClipboardPollingConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: default_poll_interval_ms(),
            max_content_bytes: default_max_content_bytes(),
        }
    }
}

/// No cursor for [`ClipboardPollingAdapter`] — anchor only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClipboardPollingCursor;

// =============================================================================
// ClipboardPollingAdapter
// =============================================================================

/// Adapter for clipboard content polling.
///
/// Polls the clipboard at `poll_interval_ms` and emits a record when the
/// content hash changes. To inject a mock backend for testing, use
/// [`ClipboardPollingAdapter::from_backend`].
pub struct ClipboardPollingAdapter {
    backend: Arc<Mutex<dyn ClipboardBackend>>,
}

impl ClipboardPollingAdapter {
    /// Create an adapter backed by `arboard`.
    pub fn new() -> ParserResult<Self> {
        let backend = ArboardBackend::new()?;
        Ok(Self {
            backend: Arc::new(Mutex::new(backend)),
        })
    }

    /// Create an adapter from a custom backend (useful for tests).
    pub fn from_backend(backend: impl ClipboardBackend + 'static) -> Self {
        Self {
            backend: Arc::new(Mutex::new(backend)),
        }
    }
}

impl Default for ClipboardPollingAdapter {
    #[allow(clippy::unwrap_used)]
    fn default() -> Self {
        Self::new().expect("failed to initialize arboard clipboard backend")
    }
}

#[async_trait]
impl InputShapeAdapter for ClipboardPollingAdapter {
    type Config = ClipboardPollingConfig;
    type Cursor = ClipboardPollingCursor;
    const KIND: InputShapeKind = InputShapeKind::Polling;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let backend = Arc::clone(&self.backend);
        let poll_interval = std::time::Duration::from_millis(config.poll_interval_ms);
        let max_bytes = config.max_content_bytes;

        let stream = build_clipboard_stream(material_id, backend, poll_interval, max_bytes);
        Ok(stream)
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(ClipboardPollingCursor)
    }
}

fn build_clipboard_stream(
    material_id: Id<SourceMaterial>,
    backend: Arc<Mutex<dyn ClipboardBackend>>,
    poll_interval: std::time::Duration,
    max_bytes: usize,
) -> BoxStream<'static, ParserResult<SourceRecord>> {
    let stream = async_stream::stream! {
        let mut last_hash: Option<[u8; 32]> = None;
        let mut change_counter: u64 = 0;

        loop {
            tokio::time::sleep(poll_interval).await;

            let content = {
                let mut guard = backend.lock().await;
                match guard.get_text() {
                    Ok(c) => c,
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            };

            let Some(text) = content else {
                // Clipboard empty or non-text — skip.
                continue;
            };

            if text.len() > max_bytes {
                // Too large — silently drop.
                continue;
            }

            let hash = *blake3::hash(text.as_bytes()).as_bytes();

            if Some(hash) == last_hash {
                // No change — skip.
                continue;
            }

            last_hash = Some(hash);

            let bytes = text.into_bytes();
            let anchor = MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: change_counter,
            };
            change_counter += 1;

            yield Ok(SourceRecord {
                material_id,
                anchor,
                bytes,
                logical_path: None,
                source_ts_hint: None,
                metadata: serde_json::Value::Null,
            });
        }
    };

    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::time::{Duration, timeout};
    use xtask::sandbox::prelude::sinex_test;

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    fn make_adapter(snapshots: Vec<Option<String>>) -> ClipboardPollingAdapter {
        ClipboardPollingAdapter::from_backend(MockClipboardBackend::new(snapshots))
    }

    #[sinex_test]
    async fn test_clipboard_emits_record_on_change() -> xtask::sandbox::TestResult<()> {
        let adapter = make_adapter(vec![
            Some("hello".into()),
            None, // empty → skip
            Some("world".into()),
        ]);
        let config = ClipboardPollingConfig {
            poll_interval_ms: 1,
            max_content_bytes: 1024,
        };

        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(2).collect())
            .await
            .unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].as_ref().unwrap().bytes, b"hello");
        assert_eq!(records[1].as_ref().unwrap().bytes, b"world");
        Ok(())
    }

    #[sinex_test]
    async fn test_clipboard_deduplicates_unchanged_content() -> xtask::sandbox::TestResult<()> {
        let adapter = make_adapter(vec![
            Some("same".into()),
            Some("same".into()),
            Some("same".into()),
            Some("different".into()),
        ]);
        let config = ClipboardPollingConfig {
            poll_interval_ms: 1,
            max_content_bytes: 1024,
        };

        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(2).collect())
            .await
            .unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].as_ref().unwrap().bytes, b"same");
        assert_eq!(records[1].as_ref().unwrap().bytes, b"different");
        Ok(())
    }

    #[sinex_test]
    async fn test_clipboard_skips_oversized_content() -> xtask::sandbox::TestResult<()> {
        let big = "x".repeat(10);
        let adapter = make_adapter(vec![Some(big.clone()), Some("small".into())]);
        let config = ClipboardPollingConfig {
            poll_interval_ms: 1,
            max_content_bytes: 5, // big will be dropped, small will pass
        };

        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(1).collect())
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].as_ref().unwrap().bytes, b"small");
        Ok(())
    }

    #[sinex_test]
    async fn test_clipboard_anchor_is_stream_frame() -> xtask::sandbox::TestResult<()> {
        let adapter = make_adapter(vec![Some("text".into())]);
        let config = ClipboardPollingConfig {
            poll_interval_ms: 1,
            max_content_bytes: 1024,
        };

        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(1).collect())
            .await
            .unwrap();

        assert!(matches!(
            records[0].as_ref().unwrap().anchor,
            MaterialAnchor::StreamFrame { frame_index: 0, .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_clipboard_change_counter_monotonic() -> xtask::sandbox::TestResult<()> {
        let adapter = make_adapter(vec![Some("a".into()), Some("b".into()), Some("c".into())]);
        let config = ClipboardPollingConfig {
            poll_interval_ms: 1,
            max_content_bytes: 1024,
        };

        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(3).collect())
            .await
            .unwrap();

        let indices: Vec<u64> = records
            .iter()
            .map(|r| match &r.as_ref().unwrap().anchor {
                MaterialAnchor::StreamFrame { frame_index, .. } => *frame_index,
                _ => panic!("wrong anchor"),
            })
            .collect();

        assert_eq!(indices, vec![0, 1, 2]);
        Ok(())
    }

    #[sinex_test]
    async fn test_clipboard_cursor_after_is_unit() -> xtask::sandbox::TestResult<()> {
        let adapter = make_adapter(vec![]);
        let record = SourceRecord {
            material_id: dummy_material_id(),
            anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0,
            },
            bytes: b"x".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let cursor = adapter.cursor_after(&record).unwrap();
        assert_eq!(cursor, ClipboardPollingCursor);
        Ok(())
    }

    #[sinex_test]
    async fn test_kind_is_polling() -> xtask::sandbox::TestResult<()> {
        assert_eq!(ClipboardPollingAdapter::KIND, InputShapeKind::Polling);
        Ok(())
    }
}
