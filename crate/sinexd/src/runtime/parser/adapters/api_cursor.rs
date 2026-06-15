//! API cursor-based pagination adapter.
//!
//! Drives a small [`ApiClient`] trait through a paginated fetch loop, with
//! built-in exponential-backoff-with-jitter retry. Suits Spotify, Goodreads,
//! Lastpass, Raindrop and similar third-party APIs whose paginated history is
//! the canonical record stream.
//!
//! The retry layer is hand-rolled (no `backon` workspace dep) because the
//! shape we need is narrow: a small number of attempts, exponential delay,
//! deterministic jitter for tests, and a single error pass-through.
//!
//! # Cursor semantics
//!
//! [`ApiCursorAdapter`] lazily fetches pages in [`InputShapeAdapter::open`],
//! yielding one [`SourceRecord`] per API record. Each page is only fetched
//! once the consumer is ready for more records, enabling per-page
//! checkpointing and ensuring a late-page failure does not discard progress
//! from earlier pages that have already been yielded and checkpointed.
//! Cursor advancement is per-record:
//!
//! - Records that are **not** the last in their page carry the **start** cursor
//!   of that page (so a mid-page failure retries the full page from the same
//!   position).
//! - The **last** record of each page carries the **next-page cursor** (so
//!   normal consumption advances past the page boundary).
//!
//! The next-page cursor is embedded in `SourceRecord.metadata` under the key
//! `"api_cursor_next"`. [`cursor_after`][InputShapeAdapter::cursor_after]
//! extracts it.

use std::{error::Error, fmt, future::Future, sync::Arc, time::Duration};

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
// ApiClient
// =============================================================================

/// Per-page response from an API client.
#[derive(Debug, Clone)]
pub struct ApiFetchPage<Record> {
    /// Records in this page.
    pub records: Vec<Record>,
    /// Cursor to pass on the next `fetch()` call; `None` signals end of stream.
    pub next_cursor: Option<String>,
    /// Optional ETag for conditional-fetch optimisation.
    pub etag: Option<String>,
}

/// Pluggable API client — one method, async, returning a single page.
///
/// Implementors supply domain-specific HTTP/auth logic. The adapter drives the
/// pagination loop and applies the [`RetryPolicy`] on transient failures.
///
/// # Example
///
/// ```rust,ignore
/// struct MyClient { token: String }
///
/// impl ApiClient for MyClient {
///     type Record = serde_json::Value;
///     type Error = MyError;
///
///     async fn fetch(&self, cursor: Option<&str>) -> Result<ApiFetchPage<Self::Record>, Self::Error> {
///         let url = format!("https://api.example.com/items?cursor={}", cursor.unwrap_or(""));
///         // … HTTP call …
///     }
/// }
/// ```
pub trait ApiClient: Send + Sync {
    /// The type of each record returned by the API.
    ///
    /// Must implement [`Serialize`] so the adapter can convert it to
    /// raw bytes for the [`SourceRecord`] payload.
    type Record: Serialize + Send + Sync + 'static;

    /// The error type returned on a failed fetch.
    type Error: Error + Send + Sync + 'static;

    /// Fetch one page of records starting from `cursor`.
    ///
    /// A `None` cursor means "start from the beginning".
    fn fetch(
        &self,
        cursor: Option<&str>,
    ) -> impl Future<Output = Result<ApiFetchPage<Self::Record>, Self::Error>> + Send;
}

// =============================================================================
// RetryPolicy
// =============================================================================

/// Exponential-backoff retry policy with deterministic jitter.
///
/// Jitter is derived from a fast xorshift seeded by the attempt index so
/// tests can observe deterministic delay sequences without needing real time.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first). Set to 1 to disable
    /// retries.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub base_delay: Duration,
    /// Upper bound applied after the exponential scale is computed.
    pub max_delay: Duration,
    /// Fractional jitter range `[1 - r, 1 + r]` applied to the scaled delay.
    pub jitter_ratio: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(10),
            jitter_ratio: 0.25,
        }
    }
}

impl RetryPolicy {
    /// No retry — fail fast on first error. Useful for tests and latency-critical code.
    #[must_use]
    pub fn never() -> Self {
        Self {
            max_attempts: 1,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter_ratio: 0.0,
        }
    }

    /// Compute the sleep duration before `attempt` (0-indexed, first call = 0).
    ///
    /// Attempt 0 → zero delay (immediate first try). Subsequent attempts
    /// scale by `2^(attempt - 1)`, capped at `max_delay`, with ±`jitter_ratio`
    /// applied via xorshift.
    fn delay_for_attempt(self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }
        // Exponential scale — cap exponent at 16 to avoid u64 overflow.
        let exponent = u32::min(attempt.saturating_sub(1), 16);
        let multiplier: u64 = 1u64 << exponent;
        let scaled = self.base_delay.saturating_mul(multiplier as u32);
        let capped = if scaled > self.max_delay {
            self.max_delay
        } else {
            scaled
        };
        if self.jitter_ratio == 0.0 {
            return capped;
        }
        // Deterministic xorshift seeded by attempt index.
        let mut x = u64::from(attempt).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        let seed = (x & 0xFFFF) as f64 / 65535.0;
        let factor = 1.0 + (seed * 2.0 - 1.0) * self.jitter_ratio;
        let nanos = (capped.as_secs_f64() * factor).max(0.0);
        Duration::from_secs_f64(nanos)
    }
}

// =============================================================================
// ApiFetchError
// =============================================================================

/// Errors raised by the API cursor adapter.
#[derive(Debug)]
pub enum ApiFetchError {
    /// All retry attempts were exhausted; wraps the most recent client error.
    Exhausted {
        attempts: u32,
        source: Box<dyn Error + Send + Sync + 'static>,
    },
}

impl fmt::Display for ApiFetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted { attempts, source } => {
                write!(f, "api fetch exhausted after {attempts} attempts: {source}")
            }
        }
    }
}

impl Error for ApiFetchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Exhausted { source, .. } => Some(&**source),
        }
    }
}

// =============================================================================
// Config and cursor
// =============================================================================

/// Configuration for [`ApiCursorAdapter`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ApiCursorConfig {
    /// Optional starting cursor value for the first page.
    ///
    /// When `None`, pagination begins at the API's natural starting point.
    /// When a runtime checkpoint is present it takes priority over this value,
    /// so this field acts as a static fallback for brand-new sources.
    #[serde(default)]
    pub initial_cursor: Option<String>,
}

/// Cursor for [`ApiCursorAdapter`] — the API-defined cursor token after the
/// last successfully consumed page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiCursorPosition {
    /// The cursor string to pass to the next page fetch.
    ///
    /// `None` here means the entire stream has been consumed (the last page had
    /// no `next_cursor`) — a *terminal* checkpoint. A brand-new source that has
    /// never been fetched is represented by the absence of any
    /// `ApiCursorPosition` (i.e. `open(cursor = None)`), not by this field, so
    /// the two states are distinguishable and `open` does not re-import a
    /// completed source on restart.
    pub last_cursor: Option<String>,
    /// ETag from the last completed page response.
    pub last_etag: Option<String>,
}

// =============================================================================
// Metadata keys embedded in SourceRecord
// =============================================================================

const META_CURSOR_AFTER: &str = "api_cursor_next";
const META_ETAG_AFTER: &str = "api_etag_after";
const META_PAGE_INDEX: &str = "api_page_index";

fn build_record_metadata(
    cursor_after: Option<&str>,
    etag_after: Option<&str>,
    page_index: u64,
) -> JsonValue {
    let mut map = Map::new();
    map.insert(
        META_CURSOR_AFTER.to_owned(),
        cursor_after
            .map(|s| JsonValue::String(s.to_owned()))
            .unwrap_or(JsonValue::Null),
    );
    map.insert(
        META_ETAG_AFTER.to_owned(),
        etag_after
            .map(|s| JsonValue::String(s.to_owned()))
            .unwrap_or(JsonValue::Null),
    );
    map.insert(
        META_PAGE_INDEX.to_owned(),
        JsonValue::Number(page_index.into()),
    );
    JsonValue::Object(map)
}

// =============================================================================
// ApiCursorAdapter
// =============================================================================

/// Input-shape adapter for API cursor-based pagination sources.
///
/// Drives an [`ApiClient`] through a full paginated walk in [`open`],
/// yielding one [`SourceRecord`] per API record (serialized as JSON bytes).
/// The adapter handles exponential-backoff retry per page.
///
/// # Cursor advancement
///
/// [`cursor_after`] reads `record.metadata["api_cursor_next"]`:
/// - Mid-page records carry the **start** cursor of their page → a failure
///   re-fetches the entire page safely.
/// - The **last** record of each page carries the **next-page** cursor →
///   normal consumption advances to the next page.
///
/// # Anchor
///
/// Each record's anchor is [`MaterialAnchor::StreamFrame`] with
/// `material_offset = page_index` and `frame_index = record_index_in_page`.
///
/// # Usage
///
/// ```rust,ignore
/// let adapter = ApiCursorAdapter::new(MySpotifyClient::new(token));
/// // Override retry policy for tests:
/// let adapter = adapter.with_retry(RetryPolicy::never());
/// let stream = adapter.open(material_id, &ApiCursorConfig::default(), None).await?;
/// ```
///
/// [`open`]: InputShapeAdapter::open
/// [`cursor_after`]: InputShapeAdapter::cursor_after
pub struct ApiCursorAdapter<C: ApiClient> {
    client: Arc<C>,
    retry: RetryPolicy,
}

/// Fetch one page from `client` with exponential-backoff retry.
///
/// Extracted as a free function so the `open()` lazy-unfold closure can call
/// it without holding a reference to `ApiCursorAdapter` across an await point.
async fn fetch_page<C: ApiClient>(
    client: &C,
    retry: RetryPolicy,
    cursor: Option<&str>,
) -> Result<ApiFetchPage<C::Record>, ApiFetchError> {
    let mut last_error: Option<Box<dyn Error + Send + Sync + 'static>> = None;

    for attempt in 0..retry.max_attempts {
        if attempt > 0 {
            let delay = retry.delay_for_attempt(attempt);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
        match client.fetch(cursor).await {
            Ok(page) => return Ok(page),
            Err(e) => last_error = Some(Box::new(e)),
        }
    }

    Err(ApiFetchError::Exhausted {
        attempts: retry.max_attempts,
        source: last_error.unwrap_or_else(|| {
            // max_attempts == 0 is degenerate; surface a synthetic error.
            struct ZeroAttempts;
            impl fmt::Display for ZeroAttempts {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("retry policy max_attempts was zero")
                }
            }
            impl fmt::Debug for ZeroAttempts {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("ZeroAttempts")
                }
            }
            impl Error for ZeroAttempts {}
            Box::new(ZeroAttempts)
        }),
    })
}

impl<C: ApiClient + 'static> ApiCursorAdapter<C> {
    /// Build a new adapter with the default [`RetryPolicy`].
    #[must_use]
    pub fn new(client: C) -> Self {
        Self {
            client: Arc::new(client),
            retry: RetryPolicy::default(),
        }
    }

    /// Override the retry policy (e.g., [`RetryPolicy::never()`] in tests).
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

}

#[async_trait]
impl<C> InputShapeAdapter for ApiCursorAdapter<C>
where
    C: ApiClient + 'static,
{
    type Config = ApiCursorConfig;
    type Cursor = ApiCursorPosition;
    const KIND: InputShapeKind = InputShapeKind::ApiCursor;

    /// Lazily walk pages from the given cursor position, yielding records
    /// one page at a time.
    ///
    /// Pages are fetched on-demand: the next page is only fetched once the
    /// consumer has polled past the last record of the previous page. This
    /// enables the runtime to checkpoint after each page and means a
    /// late-page failure does not discard progress from earlier pages that
    /// have already been yielded.
    ///
    /// A fetch failure surfaces as an `Err` item in the returned stream
    /// followed by stream termination; no records from the failed page are
    /// yielded.
    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        // Disambiguate the three startup states via the `Option<ApiCursorPosition>`
        // wrapper. A terminal checkpoint and a brand-new source both carry
        // `last_cursor == None`, so flattening them together (the old
        // `and_then(..).or(initial_cursor)`) re-imported the whole history on
        // every restart after completion (Codex review, PR #1772):
        //   - no checkpoint                  → brand-new source: start at initial_cursor
        //   - checkpoint, last_cursor = Some → resume from the saved cursor
        //   - checkpoint, last_cursor = None → prior run consumed the final page;
        //                                      resuming would re-import, so stop.
        let start_cursor: Option<String> = match cursor.as_ref() {
            Some(pos) => match pos.last_cursor.as_deref() {
                Some(saved) => Some(saved.to_owned()),
                None => return Ok(stream::empty().boxed()),
            },
            None => config.initial_cursor.clone(),
        };

        let client = Arc::clone(&self.client);
        let retry = self.retry;

        // State: None = exhausted, Some((cursor, page_index)) = next page to fetch.
        //
        // Each unfold step fetches exactly one page and yields its records as a
        // Vec. flat_map(stream::iter) flattens the per-page vecs into individual
        // SourceRecord items so the consumer sees a single flat stream.
        let page_stream = stream::unfold(
            Some((start_cursor, 0u64)),
            move |state: Option<(Option<String>, u64)>| {
                let client = Arc::clone(&client);
                async move {
                    let (current_cursor, page_index) = state?;

                    let page =
                        match fetch_page(client.as_ref(), retry, current_cursor.as_deref()).await {
                            Ok(p) => p,
                            Err(e) => {
                                // Surface the fetch error as a single Err item, then
                                // terminate the stream (next_state = None).
                                let err = Err(ParserError::Adapter(e.to_string()));
                                return Some((vec![err], None));
                            }
                        };

                    let next_page_cursor = page.next_cursor.clone();
                    let etag = page.etag.clone();
                    let total = page.records.len();

                    let records: Vec<ParserResult<SourceRecord>> = page
                        .records
                        .into_iter()
                        .enumerate()
                        .map(|(idx, record)| {
                            // Mid-page records carry the pre-page cursor → retry
                            // re-fetches the same page. Only the last record of
                            // each page carries the next-page cursor.
                            let (cursor_after, etag_after) = if idx + 1 == total {
                                (next_page_cursor.as_deref(), etag.as_deref())
                            } else {
                                (current_cursor.as_deref(), None)
                            };

                            let bytes = serde_json::to_vec(&record).map_err(|e| {
                                ParserError::Adapter(format!(
                                    "failed to serialize api record: {e}"
                                ))
                            })?;
                            let metadata =
                                build_record_metadata(cursor_after, etag_after, page_index);

                            Ok(SourceRecord {
                                material_id,
                                anchor: MaterialAnchor::StreamFrame {
                                    material_offset: page_index,
                                    frame_index: idx as u64,
                                },
                                bytes,
                                logical_path: None,
                                source_ts_hint: None,
                                metadata,
                            })
                        })
                        .collect();

                    // Advance to the next page cursor, or terminate when this was
                    // the final page (next_page_cursor == None).
                    let next_state = next_page_cursor
                        .map(|nc| (Some(nc), page_index + 1));

                    Some((records, next_state))
                }
            },
        )
        .flat_map(stream::iter);

        Ok(page_stream.boxed())
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        let last_cursor = record
            .metadata
            .get(META_CURSOR_AFTER)
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let last_etag = record
            .metadata
            .get(META_ETAG_AFTER)
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        Ok(ApiCursorPosition {
            last_cursor,
            last_etag,
        })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use futures::StreamExt;
    use xtask::sandbox::prelude::sinex_test;

    use super::*;

    // -------------------------------------------------------------------------
    // Mock ApiClient
    // -------------------------------------------------------------------------

    #[derive(Debug)]
    struct MockError(String);

    impl fmt::Display for MockError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "mock error: {}", self.0)
        }
    }

    impl Error for MockError {}

    /// A client backed by a fixed list of pages.
    struct MockClient {
        pages: Vec<Vec<serde_json::Value>>,
        fail_attempts: u32,
        attempt_counter: AtomicU32,
    }

    impl MockClient {
        fn new(pages: Vec<Vec<serde_json::Value>>) -> Self {
            Self {
                pages,
                fail_attempts: 0,
                attempt_counter: AtomicU32::new(0),
            }
        }

        /// Fail on the first `n` calls, then succeed.
        fn with_transient_failures(mut self, n: u32) -> Self {
            self.fail_attempts = n;
            self
        }
    }

    impl ApiClient for MockClient {
        type Record = serde_json::Value;
        type Error = MockError;

        async fn fetch(
            &self,
            cursor: Option<&str>,
        ) -> Result<ApiFetchPage<Self::Record>, Self::Error> {
            let attempt = self.attempt_counter.fetch_add(1, Ordering::SeqCst);
            if attempt < self.fail_attempts {
                return Err(MockError(format!("transient failure #{attempt}")));
            }

            // Cursor encodes page index as decimal string; None → page 0.
            let page_idx: usize = cursor
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);

            let records = self
                .pages
                .get(page_idx)
                .cloned()
                .unwrap_or_default();

            let next_page = page_idx + 1;
            let next_cursor = if next_page < self.pages.len() {
                Some(next_page.to_string())
            } else {
                None
            };

            Ok(ApiFetchPage {
                records,
                next_cursor,
                etag: Some(format!("etag-{page_idx}")),
            })
        }
    }

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn all_pages_walked_single_page() -> xtask::sandbox::TestResult<()> {
        let client = MockClient::new(vec![vec![
            serde_json::json!({"id": 1}),
            serde_json::json!({"id": 2}),
        ]]);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert_eq!(records.len(), 2, "expected exactly 2 records from 1 page");
        assert!(records.iter().all(|r| r.is_ok()));
        Ok(())
    }

    #[sinex_test]
    async fn all_pages_walked_multiple_pages() -> xtask::sandbox::TestResult<()> {
        let pages = vec![
            vec![serde_json::json!({"n": 1}), serde_json::json!({"n": 2})],
            vec![serde_json::json!({"n": 3}), serde_json::json!({"n": 4})],
            vec![serde_json::json!({"n": 5})],
        ];
        let client = MockClient::new(pages);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert_eq!(records.len(), 5, "expected all records across 3 pages");
        Ok(())
    }

    #[sinex_test]
    async fn cursor_terminates_at_last_page() -> xtask::sandbox::TestResult<()> {
        let pages = vec![
            vec![serde_json::json!({"a": 1}), serde_json::json!({"a": 2})],
            vec![serde_json::json!({"a": 3})],
        ];
        let client = MockClient::new(pages);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        // Last record should have no next cursor (end of stream).
        let last = records.last().unwrap().as_ref().unwrap();
        let cursor = adapter.cursor_after(last).unwrap();
        assert!(
            cursor.last_cursor.is_none(),
            "cursor after last record should be None, got {:?}",
            cursor.last_cursor
        );
        Ok(())
    }

    #[sinex_test]
    async fn mid_page_cursor_restarts_same_page() -> xtask::sandbox::TestResult<()> {
        let pages = vec![
            vec![serde_json::json!({"i": 1}), serde_json::json!({"i": 2})],
            vec![serde_json::json!({"i": 3})],
        ];
        let client = MockClient::new(pages);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        // records[0] is the first record of page 0; its cursor should be None
        // (the start of page 0, which starts from None).
        let first = records[0].as_ref().unwrap();
        let cursor_after_first = adapter.cursor_after(first).unwrap();
        assert!(
            cursor_after_first.last_cursor.is_none(),
            "first record should carry page-start cursor (None)"
        );

        // records[1] is the last record of page 0; cursor should advance to page 1.
        let second = records[1].as_ref().unwrap();
        let cursor_after_second = adapter.cursor_after(second).unwrap();
        assert_eq!(
            cursor_after_second.last_cursor.as_deref(),
            Some("1"),
            "last record of page 0 should carry next-page cursor '1'"
        );
        Ok(())
    }

    #[sinex_test]
    async fn retry_succeeds_after_transient_failure() -> xtask::sandbox::TestResult<()> {
        let client = MockClient::new(vec![vec![serde_json::json!({"ok": true})]])
            .with_transient_failures(2);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::ZERO, // no actual sleep in tests
            max_delay: Duration::ZERO,
            jitter_ratio: 0.0,
        });

        let stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert_eq!(records.len(), 1);
        assert!(records[0].is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn exhausted_retries_surface_typed_error() -> xtask::sandbox::TestResult<()> {
        // With lazy streaming, open() always succeeds; the fetch error arrives
        // as the first Err item in the stream rather than from open() itself.
        let client = MockClient::new(vec![vec![serde_json::json!({"never": "succeeds"})]])
            .with_transient_failures(999);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter_ratio: 0.0,
        });

        let stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .expect("open() must succeed even when the first page fetch will fail");

        let items: Vec<_> = stream.collect().await;
        assert_eq!(items.len(), 1, "expected exactly one error item in the stream");
        match &items[0] {
            Err(ParserError::Adapter(msg)) => {
                assert!(
                    msg.contains("exhausted after 3 attempts"),
                    "expected exhausted error message, got: {msg}"
                );
            }
            Ok(_) => panic!("expected Err item in stream, got Ok"),
            Err(e) => panic!("expected Adapter error, got: {e}"),
        }
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Laziness proof — pages must be fetched one at a time, not upfront.
    // -------------------------------------------------------------------------

    /// A mock client that exposes a shared fetch counter so tests can observe
    /// exactly how many page fetches have occurred at any point in the stream.
    struct TrackedMockClient {
        pages: Vec<Vec<serde_json::Value>>,
        fetch_count: Arc<AtomicU32>,
    }

    impl ApiClient for TrackedMockClient {
        type Record = serde_json::Value;
        type Error = MockError;

        async fn fetch(
            &self,
            cursor: Option<&str>,
        ) -> Result<ApiFetchPage<Self::Record>, Self::Error> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);

            let page_idx: usize = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);
            let records = self.pages.get(page_idx).cloned().unwrap_or_default();
            let next_page = page_idx + 1;
            let next_cursor = if next_page < self.pages.len() {
                Some(next_page.to_string())
            } else {
                None
            };
            Ok(ApiFetchPage { records, next_cursor, etag: None })
        }
    }

    #[sinex_test]
    async fn pages_fetched_lazily_one_at_a_time() -> xtask::sandbox::TestResult<()> {
        let fetch_count = Arc::new(AtomicU32::new(0));
        let client = TrackedMockClient {
            pages: vec![
                vec![serde_json::json!({"page": 0, "i": 0}), serde_json::json!({"page": 0, "i": 1})],
                vec![serde_json::json!({"page": 1, "i": 0})],
            ],
            fetch_count: Arc::clone(&fetch_count),
        };
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let mut stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .unwrap();

        // Stream not yet polled — no page should have been fetched.
        assert_eq!(
            fetch_count.load(Ordering::SeqCst),
            0,
            "no page should be fetched before the stream is polled"
        );

        // Poll the first record (page 0, record 0). This triggers the first fetch.
        let _ = stream.next().await.expect("expected a record");
        assert_eq!(
            fetch_count.load(Ordering::SeqCst),
            1,
            "exactly one page should be fetched after consuming the first record"
        );

        // Poll the second record (page 0, record 1). Still only page 0 fetched.
        let _ = stream.next().await.expect("expected a record");
        assert_eq!(
            fetch_count.load(Ordering::SeqCst),
            1,
            "still one page fetched after consuming the second record of page 0"
        );

        // Poll the third record (page 1, record 0). This triggers the second fetch.
        let _ = stream.next().await.expect("expected a record");
        assert_eq!(
            fetch_count.load(Ordering::SeqCst),
            2,
            "second page should be fetched only when the consumer advances past page 0"
        );

        // Stream should now be exhausted.
        assert!(
            stream.next().await.is_none(),
            "stream should be exhausted after all records"
        );
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2, "total pages fetched should be 2");
        Ok(())
    }

    #[sinex_test]
    async fn input_shape_kind_is_api_cursor() -> xtask::sandbox::TestResult<()> {
        assert_eq!(
            ApiCursorAdapter::<MockClient>::KIND,
            InputShapeKind::ApiCursor
        );
        Ok(())
    }

    #[sinex_test]
    async fn anchor_is_stream_frame() -> xtask::sandbox::TestResult<()> {
        let pages = vec![
            vec![serde_json::json!({"x": 1}), serde_json::json!({"x": 2})],
            vec![serde_json::json!({"x": 3})],
        ];
        let client = MockClient::new(pages);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        // Page 0, record 0.
        let r00 = records[0].as_ref().unwrap();
        assert!(
            matches!(
                r00.anchor,
                MaterialAnchor::StreamFrame { material_offset: 0, frame_index: 0 }
            ),
            "unexpected anchor: {:?}",
            r00.anchor
        );

        // Page 0, record 1.
        let r01 = records[1].as_ref().unwrap();
        assert!(
            matches!(
                r01.anchor,
                MaterialAnchor::StreamFrame { material_offset: 0, frame_index: 1 }
            ),
            "unexpected anchor: {:?}",
            r01.anchor
        );

        // Page 1, record 0.
        let r10 = records[2].as_ref().unwrap();
        assert!(
            matches!(
                r10.anchor,
                MaterialAnchor::StreamFrame { material_offset: 1, frame_index: 0 }
            ),
            "unexpected anchor: {:?}",
            r10.anchor
        );
        Ok(())
    }

    #[sinex_test]
    async fn initial_cursor_from_config() -> xtask::sandbox::TestResult<()> {
        let pages = vec![
            vec![serde_json::json!({"p": 0})], // page 0 (ignored when starting from page 1)
            vec![serde_json::json!({"p": 1})], // page 1 — the start page
        ];
        let client = MockClient::new(pages);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let config = ApiCursorConfig {
            initial_cursor: Some("1".to_owned()),
        };
        let stream = adapter
            .open(dummy_material_id(), &config, None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert_eq!(records.len(), 1, "should start from page 1 and get 1 record");
        let val: serde_json::Value =
            serde_json::from_slice(&records[0].as_ref().unwrap().bytes).unwrap();
        assert_eq!(val["p"], 1);
        Ok(())
    }

    #[sinex_test]
    async fn runtime_checkpoint_overrides_config_initial_cursor() -> xtask::sandbox::TestResult<()>
    {
        let pages = vec![
            vec![serde_json::json!({"p": 0})],
            vec![serde_json::json!({"p": 1})],
            vec![serde_json::json!({"p": 2})],
        ];
        let client = MockClient::new(pages);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        // Config says start from 0, but checkpoint says start from 2.
        let config = ApiCursorConfig {
            initial_cursor: Some("0".to_owned()),
        };
        let checkpoint = Some(ApiCursorPosition {
            last_cursor: Some("2".to_owned()),
            last_etag: None,
        });
        let stream = adapter
            .open(dummy_material_id(), &config, checkpoint)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert_eq!(records.len(), 1, "checkpoint should override config cursor");
        let val: serde_json::Value =
            serde_json::from_slice(&records[0].as_ref().unwrap().bytes).unwrap();
        assert_eq!(val["p"], 2);
        Ok(())
    }

    #[sinex_test]
    async fn terminal_checkpoint_does_not_reimport() -> xtask::sandbox::TestResult<()> {
        // A checkpoint whose last_cursor is None is terminal — the prior run
        // consumed the final page. open() must yield nothing rather than restart
        // from the beginning (or config.initial_cursor) and re-import the whole
        // history on every poll/restart (Codex review, PR #1772).
        let pages = vec![
            vec![serde_json::json!({"p": 0})],
            vec![serde_json::json!({"p": 1})],
        ];
        let client = MockClient::new(pages);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let config = ApiCursorConfig {
            initial_cursor: Some("0".to_owned()),
        };
        let terminal = Some(ApiCursorPosition {
            last_cursor: None,
            last_etag: Some("etag-final".to_owned()),
        });
        let stream = adapter
            .open(dummy_material_id(), &config, terminal)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert!(
            records.is_empty(),
            "terminal checkpoint must not re-import; got {} records",
            records.len()
        );
        Ok(())
    }

    #[sinex_test]
    async fn etag_carried_on_last_record_only() -> xtask::sandbox::TestResult<()> {
        let client = MockClient::new(vec![vec![
            serde_json::json!({"i": 1}),
            serde_json::json!({"i": 2}),
        ]]);
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        // First record should have no etag (mid-page).
        let first = records[0].as_ref().unwrap();
        let cursor_first = adapter.cursor_after(first).unwrap();
        assert!(cursor_first.last_etag.is_none(), "mid-page record should have no etag");

        // Last record carries the page's etag.
        let last = records.last().unwrap().as_ref().unwrap();
        let cursor_last = adapter.cursor_after(last).unwrap();
        assert_eq!(
            cursor_last.last_etag.as_deref(),
            Some("etag-0"),
            "last record should carry page etag"
        );
        Ok(())
    }

    // Verify retry delay computation is correct (no actual sleeps needed).
    #[sinex_test]
    async fn retry_policy_delay_scales_exponentially() -> xtask::sandbox::TestResult<()> {
        let policy = RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            jitter_ratio: 0.0,
        };

        // attempt 0 → 0ms (immediate)
        assert_eq!(policy.delay_for_attempt(0), Duration::ZERO);
        // attempt 1 → 100ms (2^0 * 100ms)
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(100));
        // attempt 2 → 200ms (2^1 * 100ms)
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(200));
        // attempt 3 → 400ms (2^2 * 100ms)
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(400));
        // attempt 4 → 800ms (2^3 * 100ms)
        assert_eq!(policy.delay_for_attempt(4), Duration::from_millis(800));
        Ok(())
    }

    #[sinex_test]
    async fn retry_policy_delay_caps_at_max() -> xtask::sandbox::TestResult<()> {
        let policy = RetryPolicy {
            max_attempts: 20,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            jitter_ratio: 0.0,
        };

        // Large attempt indices should be capped at max_delay.
        let d = policy.delay_for_attempt(10);
        assert!(
            d <= Duration::from_millis(500),
            "delay {d:?} should not exceed max_delay"
        );
        Ok(())
    }

    #[sinex_test]
    async fn empty_page_list_yields_no_records() -> xtask::sandbox::TestResult<()> {
        let client = MockClient::new(vec![vec![]]); // one empty page
        let adapter = ApiCursorAdapter::new(client).with_retry(RetryPolicy::never());

        let stream = adapter
            .open(dummy_material_id(), &ApiCursorConfig::default(), None)
            .await
            .unwrap();
        let records: Vec<_> = stream.collect().await;

        assert!(records.is_empty(), "empty page should yield zero records");
        Ok(())
    }
}
