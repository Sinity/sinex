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
                                ParserError::Adapter(format!("failed to serialize api record: {e}"))
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
                    let next_state = next_page_cursor.map(|nc| (Some(nc), page_index + 1));

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
#[path = "api_cursor_test.rs"]
mod tests;
