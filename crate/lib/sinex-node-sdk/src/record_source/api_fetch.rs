//! API-backed fetch record-source adapter.
//!
//! Drives a small `ApiClient` trait through a paginated fetch loop, with
//! built-in exponential-backoff-with-jitter retry. Suits Spotify, Goodreads,
//! Lastpass, Raindrop and similar third-party APIs whose paginated history is
//! the canonical record stream.
//!
//! The retry layer is hand-rolled (no `backon` workspace dep) because the
//! shape we need is narrow: a small number of attempts, exponential delay,
//! deterministic jitter for tests, and a single error pass-through.
//!
//! The checkpoint records `last_cursor`, `last_etag`, and the `last_fetched`
//! timestamp so callers can skip re-fetching unchanged windows.

use std::{error::Error, fmt, future::Future, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::Timestamp;
use tokio::sync::Mutex;

use super::{
    RecordReadBatch, RecordReadHorizon, RecordReadItem, RecordSource, RecordSourceDescriptor,
    RecordSourceKind, RecordSourceObservation,
};

/// Per-page response from an API client.
#[derive(Debug, Clone)]
pub struct ApiFetchPage<Record> {
    pub records: Vec<Record>,
    pub next_cursor: Option<String>,
    pub etag: Option<String>,
}

/// Pluggable API client. One method, async, returning a single page.
pub trait ApiClient: Send + Sync {
    type Record: Send + Sync + 'static;
    type Error: Error + Send + Sync + 'static;

    fn fetch(
        &self,
        cursor: Option<&str>,
    ) -> impl Future<Output = Result<ApiFetchPage<Self::Record>, Self::Error>> + Send;
}

/// Checkpoint for an API-backed fetch source.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiFetchCheckpoint {
    pub last_cursor: Option<String>,
    pub last_etag: Option<String>,
    pub last_fetched: Option<Timestamp>,
}

impl ApiFetchCheckpoint {
    #[must_use]
    pub fn new(
        last_cursor: Option<String>,
        last_etag: Option<String>,
        last_fetched: Option<Timestamp>,
    ) -> Self {
        Self {
            last_cursor,
            last_etag,
            last_fetched,
        }
    }
}

/// Hand-rolled retry policy: max attempts, base delay, multiplicative
/// backoff, deterministic jitter via xorshift seeded by attempt index.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
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
    /// No retry — fail fast on first error. Useful for tests.
    #[must_use]
    pub fn never() -> Self {
        Self {
            max_attempts: 1,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter_ratio: 0.0,
        }
    }

    fn delay_for_attempt(self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }
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

/// Errors raised by the API-backed fetch source.
#[derive(Debug)]
pub enum ApiFetchError {
    /// All retry attempts were exhausted; the most recent client error is wrapped.
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

/// API-backed fetch record source.
pub struct ApiFetchRecordSource<C: ApiClient> {
    descriptor: RecordSourceDescriptor,
    client: Arc<C>,
    retry: RetryPolicy,
    state: Arc<Mutex<()>>,
}

impl<C> ApiFetchRecordSource<C>
where
    C: ApiClient + 'static,
{
    /// Build a new API-backed fetch source with default retry policy.
    #[must_use]
    pub fn new(source_identifier: impl Into<String>, client: C) -> Self {
        Self {
            descriptor: RecordSourceDescriptor::new(RecordSourceKind::Polling, source_identifier),
            client: Arc::new(client),
            retry: RetryPolicy::default(),
            state: Arc::new(Mutex::new(())),
        }
    }

    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    async fn fetch_with_retry(
        &self,
        cursor: Option<&str>,
    ) -> Result<ApiFetchPage<C::Record>, ApiFetchError> {
        let mut last_error: Option<Box<dyn Error + Send + Sync + 'static>> = None;
        for attempt in 0..self.retry.max_attempts {
            if attempt > 0 {
                let delay = self.retry.delay_for_attempt(attempt);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
            match self.client.fetch(cursor).await {
                Ok(page) => return Ok(page),
                Err(error) => last_error = Some(Box::new(error)),
            }
        }
        Err(ApiFetchError::Exhausted {
            attempts: self.retry.max_attempts,
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
}

impl<C> RecordSource for ApiFetchRecordSource<C>
where
    C: ApiClient + 'static,
{
    type Checkpoint = ApiFetchCheckpoint;
    type Error = ApiFetchError;
    type Record = C::Record;

    fn descriptor(&self) -> &RecordSourceDescriptor {
        &self.descriptor
    }

    fn initial_checkpoint(&self) -> Self::Checkpoint {
        ApiFetchCheckpoint::default()
    }

    fn read_batch<'a>(
        &'a self,
        checkpoint: &'a Self::Checkpoint,
        _horizon: RecordReadHorizon,
    ) -> impl Future<Output = Result<RecordReadBatch<Self::Record, Self::Checkpoint>, Self::Error>>
    + Send
    + 'a {
        async move {
            let _guard = self.state.lock().await;
            let page = self
                .fetch_with_retry(checkpoint.last_cursor.as_deref())
                .await?;
            let final_checkpoint = ApiFetchCheckpoint {
                last_cursor: page.next_cursor.clone(),
                last_etag: page.etag.clone(),
                last_fetched: Some(Timestamp::now()),
            };
            // Attach per-record checkpoints so a retryable failure mid-page
            // doesn't advance the stored checkpoint past unprocessed records.
            // Records before the last carry the START checkpoint (so a retry
            // re-fetches the same page from the same cursor); only the LAST
            // record carries the page-advancing `final_checkpoint`.
            let total = page.records.len();
            let items: Vec<_> = page
                .records
                .into_iter()
                .enumerate()
                .map(|(idx, record)| {
                    let cp = if idx + 1 == total {
                        final_checkpoint.clone()
                    } else {
                        checkpoint.clone()
                    };
                    RecordReadItem::new(record, cp)
                })
                .collect();
            Ok(RecordReadBatch {
                start_checkpoint: checkpoint.clone(),
                records: items,
                final_checkpoint,
                observation: RecordSourceObservation::None,
            })
        }
    }
}
