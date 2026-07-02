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
        let page_idx: usize = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);

        let records = self.pages.get(page_idx).cloned().unwrap_or_default();

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
    let client =
        MockClient::new(vec![vec![serde_json::json!({"ok": true})]]).with_transient_failures(2);
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
    assert_eq!(
        items.len(),
        1,
        "expected exactly one error item in the stream"
    );
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
        Ok(ApiFetchPage {
            records,
            next_cursor,
            etag: None,
        })
    }
}

#[sinex_test]
async fn pages_fetched_lazily_one_at_a_time() -> xtask::sandbox::TestResult<()> {
    let fetch_count = Arc::new(AtomicU32::new(0));
    let client = TrackedMockClient {
        pages: vec![
            vec![
                serde_json::json!({"page": 0, "i": 0}),
                serde_json::json!({"page": 0, "i": 1}),
            ],
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
    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        2,
        "total pages fetched should be 2"
    );
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
            MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0
            }
        ),
        "unexpected anchor: {:?}",
        r00.anchor
    );

    // Page 0, record 1.
    let r01 = records[1].as_ref().unwrap();
    assert!(
        matches!(
            r01.anchor,
            MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 1
            }
        ),
        "unexpected anchor: {:?}",
        r01.anchor
    );

    // Page 1, record 0.
    let r10 = records[2].as_ref().unwrap();
    assert!(
        matches!(
            r10.anchor,
            MaterialAnchor::StreamFrame {
                material_offset: 1,
                frame_index: 0
            }
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

    assert_eq!(
        records.len(),
        1,
        "should start from page 1 and get 1 record"
    );
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
    assert!(
        cursor_first.last_etag.is_none(),
        "mid-page record should have no etag"
    );

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
