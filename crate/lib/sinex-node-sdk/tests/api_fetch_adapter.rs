//! API-backed fetch record-source adapter coverage.

#![cfg(feature = "messaging")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use sinex_node_sdk::{
    ApiClient, ApiFetchPage, ApiFetchRecordSource, RecordReadHorizon, RecordSource, RetryPolicy,
};
use tokio::sync::Mutex as AsyncMutex;
use xtask::sandbox::prelude::*;

/// Mock client returning a scripted sequence of pages, with optional error
/// injection on a given attempt index for retry coverage.
struct MockClient {
    pages: AsyncMutex<Vec<ApiFetchPage<String>>>,
    fail_attempts: AsyncMutex<u32>,
    total_calls: AtomicUsize,
}

impl MockClient {
    fn new(pages: Vec<ApiFetchPage<String>>, fail_attempts: u32) -> Self {
        Self {
            pages: AsyncMutex::new(pages),
            fail_attempts: AsyncMutex::new(fail_attempts),
            total_calls: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug)]
struct MockClientError(&'static str);
impl std::fmt::Display for MockClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock client error: {}", self.0)
    }
}
impl std::error::Error for MockClientError {}

impl ApiClient for MockClient {
    type Record = String;
    type Error = MockClientError;

    fn fetch(
        &self,
        cursor: Option<&str>,
    ) -> impl std::future::Future<Output = Result<ApiFetchPage<Self::Record>, Self::Error>> + Send
    {
        let cursor = cursor.map(str::to_string);
        async move {
            self.total_calls.fetch_add(1, Ordering::SeqCst);
            let mut remaining = self.fail_attempts.lock().await;
            if *remaining > 0 {
                *remaining -= 1;
                return Err(MockClientError("transient"));
            }
            drop(remaining);
            let mut pages = self.pages.lock().await;
            if pages.is_empty() {
                return Ok(ApiFetchPage {
                    records: Vec::new(),
                    next_cursor: None,
                    etag: None,
                });
            }
            let next = pages.remove(0);
            // Sanity: cursor matches the page we're about to serve via the
            // page's records (not asserted strictly so we don't constrain
            // ordering in the mock; just exercises the path).
            let _ = cursor;
            Ok(next)
        }
    }
}

#[sinex_test]
async fn api_fetch_walks_paginated_cursor_sequence() -> TestResult<()> {
    let pages = vec![
        ApiFetchPage {
            records: vec!["r1".into(), "r2".into()],
            next_cursor: Some("page-2".into()),
            etag: Some("etag-1".into()),
        },
        ApiFetchPage {
            records: vec!["r3".into()],
            next_cursor: Some("page-3".into()),
            etag: Some("etag-2".into()),
        },
        ApiFetchPage {
            records: Vec::new(),
            next_cursor: None,
            etag: Some("etag-3".into()),
        },
    ];
    let client = MockClient::new(pages, 0);
    let source = ApiFetchRecordSource::new("test://api", client).with_retry(RetryPolicy::never());

    let mut checkpoint = source.initial_checkpoint();
    assert_eq!(checkpoint.last_cursor, None);

    let batch1 = source.read_batch(&checkpoint, RecordReadHorizon::Unbounded).await?;
    let recs: Vec<_> = batch1.records.iter().map(|r| r.record.clone()).collect();
    assert_eq!(recs, vec!["r1", "r2"]);
    assert_eq!(batch1.final_checkpoint.last_cursor.as_deref(), Some("page-2"));
    assert_eq!(batch1.final_checkpoint.last_etag.as_deref(), Some("etag-1"));
    checkpoint = batch1.final_checkpoint;

    let batch2 = source.read_batch(&checkpoint, RecordReadHorizon::Unbounded).await?;
    let recs2: Vec<_> = batch2.records.iter().map(|r| r.record.clone()).collect();
    assert_eq!(recs2, vec!["r3"]);
    assert_eq!(batch2.final_checkpoint.last_cursor.as_deref(), Some("page-3"));
    checkpoint = batch2.final_checkpoint;

    let batch3 = source.read_batch(&checkpoint, RecordReadHorizon::Unbounded).await?;
    assert!(batch3.records.is_empty());
    assert_eq!(batch3.final_checkpoint.last_cursor, None);
    Ok(())
}

#[sinex_test]
async fn api_fetch_retries_transient_errors() -> TestResult<()> {
    let pages = vec![ApiFetchPage {
        records: vec!["after-retry".into()],
        next_cursor: None,
        etag: None,
    }];
    // Two failures, then success — needs at least 3 attempts.
    let client = Arc::new(MockClient::new(pages, 2));
    let source = ApiFetchRecordSource::new("test://api-retry", MockClientHandle(Arc::clone(&client)))
        .with_retry(RetryPolicy {
            max_attempts: 4,
            base_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_millis(5),
            jitter_ratio: 0.0,
        });

    let initial = source.initial_checkpoint();
    let batch = source.read_batch(&initial, RecordReadHorizon::Unbounded).await?;
    let recs: Vec<_> = batch.records.iter().map(|r| r.record.clone()).collect();
    assert_eq!(recs, vec!["after-retry"]);
    assert_eq!(client.total_calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[sinex_test]
async fn api_fetch_exhausts_after_max_attempts() -> TestResult<()> {
    let client = MockClient::new(Vec::new(), 10);
    let source = ApiFetchRecordSource::new("test://api-fail", client).with_retry(RetryPolicy {
        max_attempts: 3,
        base_delay: std::time::Duration::from_millis(1),
        max_delay: std::time::Duration::from_millis(2),
        jitter_ratio: 0.0,
    });
    let initial = source.initial_checkpoint();
    let err = source
        .read_batch(&initial, RecordReadHorizon::Unbounded)
        .await
        .expect_err("expected exhaustion");
    assert!(format!("{err}").contains("exhausted after 3 attempts"));
    Ok(())
}

/// Wrapper so the same `MockClient` is shared between assertion and source.
struct MockClientHandle(Arc<MockClient>);
impl ApiClient for MockClientHandle {
    type Record = String;
    type Error = MockClientError;

    fn fetch(
        &self,
        cursor: Option<&str>,
    ) -> impl std::future::Future<Output = Result<ApiFetchPage<Self::Record>, Self::Error>> + Send
    {
        let inner = Arc::clone(&self.0);
        let cursor = cursor.map(str::to_string);
        async move { inner.fetch(cursor.as_deref()).await }
    }
}
