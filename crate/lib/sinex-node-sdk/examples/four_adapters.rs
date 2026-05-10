//! Four-input-shape `RecordSource` adapter showcase.
//!
//! Each adapter is exercised in ~30 lines against an in-process fixture so
//! the example compiles without a live NATS, DB, or socket. Run with:
//!
//! ```bash
//! xtask check -p sinex-node-sdk --examples
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use sinex_node_sdk::{
    ApiClient, ApiFetchPage, ApiFetchRecordSource, IncrementalDumpRecordSource,
    IpcStreamRecordSource, OneTimeDumpRecordSource, RecordReadHorizon, RecordSource, RetryPolicy,
};
use tokio::{io::AsyncWriteExt, sync::Mutex as AsyncMutex};

#[derive(Debug)]
struct DemoError(&'static str);
impl std::fmt::Display for DemoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for DemoError {}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    show_ipc_stream().await?;
    show_one_time_dump().await?;
    show_incremental_dump().await?;
    show_api_fetch().await?;
    Ok(())
}

async fn show_ipc_stream() -> Result<(), Box<dyn std::error::Error>> {
    let (server, client) = tokio::io::duplex(64);
    tokio::spawn(async move {
        let mut server = server;
        let _ = server.write_all(b"hello\nworld\n").await;
    });
    // Wrap in Arc so each closure invocation can clone a shared handle into
    // the async block. A bare `&client` reference would tie the returned
    // future to the closure-call lifetime, which the `Connect: Fn() -> Fut`
    // bound on `IpcStreamRecordSource::new` cannot express.
    let client = Arc::new(AsyncMutex::new(Some(client)));
    let source = IpcStreamRecordSource::new("demo://ipc", move || {
        let client = client.clone();
        async move { client.lock().await.take().ok_or(DemoError("already connected")) }
    });
    let batch = source
        .read_batch(&source.initial_checkpoint(), RecordReadHorizon::Unbounded)
        .await?;
    println!("ipc: {} records", batch.records.len());
    Ok(())
}

async fn show_one_time_dump() -> Result<(), Box<dyn std::error::Error>> {
    let source = OneTimeDumpRecordSource::new("demo://once", || async {
        Ok::<_, DemoError>(std::io::Cursor::new(b"a\nb\nc\n".to_vec()))
    });
    let batch = source
        .read_batch(&source.initial_checkpoint(), RecordReadHorizon::Unbounded)
        .await?;
    println!("one-time: {} records, consumed={}", batch.records.len(), batch.final_checkpoint.consumed);
    Ok(())
}

async fn show_incremental_dump() -> Result<(), Box<dyn std::error::Error>> {
    let live: Arc<AsyncMutex<Vec<(u32, String)>>> = Arc::new(AsyncMutex::new(vec![
        (1, "a".into()),
        (2, "b".into()),
    ]));
    let live_for_closure = Arc::clone(&live);
    let source: IncrementalDumpRecordSource<(u32, String), u32, _, _, DemoError, _> =
        IncrementalDumpRecordSource::new(
            "demo://incremental",
            move || {
                let live = Arc::clone(&live_for_closure);
                async move { Ok::<_, DemoError>(live.lock().await.clone()) }
            },
            |row: &(u32, String)| row.0,
        );
    let mut checkpoint = source.initial_checkpoint();
    let first = source.read_batch(&checkpoint, RecordReadHorizon::Unbounded).await?;
    checkpoint = first.final_checkpoint;
    live.lock().await.push((3, "c".into()));
    let second = source.read_batch(&checkpoint, RecordReadHorizon::Unbounded).await?;
    println!("incremental: first={}, second={}", first.records.len(), second.records.len());
    Ok(())
}

struct DemoApi {
    pages: AsyncMutex<Vec<ApiFetchPage<String>>>,
    calls: AtomicUsize,
}

impl ApiClient for DemoApi {
    type Record = String;
    type Error = DemoError;

    fn fetch(
        &self,
        _cursor: Option<&str>,
    ) -> impl std::future::Future<Output = Result<ApiFetchPage<Self::Record>, Self::Error>> + Send
    {
        async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut pages = self.pages.lock().await;
            Ok(pages.remove(0))
        }
    }
}

async fn show_api_fetch() -> Result<(), Box<dyn std::error::Error>> {
    let client = DemoApi {
        pages: AsyncMutex::new(vec![
            ApiFetchPage {
                records: vec!["x".into(), "y".into()],
                next_cursor: Some("p2".into()),
                etag: None,
            },
            ApiFetchPage {
                records: vec!["z".into()],
                next_cursor: None,
                etag: None,
            },
        ]),
        calls: AtomicUsize::new(0),
    };
    let source = ApiFetchRecordSource::new("demo://api", client).with_retry(RetryPolicy::never());
    let mut checkpoint = source.initial_checkpoint();
    let batch1 = source.read_batch(&checkpoint, RecordReadHorizon::Unbounded).await?;
    checkpoint = batch1.final_checkpoint;
    let batch2 = source.read_batch(&checkpoint, RecordReadHorizon::Unbounded).await?;
    println!("api: page1={} page2={}", batch1.records.len(), batch2.records.len());
    Ok(())
}
