//! IPC-stream record-source adapter coverage.
//!
//! Uses `tokio::io::duplex` to fake a Unix-socket-style IPC connection: the
//! "server" half of the duplex writes lines into the pipe, the adapter reads
//! them via its connect closure, and reconnect cadence is asserted on EOF.

#![cfg(feature = "messaging")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use sinex_node_sdk::{IpcStreamCheckpoint, IpcStreamRecordSource, RecordReadHorizon, RecordSource};
use tokio::{
    io::{AsyncWriteExt, DuplexStream},
    sync::Mutex as AsyncMutex,
};
use xtask::sandbox::prelude::*;

#[derive(Debug)]
struct ConnectError;
impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("connect error")
    }
}
impl std::error::Error for ConnectError {}

#[sinex_test]
async fn ipc_stream_drains_lines_and_advances_reconnect_on_eof() -> TestResult<()> {
    // Server-side writers we hand back from the connect closure on each
    // (re)connection. After exhausting the queue, connect closure errors —
    // the adapter surfaces this as IpcStreamError::Connect.
    let connections: Arc<AsyncMutex<Vec<DuplexStream>>> = Arc::new(AsyncMutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));

    // Build connection #1 — sends two lines then EOFs (server half drops).
    let (server1, client1) = tokio::io::duplex(1024);
    let _server1 = tokio::spawn(async move {
        let mut server1 = server1;
        server1.write_all(b"first\nsecond\n").await?;
        server1.shutdown().await?;
        // Drop server side -> EOF on client side.
        Ok::<_, std::io::Error>(())
    });

    // Build connection #2 — sends one line then EOFs.
    let (server2, client2) = tokio::io::duplex(1024);
    let _server2 = tokio::spawn(async move {
        let mut server2 = server2;
        server2.write_all(b"third\n").await?;
        server2.shutdown().await?;
        Ok::<_, std::io::Error>(())
    });

    {
        let mut conns = connections.lock().await;
        // Pop order: client1, then client2.
        conns.push(client2);
        conns.push(client1);
    }

    let connections_for_closure = Arc::clone(&connections);
    let attempts_for_closure = Arc::clone(&attempts);
    let source = IpcStreamRecordSource::new("test://ipc-fake", move || {
        let connections = Arc::clone(&connections_for_closure);
        let attempts = Arc::clone(&attempts_for_closure);
        async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            let mut conns = connections.lock().await;
            conns.pop().ok_or(ConnectError)
        }
    });

    // First batch: drain connection #1 (two lines) + observe EOF that bumps
    // reconnect counter to 1. Records emitted carry reconnect_index = 0.
    let initial = source.initial_checkpoint();
    let batch1 = source
        .read_batch(&initial, RecordReadHorizon::Unbounded)
        .await?;
    let lines1: Vec<_> = batch1
        .records
        .iter()
        .map(|r| r.record.line.clone())
        .collect();
    assert_eq!(lines1, vec!["first".to_string(), "second".to_string()]);
    assert!(batch1.records.iter().all(|r| r.record.reconnect_index == 0));
    assert_eq!(batch1.final_checkpoint.reconnects, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // Second batch resumes from checkpoint: should reconnect (attempt #2),
    // drain the single line on connection #2, and observe a fresh EOF
    // bumping reconnects to 2.
    let batch2 = source
        .read_batch(&batch1.final_checkpoint, RecordReadHorizon::Unbounded)
        .await?;
    let lines2: Vec<_> = batch2
        .records
        .iter()
        .map(|r| r.record.line.clone())
        .collect();
    assert_eq!(lines2, vec!["third".to_string()]);
    assert_eq!(batch2.final_checkpoint.reconnects, 2);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    // Third batch: connect-closure pool is empty -> connect error.
    let err = source
        .read_batch(&batch2.final_checkpoint, RecordReadHorizon::Unbounded)
        .await
        .expect_err("expected connect error after pool exhausted");
    assert!(format!("{err}").contains("ipc stream connect failed"));

    Ok(())
}

#[sinex_test]
async fn ipc_stream_initial_checkpoint_is_zero() -> TestResult<()> {
    let (_server, client) = tokio::io::duplex(64);
    // Arc-wrap so each Fn invocation can clone a shared handle into the
    // async block. `&client` would tie the returned future to the closure-
    // call lifetime, which the `Connect: Fn() -> Fut` bound on
    // `IpcStreamRecordSource::new` cannot express.
    let client = Arc::new(AsyncMutex::new(Some(client)));
    let source = IpcStreamRecordSource::new("test://ipc-init", move || {
        let client = client.clone();
        async move { client.lock().await.take().ok_or(ConnectError) }
    });
    assert_eq!(source.initial_checkpoint(), IpcStreamCheckpoint::default());
    Ok(())
}
