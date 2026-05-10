//! Incremental dump record-source adapter coverage.

#![cfg(feature = "messaging")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use serde::{Deserialize, Serialize};
use sinex_node_sdk::{IncrementalDumpRecordSource, RecordReadHorizon, RecordSource};
use tokio::sync::Mutex as AsyncMutex;
use xtask::sandbox::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct VisitRow {
    id: String,
    url: String,
}

#[derive(Debug)]
struct LoadError;
impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("load error")
    }
}
impl std::error::Error for LoadError {}

#[sinex_test]
async fn incremental_dump_emits_only_new_records_on_second_pass() -> TestResult<()> {
    // Snapshot 1: rows a, b.
    // Snapshot 2: rows a, b, c, d, e — three new (c, d, e).
    let snapshot1 = vec![
        VisitRow { id: "a".into(), url: "https://a".into() },
        VisitRow { id: "b".into(), url: "https://b".into() },
    ];
    let snapshot2 = vec![
        VisitRow { id: "a".into(), url: "https://a".into() },
        VisitRow { id: "b".into(), url: "https://b".into() },
        VisitRow { id: "c".into(), url: "https://c".into() },
        VisitRow { id: "d".into(), url: "https://d".into() },
        VisitRow { id: "e".into(), url: "https://e".into() },
    ];

    let live: Arc<AsyncMutex<Vec<VisitRow>>> = Arc::new(AsyncMutex::new(snapshot1));
    let calls = Arc::new(AtomicUsize::new(0));

    let live_for_closure = Arc::clone(&live);
    let calls_for_closure = Arc::clone(&calls);
    let source: IncrementalDumpRecordSource<VisitRow, String, _, _, LoadError, _> =
        IncrementalDumpRecordSource::new(
            "test://incremental",
            move || {
                let live = Arc::clone(&live_for_closure);
                let calls = Arc::clone(&calls_for_closure);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, LoadError>(live.lock().await.clone())
                }
            },
            |row: &VisitRow| row.id.clone(),
        );

    let mut checkpoint = source.initial_checkpoint();
    let batch1 = source.read_batch(&checkpoint, RecordReadHorizon::Unbounded).await?;
    let ids1: Vec<_> = batch1.records.iter().map(|r| r.record.id.clone()).collect();
    assert_eq!(ids1, vec!["a".to_string(), "b".to_string()]);
    checkpoint = batch1.final_checkpoint;

    // Swap to snapshot 2.
    *live.lock().await = snapshot2;

    let batch2 = source.read_batch(&checkpoint, RecordReadHorizon::Unbounded).await?;
    let ids2: Vec<_> = batch2.records.iter().map(|r| r.record.id.clone()).collect();
    assert_eq!(ids2, vec!["c".to_string(), "d".to_string(), "e".to_string()]);
    assert_eq!(batch2.records.len(), 3);

    // Final checkpoint should contain all 5 keys.
    assert!(batch2.final_checkpoint.contains(&"a".to_string()));
    assert!(batch2.final_checkpoint.contains(&"e".to_string()));

    // Third pass against the same snapshot is a no-op.
    let batch3 = source
        .read_batch(&batch2.final_checkpoint, RecordReadHorizon::Unbounded)
        .await?;
    assert!(batch3.records.is_empty());

    assert_eq!(calls.load(Ordering::SeqCst), 3);
    Ok(())
}
