//! One-time dump record-source adapter coverage.

#![cfg(feature = "messaging")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use sinex_node_sdk::{OneTimeDumpRecordSource, RecordReadHorizon, RecordSource};
use tokio::io::AsyncRead;
use xtask::sandbox::prelude::*;

#[derive(Debug)]
struct OpenError;
impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("open error")
    }
}
impl std::error::Error for OpenError {}

fn fixture_jsonl() -> &'static str {
    "{\"row\":1}\n{\"row\":2}\n{\"row\":3}\n"
}

fn jsonl_reader() -> impl AsyncRead + Unpin + Send + 'static {
    let bytes = fixture_jsonl().as_bytes().to_vec();
    std::io::Cursor::new(bytes)
}

#[sinex_test]
async fn one_time_dump_emits_all_lines_and_flips_consumed() -> TestResult<()> {
    let opens = Arc::new(AtomicUsize::new(0));
    let opens_for_closure = Arc::clone(&opens);
    let source = OneTimeDumpRecordSource::new("test://once", move || {
        let opens = Arc::clone(&opens_for_closure);
        async move {
            opens.fetch_add(1, Ordering::SeqCst);
            Ok::<_, OpenError>(jsonl_reader())
        }
    });

    let initial = source.initial_checkpoint();
    assert!(!initial.consumed);
    let batch = source
        .read_batch(&initial, RecordReadHorizon::Unbounded)
        .await?;
    let lines: Vec<_> = batch
        .records
        .iter()
        .map(|r| r.record.line.clone())
        .collect();
    assert_eq!(
        lines,
        vec![
            "{\"row\":1}".to_string(),
            "{\"row\":2}".to_string(),
            "{\"row\":3}".to_string()
        ]
    );
    assert_eq!(batch.records.len(), 3);
    let final_checkpoint = batch.final_checkpoint;
    assert!(final_checkpoint.consumed);
    let hash = final_checkpoint
        .content_hash
        .ok_or_else(|| color_eyre::eyre::eyre!("expected content hash to be recorded"))?;
    assert_eq!(hash, *blake3::hash(fixture_jsonl().as_bytes()).as_bytes());

    // Re-reading with a consumed checkpoint short-circuits to empty.
    let again = source
        .read_batch(&final_checkpoint, RecordReadHorizon::Unbounded)
        .await?;
    assert!(again.records.is_empty());
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    Ok(())
}
