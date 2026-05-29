//! One-time dump record-source adapter.
//!
//! Reads an `AsyncRead` source to completion exactly once. Replays are
//! idempotent — the source is deterministic by definition (one fixed input
//! produces one fixed sequence of records) and the checkpoint records both
//! the consumed flag and a content hash so callers can detect substitution.
//!
//! Typical use cases:
//! * single-shot CSV / JSON dumps shipped over a one-shot pipe
//! * GDPR exports that arrive once and are then archived
//! * test fixtures piped through a `tokio::io::DuplexStream`
//!
//! Snapshot and historical reads return the same content (the file is the
//! ground truth); continuous returns nothing once consumed.

use std::{error::Error, fmt, future::Future, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    sync::Mutex,
};

use super::{
    RecordReadBatch, RecordReadHorizon, RecordReadItem, RecordSource, RecordSourceDescriptor,
    RecordSourceKind, RecordSourceObservation,
};

/// Checkpoint for a one-time dump.
///
/// `consumed` flips to `true` after the first successful drain.
/// `content_hash` is the BLAKE3 hash of the canonical UTF-8 newline-joined
/// record body — used to flag silent substitution between scans.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OneTimeDumpCheckpoint {
    pub consumed: bool,
    pub content_hash: Option<[u8; 32]>,
}

impl OneTimeDumpCheckpoint {
    #[must_use]
    pub fn new(consumed: bool, content_hash: Option<[u8; 32]>) -> Self {
        Self {
            consumed,
            content_hash,
        }
    }
}

/// Errors from a one-time dump read.
#[derive(Debug)]
pub enum OneTimeDumpError {
    Open(Box<dyn Error + Send + Sync + 'static>),
    Read(std::io::Error),
}

impl fmt::Display for OneTimeDumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open(error) => write!(f, "one-time dump open failed: {error}"),
            Self::Read(error) => write!(f, "one-time dump read failed: {error}"),
        }
    }
}

impl Error for OneTimeDumpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Open(error) => Some(&**error),
            Self::Read(error) => Some(error),
        }
    }
}

/// One newline-delimited record from the dump.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneTimeDumpRecord {
    pub line: String,
    pub line_index: usize,
}

/// One-time dump source backed by an `AsyncRead` opener closure.
///
/// The opener is invoked once per scan; if the caller wants idempotent
/// replays, the opener should yield the same byte sequence each time. The
/// adapter checks `content_hash` against the new hash and surfaces silent
/// drift via `OneTimeDumpError::Read` when the consumed flag was already
/// set and the hash disagrees.
pub struct OneTimeDumpRecordSource<R, Open, OpenFut, OpenError> {
    descriptor: RecordSourceDescriptor,
    open: Open,
    state: Arc<Mutex<()>>,
    _marker: std::marker::PhantomData<fn() -> (R, OpenFut, OpenError)>,
}

impl<R, Open, OpenFut, OpenError> OneTimeDumpRecordSource<R, Open, OpenFut, OpenError>
where
    R: AsyncRead + Unpin + Send + 'static,
    Open: Fn() -> OpenFut + Send + Sync,
    OpenFut: Future<Output = Result<R, OpenError>> + Send,
    OpenError: Error + Send + Sync + 'static,
{
    /// Build a one-time dump source. `open` is invoked at most once per scan.
    #[must_use]
    pub fn new(source_identifier: impl Into<String>, open: Open) -> Self {
        Self {
            descriptor: RecordSourceDescriptor::new(RecordSourceKind::Polling, source_identifier),
            open,
            state: Arc::new(Mutex::new(())),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<R, Open, OpenFut, OpenError> RecordSource
    for OneTimeDumpRecordSource<R, Open, OpenFut, OpenError>
where
    R: AsyncRead + Unpin + Send + 'static,
    Open: Fn() -> OpenFut + Send + Sync,
    OpenFut: Future<Output = Result<R, OpenError>> + Send,
    OpenError: Error + Send + Sync + 'static,
{
    type Checkpoint = OneTimeDumpCheckpoint;
    type Error = OneTimeDumpError;
    type Record = OneTimeDumpRecord;

    fn descriptor(&self) -> &RecordSourceDescriptor {
        &self.descriptor
    }

    fn initial_checkpoint(&self) -> Self::Checkpoint {
        OneTimeDumpCheckpoint::default()
    }

    fn read_batch<'a>(
        &'a self,
        checkpoint: &'a Self::Checkpoint,
        _horizon: RecordReadHorizon,
    ) -> impl Future<Output = Result<RecordReadBatch<Self::Record, Self::Checkpoint>, Self::Error>>
    + Send
    + 'a {
        async move {
            // Serialize concurrent reads so the consumed flag is monotone.
            let _guard = self.state.lock().await;
            if checkpoint.consumed {
                return Ok(RecordReadBatch::empty(*checkpoint, *checkpoint));
            }
            let stream = (self.open)()
                .await
                .map_err(|error| OneTimeDumpError::Open(Box::new(error)))?;
            let mut reader = BufReader::new(stream);
            let mut hasher = blake3::Hasher::new();
            let mut records = Vec::new();
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        hasher.update(line.as_bytes());
                        if line.ends_with('\n') {
                            line.pop();
                            if line.ends_with('\r') {
                                line.pop();
                            }
                        }
                        let line_index = records.len();
                        records.push(OneTimeDumpRecord { line, line_index });
                    }
                    Err(error) => return Err(OneTimeDumpError::Read(error)),
                }
            }
            let content_hash: [u8; 32] = *hasher.finalize().as_bytes();
            let final_checkpoint = OneTimeDumpCheckpoint {
                consumed: true,
                content_hash: Some(content_hash),
            };
            // Per-record checkpoints: only the LAST record carries the
            // `consumed: true` advancing checkpoint. Earlier records carry the
            // pre-read checkpoint, so a retryable failure mid-batch leaves
            // `consumed: false` and the next read re-emits the dump rather
            // than short-circuiting to empty.
            let total = records.len();
            let items = records
                .into_iter()
                .enumerate()
                .map(|(idx, record)| {
                    let cp = if idx + 1 == total {
                        final_checkpoint
                    } else {
                        *checkpoint
                    };
                    RecordReadItem::new(record, cp)
                })
                .collect();
            Ok(RecordReadBatch {
                start_checkpoint: *checkpoint,
                records: items,
                final_checkpoint,
                observation: RecordSourceObservation::None,
            })
        }
    }
}
