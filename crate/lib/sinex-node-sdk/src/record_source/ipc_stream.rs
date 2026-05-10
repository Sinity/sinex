//! Ephemeral IPC stream record-source adapter.
//!
//! Wraps any `AsyncRead` stream (D-Bus signal subscription, polkit socket,
//! custom Unix-domain socket protocol) behind the [`RecordSource`] trait.
//! Records are newline-delimited UTF-8 strings; on EOF the source lazily
//! reconnects via the supplied connect closure.
//!
//! IPC streams are inherently ephemeral — there is no "history" to scan.
//! Snapshot and historical phases return empty batches; only continuous reads
//! produce data. The checkpoint records the number of reconnects observed and
//! the last message sequence the caller wishes to remember (opt-in; many
//! protocols have no native sequence number).
//!
//! ```text
//! IpcStreamRecordSource::new(connect)
//!   .read_batch(&checkpoint, RecordReadHorizon::Unbounded)
//!     -> connects (if needed) -> drains available lines -> returns batch
//!     -> on EOF: reconnects, increments reconnects counter
//! ```
//!
//! For protocols where each frame carries a monotone sequence, the caller can
//! advance `last_message_seq` inside the user-level processing closure that
//! consumes records produced by this source.

use std::{
    error::Error,
    fmt,
    future::Future,
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    sync::Mutex,
};

use super::{
    RecordReadBatch, RecordReadHorizon, RecordReadItem, RecordSource, RecordSourceDescriptor,
    RecordSourceKind, RecordSourceObservation,
};

/// Checkpoint for an ephemeral IPC stream.
///
/// `reconnects` is incremented every time the underlying stream signals EOF
/// and the connect closure is re-invoked. `last_message_seq` is opt-in — the
/// adapter never advances it on its own; the caller updates it through the
/// processing closure when the protocol carries a sequence number.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpcStreamCheckpoint {
    pub reconnects: u64,
    pub last_message_seq: Option<u64>,
}

impl IpcStreamCheckpoint {
    #[must_use]
    pub fn new(reconnects: u64, last_message_seq: Option<u64>) -> Self {
        Self {
            reconnects,
            last_message_seq,
        }
    }
}

/// Errors raised while reading an IPC stream batch.
#[derive(Debug)]
pub enum IpcStreamError {
    Connect(Box<dyn Error + Send + Sync + 'static>),
    Read(std::io::Error),
}

impl fmt::Display for IpcStreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => write!(f, "ipc stream connect failed: {error}"),
            Self::Read(error) => write!(f, "ipc stream read failed: {error}"),
        }
    }
}

impl Error for IpcStreamError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connect(error) => Some(&**error),
            Self::Read(error) => Some(error),
        }
    }
}

/// One record read from the IPC stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpcStreamRecord {
    pub line: String,
    pub reconnect_index: u64,
}

/// Ephemeral IPC stream wrapped as a [`RecordSource`].
///
/// `S` is any `AsyncRead + Unpin + Send` stream (e.g. `tokio::net::UnixStream`,
/// `tokio::io::DuplexStream`, a wrapped D-Bus message reader). `Connect` is a
/// closure that produces a fresh `S` on demand; it is invoked lazily, on
/// first read and after every observed EOF.
pub struct IpcStreamRecordSource<S, Connect, ConnectFut, ConnectError> {
    descriptor: RecordSourceDescriptor,
    connect: Connect,
    drain_budget: usize,
    connect_timeout: Option<Duration>,
    state: Arc<Mutex<IpcStreamState<S>>>,
    _marker: PhantomData<fn() -> (ConnectFut, ConnectError)>,
}

struct IpcStreamState<S> {
    reader: Option<BufReader<S>>,
    reconnects: u64,
}

impl<S> Default for IpcStreamState<S> {
    fn default() -> Self {
        Self {
            reader: None,
            reconnects: 0,
        }
    }
}

impl<S, Connect, ConnectFut, ConnectError>
    IpcStreamRecordSource<S, Connect, ConnectFut, ConnectError>
where
    S: AsyncRead + Unpin + Send + 'static,
    Connect: Fn() -> ConnectFut + Send + Sync,
    ConnectFut: Future<Output = Result<S, ConnectError>> + Send,
    ConnectError: Error + Send + Sync + 'static,
{
    /// Build a new ephemeral IPC stream source.
    ///
    /// `source_identifier` is the logical label (e.g. `"unix:///run/foo.sock"`).
    /// `connect` is invoked lazily.
    #[must_use]
    pub fn new(source_identifier: impl Into<String>, connect: Connect) -> Self {
        Self {
            descriptor: RecordSourceDescriptor::new(RecordSourceKind::Polling, source_identifier),
            connect,
            drain_budget: 64,
            connect_timeout: None,
            state: Arc::new(Mutex::new(IpcStreamState::default())),
            _marker: PhantomData,
        }
    }

    /// Cap the number of records drained per `read_batch` call.
    #[must_use]
    pub fn with_drain_budget(mut self, drain_budget: usize) -> Self {
        self.drain_budget = drain_budget.max(1);
        self
    }

    /// Apply a per-attempt connect timeout.
    #[must_use]
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    async fn ensure_connected(
        &self,
        state: &mut IpcStreamState<S>,
    ) -> Result<(), IpcStreamError> {
        if state.reader.is_some() {
            return Ok(());
        }
        let connect = (self.connect)();
        let stream = match self.connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, connect)
                .await
                .map_err(|_| {
                    IpcStreamError::Connect(
                        format!("connect timed out after {:?}", timeout).into(),
                    )
                })?
                .map_err(|error| IpcStreamError::Connect(Box::new(error)))?,
            None => connect
                .await
                .map_err(|error| IpcStreamError::Connect(Box::new(error)))?,
        };
        state.reader = Some(BufReader::new(stream));
        Ok(())
    }
}

impl<S, Connect, ConnectFut, ConnectError> RecordSource
    for IpcStreamRecordSource<S, Connect, ConnectFut, ConnectError>
where
    S: AsyncRead + Unpin + Send + 'static,
    Connect: Fn() -> ConnectFut + Send + Sync,
    ConnectFut: Future<Output = Result<S, ConnectError>> + Send,
    ConnectError: Error + Send + Sync + 'static,
{
    type Checkpoint = IpcStreamCheckpoint;
    type Error = IpcStreamError;
    type Record = IpcStreamRecord;

    fn descriptor(&self) -> &RecordSourceDescriptor {
        &self.descriptor
    }

    fn initial_checkpoint(&self) -> Self::Checkpoint {
        IpcStreamCheckpoint::default()
    }

    fn read_batch<'a>(
        &'a self,
        checkpoint: &'a Self::Checkpoint,
        _horizon: RecordReadHorizon,
    ) -> impl Future<Output = Result<RecordReadBatch<Self::Record, Self::Checkpoint>, Self::Error>>
    + Send
    + 'a {
        async move {
            let mut state = self.state.lock().await;
            // Recover any divergence between the checkpoint the caller resumes
            // from and the in-memory reconnect counter (e.g. after a process
            // restart that rebuilt this struct).
            if state.reconnects < checkpoint.reconnects {
                state.reconnects = checkpoint.reconnects;
            }
            self.ensure_connected(&mut state).await?;

            let start_checkpoint = IpcStreamCheckpoint {
                reconnects: state.reconnects,
                last_message_seq: checkpoint.last_message_seq,
            };
            let mut records = Vec::new();
            let reader = state
                .reader
                .as_mut()
                .expect("ensure_connected populated reader");
            for _ in 0..self.drain_budget {
                let mut line = String::new();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        // EOF: drop reader, bump reconnect counter so the next
                        // read_batch call re-invokes the connect closure.
                        state.reader = None;
                        state.reconnects = state.reconnects.saturating_add(1);
                        break;
                    }
                    Ok(_) => {
                        // Strip the trailing newline; preserve any prior \r.
                        if line.ends_with('\n') {
                            line.pop();
                            if line.ends_with('\r') {
                                line.pop();
                            }
                        }
                        records.push(IpcStreamRecord {
                            line,
                            reconnect_index: state.reconnects,
                        });
                    }
                    Err(error) => {
                        return Err(IpcStreamError::Read(error));
                    }
                }
            }

            let final_checkpoint = IpcStreamCheckpoint {
                reconnects: state.reconnects,
                last_message_seq: checkpoint.last_message_seq,
            };
            let items = records
                .into_iter()
                .map(|record| RecordReadItem::new(record, final_checkpoint))
                .collect();
            Ok(RecordReadBatch {
                start_checkpoint,
                records: items,
                final_checkpoint,
                observation: RecordSourceObservation::None,
            })
        }
    }
}
