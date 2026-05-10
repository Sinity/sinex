//! Incremental dump record-source adapter.
//!
//! Re-reads the full dump on every scan, but emits only the records whose key
//! is not yet in the checkpoint's seen-set. Browser-history exports, Reddit /
//! Wykop GDPR dumps, and similar refreshable archives use this shape: the
//! source file is rewritten end-to-end each export but the relevant rows
//! accumulate over time.
//!
//! Key extraction is caller-supplied. The checkpoint stores keys as a
//! `BTreeSet<K>` so the JSON encoding is stable and diffable.

use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    future::Future,
    hash::Hash,
    sync::Arc,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::Mutex;

use super::{
    RecordReadBatch, RecordReadHorizon, RecordReadItem, RecordSource, RecordSourceDescriptor,
    RecordSourceKind, RecordSourceObservation,
};

/// Checkpoint for an incremental dump.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalDumpCheckpoint<K>
where
    K: Ord,
{
    pub seen: BTreeSet<K>,
}

impl<K> IncrementalDumpCheckpoint<K>
where
    K: Ord,
{
    #[must_use]
    pub fn new(seen: BTreeSet<K>) -> Self {
        Self { seen }
    }

    pub fn contains(&self, key: &K) -> bool {
        self.seen.contains(key)
    }
}

/// Errors raised while loading or keying an incremental dump.
#[derive(Debug)]
pub enum IncrementalDumpError {
    Load(Box<dyn Error + Send + Sync + 'static>),
}

impl fmt::Display for IncrementalDumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load(error) => write!(f, "incremental dump load failed: {error}"),
        }
    }
}

impl Error for IncrementalDumpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Load(error) => Some(&**error),
        }
    }
}

/// Incremental dump source.
///
/// `Load` returns the full record sequence on every scan. `Key` extracts the
/// per-record dedup key. The adapter computes the symmetric difference
/// against the checkpoint and emits only the new records, advancing the
/// per-record checkpoint as it goes so partial progress is durable.
pub struct IncrementalDumpRecordSource<Record, K, Load, LoadFut, LoadError, Key> {
    descriptor: RecordSourceDescriptor,
    load: Load,
    key: Key,
    state: Arc<Mutex<()>>,
    _marker: std::marker::PhantomData<fn() -> (Record, K, LoadFut, LoadError)>,
}

impl<Record, K, Load, LoadFut, LoadError, Key>
    IncrementalDumpRecordSource<Record, K, Load, LoadFut, LoadError, Key>
where
    Record: Send + Sync + 'static,
    K: Clone + Ord + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
    Load: Fn() -> LoadFut + Send + Sync,
    LoadFut: Future<Output = Result<Vec<Record>, LoadError>> + Send,
    LoadError: Error + Send + Sync + 'static,
    Key: Fn(&Record) -> K + Send + Sync,
{
    #[must_use]
    pub fn new(source_identifier: impl Into<String>, load: Load, key: Key) -> Self {
        Self {
            descriptor: RecordSourceDescriptor::new(RecordSourceKind::Polling, source_identifier),
            load,
            key,
            state: Arc::new(Mutex::new(())),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Record, K, Load, LoadFut, LoadError, Key> RecordSource
    for IncrementalDumpRecordSource<Record, K, Load, LoadFut, LoadError, Key>
where
    Record: Send + Sync + 'static,
    K: Clone + Ord + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
    Load: Fn() -> LoadFut + Send + Sync,
    LoadFut: Future<Output = Result<Vec<Record>, LoadError>> + Send,
    LoadError: Error + Send + Sync + 'static,
    Key: Fn(&Record) -> K + Send + Sync,
{
    type Checkpoint = IncrementalDumpCheckpoint<K>;
    type Error = IncrementalDumpError;
    type Record = Record;

    fn descriptor(&self) -> &RecordSourceDescriptor {
        &self.descriptor
    }

    fn initial_checkpoint(&self) -> Self::Checkpoint {
        IncrementalDumpCheckpoint::default()
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
            let all = (self.load)()
                .await
                .map_err(|error| IncrementalDumpError::Load(Box::new(error)))?;
            let mut running = checkpoint.clone();
            let mut items = Vec::new();
            for record in all {
                let key = (self.key)(&record);
                if running.seen.contains(&key) {
                    continue;
                }
                running.seen.insert(key);
                items.push(RecordReadItem::new(record, running.clone()));
            }
            let final_checkpoint = running;
            Ok(RecordReadBatch {
                start_checkpoint: checkpoint.clone(),
                records: items,
                final_checkpoint,
                observation: RecordSourceObservation::None,
            })
        }
    }
}
