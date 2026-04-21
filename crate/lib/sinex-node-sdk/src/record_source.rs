//! Record-source acquisition primitives.
//!
//! Nodes own source-specific parsing and event semantics. This module owns the
//! repeatable acquisition shape around those parsers: typed checkpoints, read
//! batches, stable record bytes, append-stream materialization, and standard
//! retry/skip cursor advancement.

use crate::{
    AppendOnlyFileChange, AppendOnlyFileState, NodeResult, SourceRecordAnchor, TailError,
    acquisition_manager::{
        AppendStreamAcquirer, BufferedAppendStreamWriter, BufferedAppendStreamWriterConfig,
    },
    poll_utf8_lines,
};
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinex_primitives::{SinexError, temporal::Timestamp};
use std::{error::Error, fmt, future::Future, marker::PhantomData};

/// Stable category for a source adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordSourceKind {
    Sqlite,
    AppendOnlyFile,
    Journal,
    Polling,
    Mock,
}

/// Runtime identity for a record source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordSourceDescriptor {
    pub kind: RecordSourceKind,
    pub source_identifier: String,
}

impl RecordSourceDescriptor {
    #[must_use]
    pub fn new(kind: RecordSourceKind, source_identifier: impl Into<String>) -> Self {
        Self {
            kind,
            source_identifier: source_identifier.into(),
        }
    }
}

/// Optional upper bound for a source read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecordReadHorizon {
    #[default]
    Unbounded,
    Until(Timestamp),
}

impl RecordReadHorizon {
    #[must_use]
    pub fn end_time(self) -> Option<Timestamp> {
        match self {
            Self::Unbounded => None,
            Self::Until(timestamp) => Some(timestamp),
        }
    }
}

/// One source record plus the checkpoint that becomes safe after processing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordReadItem<Record, Checkpoint> {
    pub record: Record,
    pub checkpoint_after: Checkpoint,
}

impl<Record, Checkpoint> RecordReadItem<Record, Checkpoint> {
    #[must_use]
    pub fn new(record: Record, checkpoint_after: Checkpoint) -> Self {
        Self {
            record,
            checkpoint_after,
        }
    }
}

/// Additional typed observations produced while reading a source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordSourceObservation {
    None,
    AppendOnlyFile {
        file_size: u64,
        bytes_consumed: u64,
        change: AppendOnlyFileChange,
    },
}

/// Output of one checkpointed source read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordReadBatch<Record, Checkpoint> {
    pub start_checkpoint: Checkpoint,
    pub records: Vec<RecordReadItem<Record, Checkpoint>>,
    pub final_checkpoint: Checkpoint,
    pub observation: RecordSourceObservation,
}

impl<Record, Checkpoint> RecordReadBatch<Record, Checkpoint> {
    #[must_use]
    pub fn empty(start_checkpoint: Checkpoint, final_checkpoint: Checkpoint) -> Self {
        Self {
            start_checkpoint,
            records: Vec::new(),
            final_checkpoint,
            observation: RecordSourceObservation::None,
        }
    }

    #[must_use]
    pub fn empty_at(checkpoint: Checkpoint) -> Self
    where
        Checkpoint: Clone,
    {
        Self::empty(checkpoint.clone(), checkpoint)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// A source that can read new records from a typed checkpoint.
pub trait RecordSource {
    type Record;
    type Checkpoint: Clone + DeserializeOwned + Serialize + Send + Sync + 'static;
    type Error: Error + Send + Sync + 'static;

    fn descriptor(&self) -> &RecordSourceDescriptor;

    fn initial_checkpoint(&self) -> Self::Checkpoint;

    fn read_batch<'a>(
        &'a self,
        checkpoint: &'a Self::Checkpoint,
        horizon: RecordReadHorizon,
    ) -> impl Future<Output = Result<RecordReadBatch<Self::Record, Self::Checkpoint>, Self::Error>>
    + Send
    + 'a;
}

/// Per-record context passed by [`RecordSourceHarness`].
#[derive(Clone)]
pub struct RecordProcessContext<Sink> {
    descriptor: RecordSourceDescriptor,
    materializer: RecordMaterializer<Sink>,
}

impl<Sink> RecordProcessContext<Sink>
where
    Sink: RecordMaterialSink,
{
    #[must_use]
    pub fn descriptor(&self) -> &RecordSourceDescriptor {
        &self.descriptor
    }

    #[must_use]
    pub fn materializer(&self) -> &RecordMaterializer<Sink> {
        &self.materializer
    }

    pub async fn append_json_line<T>(&self, record: &T) -> NodeResult<SourceRecordAnchor>
    where
        T: Serialize + ?Sized,
    {
        self.materializer.append_json_line(record).await
    }
}

/// Framework-style runner for one record source and one material sink.
pub struct RecordSourceHarness<Source, Sink> {
    source: Source,
    materializer: RecordMaterializer<Sink>,
}

impl<Source, Sink> RecordSourceHarness<Source, Sink>
where
    Source: RecordSource,
    Sink: RecordMaterialSink,
{
    #[must_use]
    pub fn new(source: Source, materializer: RecordMaterializer<Sink>) -> Self {
        Self {
            source,
            materializer,
        }
    }

    #[must_use]
    pub fn source(&self) -> &Source {
        &self.source
    }

    #[must_use]
    pub fn materializer(&self) -> &RecordMaterializer<Sink> {
        &self.materializer
    }

    pub async fn read_process_lenient<Warning, Process, ProcessFuture, Warn>(
        &self,
        checkpoint: &mut Source::Checkpoint,
        horizon: RecordReadHorizon,
        mut process: Process,
        warning_disposition: Warn,
    ) -> NodeResult<RecordProcessReport<Source::Checkpoint, Warning>>
    where
        Process: FnMut(Source::Record, RecordProcessContext<Sink>) -> ProcessFuture,
        ProcessFuture: Future<Output = Result<RecordProcessingOutcome, Warning>>,
        Warn: Fn(&Warning) -> RecordWarningDisposition,
    {
        let descriptor = self.source.descriptor().clone();
        let batch = self
            .source
            .read_batch(checkpoint, horizon)
            .await
            .map_err(|error| {
                SinexError::processing("failed to read record source batch")
                    .with_context("source_kind", format!("{:?}", descriptor.kind))
                    .with_context("source_identifier", descriptor.source_identifier.clone())
                    .with_std_error(&error)
            })?;
        Ok(process_record_batch_lenient(
            checkpoint,
            batch,
            |record| {
                let ctx = RecordProcessContext {
                    descriptor: descriptor.clone(),
                    materializer: self.materializer.clone(),
                };
                process(record, ctx)
            },
            warning_disposition,
        )
        .await)
    }

    pub async fn finalize(&self, reason: &str) -> NodeResult<()> {
        self.materializer.finalize(reason).await
    }
}

/// Outcome of processing one source record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordProcessingOutcome {
    Processed,
    Skipped,
}

/// Whether a failed record should hold the cursor for retry or be skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordWarningDisposition {
    Retry,
    SkipRecord,
}

/// Report from processing a source read batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordProcessReport<Checkpoint, Warning = String> {
    pub processed_records: usize,
    pub final_checkpoint: Checkpoint,
    pub warnings: Vec<Warning>,
}

/// Process a batch while applying one standard cursor advancement policy.
///
/// Successful records and explicitly skipped warnings advance to the record's
/// `checkpoint_after`; retryable warnings stop processing before advancing.
/// When the whole returned batch is handled without retry, the checkpoint
/// advances to the source read frontier so source-level internal skips, such as
/// malformed SQLite rows filtered by the reader, are acknowledged exactly once.
pub async fn process_record_batch_lenient<
    Record,
    Checkpoint,
    Warning,
    Process,
    ProcessFuture,
    Warn,
>(
    checkpoint: &mut Checkpoint,
    batch: RecordReadBatch<Record, Checkpoint>,
    mut process: Process,
    warning_disposition: Warn,
) -> RecordProcessReport<Checkpoint, Warning>
where
    Checkpoint: Clone,
    Process: FnMut(Record) -> ProcessFuture,
    ProcessFuture: Future<Output = Result<RecordProcessingOutcome, Warning>>,
    Warn: Fn(&Warning) -> RecordWarningDisposition,
{
    let mut processed_records = 0usize;
    let mut warnings = Vec::new();
    let mut blocked_by_retry = false;

    for item in batch.records {
        match process(item.record).await {
            Ok(outcome) => {
                if matches!(outcome, RecordProcessingOutcome::Processed) {
                    processed_records = processed_records.saturating_add(1);
                }
                *checkpoint = item.checkpoint_after;
            }
            Err(warning) => {
                let disposition = warning_disposition(&warning);
                warnings.push(warning);
                match disposition {
                    RecordWarningDisposition::Retry => {
                        blocked_by_retry = true;
                        break;
                    }
                    RecordWarningDisposition::SkipRecord => {
                        *checkpoint = item.checkpoint_after;
                    }
                }
            }
        }
    }

    if !blocked_by_retry {
        *checkpoint = batch.final_checkpoint;
    }

    RecordProcessReport {
        processed_records,
        final_checkpoint: checkpoint.clone(),
        warnings,
    }
}

/// `SQLite` ROWID checkpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteRowCheckpoint {
    pub row_id: i64,
}

impl SqliteRowCheckpoint {
    #[must_use]
    pub fn new(row_id: i64) -> Self {
        Self { row_id }
    }
}

/// Timestamp checkpoint for journal-like sources.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampRecordCheckpoint {
    pub timestamp: Option<Timestamp>,
}

impl TimestampRecordCheckpoint {
    #[must_use]
    pub fn new(timestamp: Option<Timestamp>) -> Self {
        Self { timestamp }
    }
}

/// Line returned by an append-only UTF-8 file source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendOnlyTextRecord {
    pub line: String,
    pub batch_index: usize,
    pub start_offset_bytes: u64,
    pub end_offset_bytes: u64,
}

/// Factory namespace for built-in source adapters.
pub struct RecordSources;

impl RecordSources {
    #[must_use]
    pub fn append_only_utf8_file(path: impl Into<Utf8PathBuf>) -> AppendOnlyUtf8FileSource {
        AppendOnlyUtf8FileSource::new(path)
    }

    #[must_use]
    pub fn sqlite<Record, Read, RowId, ReadError>(
        path: impl Into<Utf8PathBuf>,
        source_identifier: impl Into<String>,
        read: Read,
        row_id: RowId,
    ) -> SqliteRecordSource<Record, Read, RowId, ReadError>
    where
        Read: Fn(&Utf8PathBuf, i64, Option<Timestamp>) -> Result<(Vec<Record>, i64), ReadError>,
        RowId: Fn(&Record) -> i64,
    {
        SqliteRecordSource::new(path, source_identifier, read, row_id)
    }

    #[must_use]
    pub fn polling<Record, Checkpoint, Poll, PollError>(
        source_identifier: impl Into<String>,
        initial_checkpoint: Checkpoint,
        poll: Poll,
    ) -> PollingRecordSource<Record, Checkpoint, Poll, PollError> {
        PollingRecordSource::new(
            RecordSourceKind::Polling,
            source_identifier,
            initial_checkpoint,
            poll,
        )
    }

    #[must_use]
    pub fn journal<Record, Poll, PollError>(
        source_identifier: impl Into<String>,
        initial_checkpoint: TimestampRecordCheckpoint,
        poll: Poll,
    ) -> PollingRecordSource<Record, TimestampRecordCheckpoint, Poll, PollError> {
        PollingRecordSource::new(
            RecordSourceKind::Journal,
            source_identifier,
            initial_checkpoint,
            poll,
        )
    }
}

pub struct AppendOnlyUtf8FileSource {
    descriptor: RecordSourceDescriptor,
    path: Utf8PathBuf,
}

impl AppendOnlyUtf8FileSource {
    #[must_use]
    pub fn new(path: impl Into<Utf8PathBuf>) -> Self {
        let path = path.into();
        Self {
            descriptor: RecordSourceDescriptor::new(
                RecordSourceKind::AppendOnlyFile,
                path.as_str(),
            ),
            path,
        }
    }

    #[must_use]
    pub fn path(&self) -> &Utf8Path {
        &self.path
    }
}

impl RecordSource for AppendOnlyUtf8FileSource {
    type Checkpoint = AppendOnlyFileState;
    type Error = TailError;
    type Record = AppendOnlyTextRecord;

    fn descriptor(&self) -> &RecordSourceDescriptor {
        &self.descriptor
    }

    fn initial_checkpoint(&self) -> Self::Checkpoint {
        AppendOnlyFileState::default()
    }

    fn read_batch<'a>(
        &'a self,
        checkpoint: &'a Self::Checkpoint,
        _horizon: RecordReadHorizon,
    ) -> impl Future<Output = Result<RecordReadBatch<Self::Record, Self::Checkpoint>, Self::Error>>
    + Send
    + 'a {
        async move {
            let polled = poll_utf8_lines(&self.path, checkpoint.clone()).await?;
            let mut checkpoint_after = polled.state.clone();
            let records = polled
                .records
                .into_iter()
                .enumerate()
                .map(|(batch_index, line_record)| {
                    checkpoint_after.offset_bytes = line_record.end_offset_bytes;
                    RecordReadItem::new(
                        AppendOnlyTextRecord {
                            line: line_record.text,
                            batch_index,
                            start_offset_bytes: line_record.start_offset_bytes,
                            end_offset_bytes: line_record.end_offset_bytes,
                        },
                        checkpoint_after.clone(),
                    )
                })
                .collect();

            Ok(RecordReadBatch {
                start_checkpoint: checkpoint.clone(),
                records,
                final_checkpoint: polled.state,
                observation: RecordSourceObservation::AppendOnlyFile {
                    file_size: polled.file_size,
                    bytes_consumed: polled.bytes_consumed,
                    change: polled.change,
                },
            })
        }
    }
}

pub struct SqliteRecordSource<Record, Read, RowId, ReadError> {
    descriptor: RecordSourceDescriptor,
    path: Utf8PathBuf,
    read: Read,
    row_id: RowId,
    _marker: PhantomData<(Record, ReadError)>,
}

impl<Record, Read, RowId, ReadError> SqliteRecordSource<Record, Read, RowId, ReadError> {
    #[must_use]
    pub fn new(
        path: impl Into<Utf8PathBuf>,
        source_identifier: impl Into<String>,
        read: Read,
        row_id: RowId,
    ) -> Self {
        Self {
            descriptor: RecordSourceDescriptor::new(RecordSourceKind::Sqlite, source_identifier),
            path: path.into(),
            read,
            row_id,
            _marker: PhantomData,
        }
    }
}

impl<Record, Read, RowId, ReadError> RecordSource
    for SqliteRecordSource<Record, Read, RowId, ReadError>
where
    Record: Send + Sync + 'static,
    Read: Fn(&Utf8PathBuf, i64, Option<Timestamp>) -> Result<(Vec<Record>, i64), ReadError>
        + Send
        + Sync
        + 'static,
    RowId: Fn(&Record) -> i64 + Send + Sync + 'static,
    ReadError: Error + Send + Sync + 'static,
{
    type Checkpoint = SqliteRowCheckpoint;
    type Error = ReadError;
    type Record = Record;

    fn descriptor(&self) -> &RecordSourceDescriptor {
        &self.descriptor
    }

    fn initial_checkpoint(&self) -> Self::Checkpoint {
        SqliteRowCheckpoint::default()
    }

    fn read_batch<'a>(
        &'a self,
        checkpoint: &'a Self::Checkpoint,
        horizon: RecordReadHorizon,
    ) -> impl Future<Output = Result<RecordReadBatch<Self::Record, Self::Checkpoint>, Self::Error>>
    + Send
    + 'a {
        async move {
            let (records, last_row_id) =
                (self.read)(&self.path, checkpoint.row_id, horizon.end_time())?;
            let records = records
                .into_iter()
                .map(|record| {
                    let row_id = (self.row_id)(&record);
                    RecordReadItem::new(record, SqliteRowCheckpoint::new(row_id))
                })
                .collect();
            Ok(RecordReadBatch {
                start_checkpoint: *checkpoint,
                records,
                final_checkpoint: SqliteRowCheckpoint::new(last_row_id),
                observation: RecordSourceObservation::None,
            })
        }
    }
}

pub struct PollingRecordSource<Record, Checkpoint, Poll, PollError> {
    descriptor: RecordSourceDescriptor,
    initial_checkpoint: Checkpoint,
    poll: Poll,
    _marker: PhantomData<(Record, PollError)>,
}

impl<Record, Checkpoint, Poll, PollError> PollingRecordSource<Record, Checkpoint, Poll, PollError> {
    #[must_use]
    pub fn new(
        kind: RecordSourceKind,
        source_identifier: impl Into<String>,
        initial_checkpoint: Checkpoint,
        poll: Poll,
    ) -> Self {
        Self {
            descriptor: RecordSourceDescriptor::new(kind, source_identifier),
            initial_checkpoint,
            poll,
            _marker: PhantomData,
        }
    }
}

impl<Record, Checkpoint, Poll, PollError> RecordSource
    for PollingRecordSource<Record, Checkpoint, Poll, PollError>
where
    Record: Send + Sync + 'static,
    Checkpoint: Clone + DeserializeOwned + Serialize + Send + Sync + 'static,
    Poll: Fn(&Checkpoint, RecordReadHorizon) -> Result<RecordReadBatch<Record, Checkpoint>, PollError>
        + Send
        + Sync
        + 'static,
    PollError: Error + Send + Sync + 'static,
{
    type Checkpoint = Checkpoint;
    type Error = PollError;
    type Record = Record;

    fn descriptor(&self) -> &RecordSourceDescriptor {
        &self.descriptor
    }

    fn initial_checkpoint(&self) -> Self::Checkpoint {
        self.initial_checkpoint.clone()
    }

    fn read_batch<'a>(
        &'a self,
        checkpoint: &'a Self::Checkpoint,
        horizon: RecordReadHorizon,
    ) -> impl Future<Output = Result<RecordReadBatch<Self::Record, Self::Checkpoint>, Self::Error>>
    + Send
    + 'a {
        async move { (self.poll)(checkpoint, horizon) }
    }
}

/// A material sink that appends stable record bytes and returns byte anchors.
pub trait RecordMaterialSink: Clone + Send + Sync + 'static {
    fn append_record(
        &self,
        bytes: Vec<u8>,
    ) -> impl Future<Output = NodeResult<SourceRecordAnchor>> + Send + '_;

    fn finalize<'a>(&'a self, reason: &'a str) -> impl Future<Output = NodeResult<()>> + Send + 'a;
}

#[derive(Clone)]
pub struct BufferedRecordSink {
    writer: BufferedAppendStreamWriter,
}

impl BufferedRecordSink {
    #[must_use]
    pub fn new(writer: BufferedAppendStreamWriter) -> Self {
        Self { writer }
    }

    #[must_use]
    pub fn spawn(
        stream: AppendStreamAcquirer,
        source_identifier: impl Into<String>,
        config: BufferedAppendStreamWriterConfig,
    ) -> Self {
        Self::new(BufferedAppendStreamWriter::spawn(
            stream,
            source_identifier,
            config,
        ))
    }
}

impl RecordMaterialSink for BufferedRecordSink {
    fn append_record(
        &self,
        bytes: Vec<u8>,
    ) -> impl Future<Output = NodeResult<SourceRecordAnchor>> + Send + '_ {
        async move { self.writer.append(bytes).await }
    }

    fn finalize<'a>(&'a self, reason: &'a str) -> impl Future<Output = NodeResult<()>> + Send + 'a {
        async move { self.writer.finalize(reason).await }
    }
}

/// Stable-byte materializer over a record sink.
#[derive(Clone)]
pub struct RecordMaterializer<Sink> {
    sink: Sink,
}

impl<Sink> RecordMaterializer<Sink>
where
    Sink: RecordMaterialSink,
{
    #[must_use]
    pub fn new(sink: Sink) -> Self {
        Self { sink }
    }

    #[must_use]
    pub fn sink(&self) -> &Sink {
        &self.sink
    }

    pub async fn append_stable_bytes(&self, bytes: Vec<u8>) -> NodeResult<SourceRecordAnchor> {
        if bytes.is_empty() {
            return Err(SinexError::validation(
                "source material records must not be empty",
            ));
        }
        self.sink.append_record(bytes).await
    }

    pub async fn append_json_line<T>(&self, record: &T) -> NodeResult<SourceRecordAnchor>
    where
        T: Serialize + ?Sized,
    {
        self.append_stable_bytes(stable_json_line(record)?).await
    }

    pub async fn finalize(&self, reason: &str) -> NodeResult<()> {
        self.sink.finalize(reason).await
    }
}

pub fn stable_json_line<T>(record: &T) -> NodeResult<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let mut data = serde_json::to_vec(record).map_err(|error| {
        SinexError::serialization("failed to serialize source stream record").with_std_error(&error)
    })?;
    data.push(b'\n');
    Ok(data)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockRecordSource<Record, Checkpoint> {
    descriptor: RecordSourceDescriptor,
    initial_checkpoint: Checkpoint,
    batches: Vec<RecordReadBatch<Record, Checkpoint>>,
}

impl<Record, Checkpoint> MockRecordSource<Record, Checkpoint> {
    #[must_use]
    pub fn new(
        source_identifier: impl Into<String>,
        initial_checkpoint: Checkpoint,
        batches: Vec<RecordReadBatch<Record, Checkpoint>>,
    ) -> Self {
        Self {
            descriptor: RecordSourceDescriptor::new(RecordSourceKind::Mock, source_identifier),
            initial_checkpoint,
            batches,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MockRecordSourceError;

impl fmt::Display for MockRecordSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mock record source has no matching batch")
    }
}

impl Error for MockRecordSourceError {}

impl<Record, Checkpoint> RecordSource for MockRecordSource<Record, Checkpoint>
where
    Record: Clone + Send + Sync + 'static,
    Checkpoint: Clone + DeserializeOwned + PartialEq + Serialize + Send + Sync + 'static,
{
    type Checkpoint = Checkpoint;
    type Error = MockRecordSourceError;
    type Record = Record;

    fn descriptor(&self) -> &RecordSourceDescriptor {
        &self.descriptor
    }

    fn initial_checkpoint(&self) -> Self::Checkpoint {
        self.initial_checkpoint.clone()
    }

    fn read_batch<'a>(
        &'a self,
        checkpoint: &'a Self::Checkpoint,
        _horizon: RecordReadHorizon,
    ) -> impl Future<Output = Result<RecordReadBatch<Self::Record, Self::Checkpoint>, Self::Error>>
    + Send
    + 'a {
        async move {
            self.batches
                .iter()
                .find(|batch| &batch.start_checkpoint == checkpoint)
                .cloned()
                .ok_or(MockRecordSourceError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn lenient_processor_advances_on_success_and_skip_but_not_retry() -> TestResult<()> {
        let batch = RecordReadBatch {
            start_checkpoint: 0,
            records: vec![
                RecordReadItem::new("ok", 1_u64),
                RecordReadItem::new("skip", 2_u64),
                RecordReadItem::new("retry", 3_u64),
                RecordReadItem::new("never", 4_u64),
            ],
            final_checkpoint: 4,
            observation: RecordSourceObservation::None,
        };
        let mut checkpoint = 0_u64;

        let report = process_record_batch_lenient(
            &mut checkpoint,
            batch,
            |record| async move {
                match record {
                    "ok" => Ok(RecordProcessingOutcome::Processed),
                    "skip" => Err(("skip", RecordWarningDisposition::SkipRecord)),
                    "retry" => Err(("retry", RecordWarningDisposition::Retry)),
                    other => Err((other, RecordWarningDisposition::Retry)),
                }
            },
            |warning| warning.1,
        )
        .await;

        assert_eq!(checkpoint, 2);
        assert_eq!(report.final_checkpoint, 2);
        assert_eq!(report.processed_records, 1);
        assert_eq!(
            report.warnings,
            vec![
                ("skip", RecordWarningDisposition::SkipRecord),
                ("retry", RecordWarningDisposition::Retry)
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn lenient_processor_advances_to_source_frontier_after_successful_batch() -> TestResult<()>
    {
        let batch = RecordReadBatch {
            start_checkpoint: 5_u64,
            records: vec![RecordReadItem::new("ok", 6_u64)],
            final_checkpoint: 9_u64,
            observation: RecordSourceObservation::None,
        };
        let mut checkpoint = 5_u64;

        let report = process_record_batch_lenient(
            &mut checkpoint,
            batch,
            |_record| async move {
                Ok::<_, (&'static str, RecordWarningDisposition)>(
                    RecordProcessingOutcome::Processed,
                )
            },
            |warning| warning.1,
        )
        .await;

        assert_eq!(checkpoint, 9);
        assert_eq!(report.final_checkpoint, 9);
        assert_eq!(report.processed_records, 1);
        assert!(report.warnings.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn lenient_processor_advances_empty_batches_to_source_frontier() -> TestResult<()> {
        let batch = RecordReadBatch::<&str, u64> {
            start_checkpoint: 5,
            records: Vec::new(),
            final_checkpoint: 9,
            observation: RecordSourceObservation::None,
        };
        let mut checkpoint = 5_u64;

        let report = process_record_batch_lenient(
            &mut checkpoint,
            batch,
            |_record| async move {
                Ok::<_, (&'static str, RecordWarningDisposition)>(
                    RecordProcessingOutcome::Processed,
                )
            },
            |warning| warning.1,
        )
        .await;

        assert_eq!(checkpoint, 9);
        assert_eq!(report.final_checkpoint, 9);
        assert_eq!(report.processed_records, 0);
        assert!(report.warnings.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn append_only_source_reads_complete_lines_from_checkpoint() -> TestResult<()> {
        let temp = tempfile::NamedTempFile::new()?;
        tokio::fs::write(temp.path(), b"one\ntwo\npartial").await?;
        let path = Utf8PathBuf::from_path_buf(temp.path().to_path_buf())
            .map_err(|path| color_eyre::eyre::eyre!("non-utf8 temp path: {path:?}"))?;
        let source = RecordSources::append_only_utf8_file(path);
        let batch = source
            .read_batch(&source.initial_checkpoint(), RecordReadHorizon::Unbounded)
            .await?;

        let lines: Vec<_> = batch
            .records
            .iter()
            .map(|item| item.record.line.as_str())
            .collect();
        assert_eq!(lines, vec!["one", "two"]);
        assert_eq!(batch.records[0].record.start_offset_bytes, 0);
        assert_eq!(batch.records[0].record.end_offset_bytes, 4);
        assert_eq!(batch.records[0].checkpoint_after.offset_bytes, 4);
        assert_eq!(batch.records[1].record.start_offset_bytes, 4);
        assert_eq!(batch.records[1].record.end_offset_bytes, 8);
        assert_eq!(batch.records[1].checkpoint_after.offset_bytes, 8);
        assert_eq!(batch.final_checkpoint.offset_bytes, 8);
        match batch.observation {
            RecordSourceObservation::AppendOnlyFile {
                file_size,
                bytes_consumed,
                ..
            } => {
                assert_eq!(file_size, 15);
                assert_eq!(bytes_consumed, 8);
            }
            other => {
                return Err(color_eyre::eyre::eyre!(
                    "expected append-only observation, got {other:?}"
                ));
            }
        }
        Ok(())
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestRow {
        row_id: i64,
        value: &'static str,
    }

    #[derive(Debug, Clone, Copy)]
    struct TestReadError;

    impl fmt::Display for TestReadError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test read error")
        }
    }

    impl std::error::Error for TestReadError {}

    #[sinex_test]
    async fn sqlite_source_uses_typed_row_checkpoint() -> TestResult<()> {
        let source = RecordSources::sqlite(
            Utf8PathBuf::from("/tmp/history.db"),
            "test-sqlite",
            |_path, from_row_id, _end_time| -> Result<(Vec<TestRow>, i64), TestReadError> {
                Ok((
                    vec![
                        TestRow {
                            row_id: from_row_id + 1,
                            value: "one",
                        },
                        TestRow {
                            row_id: from_row_id + 2,
                            value: "two",
                        },
                    ],
                    from_row_id + 2,
                ))
            },
            |row: &TestRow| row.row_id,
        );

        let batch = source
            .read_batch(&SqliteRowCheckpoint::new(5), RecordReadHorizon::Unbounded)
            .await?;
        assert_eq!(batch.final_checkpoint, SqliteRowCheckpoint::new(7));
        assert_eq!(
            batch.records[0].checkpoint_after,
            SqliteRowCheckpoint::new(6)
        );
        assert_eq!(batch.records[1].record.value, "two");
        Ok(())
    }

    #[sinex_test]
    async fn stable_json_line_has_trailing_newline() -> TestResult<()> {
        let bytes = stable_json_line(&json!({ "b": 2, "a": 1 }))?;
        assert_eq!(bytes.last(), Some(&b'\n'));
        Ok(())
    }
}
