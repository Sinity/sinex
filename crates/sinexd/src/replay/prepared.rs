//! Complete replacement batches, ready before the writer owns a transaction.
//! Parsing in the daemon uses a private unlinked file in the blob preparation directory;
//! one batch is encoded or decoded at a time. This is disposable preparation,
//! not replay progress: only the admission commit records a completed sequence.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sinex_model::{Batch, BatchKind, RecordOutcome, RunId, SourceRef};
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, Write};
use std::path::Path;

pub(crate) enum Batches {
    Ready(std::vec::IntoIter<Batch>),
    Prepared(Box<Prepared>),
}

impl Batches {
    pub(crate) fn ready(batches: Vec<Batch>) -> Self {
        Self::Ready(batches.into_iter())
    }

    pub(crate) fn next(&mut self) -> Option<Result<Batch>> {
        match self {
            Self::Ready(batches) => batches.next().map(Ok),
            Self::Prepared(prepared) => prepared.next(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ReadBatch {
    source: Option<SourceRef>,
    host: Option<String>,
    records: Vec<RecordOutcome>,
}

pub(crate) struct Builder {
    file: BufWriter<File>,
    identity: Option<(RunId, sinex_model::OperationId)>,
    batches: u64,
}

impl Builder {
    pub(crate) fn new(blob_root: &Path) -> Result<Self> {
        let scratch = blob_root.join("incoming");
        std::fs::create_dir_all(&scratch)?;
        Ok(Self {
            file: BufWriter::new(tempfile::tempfile_in(scratch)?),
            identity: None,
            batches: 0,
        })
    }

    pub(crate) fn push(&mut self, batch: Batch) -> Result<()> {
        let BatchKind::Replay {
            operation,
            source,
            host,
        } = batch.kind
        else {
            return Err(Error::internal("daemon replay prepared a non-replay batch"));
        };
        let identity = (batch.run, operation);
        if self.identity.is_some_and(|expected| expected != identity) {
            return Err(Error::internal("daemon replay changed its operation"));
        }
        self.identity = Some(identity);
        if !batch.archive.is_empty() {
            return Err(Error::internal(
                "daemon replay archives only its first batch",
            ));
        }
        serde_json::to_writer(
            &mut self.file,
            &ReadBatch {
                source,
                host,
                records: batch.records,
            },
        )?;
        self.file.write_all(b"\n")?;
        self.batches += 1;
        Ok(())
    }

    pub(crate) fn finish(mut self, first: Batch) -> Result<Batches> {
        let BatchKind::Replay { operation, .. } = first.kind else {
            return Err(Error::internal("daemon replay prepared a non-replay scope"));
        };
        if self
            .identity
            .is_some_and(|expected| expected != (first.run, operation))
        {
            return Err(Error::internal("daemon replay changed its operation"));
        }
        self.file.flush()?;
        let mut file = self.file.into_inner().map_err(|e| e.into_error())?;
        file.rewind()?;
        let mut records = serde_json::Deserializer::from_reader(BufReader::new(file));
        // These readings have already been built by the parser. Wrapping them
        // in preparation must not impose a smaller payload nesting allowance.
        records.disable_recursion_limit();
        Ok(Batches::Prepared(Box::new(Prepared {
            run: first.run,
            operation,
            first: Some(first),
            remaining: self.batches,
            records: records.into_iter(),
        })))
    }
}

#[cfg(test)]
mod tests;

pub(crate) struct Prepared {
    run: RunId,
    operation: sinex_model::OperationId,
    first: Option<Batch>,
    remaining: u64,
    records:
        serde_json::StreamDeserializer<'static, serde_json::de::IoRead<BufReader<File>>, ReadBatch>,
}

impl Prepared {
    fn next(&mut self) -> Option<Result<Batch>> {
        if let Some(first) = self.first.take() {
            return Some(Ok(first));
        }
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(match self.records.next() {
            Some(Ok(read)) => Ok(Batch {
                run: self.run,
                kind: BatchKind::Replay {
                    operation: self.operation,
                    source: read.source,
                    host: read.host,
                },
                records: read.records,
                archive: Vec::new(),
            }),
            Some(Err(error)) => Err(error.into()),
            None => Err(Error::internal(
                "prepared replay ended before its last batch",
            )),
        })
    }
}
