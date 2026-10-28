//! Owned synchronous work, with a shared worker budget and two batches per submission.
use crate::writer::Priority;
use sinex_model::Batch;
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

// Waiting for work capacity never rejects input or changes a drain's batch targets.
static WORKERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));
static BULK_WORKERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(3)));

struct WorkPermit {
    _worker: OwnedSemaphorePermit,
    _bulk: Option<OwnedSemaphorePermit>,
}

async fn capacity(priority: Priority) -> Result<WorkPermit, String> {
    // Bulk acquisitions can wait on many commits. Keep a worker available for live requests.
    let bulk = if priority == Priority::Bulk {
        Some(
            BULK_WORKERS
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| e.to_string())?,
        )
    } else {
        None
    };
    let worker = WORKERS
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| e.to_string())?;
    Ok(WorkPermit {
        _worker: worker,
        _bulk: bulk,
    })
}

pub(crate) async fn run<T: Send + 'static>(
    priority: Priority,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, String> {
    let permit = capacity(priority).await?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|e| e.to_string())
}

pub(super) struct Parsed {
    pub batch: Batch,
    pub elapsed: Duration,
    pub material_length: Option<Option<usize>>,
    pub permit: BatchPermit,
}

pub(super) struct BatchPermit {
    _permit: OwnedSemaphorePermit,
    permits: Arc<Semaphore>,
    committed: bool,
}

impl BatchPermit {
    pub(super) fn committed(&mut self) {
        self.committed = true;
    }
}

impl Drop for BatchPermit {
    fn drop(&mut self) {
        // Close parsing before releasing a failed or abandoned batch's slot.
        if !self.committed {
            self.permits.close();
        }
    }
}

pub(super) struct BatchSender {
    send: mpsc::Sender<Result<Parsed, String>>,
    permits: Arc<Semaphore>,
    runtime: tokio::runtime::Handle,
}

impl BatchSender {
    pub(super) fn permit(&self) -> Option<BatchPermit> {
        let permit = self
            .runtime
            .block_on(self.permits.clone().acquire_owned())
            .ok()?;
        Some(BatchPermit {
            _permit: permit,
            permits: self.permits.clone(),
            committed: false,
        })
    }

    pub(super) fn send(&self, batch: Parsed) -> bool {
        self.send.blocking_send(Ok(batch)).is_ok()
    }
}

pub(super) struct Batches {
    receive: mpsc::Receiver<Result<Parsed, String>>,
    permits: Arc<Semaphore>,
    worker: Option<tokio::task::JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
}

impl Batches {
    pub(super) async fn start(
        priority: Priority,
        work: impl FnOnce(BatchSender) -> Result<(), String> + Send + 'static,
    ) -> Result<Self, String> {
        let budget = capacity(priority).await?;
        let (send, receive) = mpsc::channel(1);
        let permits = Arc::new(Semaphore::new(2));
        let sender = BatchSender {
            send: send.clone(),
            permits: permits.clone(),
            runtime: tokio::runtime::Handle::current(),
        };
        let worker = tokio::task::spawn_blocking(move || {
            let _budget = budget;
            if let Err(error) = work(sender) {
                let _ = send.blocking_send(Err(error));
            }
        });
        Ok(Self {
            receive,
            permits,
            worker: Some(worker),
            cancelled: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(super) fn cancellation_check(&self) -> crate::writer::AdmissionCheck {
        let cancelled = self.cancelled.clone();
        Arc::new(move || {
            if cancelled.load(Ordering::Acquire) {
                Err("submission cancelled".into())
            } else {
                Ok(())
            }
        })
    }

    pub(super) async fn next(&mut self) -> Option<Result<Parsed, String>> {
        self.receive.recv().await
    }

    pub(super) async fn finish(mut self) -> Result<(), String> {
        self.permits.close();
        self.receive.close();
        // Drop queued batches before joining work that may be waiting for a permit.
        while self.receive.try_recv().is_ok() {}
        self.worker
            .take()
            .expect("blocking worker")
            .await
            .map_err(|e| e.to_string())
    }
}

impl Drop for Batches {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        self.permits.close();
        self.receive.close();
    }
}

#[cfg(test)]
mod tests;
