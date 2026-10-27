//! Owned synchronous work, with a shared worker budget and two batches per submission.
use sinex_model::Batch;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

// Waiting for work capacity never rejects input or changes a drain's batch targets.
static WORKERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));

pub(crate) async fn run<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, String> {
    let permit = WORKERS
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| e.to_string())?;
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
    pub _permit: OwnedSemaphorePermit,
}

pub(super) struct BatchSender {
    send: mpsc::Sender<Result<Parsed, String>>,
    permits: Arc<Semaphore>,
    runtime: tokio::runtime::Handle,
}

impl BatchSender {
    pub fn permit(&self) -> Option<OwnedSemaphorePermit> {
        self.runtime
            .block_on(self.permits.clone().acquire_owned())
            .ok()
    }

    pub fn send(&self, batch: Parsed) -> bool {
        self.send.blocking_send(Ok(batch)).is_ok()
    }
}

pub(super) struct Batches {
    receive: mpsc::Receiver<Result<Parsed, String>>,
    permits: Arc<Semaphore>,
    worker: Option<tokio::task::JoinHandle<()>>,
}

impl Batches {
    pub async fn start(
        work: impl FnOnce(BatchSender) -> Result<(), String> + Send + 'static,
    ) -> Result<Self, String> {
        let budget = WORKERS
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| e.to_string())?;
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
        })
    }

    pub async fn next(&mut self) -> Option<Result<Parsed, String>> {
        self.receive.recv().await
    }

    pub async fn finish(mut self) -> Result<(), String> {
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
        self.permits.close();
        self.receive.close();
    }
}
