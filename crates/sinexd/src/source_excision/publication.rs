//! One native material publication owns its private stages through installation
//! and the first durable Writer response. Cancellation cannot strand readable
//! bytes without the reservation that names their exact original version.
use super::AcquisitionGuard;
use crate::writer::{NativePermit, WriteError};
use sinex_model::{Batch, BatchKind};
use sinex_sources::drivers::MaterialSink;
use sinex_store::{Blobs, Outcome, blobs::PreparedPlain, native_materials};
use sqlx::Connection;
use std::sync::Mutex;

pub(crate) struct StagingSink {
    blobs: Blobs,
    stages: Mutex<Vec<PreparedPlain>>,
}
impl StagingSink {
    pub fn new(blobs: Blobs) -> Self {
        Self {
            blobs,
            stages: Mutex::new(Vec::new()),
        }
    }
    fn prepare(&self, reader: impl std::io::Read) -> Result<String, String> {
        let prepared = self
            .blobs
            .prepare_plain(reader)
            .map_err(|e| e.to_string())?;
        let name = prepared.name();
        self.stages
            .lock()
            .map_err(|_| "native staging poisoned")?
            .push(prepared);
        Ok(name)
    }
    pub fn take(&self) -> Result<Vec<PreparedPlain>, String> {
        Ok(std::mem::take(
            &mut *self.stages.lock().map_err(|_| "native staging poisoned")?,
        ))
    }
}
impl MaterialSink for StagingSink {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.prepare(bytes)
    }
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        self.prepare(std::fs::File::open(path).map_err(|e| e.to_string())?)
    }
}

pub(crate) struct Publication {
    pub pending: native_materials::Pending,
    pub stages: Vec<PreparedPlain>,
    pub batch: Batch,
    pub permit: NativePermit,
    pub guard: AcquisitionGuard,
}
impl Publication {
    pub fn start(self) -> tokio::task::JoinHandle<Result<Outcome, WriteError>> {
        tokio::spawn(self.publish())
    }
    async fn publish(mut self) -> Result<Outcome, WriteError> {
        let refuse = |e: &dyn std::fmt::Display| WriteError::Refused(e.to_string());
        if self.guard.source() != self.pending.source {
            let _ = self.guard.release().await;
            return Err(WriteError::Refused(
                "native publication source differs from its guard".into(),
            ));
        }
        // Reserve with an awaited transaction end. A cancellation after this
        // point cannot drop the private handles: this owned task finishes them.
        let reserved = async {
            let mut tx = self
                .guard
                .connection()
                .begin()
                .await
                .map_err(|e| refuse(&e))?;
            let result = async {
                sinex_store::excision::validate_basis(&mut tx, &self.batch)
                    .await
                    .map_err(|e| refuse(&e))?;
                native_materials::reserve(&mut tx, &self.pending)
                    .await
                    .map_err(|e| refuse(&e))
            }
            .await;
            match result {
                Ok(()) => tx.commit().await.map_err(|e| refuse(&e)),
                Err(error) => {
                    tx.rollback().await.map_err(|e| refuse(&e))?;
                    Err(error)
                }
            }
        }
        .await;
        if let Err(error) = reserved {
            let _ = self.guard.release().await;
            return Err(error);
        }
        let stages = std::mem::take(&mut self.stages);
        let installed = tokio::task::spawn_blocking(move || {
            for stage in stages {
                stage.install().map_err(|e| e.to_string())?;
            }
            Ok::<_, String>(())
        })
        .await
        .map_err(|e| refuse(&e))
        .and_then(|r| r.map_err(WriteError::Refused));
        if let Err(error) = installed {
            let _ = self.guard.release().await;
            return Err(error);
        }
        let result =
            native_materials::installed(self.guard.connection(), self.pending.material).await;
        if let Err(error) = result {
            let _ = self.guard.release().await;
            return Err(refuse(&error));
        }
        let BatchKind::Source(progress) = &mut self.batch.kind else {
            let _ = self.guard.release().await;
            return Err(WriteError::Refused(
                "native publication requires source progress".into(),
            ));
        };
        progress.native_pending = Some(self.pending.material);
        self.permit.admit(self.batch, self.guard).await
    }
}

#[cfg(test)]
mod tests;
