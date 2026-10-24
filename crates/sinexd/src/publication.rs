//! One source material publication owns its private stages through installation
//! and the first durable Writer response. Cancellation cannot strand readable
//! bytes without the reservation that names their exact original version.
mod authored;
pub(crate) use authored::{resume_authored, submit_authored};

use crate::excision::AcquisitionGuard;
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
    pub fn prepare(&self, reader: impl std::io::Read) -> Result<String, String> {
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

pub(crate) enum NativeStage {
    Plain(PreparedPlain),
    Sealed(sinex_store::blobs::PreparedSealed),
}
impl NativeStage {
    fn install(self) -> Result<String, String> {
        match self {
            Self::Plain(stage) => stage.install(),
            Self::Sealed(stage) => stage.install(),
        }
        .map_err(|e| e.to_string())
    }
}

pub(crate) struct Verification {
    pub key: Option<sinex_store::sealed::DataKey>,
    pub authority: Option<crate::sealed_access::Authority>,
    pub check: std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>,
}
impl Default for Verification {
    fn default() -> Self {
        Self {
            key: None,
            authority: None,
            check: std::sync::Arc::new(|| Ok(())),
        }
    }
}
impl Verification {
    fn check(&self) -> Result<(), String> {
        (self.check)()?;
        if let Some(authority) = &self.authority {
            authority.check()?;
        }
        Ok(())
    }
    pub fn queued_check(&self) -> crate::writer::NativeCheck {
        let check = self.check.clone();
        let authority = self.authority.clone();
        std::sync::Arc::new(move || {
            check()?;
            if let Some(authority) = &authority {
                authority.check()?;
            }
            Ok(())
        })
    }
    fn body(&self, blobs: &Blobs, pending: &native_materials::Pending) -> Result<(), String> {
        self.check()?;
        for (blob, length) in
            native_materials::citation_set(&pending.cited).map_err(|e| e.to_string())?
        {
            let check = self.queued_check();
            let mut body = blobs
                .read_plain(&blob, Some(length), move || {
                    check().map_err(std::io::Error::other).map_err(Into::into)
                })
                .map_err(|e| e.to_string())?;
            std::io::copy(&mut body, &mut std::io::sink()).map_err(|e| e.to_string())?;
            self.check()?;
        }
        let Some(material) = &pending.progress.material else {
            return Ok(());
        };
        if let Some(key) = &self.key {
            if !blobs.is_sealed(&material.blob).map_err(|e| e.to_string())?
                || pending
                    .progress
                    .material_keys
                    .iter()
                    .filter(|k| k.material == material.id && k.key_ref == key.key_ref)
                    .count()
                    != 1
            {
                return Err("the verification key differs from its pending material".into());
            }
            let check = self.check.clone();
            let authority = self.authority.clone();
            let mut body = blobs
                .read_sealed(
                    &material.blob,
                    Some(material.length),
                    key,
                    crate::sealed_access::read_limits(material.length)?,
                    move || {
                        check().map_err(std::io::Error::other)?;
                        if let Some(authority) = &authority {
                            authority.check().map_err(std::io::Error::other)?;
                        }
                        Ok(())
                    },
                )
                .map_err(|e| e.to_string())?;
            std::io::copy(&mut body, &mut std::io::sink()).map_err(|e| e.to_string())?;
        } else if blobs.is_sealed(&material.blob).map_err(|e| e.to_string())? {
            crate::retained::open_pending(blobs, pending, self.authority.as_ref())?;
        } else {
            let check = self.check.clone();
            let mut body = blobs
                .read_plain(&material.blob, Some(material.length), move || {
                    check().map_err(std::io::Error::other).map_err(Into::into)
                })
                .map_err(|e| e.to_string())?;
            std::io::copy(&mut body, &mut std::io::sink()).map_err(|e| e.to_string())?;
        }
        self.check()
    }
}

pub(crate) struct Publication {
    pub pending: native_materials::Pending,
    pub stages: Vec<NativeStage>,
    pub verification: Verification,
    pub blobs: Blobs,
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
        if let Err(error) = self.verification.check() {
            let _ = self.guard.release().await;
            return Err(WriteError::Refused(error));
        }
        let cited = self
            .batch
            .events()
            .flat_map(|event| event.cites.iter().cloned())
            .collect::<Vec<_>>();
        let citations = native_materials::citation_set(&cited).and_then(|actual| {
            let expected = native_materials::citation_set(&self.pending.cited)?;
            if actual != expected {
                return Err(sinex_store::Error::Invalid(
                    "prepared citations differ from frozen publication".into(),
                ));
            }
            Ok(())
        });
        if let Err(error) = citations {
            let _ = self.guard.release().await;
            return Err(refuse(&error));
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
                sinex_store::excision::exclusions::check(&mut tx, &self.batch)
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
        let check = self.verification.check.clone();
        let authority = self.verification.authority.clone();
        let installed = tokio::task::spawn_blocking(move || {
            for stage in stages {
                check()?;
                if let Some(authority) = &authority {
                    authority.check()?;
                }
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
        let pending = self.pending;
        let verification = self.verification;
        let blobs = self.blobs.clone();
        let (pending, verification, verified) = match tokio::task::spawn_blocking(move || {
            let result = verification.body(&blobs, &pending);
            (pending, verification, result)
        })
        .await
        {
            Ok(result) => result,
            Err(error) => {
                let _ = self.guard.release().await;
                return Err(refuse(&error));
            }
        };
        self.pending = pending;
        self.verification = verification;
        if let Err(error) = verified {
            let _ = self.guard.release().await;
            return Err(WriteError::Refused(error));
        }
        let installed = async {
            let mut tx = self
                .guard
                .connection()
                .begin()
                .await
                .map_err(|e| refuse(&e))?;
            let result = async {
                native_materials::installed(&mut tx, self.pending.material)
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
        if let Err(error) = installed {
            let _ = self.guard.release().await;
            return Err(error);
        }
        if let Err(error) = self.verification.check() {
            let _ = self.guard.release().await;
            return Err(WriteError::Refused(error));
        }
        let BatchKind::Source(progress) = &mut self.batch.kind else {
            let _ = self.guard.release().await;
            return Err(WriteError::Refused(
                "native publication requires source progress".into(),
            ));
        };
        progress.native_pending = Some(self.pending.material);
        self.permit
            .admit(self.batch, self.guard, self.verification.queued_check())
            .await
    }
}

#[cfg(test)]
mod tests;
