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
                return Err(
                    "local verification capability differs from its pending material".into(),
                );
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
            if let Some(replay) = &pending.progress.erased_replay {
                let authority = self
                    .authority
                    .as_ref()
                    .ok_or("replay verification requires scoped authority")?;
                let scope = sinex_store::sealed::metadata::Scope::Replay {
                    source: pending.source.clone(),
                    material: material.id,
                    parser: material.parser.clone(),
                    parser_version: i32::try_from(material.parser_version)
                        .map_err(|_| "parser version exceeds i32")?,
                    length: material.length,
                    whole: false,
                    append_end: material
                        .manifest
                        .get("append_resume")
                        .and_then(|r| r.get("end"))
                        .cloned(),
                };
                let check = self.check.clone();
                let scoped = authority.clone();
                let body = blobs
                    .read_plain(&replay.blob, Some(replay.length), move || {
                        check().map_err(std::io::Error::other)?;
                        scoped.check().map_err(std::io::Error::other)?;
                        Ok(())
                    })
                    .map_err(|e| e.to_string())?;
                authority.open_replay_metadata(body, &scope)?;
            }
        } else if blobs.is_sealed(&material.blob).map_err(|e| e.to_string())?
            || pending.progress.erased_replay.is_some()
        {
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

pub(crate) enum ImageVerification {
    Pinned(crate::sqlite_images::PreparedImage),
    Retained {
        blobs: Blobs,
        plan: native_materials::PendingImage,
        previous: Option<sinex_model::SqliteImageMeta>,
    },
}
impl ImageVerification {
    fn verify(self) -> Result<sinex_model::SqliteImagePublication, String> {
        match self {
            Self::Pinned(image) => image.verify(),
            Self::Retained {
                blobs,
                plan,
                previous,
            } => crate::sqlite_images::verify_retained_plan(&blobs, &plan, previous.as_ref()),
        }
    }
}

pub(crate) struct Published {
    pub outcome: Outcome,
    pub image: Option<sinex_model::SqliteImagePublication>,
}
impl std::fmt::Debug for Published {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Published")
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

pub(crate) struct Publication {
    pub pending: native_materials::Pending,
    pub stages: Vec<NativeStage>,
    pub image: Option<ImageVerification>,
    pub verification: Verification,
    pub blobs: Blobs,
    pub batch: Batch,
    pub permit: NativePermit,
    pub guard: AcquisitionGuard,
}
impl Publication {
    pub fn start(self) -> tokio::task::JoinHandle<Result<Published, WriteError>> {
        tokio::spawn(self.publish())
    }
    async fn publish(mut self) -> Result<Published, WriteError> {
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
        let completed = if let Some(image) = self.image.take() {
            match tokio::task::spawn_blocking(move || image.verify()).await {
                Ok(Ok(image)) => Some(image),
                result => {
                    let _ = self.guard.release().await;
                    return Err(WriteError::Refused(match result {
                        Ok(Err(error)) => error,
                        Err(error) => error.to_string(),
                        _ => unreachable!(),
                    }));
                }
            }
        } else {
            None
        };
        let installed = async {
            let mut tx = self
                .guard
                .connection()
                .begin()
                .await
                .map_err(|e| refuse(&e))?;
            let result = async {
                if let Some(image) = &completed {
                    if self.pending.image.is_some() {
                        native_materials::finalize_image(
                            &mut tx,
                            self.pending.material,
                            image.clone(),
                        )
                        .await
                        .map_err(|e| refuse(&e))?;
                    } else if self.pending.progress.image.as_ref() != Some(image) {
                        return Err(WriteError::Refused(
                            "verified retained image differs from frozen publication".into(),
                        ));
                    }
                }
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
        let published_image = completed.clone().or_else(|| progress.image.clone());
        progress.image = completed.or(progress.image.take());
        let outcome = self
            .permit
            .admit(self.batch, self.guard, self.verification.queued_check())
            .await?;
        Ok(Published {
            outcome,
            image: published_image,
        })
    }
}

#[cfg(test)]
mod tests;
