//! Open a stored material's bytes for reading again: plain, or sealed with the
//! operator's unlock.
use super::StoredReader;
use crate::sealed_access::Unlock;
use sinex_model::{MaterialId, MaterialRef};
use sinex_sources::drain::RecordRead;
use sinex_sources::drivers::StoredRecords;
use sinex_store::{Blobs, sealed::DataKey};
use sqlx::PgConnection;
use std::{io::Seek, sync::Arc};

pub(crate) struct Opened {
    pub reference: MaterialRef,
    pub body: Arc<StoredReader>,
    /// The data key of a sealed material.
    pub key: Option<DataKey>,
}
impl Opened {
    pub(crate) fn records(&self) -> StoredRecords {
        StoredRecords {
            blob: self.reference.blob.clone(),
            bytes: self.body.clone(),
        }
    }
}

pub(crate) async fn open(
    conn: &mut PgConnection,
    blobs: &Blobs,
    material: MaterialId,
    unlock: Option<&Unlock>,
) -> Result<Opened, String> {
    let reference = sinex_store::material_bytes::reference(conn, material)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("stored material is missing")?;
    let key = if blobs
        .is_sealed(&reference.blob)
        .map_err(|e| e.to_string())?
    {
        let unlock = unlock.ok_or("stored sealed material requires explicit unlock")?;
        Some(unlock.data_key(conn, material).await?)
    } else {
        None
    };
    open_checked(blobs, reference, key, unlock, Arc::new(|| Ok(())))
}

/// A stored material's bytes, opened on the first read. A sealed material's
/// key is fetched when the operation is unlocked; without it, reading fails.
pub(crate) struct MaterialOnDemand {
    blobs: Blobs,
    reference: MaterialRef,
    sealed: bool,
    key: Option<DataKey>,
    unlock: Option<Unlock>,
    opened: std::sync::OnceLock<Result<Arc<StoredReader>, String>>,
}
impl MaterialOnDemand {
    pub(crate) async fn new(
        conn: &mut PgConnection,
        blobs: &Blobs,
        reference: MaterialRef,
        unlock: Option<&Unlock>,
    ) -> Result<Self, String> {
        let sealed = blobs
            .is_sealed(&reference.blob)
            .map_err(|e| e.to_string())?;
        let key = match (sealed, unlock) {
            (true, Some(unlock)) => Some(unlock.data_key(conn, reference.id).await?),
            _ => None,
        };
        Ok(Self {
            blobs: blobs.clone(),
            reference,
            sealed,
            key,
            unlock: unlock.cloned(),
            opened: std::sync::OnceLock::new(),
        })
    }
    pub(crate) fn sealed(&self) -> bool {
        self.sealed
    }
    fn body(&self) -> std::io::Result<&Arc<StoredReader>> {
        self.opened
            .get_or_init(|| {
                if self.sealed && self.key.is_none() {
                    return Err("stored sealed material requires explicit unlock".into());
                }
                open_checked(
                    &self.blobs,
                    self.reference.clone(),
                    self.key.clone(),
                    self.unlock.as_ref(),
                    Arc::new(|| Ok(())),
                )
                .map(|opened| opened.body)
            })
            .as_ref()
            .map_err(|e| std::io::Error::other(e.clone()))
    }
}
impl RecordRead for MaterialOnDemand {
    fn length(&self) -> u64 {
        self.reference.length
    }
    fn read_exact_at(&self, start: u64, bytes: &mut [u8]) -> std::io::Result<()> {
        self.body()?.read_exact_at(start, bytes)
    }
}

/// Open `reference` with `check` run before each read: plain, or sealed
/// under `key`.
pub(crate) fn open_checked(
    blobs: &Blobs,
    reference: MaterialRef,
    key: Option<DataKey>,
    unlock: Option<&Unlock>,
    check: super::ReadCheck,
) -> Result<Opened, String> {
    let caller_check = check;
    let unlock = unlock.cloned();
    let check: super::ReadCheck = Arc::new(move || {
        caller_check()?;
        if let Some(unlock) = &unlock {
            unlock.check()?;
        }
        Ok(())
    });
    check()?;
    let cancel = Arc::clone(&check);
    let body = if let Some(key) = &key {
        let mut input = blobs
            .read_sealed(
                &reference.blob,
                Some(reference.length),
                key,
                crate::sealed_access::read_limits(reference.length)?,
                move || cancel().map_err(std::io::Error::other).map_err(Into::into),
            )
            .map_err(|e| e.to_string())?;
        // Authenticate every byte before any is used.
        std::io::copy(&mut input, &mut std::io::sink()).map_err(|e| e.to_string())?;
        input.rewind().map_err(|e| e.to_string())?;
        let cancel = Arc::clone(&check);
        Arc::new(StoredReader::checked(input, reference.length, move || {
            cancel().map_err(std::io::Error::other)
        }))
    } else {
        let input = blobs
            .read_plain_seek(&reference.blob, reference.length, move || {
                cancel().map_err(std::io::Error::other).map_err(Into::into)
            })
            .map_err(|e| e.to_string())?;
        let cancel = Arc::clone(&check);
        Arc::new(StoredReader::checked(input, reference.length, move || {
            cancel().map_err(std::io::Error::other)
        }))
    };
    check()?;
    Ok(Opened {
        reference,
        body,
        key,
    })
}
