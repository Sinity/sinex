//! Open a stored material's bytes for reading again: plain, or sealed with the
//! operator's unlock.
use super::Retained;
use crate::sealed_access::Authority;
use sinex_model::{MaterialId, MaterialRef};
use sinex_sources::drivers::RetainedRecords;
use sinex_store::{Blobs, sealed::DataKey};
use sqlx::PgConnection;
use std::{io::Seek, sync::Arc};

pub(crate) struct Opened {
    pub reference: MaterialRef,
    pub body: Arc<Retained>,
    /// The data key of a sealed material.
    pub key: Option<DataKey>,
}
impl Opened {
    pub fn records(&self) -> RetainedRecords {
        RetainedRecords {
            blob: self.reference.blob.clone(),
            bytes: self.body.clone(),
        }
    }
}

pub(crate) async fn open(
    conn: &mut PgConnection,
    blobs: &Blobs,
    material: MaterialId,
    authority: Option<&Authority>,
) -> Result<Opened, String> {
    open_checked(conn, blobs, material, authority, Arc::new(|| Ok(()))).await
}
pub(crate) async fn open_checked(
    conn: &mut PgConnection,
    blobs: &Blobs,
    material: MaterialId,
    authority: Option<&Authority>,
    check: super::ReadCheck,
) -> Result<Opened, String> {
    check()?;
    let reference = sinex_store::material_bytes::reference(conn, material)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("stored material is missing")?;
    let key = if blobs
        .is_sealed(&reference.blob)
        .map_err(|e| e.to_string())?
    {
        let authority = authority.ok_or("stored sealed material requires explicit unlock")?;
        Some(authority.material_key(conn, material).await?)
    } else {
        None
    };
    open_owned(blobs, reference, key, authority, check)
}

/// Only the frozen storage-owned publication supplies these facts. Missing
/// installed bytes stay unavailable; this never observes the provider again.
pub(crate) fn open_pending(
    blobs: &Blobs,
    pending: &sinex_store::native_materials::Pending,
    authority: Option<&Authority>,
) -> Result<Opened, String> {
    let reference = pending
        .progress
        .material
        .clone()
        .ok_or("pending publication has no row/record material")?;
    if reference.id != pending.material
        || pending.progress.binding != pending.binding
        || pending
            .progress
            .exclusions
            .as_ref()
            .is_none_or(|b| b.source != pending.source)
    {
        return Err("pending material ownership differs".into());
    }
    let mut keys = pending
        .progress
        .material_keys
        .iter()
        .filter(|key| key.material == reference.id);
    let wrapped = keys.next();
    if keys.next().is_some() {
        return Err("pending material has multiple data keys".into());
    }
    let key = if blobs
        .is_sealed(&reference.blob)
        .map_err(|e| e.to_string())?
    {
        let wrapped = wrapped.ok_or("pending sealed material lacks its data key")?;
        let authority = authority.ok_or("pending sealed material requires explicit unlock")?;
        Some(DataKey {
            key_ref: wrapped.key_ref.clone(),
            key: authority.unwrap(&wrapped.wrapped)?,
        })
    } else {
        if wrapped.is_some() {
            return Err("pending data key does not match a sealed body".into());
        }
        None
    };
    open_owned(blobs, reference, key, authority, Arc::new(|| Ok(())))
}

fn open_owned(
    blobs: &Blobs,
    reference: MaterialRef,
    key: Option<DataKey>,
    authority: Option<&Authority>,
    check: super::ReadCheck,
) -> Result<Opened, String> {
    let caller_check = check;
    let unlock = authority.cloned();
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
        Arc::new(Retained::checked(input, reference.length, move || {
            cancel().map_err(std::io::Error::other)
        }))
    } else {
        let input = blobs
            .read_plain_seek(&reference.blob, reference.length, move || {
                cancel().map_err(std::io::Error::other).map_err(Into::into)
            })
            .map_err(|e| e.to_string())?;
        let cancel = Arc::clone(&check);
        Arc::new(Retained::checked(input, reference.length, move || {
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
