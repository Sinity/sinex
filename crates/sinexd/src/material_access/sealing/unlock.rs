//! An operator unlock held by one operation for a limited time.
use sinex_model::MaterialId;
use sinex_sealing::{DataKey, Identity};
use sqlx::PgConnection;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) struct Unlock {
    identity: Arc<Identity>,
    expiry: Arc<Expiry>,
}
struct Expiry {
    until: Instant,
    revoked: AtomicBool,
}
impl Unlock {
    pub(crate) fn new(identity: Identity, duration: Duration) -> Result<Self, String> {
        let until = Instant::now()
            .checked_add(duration)
            .ok_or("unlock duration exceeds the clock range")?;
        Ok(Self {
            identity: Arc::new(identity),
            expiry: Arc::new(Expiry {
                until,
                revoked: AtomicBool::new(false),
            }),
        })
    }
    pub(crate) fn recipient(&self) -> sinex_sealing::Recipient {
        self.identity.to_public()
    }

    pub(crate) fn check(&self) -> Result<(), String> {
        if self.expiry.revoked.load(Ordering::Acquire) || Instant::now() >= self.expiry.until {
            Err("this operation's sealed unlock expired or was revoked".into())
        } else {
            Ok(())
        }
    }
    pub(crate) fn revoke(&self) {
        self.expiry.revoked.store(true, Ordering::Release);
    }
    pub(crate) fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32], String> {
        self.check()?;
        let key = sinex_sealing::unwrap(wrapped, &self.identity).map_err(|e| e.to_string())?;
        self.check()?;
        Ok(key)
    }
    /// Call only for material IDs in the explicit operation's resolved scope.
    pub(crate) async fn data_key(
        &self,
        conn: &mut PgConnection,
        material: MaterialId,
    ) -> Result<DataKey, String> {
        self.check()?;
        let (key_ref, wrapped) = sinex_store::sealed::data_key(conn, material)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("material {material}: sealed key unavailable"))?;
        Ok(DataKey {
            key_ref,
            key: self.unwrap(&wrapped)?,
        })
    }
}

#[cfg(test)]
mod tests;

// Sealed writers use this frame size; the index allowance includes footer
// metadata.
pub(crate) fn read_limits(length: u64) -> Result<sinex_store::blobs::SealedReadLimits, String> {
    let frame = sinex_store::blobs::BLOB_FRAME_BYTES;
    let frames = length.div_ceil(frame as u64).max(1);
    let index = frames
        .checked_mul(128)
        .and_then(|n| n.checked_add(65536))
        .ok_or("sealed index budget overflow")?;
    Ok(sinex_store::blobs::SealedReadLimits {
        frame_bytes: frame,
        index_bytes: usize::try_from(index).map_err(|_| "sealed index exceeds address space")?,
    })
}
