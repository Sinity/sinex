//! Explicit, operation-scoped use of the operator's unlock authority.
use sinex_model::MaterialId;
use sinex_store::sealed::{DataKey, Identity};
use sqlx::PgConnection;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

/// Capture can use a public recipient; historical plaintext requires the
/// existing operation-scoped authority.
#[derive(Clone, Copy, Default)]
pub(crate) struct Access<'a> {
    pub recipient: Option<&'a sinex_store::sealed::Recipient>,
    pub authority: Option<&'a Authority>,
}

#[derive(Clone)]
pub(crate) struct Authority {
    identity: Arc<Identity>,
    lease: Arc<Lease>,
}
struct Lease {
    until: Instant,
    revoked: AtomicBool,
}
impl Authority {
    pub fn new(identity: Identity, duration: Duration) -> Result<Self, String> {
        let until = Instant::now()
            .checked_add(duration)
            .ok_or("unlock duration exceeds the clock range")?;
        Ok(Self {
            identity: Arc::new(identity),
            lease: Arc::new(Lease {
                until,
                revoked: AtomicBool::new(false),
            }),
        })
    }
    pub fn recipient(&self) -> sinex_store::sealed::Recipient {
        self.identity.to_public()
    }

    pub fn check(&self) -> Result<(), String> {
        if self.lease.revoked.load(Ordering::Acquire) || Instant::now() >= self.lease.until {
            Err("this operation's sealed unlock authority expired or was revoked".into())
        } else {
            Ok(())
        }
    }
    pub fn revoke(&self) {
        self.lease.revoked.store(true, Ordering::Release);
    }
    pub fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32], String> {
        self.check()?;
        let key =
            sinex_store::sealed::unwrap(wrapped, &self.identity).map_err(|e| e.to_string())?;
        self.check()?;
        Ok(key)
    }
    /// Call only for material IDs in the explicit operation's resolved scope.
    pub async fn material_key(
        &self,
        conn: &mut PgConnection,
        material: MaterialId,
    ) -> Result<DataKey, String> {
        self.check()?;
        let row = sqlx::query!(
            "SELECT key_ref,wrapped_key FROM privacy.material_keys WHERE material_id=$1",
            material.0
        )
        .fetch_optional(conn)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("material {material}: sealed key unavailable"))?;
        Ok(DataKey {
            key_ref: row.key_ref,
            key: self.unwrap(&row.wrapped_key)?,
        })
    }
}

#[cfg(test)]
mod tests;

// Native sealed writers use this frame size; the index allowance includes footer metadata.
pub(crate) fn read_limits(length: u64) -> Result<sinex_store::blobs::SealedReadLimits, String> {
    let frame = sinex_store::blobs::BLOB_FRAME_BYTES;
    let frames = length.div_ceil(frame as u64).max(1);
    let index = frames
        .checked_mul(8)
        .and_then(|n| n.checked_add(65536))
        .ok_or("sealed index budget overflow")?;
    Ok(sinex_store::blobs::SealedReadLimits {
        frame_bytes: frame,
        index_bytes: usize::try_from(index).map_err(|_| "sealed index exceeds address space")?,
    })
}
