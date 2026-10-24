//! Explicit, operation-scoped use of the operator's unlock authority.
use sinex_model::MaterialId;
use sinex_store::sealed::{DataKey, Identity, MaterialSecrets};
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
    pub fn unwrap_secrets(&self, wrapped: &[u8]) -> Result<MaterialSecrets, String> {
        self.check()?;
        let secrets = sinex_store::sealed::unwrap_secrets(
            wrapped,
            &self.identity,
            sinex_model::Class::Sealed,
        )
        .map_err(|e| e.to_string())?;
        self.check()?;
        Ok(secrets)
    }
    pub fn open_replay_metadata(
        &self,
        input: impl std::io::Read,
        scope: &sinex_store::sealed::metadata::Scope,
    ) -> Result<Vec<sinex_store::sealed::metadata::ErasedRecord>, String> {
        sinex_store::sealed::metadata::open_replay(input, &self.identity, scope, || {
            self.check().map_err(sinex_store::Error::Invalid)
        })
        .map_err(|e| e.to_string())
    }
    pub fn open_current_carry(
        &self,
        input: impl std::io::Read,
        scope: &sinex_store::sealed::metadata::Scope,
    ) -> Result<serde_json::Value, String> {
        self.check()?;
        let value = sinex_store::sealed::metadata::open_carry(input, &self.identity, scope)
            .map_err(|e| e.to_string())?;
        self.check()?;
        Ok(value)
    }
    /// Call only for material IDs in the explicit operation's resolved scope.
    pub async fn material_secrets(
        &self,
        conn: &mut PgConnection,
        material: MaterialId,
    ) -> Result<(String, MaterialSecrets), String> {
        self.check()?;
        let row = sqlx::query!(
            "SELECT key_ref,class,wrapped_key,role FROM privacy.material_keys WHERE material_id=$1",
            material.0
        )
        .fetch_optional(conn)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("material {material}: sealed key unavailable or destroyed"))?;
        if row.class != "sealed" {
            return Err("wrapped capability belongs to a different material class".into());
        }
        let secrets = self.unwrap_secrets(&row.wrapped_key)?;
        if secrets.role().as_str() != row.role {
            return Err("wrapped capability role does not match material key metadata".into());
        }
        Ok((row.key_ref, secrets))
    }
    /// Call only for material IDs in the explicit operation's resolved scope.
    pub async fn material_key(
        &self,
        conn: &mut PgConnection,
        material: MaterialId,
    ) -> Result<DataKey, String> {
        let (reference, secrets) = self.material_secrets(conn, material).await?;
        secrets.data_key(reference).map_err(|e| e.to_string())
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
