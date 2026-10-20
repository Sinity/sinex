//! Explicit, operation-scoped use of the operator's unlock authority.
use sinex_model::MaterialId;
use sinex_store::sealed::{DataKey, Identity};
use sqlx::PgConnection;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

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
        .ok_or_else(|| format!("material {material}: sealed key unavailable or destroyed"))?;
        Ok(DataKey {
            key_ref: row.key_ref,
            key: self.unwrap(&row.wrapped_key)?,
        })
    }
}

#[cfg(test)]
mod tests;
