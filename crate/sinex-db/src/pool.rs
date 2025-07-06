use crate::{DbPool, PoolConfig};
use anyhow::Result;
use once_cell::sync::OnceCell;
use tracing::info;

static POOL: OnceCell<DbPool> = OnceCell::new();

/// Get or create the global database pool with default configuration
pub async fn get_pool() -> Result<&'static DbPool> {
    get_pool_with_config(None).await
}

/// Get or create the global database pool with custom configuration
pub async fn get_pool_with_config(config: Option<PoolConfig>) -> Result<&'static DbPool> {
    if let Some(pool) = POOL.get() {
        return Ok(pool);
    }

    let pool = match config {
        Some(config) => crate::create_pool_with_config_and_fallbacks(&config).await?,
        None => crate::create_pool_with_fallbacks().await?,
    };

    POOL.set(pool)
        .map_err(|_| anyhow::anyhow!("Failed to set global pool"))?;

    info!("Global database pool initialized");
    Ok(POOL.get().ok_or_else(|| anyhow::anyhow!("Pool not initialized"))?)
}

/// Get or create the global database pool with graceful fallbacks (deprecated - use get_pool)
#[deprecated(note = "Use get_pool() instead - it now includes graceful fallbacks by default")]
pub async fn get_pool_with_fallbacks() -> Result<&'static DbPool> {
    get_pool().await
}
