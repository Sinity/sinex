#![doc = include_str!("../../docs/pool.md")]

use crate::{DbPool, PoolConfig};
use color_eyre::eyre::Result;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tracing::info;

static POOL: Lazy<RwLock<Option<DbPool>>> = Lazy::new(|| RwLock::new(None));

/// Get or create the global database pool with default configuration
pub async fn get_pool() -> Result<DbPool> {
    get_pool_with_config(None).await
}

/// Get or create the global database pool with custom configuration
pub async fn get_pool_with_config(config: Option<PoolConfig>) -> Result<DbPool> {
    if let Some(pool) = POOL.read().await.as_ref().cloned() {
        return Ok(pool);
    }

    let pool = match config {
        Some(config) => crate::create_pool_with_config_strict(&config).await?,
        None => crate::create_pool_strict().await?,
    };

    let mut guard = POOL.write().await;
    if let Some(pool) = guard.as_ref().cloned() {
        return Ok(pool);
    }

    info!("Global database pool initialized");
    *guard = Some(pool.clone());
    Ok(pool)
}

#[cfg(any(test, feature = "testing"))]
pub async fn reset_pool_for_tests() -> Result<()> {
    let mut guard = POOL.write().await;
    if let Some(pool) = guard.take() {
        pool.close().await;
        info!("Global database pool reset for tests");
    }
    Ok(())
}
