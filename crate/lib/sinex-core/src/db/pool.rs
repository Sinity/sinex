#![doc = include_str!("../../doc/pool.md")]

use crate::{DbPool, PoolConfig};
use color_eyre::eyre::Result;
use once_cell::sync::OnceCell;
use tracing::info;

static POOL: OnceCell<DbPool> = OnceCell::new();

/// Get or create the global database pool with default configuration
pub async fn get_pool() -> Result<&'static DbPool> {
    get_pool_with_config(None).await
}

/// Get or create the global database pool with custom configuration
pub async fn get_pool_with_config(config: Option<PoolConfig>) -> Result<&'static DbPool> {
    // Check if pool is already initialized
    if let Some(pool) = POOL.get() {
        return Ok(pool);
    }

    // Initialize the pool
    let pool = match config {
        Some(config) => crate::create_pool_with_config_strict(&config).await?,
        None => crate::create_pool_strict().await?,
    };

    info!("Global database pool initialized");

    // Try to set the pool, using get_or_init to handle race condition
    Ok(POOL.get_or_init(|| pool))
}
