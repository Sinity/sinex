use crate::DbPool;
use anyhow::Result;
use once_cell::sync::OnceCell;
use std::env;
use tracing::info;

static POOL: OnceCell<DbPool> = OnceCell::new();

/// Get or create the global database pool
pub async fn get_pool() -> Result<&'static DbPool> {
    if let Some(pool) = POOL.get() {
        return Ok(pool);
    }

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = crate::create_pool(&database_url).await?;

    POOL.set(pool)
        .map_err(|_| anyhow::anyhow!("Failed to set global pool"))?;

    info!("Global database pool initialized");
    Ok(POOL.get().unwrap())
}
