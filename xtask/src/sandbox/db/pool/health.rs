//! Pool health checks, reset, prime, and benchmarks.

use crate::sandbox::prelude::*;
use sqlx::postgres::PgConnection;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::config::PoolConfig;
use super::metrics::POOL_METRICS;
use super::provisioning::{connect_admin_with_retry, ensure_pool_database_exists};
use super::stats::PoolStats;
use super::{DatabasePool, POOL};

/// Pool health report
#[derive(Debug, Clone)]
pub struct PoolHealthReport {
    pub total_slots: usize,
    pub healthy_slots: usize,
    pub unhealthy_slots: usize,
    pub stats: PoolStats,
}

/// Health check for the entire pool
pub async fn check_pool_health() -> TestResult<PoolHealthReport> {
    let pool_lock = POOL.lock().await;

    if let Some(pool) = pool_lock.as_ref() {
        let mut healthy_slots = 0;
        let mut unhealthy_slots = 0;
        let mut total_slots = 0;

        for slot in &pool.slots {
            total_slots += 1;

            if slot.in_use.load(Ordering::Acquire) {
                // Skip in-use slots
                continue;
            }

            // Try to connect to this slot's database
            match sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(2))
                .connect(&slot.url)
                .await
            {
                Ok(pool) => {
                    match sqlx::query("SELECT 1").fetch_one(&pool).await {
                        Ok(_) => healthy_slots += 1,
                        Err(_) => unhealthy_slots += 1,
                    }
                    pool.close().await;
                }
                Err(_) => unhealthy_slots += 1,
            }
        }

        Ok(PoolHealthReport {
            total_slots,
            healthy_slots,
            unhealthy_slots,
            stats: POOL_METRICS.get_stats(),
        })
    } else {
        Ok(PoolHealthReport {
            total_slots: 0,
            healthy_slots: 0,
            unhealthy_slots: 0,
            stats: POOL_METRICS.get_stats(),
        })
    }
}

/// Current number of slots available in the database pool.
pub async fn pool_slot_count() -> usize {
    let pool_lock = POOL.lock().await;
    pool_lock.as_ref().map_or(0, |pool| pool.slots.len())
}

/// Acquire a connection to the Postgres admin database with retry logic.
pub async fn acquire_admin_connection() -> TestResult<PgConnection> {
    let config = PoolConfig::default();
    connect_admin_with_retry(&config.admin_url).await
}

/// Emergency pool reset function (for testing/debugging)
pub async fn reset_pool() -> TestResult<()> {
    let mut pool_lock = POOL.lock().await;

    if let Some(pool) = pool_lock.take() {
        // Close all connections
        for slot in &pool.slots {
            let pool_to_close = {
                let mut pool_opt = slot.pool.lock();
                pool_opt.take()
            };

            if let Some(pool) = pool_to_close {
                pool.close().await;
            }
        }
    }

    // Force reinitialize on next acquisition
    *pool_lock = None;

    Ok(())
}

/// Prime the pool by ensuring the template and all pool databases exist.
pub async fn prime_pool() -> TestResult<()> {
    let (pool, initialized_now) = {
        let mut pool_lock = POOL.lock().await;
        if let Some(pool) = pool_lock.as_ref().cloned() {
            (pool, false)
        } else {
            let config = PoolConfig::default();
            let pool = Arc::new(DatabasePool::new(config, true).await?);
            *pool_lock = Some(pool.clone());
            (pool, true)
        }
    };

    // DatabasePool::new(..., force_eager = true) already provisions all slots.
    // Avoid re-running per-slot provisioning immediately after eager init.
    if initialized_now {
        return Ok(());
    }

    for slot in &pool.slots {
        ensure_pool_database_exists(&slot.name, &slot.url).await?;
    }

    Ok(())
}

/// Initialize pool with custom configuration (for testing)
async fn _init_pool_with_config(config: PoolConfig) -> TestResult<()> {
    let mut pool_lock = POOL.lock().await;
    let pool = Arc::new(DatabasePool::new(config, true).await?);
    *pool_lock = Some(pool);
    Ok(())
}

/// Get pool configuration (for debugging)
fn _get_pool_config() -> PoolConfig {
    PoolConfig::default()
}

// ── Benchmarks ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod benches {

    use xtask_macros::*;

    /// Benchmark database acquisition from pool
    ///
    /// This measures the time to acquire a clean database from the pool,
    /// including advisory lock acquisition and cleanup verification.
    #[sinex_bench]
    async fn bench_acquire_database() -> TestResult<()> {
        let db = super::super::acquire_test_database().await?;
        // Database is automatically returned on drop
        drop(db);
        Ok(())
    }

    /// Benchmark concurrent database acquisition
    ///
    /// Measures contention and performance when multiple tasks
    /// try to acquire databases simultaneously.
    #[sinex_bench(args = [2, 4, 8, 16])]
    async fn bench_concurrent_acquisition(arg: usize) -> TestResult<()> {
        let concurrency = arg;
        let handles: Vec<_> = (0..concurrency)
            .map(|_| {
                tokio::spawn(async move {
                    super::super::acquire_test_database().await.map_err(|e| {
                        tracing::error!("Benchmark database acquisition failed: {}", e);
                        e
                    })
                })
            })
            .collect();

        // Wait for all to complete
        for handle in handles {
            let db = handle.await?;
            drop(db);
        }
        Ok(())
    }

    /// Benchmark template database operations
    #[sinex_bench]
    async fn bench_ensure_template_database() -> TestResult<()> {
        let config = super::PoolConfig::default();
        // This should be fast after first run (cached)
        let guard = super::super::template::ensure_template_database(
            &config.admin_url,
            &config.base_url,
            config.slot_max_connections,
        )
        .await?;
        guard.release().await?;
        Ok(())
    }

    /// Benchmark pool health check
    #[sinex_bench]
    async fn bench_pool_health_check() -> TestResult<()> {
        // Ensure pool is initialized
        let _ = super::super::acquire_test_database().await?;

        super::check_pool_health().await?;
        Ok(())
    }
}
