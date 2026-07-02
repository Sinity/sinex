
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
