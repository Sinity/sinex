use super::*;
use sinex_primitives::SinexError;
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn cleanup_task_exits_when_shutdown_sender_is_dropped() -> TestResult<()> {
    let limiter = Arc::new(TokenRateLimiter::new(RateLimitConfig::default()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = limiter.spawn_cleanup_task(shutdown_rx);

    drop(shutdown_tx);

    timeout(Duration::from_secs(1), handle)
        .await
        .map_err(|_| {
            SinexError::timeout("cleanup task should exit after shutdown sender drops")
        })??;
    Ok(())
}

#[sinex_test]
async fn per_role_buckets_apply_distinct_capacity() -> TestResult<()> {
    // Build a config where ReadOnly has more headroom than Admin so we
    // can exhaust the admin bucket while readonly still passes.
    let config = RateLimitConfig {
        readonly_rps: NonZeroU32::new(100).unwrap(),
        write_rps: NonZeroU32::new(50).unwrap(),
        admin_rps: NonZeroU32::new(2).unwrap(),
        burst_size: NonZeroU32::new(2).unwrap(),
        idle_timeout: Duration::from_mins(1),
        enabled: true,
    };
    let limiter = TokenRateLimiter::new(config);
    let token = "sinex_abcdefghij";

    // Burst capacity 2 — first two admin requests should pass, the third trips.
    assert!(limiter.check(token, Role::Admin).is_ok());
    assert!(limiter.check(token, Role::Admin).is_ok());
    assert!(
        limiter.check(token, Role::Admin).is_err(),
        "third admin request should be rate limited"
    );

    // ReadOnly bucket on the same token must be independent and unaffected.
    assert!(limiter.check(token, Role::ReadOnly).is_ok());
    assert!(limiter.check(token, Role::ReadOnly).is_ok());
    Ok(())
}

#[sinex_test]
async fn rate_limiting_disabled_short_circuits() -> TestResult<()> {
    let mut config = RateLimitConfig::default();
    config.enabled = false;
    let limiter = TokenRateLimiter::new(config);
    for _ in 0..1_000 {
        assert!(limiter.check("any", Role::Admin).is_ok());
    }
    Ok(())
}

/// sinex-u269: `rpc_rate_limit_requests_per_sec` / `SINEX_API_RATE_LIMIT_REQUESTS_PER_SEC`
/// is fully wired through NixOS deployment and documented as "the in-memory
/// token bucket refill rate", but `RateLimitConfig::from_gateway_config`
/// never calls `rate_limit_requests_per_second()` -- only the per-role
/// (readonly/write/admin) fields are read. Changing the master knob has
/// zero effect on the derived limiter.
#[sinex_test]
#[ignore = "sinex-u269 open: rpc_rate_limit_requests_per_sec is never read by RateLimitConfig::from_gateway_config"]
async fn from_gateway_config_reflects_master_requests_per_sec() -> TestResult<()> {
    let mut low = GatewayConfig::default();
    low.rpc_rate_limit_requests_per_sec = 5;
    let mut high = GatewayConfig::default();
    high.rpc_rate_limit_requests_per_sec = 5_000;
    // Per-role fields identical in both configs -- only the master knob differs.

    let low_derived = RateLimitConfig::from_gateway_config(&low);
    let high_derived = RateLimitConfig::from_gateway_config(&high);

    assert_ne!(
        low_derived.readonly_rps, high_derived.readonly_rps,
        "sinex-u269: rpc_rate_limit_requests_per_sec has no effect on the derived \
         per-role rate limits -- the master knob is never read"
    );
    Ok(())
}
