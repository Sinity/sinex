use sinex_gateway::auth::Role;
use sinex_gateway::rate_limit::{RateLimitConfig, TokenRateLimiter};
use std::num::NonZeroU32;
use std::time::Duration;
use xtask::sandbox::prelude::*;

fn non_zero(value: u32, field: &str) -> TestResult<NonZeroU32> {
    NonZeroU32::new(value).ok_or_else(|| color_eyre::eyre::eyre!("{field} must be non-zero"))
}

fn cfg(rps: u32, burst: u32, idle: Duration, enabled: bool) -> TestResult<RateLimitConfig> {
    Ok(RateLimitConfig {
        readonly_rps: non_zero(rps, "readonly_rps")?,
        write_rps: non_zero(rps, "write_rps")?,
        admin_rps: non_zero(rps, "admin_rps")?,
        burst_size: non_zero(burst, "burst_size")?,
        idle_timeout: idle,
        enabled,
    })
}

#[sinex_test]
async fn test_rate_limiter_allows_initial_requests() -> TestResult<()> {
    let limiter = TokenRateLimiter::new(cfg(100, 50, Duration::from_mins(1), true)?);

    for i in 0..50 {
        assert!(
            limiter.check("test-token", Role::Write).is_ok(),
            "Request {i} should succeed within burst capacity"
        );
    }

    let mut limited = false;
    for _ in 0..200 {
        if limiter.check("test-token", Role::Write).is_err() {
            limited = true;
            break;
        }
    }
    assert!(limited, "Should eventually be rate limited after burst");
    Ok(())
}

#[sinex_test]
async fn test_rate_limiter_disabled() -> TestResult<()> {
    let limiter = TokenRateLimiter::new(cfg(1, 1, Duration::from_mins(1), false)?);

    for _ in 0..1000 {
        assert!(limiter.check("test-token", Role::Admin).is_ok());
    }
    Ok(())
}

#[sinex_test]
async fn test_separate_tokens_have_separate_limits() -> TestResult<()> {
    let limiter = TokenRateLimiter::new(cfg(5, 5, Duration::from_mins(1), true)?);

    for _ in 0..20 {
        let _ = limiter.check("token1", Role::ReadOnly);
    }

    assert!(limiter.check("token2", Role::ReadOnly).is_ok());
    Ok(())
}

#[sinex_test]
async fn test_cleanup_removes_stale_entries() -> TestResult<()> {
    let limiter = TokenRateLimiter::new(cfg(10, 5, Duration::from_millis(1), true)?);

    limiter.check("token1", Role::ReadOnly).ok();
    limiter.check("token2", Role::Write).ok();
    assert_eq!(limiter.token_count(), 2);

    tokio::time::sleep(Duration::from_millis(10)).await;

    limiter.cleanup_stale();
    assert_eq!(limiter.token_count(), 0);
    Ok(())
}

#[sinex_test]
async fn test_per_role_buckets_independent_for_same_token() -> TestResult<()> {
    // Admin gets 1 RPS / burst 1. ReadOnly gets 100 RPS / burst 100.
    let config = RateLimitConfig {
        readonly_rps: non_zero(100, "readonly_rps")?,
        write_rps: non_zero(100, "write_rps")?,
        admin_rps: non_zero(1, "admin_rps")?,
        burst_size: non_zero(1, "burst_size")?,
        idle_timeout: Duration::from_mins(1),
        enabled: true,
    };
    let limiter = TokenRateLimiter::new(config);

    // Admin bucket exhausts after one request.
    assert!(limiter.check("token", Role::Admin).is_ok());
    assert!(limiter.check("token", Role::Admin).is_err());

    // ReadOnly bucket on the same token is independent.
    assert!(limiter.check("token", Role::ReadOnly).is_ok());
    Ok(())
}
