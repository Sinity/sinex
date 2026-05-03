use sinex_gateway::rate_limit::{RateLimitConfig, TokenRateLimiter};
use std::num::NonZeroU32;
use std::time::Duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_rate_limiter_allows_initial_requests() -> TestResult<()> {
    let limiter = TokenRateLimiter::new(RateLimitConfig {
        requests_per_second: NonZeroU32::new(100).expect("100 is a valid NonZero value"),
        burst_size: NonZeroU32::new(50).expect("50 is a valid NonZero value"),
        idle_timeout: Duration::from_mins(1),
        enabled: true,
    });

    for i in 0..50 {
        assert!(
            limiter.check("test-token").is_ok(),
            "Request {i} should succeed within burst capacity"
        );
    }

    let mut limited = false;
    for _ in 0..200 {
        if limiter.check("test-token").is_err() {
            limited = true;
            break;
        }
    }
    assert!(limited, "Should eventually be rate limited after burst");
    Ok(())
}

#[sinex_test]
async fn test_rate_limiter_disabled() -> TestResult<()> {
    let limiter = TokenRateLimiter::new(RateLimitConfig {
        requests_per_second: NonZeroU32::new(1).expect("1 is a valid NonZero value"),
        burst_size: NonZeroU32::new(1).expect("1 is a valid NonZero value"),
        idle_timeout: Duration::from_mins(1),
        enabled: false,
    });

    for _ in 0..1000 {
        assert!(limiter.check("test-token").is_ok());
    }
    Ok(())
}

#[sinex_test]
async fn test_separate_tokens_have_separate_limits() -> TestResult<()> {
    let limiter = TokenRateLimiter::new(RateLimitConfig {
        requests_per_second: NonZeroU32::new(5).expect("5 is a valid NonZero value"),
        burst_size: NonZeroU32::new(5).expect("5 is a valid NonZero value"),
        idle_timeout: Duration::from_mins(1),
        enabled: true,
    });

    for _ in 0..20 {
        let _ = limiter.check("token1");
    }

    assert!(limiter.check("token2").is_ok());
    Ok(())
}

#[sinex_test]
async fn test_cleanup_removes_stale_entries() -> TestResult<()> {
    let limiter = TokenRateLimiter::new(RateLimitConfig {
        requests_per_second: NonZeroU32::new(10).expect("10 is a valid NonZero value"),
        burst_size: NonZeroU32::new(5).expect("5 is a valid NonZero value"),
        idle_timeout: Duration::from_millis(1),
        enabled: true,
    });

    limiter.check("token1").ok();
    limiter.check("token2").ok();
    assert_eq!(limiter.token_count(), 2);

    tokio::time::sleep(Duration::from_millis(10)).await;

    limiter.cleanup_stale();
    assert_eq!(limiter.token_count(), 0);
    Ok(())
}
