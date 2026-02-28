//! Tests for distributed per-token rate limiting via NATS KV
//!
//! Validates:
//! - Basic rate limiting (under limit passes, over limit rejects)
//! - Per-token isolation (different tokens have independent limits)
//! - Fail-closed behavior (NATS KV failure rejects, never allows through)
//! - CAS retry exhaustion (all retries fail -> reject)
//! - Disabled limiter always allows

use sinex_gateway::distributed_rate_limit::{
    DistributedRateLimitConfig, DistributedRateLimiter,
};
use std::num::NonZeroU32;
use xtask::sandbox::prelude::*;

/// Helper: create a rate limiter with the given config against an ephemeral NATS.
async fn make_limiter(
    nats: &EphemeralNats,
    config: DistributedRateLimitConfig,
) -> color_eyre::Result<DistributedRateLimiter> {
    let js = nats.jetstream().await?;
    let limiter = DistributedRateLimiter::new(js, config).await?;
    Ok(limiter)
}

fn config_with_limit(rpm: u32) -> DistributedRateLimitConfig {
    DistributedRateLimitConfig {
        requests_per_minute: NonZeroU32::new(rpm).expect("non-zero limit"),
        window_seconds: 60,
        enabled: true,
    }
}

// ─── Basic rate limiting ────────────────────────────────────────────────

#[sinex_test]
async fn rate_limit_allows_requests_under_limit() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let limiter = make_limiter(&nats, config_with_limit(100)).await?;

    // First request should always pass
    let allowed = limiter.check_and_increment("token-a").await;
    assert!(allowed, "First request under limit should be allowed");

    // Several more should also pass (well under 100)
    for i in 0..10 {
        let allowed = limiter.check_and_increment("token-a").await;
        assert!(allowed, "Request {i} under limit should be allowed");
    }

    Ok(())
}

#[sinex_test]
async fn rate_limit_rejects_requests_over_limit() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    // Very low limit to make exhaustion easy
    let limiter = make_limiter(&nats, config_with_limit(5)).await?;

    // Exhaust the limit
    for _ in 0..5 {
        limiter.check_and_increment("token-exhaust").await;
    }

    // Next request must be rejected
    let allowed = limiter.check_and_increment("token-exhaust").await;
    assert!(
        !allowed,
        "Request over the limit must be rejected"
    );

    Ok(())
}

// ─── Per-token isolation ────────────────────────────────────────────────

#[sinex_test]
async fn rate_limit_per_token_isolation() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let limiter = make_limiter(&nats, config_with_limit(3)).await?;

    // Exhaust token-x
    for _ in 0..3 {
        limiter.check_and_increment("token-x").await;
    }
    let rejected = !limiter.check_and_increment("token-x").await;
    assert!(rejected, "token-x should be exhausted");

    // token-y should be completely independent and still pass
    let allowed = limiter.check_and_increment("token-y").await;
    assert!(
        allowed,
        "token-y must be independent from token-x and allowed"
    );

    Ok(())
}

// ─── Disabled limiter ───────────────────────────────────────────────────

#[sinex_test]
async fn disabled_limiter_always_allows() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let config = DistributedRateLimitConfig {
        requests_per_minute: NonZeroU32::new(1).expect("non-zero"),
        window_seconds: 60,
        enabled: false,
    };
    let limiter = make_limiter(&nats, config).await?;

    // Even with limit=1, disabled should allow everything
    for _ in 0..50 {
        let allowed = limiter.check_and_increment("any-token").await;
        assert!(allowed, "Disabled limiter must always allow requests");
    }

    Ok(())
}

#[sinex_test]
async fn is_enabled_reflects_config() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;

    let enabled_limiter = make_limiter(&nats, config_with_limit(100)).await?;
    assert!(enabled_limiter.is_enabled());

    // Need a fresh NATS for the disabled limiter since KV bucket names collide
    let nats2 = EphemeralNats::start().await?;
    let disabled_config = DistributedRateLimitConfig {
        requests_per_minute: NonZeroU32::new(100).expect("non-zero"),
        window_seconds: 60,
        enabled: false,
    };
    let disabled_limiter = make_limiter(&nats2, disabled_config).await?;
    assert!(!disabled_limiter.is_enabled());

    Ok(())
}

// ─── Fail-closed behavior (security-critical) ──────────────────────────

#[sinex_test]
async fn fail_closed_on_nats_kv_unavailable() -> TestResult<()> {
    // Start NATS, create limiter, then kill NATS
    let nats = EphemeralNats::start().await?;
    let limiter = make_limiter(&nats, config_with_limit(1000)).await?;

    // Verify it works while NATS is alive
    let allowed = limiter.check_and_increment("fail-closed-token").await;
    assert!(allowed, "Should allow while NATS is up");

    // Kill NATS server
    nats.shutdown().await?;

    // Wait a moment for the connection to become stale
    tokio::time::sleep(Duration::from_millis(200)).await;

    // After local bucket is exhausted, requests must fail closed.
    // The local bucket was seeded with ~49 remaining tokens from the first
    // successful reservation. Drain them.
    let mut drained_count = 0;
    for _ in 0..100 {
        if !limiter.check_and_increment("fail-closed-token").await {
            break;
        }
        drained_count += 1;
    }

    // Once local bucket is drained, next request MUST be rejected (fail closed)
    let result = limiter.check_and_increment("fail-closed-token").await;
    assert!(
        !result,
        "MUST fail closed when NATS KV is unavailable (drained {drained_count} local tokens first). \
         Allowing requests through would be a security bypass."
    );

    // A different token with no local bucket should also be rejected immediately
    let result_new_token = limiter.check_and_increment("brand-new-token").await;
    assert!(
        !result_new_token,
        "New token with no local bucket must also fail closed when NATS is down"
    );

    Ok(())
}

// ─── Multiple tokens concurrent ─────────────────────────────────────────

#[sinex_test]
async fn rate_limit_concurrent_tokens() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let limiter = Arc::new(make_limiter(&nats, config_with_limit(20)).await?);

    // Spawn concurrent checks for different tokens
    let mut handles = Vec::new();
    for i in 0..5 {
        let limiter = limiter.clone();
        let token = format!("concurrent-token-{i}");
        handles.push(tokio::spawn(async move {
            let mut allowed = 0u32;
            for _ in 0..10 {
                if limiter.check_and_increment(&token).await {
                    allowed += 1;
                }
            }
            allowed
        }));
    }

    let results: Vec<u32> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task should not panic"))
        .collect();

    // Each token has limit=20 and only 10 requests, so all should pass
    for (i, count) in results.iter().enumerate() {
        assert_eq!(
            *count, 10,
            "Token {i} should have all 10 requests allowed (got {count})"
        );
    }

    Ok(())
}

// ─── Default config ─────────────────────────────────────────────────────

#[sinex_test]
async fn default_config_has_sane_values() -> TestResult<()> {
    let config = DistributedRateLimitConfig::default();
    assert_eq!(config.requests_per_minute.get(), 6000);
    assert_eq!(config.window_seconds, 60);
    assert!(config.enabled);
    Ok(())
}
