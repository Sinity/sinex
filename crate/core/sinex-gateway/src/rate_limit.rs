#![doc = include_str!("../docs/rate_limit.md")]
#![allow(clippy::expect_used)] // All expects are on compile-time NonZeroU32 constants

use crate::auth::Role;
use crate::config::GatewayConfig;
use dashmap::DashMap;
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::debug;

/// Configuration for per-token rate limiting
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per second per `ReadOnly`-role token
    pub readonly_rps: NonZeroU32,
    /// Maximum requests per second per `Write`-role token
    pub write_rps: NonZeroU32,
    /// Maximum requests per second per `Admin`-role token
    pub admin_rps: NonZeroU32,
    /// Burst capacity (additional requests allowed in a burst)
    pub burst_size: NonZeroU32,
    /// How long to keep idle token limiters before eviction
    pub idle_timeout: Duration,
    /// Whether rate limiting is enabled
    pub enabled: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            readonly_rps: NonZeroU32::new(200).expect("200 is a valid NonZero value"),
            write_rps: NonZeroU32::new(100).expect("100 is a valid NonZero value"),
            admin_rps: NonZeroU32::new(50).expect("50 is a valid NonZero value"),
            burst_size: NonZeroU32::new(50).expect("50 is a valid NonZero value"),
            idle_timeout: Duration::from_hours(1), // 1 hour
            enabled: true,
        }
    }
}

impl RateLimitConfig {
    #[must_use]
    pub fn from_gateway_config(config: &GatewayConfig) -> Self {
        Self {
            readonly_rps: config.rate_limit_readonly_rps(),
            write_rps: config.rate_limit_write_rps(),
            admin_rps: config.rate_limit_admin_rps(),
            burst_size: config.rate_limit_burst(),
            idle_timeout: Duration::from_secs(config.rpc_rate_limit_idle_timeout_secs),
            enabled: config.rpc_rate_limit_enabled,
        }
    }

    fn rps_for_role(&self, role: Role) -> NonZeroU32 {
        match role {
            Role::ReadOnly => self.readonly_rps,
            Role::Write => self.write_rps,
            Role::Admin => self.admin_rps,
        }
    }
}

/// Entry for a token's rate limiter with last-access tracking
struct TokenEntry {
    limiter: RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    last_access: Instant,
}

/// Per-(token, role) rate limiter
///
/// Maintains a separate rate limiter for each (authentication-token, role) pair,
/// so distinct roles draw from independent buckets even if a token were ever
/// presented under different role suffixes. Stale entries are evicted on a
/// background cleanup task.
pub struct TokenRateLimiter {
    limiters: DashMap<(String, Role), TokenEntry>,
    config: RateLimitConfig,
}

impl TokenRateLimiter {
    /// Create a new token rate limiter with the given configuration
    #[must_use]
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            limiters: DashMap::new(),
            config,
        }
    }

    /// Create a rate limiter from loaded gateway configuration.
    #[must_use]
    pub fn from_gateway_config(config: &GatewayConfig) -> Self {
        Self::new(RateLimitConfig::from_gateway_config(config))
    }

    /// Check if rate limiting is enabled
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Check if the given (token, role) is allowed to make a request.
    ///
    /// Each (token, role) pair gets its own bucket, sized by the role-specific
    /// `requests_per_second` from the gateway config. Returns `Ok(())` when
    /// allowed and `Err(())` when the bucket is empty.
    pub fn check(&self, token: &str, role: Role) -> Result<(), ()> {
        if !self.config.enabled {
            return Ok(());
        }

        let now = Instant::now();
        let rps = self.config.rps_for_role(role);

        // Get or create limiter for this (token, role) pair
        let mut entry = self
            .limiters
            .entry((token.to_string(), role))
            .or_insert_with(|| {
                let quota = Quota::per_second(rps).allow_burst(self.config.burst_size);
                TokenEntry {
                    limiter: RateLimiter::direct(quota),
                    last_access: now,
                }
            });

        // Update last access time
        entry.last_access = now;

        // Check rate limit
        if entry.limiter.check() == Ok(()) {
            Ok(())
        } else {
            debug!(
                token_prefix = &token[..8.min(token.len())],
                role = %role,
                "Rate limit exceeded"
            );
            Err(())
        }
    }

    /// Clean up stale token entries that haven't been accessed recently
    ///
    /// Call this periodically (e.g., every 10 minutes) to prevent memory bloat.
    pub fn cleanup_stale(&self) {
        let now = Instant::now();
        let threshold = self.config.idle_timeout;
        let mut removed = 0;

        self.limiters.retain(|_, entry| {
            let keep = now.duration_since(entry.last_access) < threshold;
            if !keep {
                removed += 1;
            }
            keep
        });

        if removed > 0 {
            debug!(removed, "Cleaned up stale rate limiter entries");
        }
    }

    /// Get the current number of tracked tokens
    #[must_use]
    pub fn token_count(&self) -> usize {
        self.limiters.len()
    }

    /// Spawn a background cleanup task
    #[must_use]
    pub fn spawn_cleanup_task(
        self: Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_mins(10)); // 10 minutes
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        self.cleanup_stale();
                    }
                    shutdown_result = shutdown.changed() => {
                        if shutdown_result.is_err() {
                            debug!("Rate limiter cleanup shutdown channel dropped before explicit shutdown");
                        }
                        if shutdown_result.is_err() || *shutdown.borrow() {
                            debug!("Rate limiter cleanup task shutting down");
                            // Final cleanup before shutdown
                            self.cleanup_stale();
                            break;
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
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
}
