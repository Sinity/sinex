#![doc = include_str!("../docs/rate_limit.md")]
#![allow(clippy::expect_used)] // All expects are on compile-time NonZeroU32 constants

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
    /// Maximum requests per second per token
    pub requests_per_second: NonZeroU32,
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
            requests_per_second: NonZeroU32::new(100).expect("100 is a valid NonZero value"),
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
            requests_per_second: config.rate_limit_requests_per_second(),
            burst_size: config.rate_limit_burst(),
            idle_timeout: Duration::from_secs(config.rpc_rate_limit_idle_timeout_secs),
            enabled: config.rpc_rate_limit_enabled,
        }
    }
}

/// Entry for a token's rate limiter with last-access tracking
struct TokenEntry {
    limiter: RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    last_access: Instant,
}

/// Per-token rate limiter
///
/// Maintains a separate rate limiter for each authentication token,
/// with automatic cleanup of stale entries.
pub struct TokenRateLimiter {
    limiters: DashMap<String, TokenEntry>,
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

    /// Check if the given token is allowed to make a request
    ///
    /// Returns `Ok(())` if allowed, `Err(())` if rate limited
    pub fn check(&self, token: &str) -> Result<(), ()> {
        if !self.config.enabled {
            return Ok(());
        }

        let now = Instant::now();

        // Get or create limiter for this token
        let mut entry = self.limiters.entry(token.to_string()).or_insert_with(|| {
            let quota = Quota::per_second(self.config.requests_per_second)
                .allow_burst(self.config.burst_size);
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
                color_eyre::eyre::eyre!("cleanup task should exit after shutdown sender drops")
            })??;
        Ok(())
    }
}
