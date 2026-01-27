#![doc = include_str!("../docs/rate_limit.md")]

use dashmap::DashMap;
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
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
            requests_per_second: NonZeroU32::new(100).unwrap(),
            burst_size: NonZeroU32::new(50).unwrap(),
            idle_timeout: Duration::from_secs(3600), // 1 hour
            enabled: true,
        }
    }
}

impl RateLimitConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let enabled = std::env::var("SINEX_RPC_RATE_LIMIT_ENABLED")
            .map(|v| v.to_lowercase() != "false" && v != "0")
            .unwrap_or(true);

        let requests_per_second = std::env::var("SINEX_RPC_RATE_LIMIT_REQUESTS_PER_SEC")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .and_then(NonZeroU32::new)
            .unwrap_or_else(|| NonZeroU32::new(100).unwrap());

        let burst_size = std::env::var("SINEX_RPC_RATE_LIMIT_BURST")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .and_then(NonZeroU32::new)
            .unwrap_or_else(|| NonZeroU32::new(50).unwrap());

        let idle_timeout_secs = std::env::var("SINEX_RPC_RATE_LIMIT_IDLE_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3600);

        Self {
            requests_per_second,
            burst_size,
            idle_timeout: Duration::from_secs(idle_timeout_secs),
            enabled,
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
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            limiters: DashMap::new(),
            config,
        }
    }

    /// Create a rate limiter from environment configuration
    pub fn from_env() -> Self {
        Self::new(RateLimitConfig::from_env())
    }

    /// Check if rate limiting is enabled
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
        match entry.limiter.check() {
            Ok(()) => Ok(()),
            Err(_) => {
                debug!(
                    token_prefix = &token[..8.min(token.len())],
                    "Rate limit exceeded"
                );
                Err(())
            }
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
    pub fn token_count(&self) -> usize {
        self.limiters.len()
    }

    /// Spawn a background cleanup task
    pub fn spawn_cleanup_task(
        self: Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(600)); // 10 minutes
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        self.cleanup_stale();
                    }
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
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

    #[test]
    fn test_rate_limiter_allows_initial_requests() {
        // With burst_size=50 and requests_per_second=100, initial burst allows
        // consuming up to burst capacity quickly before rate limiting kicks in
        let limiter = TokenRateLimiter::new(RateLimitConfig {
            requests_per_second: NonZeroU32::new(100).unwrap(),
            burst_size: NonZeroU32::new(50).unwrap(),
            idle_timeout: Duration::from_secs(60),
            enabled: true,
        });

        // Burst capacity (50) requests should succeed immediately
        for i in 0..50 {
            assert!(
                limiter.check("test-token").is_ok(),
                "Request {} should succeed within burst capacity",
                i
            );
        }

        // After exhausting burst, should eventually be rate limited
        let mut limited = false;
        for _ in 0..200 {
            if limiter.check("test-token").is_err() {
                limited = true;
                break;
            }
        }
        assert!(limited, "Should eventually be rate limited after burst");
    }

    #[test]
    fn test_rate_limiter_disabled() {
        let limiter = TokenRateLimiter::new(RateLimitConfig {
            requests_per_second: NonZeroU32::new(1).unwrap(),
            burst_size: NonZeroU32::new(1).unwrap(),
            idle_timeout: Duration::from_secs(60),
            enabled: false,
        });

        // Should never rate limit when disabled
        for _ in 0..1000 {
            assert!(limiter.check("test-token").is_ok());
        }
    }

    #[test]
    fn test_separate_tokens_have_separate_limits() {
        let limiter = TokenRateLimiter::new(RateLimitConfig {
            requests_per_second: NonZeroU32::new(5).unwrap(),
            burst_size: NonZeroU32::new(5).unwrap(),
            idle_timeout: Duration::from_secs(60),
            enabled: true,
        });

        // Exhaust token1's limit
        for _ in 0..20 {
            let _ = limiter.check("token1");
        }

        // token2 should still be allowed
        assert!(limiter.check("token2").is_ok());
    }

    #[test]
    fn test_cleanup_removes_stale_entries() {
        let limiter = TokenRateLimiter::new(RateLimitConfig {
            requests_per_second: NonZeroU32::new(10).unwrap(),
            burst_size: NonZeroU32::new(5).unwrap(),
            idle_timeout: Duration::from_millis(1), // Very short for testing
            enabled: true,
        });

        // Create some entries
        limiter.check("token1").ok();
        limiter.check("token2").ok();
        assert_eq!(limiter.token_count(), 2);

        // Wait for them to become stale
        std::thread::sleep(Duration::from_millis(10));

        // Cleanup should remove them
        limiter.cleanup_stale();
        assert_eq!(limiter.token_count(), 0);
    }
}
