#![doc = include_str!("../docs/rate_limit.md")]
#![allow(clippy::expect_used)] // All expects are on compile-time NonZeroU32 constants

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
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let enabled = std::env::var("SINEX_RPC_RATE_LIMIT_ENABLED")
            .map_or(true, |v| v.to_lowercase() != "false" && v != "0");

        let requests_per_second = std::env::var("SINEX_RPC_RATE_LIMIT_REQUESTS_PER_SEC")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .and_then(NonZeroU32::new)
            .unwrap_or_else(|| NonZeroU32::new(100).expect("100 is a valid NonZero value"));

        let burst_size = std::env::var("SINEX_RPC_RATE_LIMIT_BURST")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .and_then(NonZeroU32::new)
            .unwrap_or_else(|| NonZeroU32::new(50).expect("50 is a valid NonZero value"));

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
    #[must_use]
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            limiters: DashMap::new(),
            config,
        }
    }

    /// Create a rate limiter from environment configuration
    #[must_use]
    pub fn from_env() -> Self {
        Self::new(RateLimitConfig::from_env())
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
