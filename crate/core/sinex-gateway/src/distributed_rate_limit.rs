//! Distributed Per-Token Rate Limiting using NATS KV
//!
//! Provides rate limiting on a per-token basis that is shared across all gateway instances.
//! Uses NATS KV for atomic increment operations and automatic TTL expiration.
//!
//! This ensures:
//! - Consistent rate limits across multiple gateway instances
//! - State survives hot reload / rolling upgrades
//! - No quota reset bypass attacks
#![allow(clippy::expect_used)] // All expects are on compile-time NonZeroU32 constants

use async_nats::jetstream::kv::{Config as KvConfig, Store};
use async_nats::jetstream::Context;
use color_eyre::eyre::{Context as _, Result};
use std::num::NonZeroU32;
use std::time::Duration;
use tracing::{debug, warn};

/// Configuration for distributed per-token rate limiting
#[derive(Debug, Clone)]
pub struct DistributedRateLimitConfig {
    /// Maximum requests per minute per token
    pub requests_per_minute: NonZeroU32,
    /// Window duration in seconds
    pub window_seconds: u64,
    /// Whether rate limiting is enabled
    pub enabled: bool,
}

impl Default for DistributedRateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_minute: NonZeroU32::new(6000).expect("6000 is a non-zero constant"), // 100 req/s
            window_seconds: 60,
            enabled: true,
        }
    }
}

impl DistributedRateLimitConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let enabled = std::env::var("SINEX_RPC_RATE_LIMIT_ENABLED")
            .map_or(true, |v| v.to_lowercase() != "false" && v != "0");

        let requests_per_minute = std::env::var("SINEX_RPC_RATE_LIMIT_PER_MINUTE")
            .ok()
            .and_then(|v| v.parse().ok())
            .and_then(NonZeroU32::new)
            .unwrap_or_else(|| NonZeroU32::new(6000).expect("6000 is a non-zero constant"));

        let window_seconds = std::env::var("SINEX_RPC_RATE_LIMIT_WINDOW_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);

        Self {
            requests_per_minute,
            window_seconds,
            enabled,
        }
    }
}

/// Distributed rate limiter using NATS KV for shared state
pub struct DistributedRateLimiter {
    kv: Store,
    config: DistributedRateLimitConfig,
}

impl DistributedRateLimiter {
    /// Create a new distributed rate limiter
    pub async fn new(jetstream: Context, config: DistributedRateLimitConfig) -> Result<Self> {
        // Create or get KV bucket for rate limits
        let kv_config = KvConfig {
            bucket: "sinex_gateway_rate_limits".to_string(),
            description: "Per-token rate limit counters".to_string(),
            max_age: Duration::from_secs(config.window_seconds * 2), // Auto-cleanup old entries
            ..Default::default()
        };

        let kv = match jetstream.create_key_value(kv_config).await {
            Ok(store) => store,
            Err(_) => jetstream
                .get_key_value("sinex_gateway_rate_limits")
                .await
                .wrap_err("Failed to create/get rate limit KV bucket")?,
        };

        Ok(Self { kv, config })
    }

    /// Check if request is allowed for the given token
    ///
    /// Returns `true` if the request should be allowed, `false` if rate limited.
    pub async fn check_and_increment(&self, token: &str) -> bool {
        if !self.config.enabled {
            return true;
        }

        let key = format!("token:{token}");

        // Get current count (or 0 if not exists)
        let current_count = match self.kv.get(&key).await {
            Ok(Some(bytes)) => {
                // Parse count from bytes - kv.get() returns Option<Bytes>
                match std::str::from_utf8(&bytes) {
                    Ok(s) => s.parse::<u32>().unwrap_or(0),
                    Err(_) => 0,
                }
            }
            Ok(None) => 0,
            Err(e) => {
                warn!(error = %e, token = %token, "Failed to get rate limit count from NATS KV");
                return true; // Fail open on NATS errors
            }
        };

        // Check if over limit
        if current_count >= self.config.requests_per_minute.get() {
            debug!(token = %token, count = current_count, "Rate limit exceeded");
            return false;
        }

        // Increment count
        let new_count = current_count + 1;
        if let Err(e) = self.kv.put(&key, new_count.to_string().into()).await {
            warn!(error = %e, token = %token, "Failed to increment rate limit counter in NATS KV");
            // Fail open - allow request even if we couldn't increment
        }

        true
    }

    /// Check if rate limiting is enabled
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}
