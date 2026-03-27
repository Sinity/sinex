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

use crate::config::GatewayConfig;
use async_nats::jetstream::Context;
use async_nats::jetstream::kv::{Config as KvConfig, Store};
use color_eyre::eyre::{Context as _, Result};
use sinex_primitives::nats::create_or_open_kv_store;
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
    #[must_use]
    pub fn from_gateway_config(config: &GatewayConfig) -> Self {
        Self {
            requests_per_minute: config.rate_limit_per_minute(),
            window_seconds: config.rpc_rate_limit_window_secs,
            enabled: config.rpc_rate_limit_enabled,
        }
    }
}

use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Default number of tokens to reserve from NATS in one batch
const RESERVATION_BATCH_SIZE: u32 = 50;

/// Evict exhausted local buckets every N calls to bound `DashMap` memory.
/// Eviction is safe: a token with a zero local bucket will simply re-hit
/// NATS KV on its next request, where the global counter is authoritative.
const BUCKET_EVICTION_INTERVAL: u64 = 10_000;

/// Distributed rate limiter using NATS KV for shared state
pub struct DistributedRateLimiter {
    kv: Store,
    config: DistributedRateLimitConfig,
    /// Local reservation buckets keyed by hashed token identifiers.
    local_buckets: DashMap<String, Arc<AtomicU32>>,
    /// Call counter used to trigger periodic eviction of exhausted buckets
    call_count: AtomicU64,
}

#[derive(Debug, Clone)]
struct TokenIdentity {
    hashed_token: String,
    kv_key: String,
    fingerprint: String,
}

fn token_identity(token: &str) -> TokenIdentity {
    let hashed_token = blake3::hash(token.as_bytes()).to_hex().to_string();
    TokenIdentity {
        kv_key: format!("token.{hashed_token}"),
        fingerprint: hashed_token[..16].to_string(),
        hashed_token,
    }
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

        let kv = create_or_open_kv_store(&jetstream, kv_config)
            .await
            .wrap_err("Failed to create/get rate limit KV bucket")?;

        Ok(Self {
            kv,
            config,
            local_buckets: DashMap::new(),
            call_count: AtomicU64::new(0),
        })
    }

    /// Check if request is allowed for the given token
    ///
    /// Uses a local reservation system to batch NATS operations:
    /// 1. Consumes from local reservation if available.
    /// 2. If empty, attempts to reserve a batch (50) from NATS KV.
    /// 3. Updates local reservation if successful.
    pub async fn check_and_increment(&self, token: &str) -> bool {
        if !self.config.enabled {
            return true;
        }

        let token_identity = token_identity(token);

        // Periodically evict exhausted local buckets to prevent unbounded DashMap growth.
        // Tokens with zero local capacity re-hit NATS KV on the next call, which is correct.
        let count = self.call_count.fetch_add(1, Ordering::Relaxed);
        if count.is_multiple_of(BUCKET_EVICTION_INTERVAL) && count > 0 {
            self.local_buckets
                .retain(|_, v| v.load(Ordering::Relaxed) > 0);
        }

        // 1. Get local bucket (lock-free access via Arc)
        let bucket = self
            .local_buckets
            .entry(token_identity.hashed_token.clone())
            .or_insert_with(|| Arc::new(AtomicU32::new(0)))
            .clone();

        'consume: loop {
            // 2. Try to consume locally.
            loop {
                let current = bucket.load(Ordering::Relaxed);
                if current > 0 {
                    if bucket
                        .compare_exchange_weak(
                            current,
                            current - 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        // Success: consumed 1 local token
                        return true;
                    }
                } else {
                    break; // Local bucket empty, fall through to NATS reservation
                }
            }

            // 3. Replenish from NATS (with CAS loop).
            // Never use the raw bearer token as a NATS KV key or log field.
            let key = token_identity.kv_key.clone();
            let limit = self.config.requests_per_minute.get();
            let batch_size = RESERVATION_BATCH_SIZE;

            // Exponential backoff for high contention CAS loops
            let mut backoff = Duration::from_millis(5);

            for attempt in 0..5 {
                // Another contender may have successfully refilled the shared local bucket
                // since this caller last checked it. Re-check before touching the global
                // counter so same-process stampedes can consume the reservation.
                let current = bucket.load(Ordering::Relaxed);
                if current > 0 {
                    continue 'consume;
                }

                // Get current global count
                let (entry_value, revision) = match self.kv.entry(&key).await {
                    Ok(Some(entry)) => {
                        let val = if let Some(v) = std::str::from_utf8(&entry.value)
                            .ok()
                            .and_then(|s| s.parse::<u32>().ok())
                        {
                            v
                        } else {
                            warn!(
                                token_fingerprint = %token_identity.fingerprint,
                                raw = ?entry.value,
                                "Corrupt rate limit counter in NATS KV; failing closed"
                            );
                            return false;
                        };
                        (val, entry.revision)
                    }
                    Ok(None) => (0, 0), // Key doesn't exist yet, revision 0 signals create
                    Err(e) => {
                        warn!(
                            error = %e,
                            token_fingerprint = %token_identity.fingerprint,
                            "NATS KV read failed; failing closed (rate limit enforced)"
                        );
                        return false; // Fail closed: NATS outage should not bypass rate limits
                    }
                };

                // Check hard limit
                if entry_value >= limit {
                    if bucket.load(Ordering::Relaxed) > 0 {
                        continue 'consume;
                    }
                    debug!(
                        token_fingerprint = %token_identity.fingerprint,
                        used = entry_value,
                        limit = limit,
                        "Rate limit exceeded (global)"
                    );
                    // Cache 0 locally to prevent hammering this loop
                    bucket.store(0, Ordering::Relaxed);
                    return false;
                }

                // Calculate safe reservation amount
                let available = limit - entry_value;
                let to_reserve = std::cmp::min(available, batch_size);

                // CAS: use create() for new keys, update() for existing ones
                let new_value = entry_value + to_reserve;
                let cas_result: std::result::Result<u64, sinex_primitives::SinexError> =
                    if revision == 0 {
                        // Key doesn't exist yet — create fails if another contender won
                        // the initial reservation race, forcing a retry against the
                        // authoritative revision instead of silently oversubscribing.
                        self.kv
                            .create(&key, new_value.to_string().into())
                            .await
                            .map_err(|e| {
                                sinex_primitives::SinexError::kv("rate limit CAS create failed")
                                    .with_context("key", &key)
                                    .with_source(e)
                            })
                    } else {
                        // Key exists — CAS update against known revision
                        self.kv
                            .update(&key, new_value.to_string().into(), revision)
                            .await
                            .map_err(|e| {
                                sinex_primitives::SinexError::kv("rate limit CAS update failed")
                                    .with_context("key", &key)
                                    .with_context("revision", revision)
                                    .with_source(e)
                            })
                    };

                match cas_result {
                    Ok(_) => {
                        // Success!
                        // We consume 1 for *this* request immediately, store the rest
                        bucket.store(to_reserve - 1, Ordering::Relaxed);
                        debug!(
                            token_fingerprint = %token_identity.fingerprint,
                            reserved = to_reserve,
                            new_global = new_value,
                            "Refilled local rate limit bucket"
                        );
                        return true;
                    }
                    Err(e) => {
                        // Conflict or error — retry with backoff
                        debug!(
                            error = %e,
                            attempt,
                            "CAS failure/conflict reserving tokens; retrying"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = std::cmp::min(backoff * 2, Duration::from_millis(100));
                        continue;
                    }
                }
            }

            warn!(
                token_fingerprint = %token_identity.fingerprint,
                "Failed to reserve rate limit tokens after retries; failing closed"
            );
            return false; // Fail closed on persistent CAS failure
        }
    }

    /// Check if rate limiting is enabled
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}
