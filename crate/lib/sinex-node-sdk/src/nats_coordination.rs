//! NATS-based coordination for Edge Mode
//!
//! This module provides coordination primitives (Leader Election, Distributed Locks)
//! using NATS JetStream Key-Value store (KV) instead of PostgreSQL advisory locks.
//!
//! # Architecture
//!
//! - **Leader Election**: Uses KV optimistic concurrency (CAS) to acquire a "lease"
//!   key with a TTL.
//! - **Resource Locking**: Maps resource IDs to KV keys.
//! - **Safety**: Relies on NATS JetStream guarantees.
//!
//! # Status
//!
//! Experimental / Prototype. To be integrated into `NodeCoordination`.

use crate::{NodeError, NodeResult};
use async_nats::jetstream::kv::{Operation, Store};
use serde::{Deserialize, Serialize};
use sinex_core::types::utils::CoordinationPrimitive;
use sinex_core::types::Seconds;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Configuration for NATS coordination
#[derive(Debug, Clone)]
pub struct NatsCoordinationConfig {
    pub bucket: String,
    pub lease_ttl_secs: Seconds,
}

impl Default for NatsCoordinationConfig {
    fn default() -> Self {
        Self {
            bucket: "sinex_coordination".to_string(),
            lease_ttl_secs: Seconds::from_secs(15),
        }
    }
}

/// NATS-based Lease Manager
pub struct NatsLeaseManager {
    kv_store: Store,
    config: NatsCoordinationConfig,
    client_id: String,
    lease_failures: CoordinationPrimitive,
}

#[derive(Serialize, Deserialize)]
struct LeaseValue {
    holder: String,
    acquired_at: i64,
}

impl NatsLeaseManager {
    pub async fn new(
        js: async_nats::jetstream::Context,
        config: NatsCoordinationConfig,
        client_id: String,
    ) -> NodeResult<Self> {
        let kv_store = match js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: config.bucket.clone(),
                ttl: Duration::from_secs(config.lease_ttl_secs.as_secs()),
                ..Default::default()
            })
            .await
        {
            Ok(store) => store,
            Err(create_err) => {
                js.get_key_value(&config.bucket)
                    .await
                    .map_err(|e| {
                        NodeError::Infrastructure(format!(
                            "Failed to init NATS KV (create: {create_err}, open: {e})"
                        ))
                    })?
            }
        };

        Ok(Self {
            kv_store,
            config,
            client_id,
            lease_failures: CoordinationPrimitive::event_counter(0, "nats_lease_failures"),
        })
    }

    /// Attempt to acquire leadership for a given group/resource
    pub async fn acquire_leadership(&self, resource_id: &str) -> NodeResult<bool> {
        let key = format!("leader.{}", resource_id);
        let now = chrono::Utc::now().timestamp_millis();

        // 1. Check existing
        let entry = self.kv_store.entry(&key).await.map_err(|e| {
            self.record_lease_failure("read_entry", &e);
            NodeError::Infrastructure(format!("KV read error: {}", e))
        })?;

        let value = serde_json::to_vec(&LeaseValue {
            holder: self.client_id.clone(),
            acquired_at: now,
        })
        .unwrap();

        match entry {
            Some(e) => {
                // Already held?
                // Note: NATS TTL handles expiration naturally. If key exists, someone holds it.
                // We could check if it's US and extend it.
                if let Ok(lease) = serde_json::from_slice::<LeaseValue>(&e.value) {
                    if lease.holder == self.client_id {
                        // Refresh lease
                        match self.kv_store.update(&key, value.into(), e.revision).await {
                            Ok(_) => Ok(true),
                            Err(err) => {
                                self.record_lease_failure("refresh", &err);
                                Ok(false)
                            }
                        }
                    } else {
                        // Held by someone else
                        Ok(false)
                    }
                } else {
                    self.record_lease_failure("decode_entry", "invalid lease payload");
                    Ok(false)
                }
            }
            None => {
                // Try create
                match self.kv_store.create(&key, value.into()).await {
                    Ok(_) => Ok(true),
                    Err(err) => {
                        self.record_lease_failure("create", &err);
                        Ok(false)
                    }
                }
            }
        }
    }

    /// Release leadership
    pub async fn release_leadership(&self, resource_id: &str) -> NodeResult<()> {
        let key = format!("leader.{}", resource_id);
        // Delete the key. NATS KV delete is a tombstone or purge.
        // We probably shouldn't just delete blindly if we don't hold it, but for now simple.
        self.kv_store
            .delete(&key)
            .await
            .map_err(|e| {
                self.record_lease_failure("delete", &e);
                NodeError::Infrastructure(format!("KV delete error: {}", e))
            })?;
        Ok(())
    }

    fn record_lease_failure(&self, context: &str, error: impl std::fmt::Display) {
        let failures = self.lease_failures.add(1);
        warn!(
            lease_failures = failures,
            context,
            error = %error,
            "NATS lease operation failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::{sinex_test, EphemeralNats};

    #[sinex_test]
    async fn lease_failures_increment_on_broker_error() -> color_eyre::Result<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let js = async_nats::jetstream::new(client);
        let manager = NatsLeaseManager::new(
            js,
            NatsCoordinationConfig::default(),
            "lease-tester".to_string(),
        )
        .await?;

        drop(nats);

        assert!(manager.acquire_leadership("resource").await.is_err());
        assert!(manager.lease_failures.get() >= 1);
        Ok(())
    }
}
