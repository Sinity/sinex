//! Leader election and coordination via NATS KV buckets
//!
//! This module provides lease-based leader election for automata using NATS KV.

use crate::{SatelliteError, SatelliteResult};
use async_nats::jetstream::kv;
use sinex_core::environment::SinexEnvironment;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// Default lease configuration values
const DEFAULT_LEASE_TTL_SECS: u64 = 30;
const DEFAULT_LEASE_RENEWAL_INTERVAL_SECS: u64 = 10;

/// Lease status for a processor instance
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseStatus {
    /// This instance is the leader
    Leader,
    /// This instance is a standby
    Standby,
    /// Lease status unknown (initializing or error)
    Unknown,
}

/// Configuration for lease manager
#[derive(Debug, Clone)]
pub struct LeaseManagerConfig {
    /// Processor name for lease identification
    pub processor_name: String,
    /// Instance ID for this processor instance
    pub instance_id: String,
    /// Lease TTL (time to live)
    pub lease_ttl: Duration,
    /// Lease renewal interval
    pub renewal_interval: Duration,
}

impl Default for LeaseManagerConfig {
    fn default() -> Self {
        Self {
            processor_name: "automaton".to_string(),
            instance_id: uuid::Uuid::new_v4().to_string(),
            lease_ttl: Duration::from_secs(DEFAULT_LEASE_TTL_SECS),
            renewal_interval: Duration::from_secs(DEFAULT_LEASE_RENEWAL_INTERVAL_SECS),
        }
    }
}

/// Lease manager for leader election using NATS KV
pub struct LeaseManager {
    nats_client: async_nats::Client,
    env: SinexEnvironment,
    config: LeaseManagerConfig,
    status: Arc<RwLock<LeaseStatus>>,
    running: Arc<RwLock<bool>>,
}

impl LeaseManager {
    /// Create a new lease manager
    pub fn new(
        nats_client: async_nats::Client,
        env: SinexEnvironment,
        config: LeaseManagerConfig,
    ) -> Self {
        Self {
            nats_client,
            env,
            config,
            status: Arc::new(RwLock::new(LeaseStatus::Unknown)),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the lease manager
    pub async fn start(&self) -> SatelliteResult<()> {
        {
            let mut running = self.running.write().await;
            if *running {
                return Err(SatelliteError::Lifecycle(
                    "Lease manager already running".to_string(),
                ));
            }
            *running = true;
        }

        info!(
            "Starting lease manager for processor: {}",
            self.config.processor_name
        );

        let js = async_nats::jetstream::new(self.nats_client.clone());

        let kv_bucket_name = self.env.nats_subject("leadership_leases");
        let kv = js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: kv_bucket_name.clone(),
                history: 5,
                max_age: self.config.lease_ttl,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SatelliteError::Processing(format!("Failed to create KV bucket: {}", e))
            })?;

        let status = self.status.clone();
        let running = self.running.clone();
        let lease_key = self.config.processor_name.clone();
        let instance_id = self.config.instance_id.clone();
        let renewal_interval = self.config.renewal_interval;

        tokio::spawn(async move {
            Self::lease_loop(
                kv,
                lease_key,
                instance_id,
                status,
                running,
                renewal_interval,
            )
            .await;
        });

        Ok(())
    }

    /// Stop the lease manager
    pub async fn stop(&self) {
        info!("Stopping lease manager");
        *self.running.write().await = false;

        if *self.status.read().await == LeaseStatus::Leader {
            if let Err(e) = self.release_lease().await {
                error!("Failed to release lease on shutdown: {}", e);
            }
        }
    }

    /// Get current lease status
    pub async fn status(&self) -> LeaseStatus {
        self.status.read().await.clone()
    }

    /// Check if this instance is the leader
    pub async fn is_leader(&self) -> bool {
        *self.status.read().await == LeaseStatus::Leader
    }

    async fn lease_loop(
        kv: kv::Store,
        lease_key: String,
        instance_id: String,
        status: Arc<RwLock<LeaseStatus>>,
        running: Arc<RwLock<bool>>,
        renewal_interval: Duration,
    ) {
        let mut ticker = tokio::time::interval(renewal_interval);

        while *running.read().await {
            ticker.tick().await;

            match Self::try_acquire_or_renew_lease(&kv, &lease_key, &instance_id).await {
                Ok(is_leader) => {
                    let mut current_status = status.write().await;
                    if is_leader {
                        if *current_status != LeaseStatus::Leader {
                            info!("Instance {} became leader", instance_id);
                            *current_status = LeaseStatus::Leader;
                        } else {
                            debug!("Instance {} renewed leader lease", instance_id);
                        }
                    } else {
                        if *current_status != LeaseStatus::Standby {
                            info!("Instance {} is standby", instance_id);
                            *current_status = LeaseStatus::Standby;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to acquire/renew lease: {}", e);
                    *status.write().await = LeaseStatus::Unknown;
                }
            }
        }

        info!("Lease loop stopped for instance {}", instance_id);
    }

    async fn try_acquire_or_renew_lease(
        kv: &kv::Store,
        lease_key: &str,
        instance_id: &str,
    ) -> SatelliteResult<bool> {
        match kv.get(lease_key).await {
            Ok(Some(entry)) => {
                let current_holder = String::from_utf8_lossy(&entry);
                if current_holder == instance_id {
                    kv.put(lease_key, instance_id.as_bytes().to_vec().into())
                        .await
                        .map_err(|e| {
                            SatelliteError::Processing(format!("Failed to renew lease: {}", e))
                        })?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Ok(None) => {
                kv.put(lease_key, instance_id.as_bytes().to_vec().into())
                    .await
                    .map_err(|e| {
                        SatelliteError::Processing(format!("Failed to create lease: {}", e))
                    })?;
                info!("Acquired new lease for instance {}", instance_id);
                Ok(true)
            }
            Err(e) => Err(SatelliteError::Processing(format!(
                "Failed to get lease: {}",
                e
            ))),
        }
    }

    async fn release_lease(&self) -> SatelliteResult<()> {
        info!("Releasing lease for instance {}", self.config.instance_id);

        let js = async_nats::jetstream::new(self.nats_client.clone());
        let kv_bucket_name = self.env.nats_subject("leadership_leases");

        let kv = js
            .get_key_value(&kv_bucket_name)
            .await
            .map_err(|e| SatelliteError::Processing(format!("Failed to get KV bucket: {}", e)))?;

        let lease_key = &self.config.processor_name;

        match kv.get(lease_key).await {
            Ok(Some(entry)) => {
                let current_holder = String::from_utf8_lossy(&entry);
                if current_holder == self.config.instance_id {
                    kv.delete(lease_key).await.map_err(|e| {
                        SatelliteError::Processing(format!("Failed to delete lease: {}", e))
                    })?;
                    info!("Lease released successfully");
                }
            }
            Ok(None) => {
                debug!("No lease to release");
            }
            Err(e) => {
                warn!("Error checking lease during release: {}", e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::sinex_test;

    #[allow(dead_code)]
    #[sinex_test]
    fn test_lease_manager_config_defaults() -> TestResult<()> {
        let config = LeaseManagerConfig::default();
        assert_eq!(config.processor_name, "automaton");
        assert!(!config.instance_id.is_empty());
        assert_eq!(config.lease_ttl, Duration::from_secs(30));
        assert_eq!(config.renewal_interval, Duration::from_secs(10));
        Ok(())
    }

    #[sinex_test]
    fn test_lease_status() -> TestResult<()> {
        assert_eq!(LeaseStatus::Leader, LeaseStatus::Leader);
        assert_ne!(LeaseStatus::Leader, LeaseStatus::Standby);
        Ok(())
    }
}
