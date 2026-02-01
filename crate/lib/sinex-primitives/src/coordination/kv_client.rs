use crate::units::Seconds;
use crate::SinexError;
use async_nats::jetstream::{kv::Store, Context};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

const LEADERSHIP_TTL_SECS: Seconds = Seconds::from_secs(15);

/// Client for interacting with the Coordination KV Store.
/// Handles node registration, heartbeats, and leader election.
#[derive(Clone)]
pub struct CoordinationKvClient {
    js: Context,
    service_name: String,
    instances_bucket: String,
    leadership_bucket: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceMetadata {
    pub instance_id: String,
    pub hostname: String,
    pub version: String,
    pub started_at: i64,
    pub last_heartbeat: i64,
}

impl CoordinationKvClient {
    pub fn new(js: Context, service_name: String) -> Self {
        let env = crate::environment::environment();
        let instances_bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_instances"));
        let leadership_bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_leadership"));
        Self {
            js,
            service_name,
            instances_bucket,
            leadership_bucket,
        }
    }

    async fn instances_bucket(&self) -> Result<Store, SinexError> {
        self.js
            .get_key_value(&self.instances_bucket)
            .await
            .map_err(|e| SinexError::kv(format!("Failed to get instances bucket: {}", e)))
    }

    async fn leadership_bucket(&self) -> Result<Store, SinexError> {
        let config = async_nats::jetstream::kv::Config {
            bucket: self.leadership_bucket.clone(),
            history: 5,
            max_age: Duration::from_secs(LEADERSHIP_TTL_SECS.as_secs()),
            ..Default::default()
        };

        match self.js.create_key_value(config).await {
            Ok(store) => Ok(store),
            Err(create_err) => self
                .js
                .get_key_value(&self.leadership_bucket)
                .await
                .map_err(|e| {
                    SinexError::kv(format!(
                        "Failed to get leadership bucket (create: {}, open: {})",
                        create_err, e
                    ))
                }),
        }
    }

    /// Register a node instance in the KV store.
    /// Key: `{service}.{instance}`
    pub async fn register_instance(&self, metadata: &InstanceMetadata) -> Result<(), SinexError> {
        let bucket = self.instances_bucket().await?;
        let key = format!("{}.{}", self.service_name, metadata.instance_id);
        let value = serde_json::to_vec(metadata).map_err(|e| {
            SinexError::serialization(format!("Failed to serialize instance metadata: {}", e))
        })?;

        bucket
            .put(key, value.into())
            .await
            .map_err(|e| SinexError::kv(format!("Failed to register instance: {}", e)))?;

        info!(
            service = %self.service_name,
            instance = %metadata.instance_id,
            "Registered instance in KV"
        );
        Ok(())
    }

    /// Update heartbeat for the instance.
    pub async fn heartbeat(
        &self,
        _instance_id: &str,
        metadata: &InstanceMetadata,
    ) -> Result<(), SinexError> {
        self.register_instance(metadata).await
    }

    /// Attempt to acquire leadership for the service.
    /// Uses CAS on `leadership.{service}` in `KV_sinex_leadership`.
    /// Returns true if acquired (or already held), false if held by another.
    pub async fn acquire_leadership(&self, candidate_id: &str) -> Result<bool, SinexError> {
        let bucket = self.leadership_bucket().await?;
        let key = &self.service_name;

        // 1. Try to create if not exists using update with revision 0
        match bucket.update(key, candidate_id.to_string().into(), 0).await {
            Ok(_) => {
                info!("Acquired leadership for {}", self.service_name);
                return Ok(true);
            }
            Err(_) => {
                // Failed to create (exists or deleted with history)
            }
        }

        // 2. Check current state via entry()
        // We use entry() to get revision info and handle Tombstones
        let entry = bucket
            .entry(key)
            .await
            .map_err(|e| SinexError::kv(format!("Failed to get leadership key entry: {}", e)))?;

        if let Some(entry) = entry {
            use async_nats::jetstream::kv::Operation;

            // If deleted, we can try to claim it by updating against current revision
            if matches!(entry.operation, Operation::Delete | Operation::Purge) {
                match bucket
                    .update(key, candidate_id.to_string().into(), entry.revision)
                    .await
                {
                    Ok(_) => {
                        info!(
                            "Acquired leadership (was deleted) for {}",
                            self.service_name
                        );
                        return Ok(true);
                    }
                    Err(_) => {
                        // Raced?
                        return Ok(false);
                    }
                }
            }

            // Check if we are already the leader
            let current_leader = std::str::from_utf8(&entry.value).unwrap_or("");
            if current_leader == candidate_id {
                match bucket
                    .update(key, candidate_id.to_string().into(), entry.revision)
                    .await
                {
                    Ok(_) => return Ok(true),
                    Err(err) => {
                        warn!(
                            error = %err,
                            "Failed to refresh leadership lease for {}",
                            self.service_name
                        );
                        return Ok(false);
                    }
                }
            }
            return Ok(false);
        }

        // If we really got None but update(0) failed, it's a weird race or purge.
        // We can safely return false and retry next time.
        Ok(false)
    }

    /// Step down from leadership.
    pub async fn release_leadership(&self, candidate_id: &str) -> Result<(), SinexError> {
        let bucket = self.leadership_bucket().await?;
        let key = &self.service_name;

        let entry = bucket
            .get(key)
            .await
            .map_err(|e| SinexError::kv(format!("Failed to get leadership key: {}", e)))?;

        if let Some(entry) = entry {
            let current_leader = std::str::from_utf8(&entry).unwrap_or("");
            if current_leader == candidate_id {
                // Warning: TOCTTOU race condition (BUG-002). Between the check and delete,
                // another instance could have acquired leadership. NATS KV delete is unconditional.
                bucket
                    .delete(key)
                    .await
                    .map_err(|e| SinexError::kv(format!("Failed to release leadership: {}", e)))?;
                info!("Released leadership for {}", self.service_name);
            }
        }
        Ok(())
    }

    /// List all registered instances for this service
    ///
    /// Returns metadata for all instances that have registered in the KV store.
    /// Used for detecting older versions during startup for handoff coordination.
    pub async fn list_instances(&self) -> Result<Vec<InstanceMetadata>, SinexError> {
        let bucket = self.instances_bucket().await?;

        // Get all keys for this service (keys are formatted as "service_name.instance_id")
        let prefix = format!("{}.", self.service_name);
        let mut instances = Vec::new();

        // Note: NATS KV doesn't have a native prefix scan, so we need to list all keys
        // In a production system with many instances, consider maintaining a service-level
        // index or using a different pattern
        let mut keys = bucket
            .keys()
            .await
            .map_err(|e| SinexError::kv(format!("Failed to list instance keys: {}", e)))?;

        while let Some(key_result) = keys.next().await {
            if let Ok(key) = key_result {
                if key.starts_with(&prefix) {
                    if let Ok(Some(entry)) = bucket.get(&key).await {
                        if let Ok(metadata) = serde_json::from_slice::<InstanceMetadata>(&entry) {
                            instances.push(metadata);
                        }
                    }
                }
            }
        }

        Ok(instances)
    }

    /// Get the current leader for this service
    ///
    /// Returns the instance ID of the current leader, or None if no leader exists.
    pub async fn get_leader(&self) -> Result<Option<String>, SinexError> {
        let bucket = self.leadership_bucket().await?;
        let key = &self.service_name;

        let entry = bucket
            .get(key)
            .await
            .map_err(|e| SinexError::kv(format!("Failed to get leadership key: {}", e)))?;

        if let Some(entry) = entry {
            let leader = std::str::from_utf8(&entry)
                .map_err(|e| {
                    SinexError::serialization(format!("Invalid leader ID encoding: {}", e))
                })?
                .to_string();
            Ok(Some(leader))
        } else {
            Ok(None)
        }
    }

    /// Get instance metadata by ID
    ///
    /// Returns the metadata for a specific instance if it exists.
    pub async fn get_instance(
        &self,
        instance_id: &str,
    ) -> Result<Option<InstanceMetadata>, SinexError> {
        let bucket = self.instances_bucket().await?;
        let key = format!("{}.{}", self.service_name, instance_id);

        let entry = bucket
            .get(&key)
            .await
            .map_err(|e| SinexError::kv(format!("Failed to get instance: {}", e)))?;

        if let Some(entry) = entry {
            let metadata = serde_json::from_slice::<InstanceMetadata>(&entry).map_err(|e| {
                SinexError::serialization(format!("Failed to deserialize instance metadata: {}", e))
            })?;
            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }
}
