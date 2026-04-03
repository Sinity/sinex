use crate::SinexError;
use crate::nats::create_or_open_kv_store;
use crate::units::Seconds;
use async_nats::jetstream::{Context, kv::Store};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, info, warn};

const DEFAULT_HEARTBEAT_SECS: Seconds = Seconds::from_secs(5);
const DEFAULT_LEADERSHIP_TIMEOUT_SECS: Seconds = Seconds::from_secs(30);
const DEFAULT_HANDOFF_TIMEOUT_SECS: Seconds = Seconds::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CoordinationTiming {
    heartbeat_secs: Seconds,
    leadership_timeout_secs: Seconds,
    handoff_timeout_secs: Seconds,
}

impl Default for CoordinationTiming {
    fn default() -> Self {
        Self {
            heartbeat_secs: DEFAULT_HEARTBEAT_SECS,
            leadership_timeout_secs: DEFAULT_LEADERSHIP_TIMEOUT_SECS,
            handoff_timeout_secs: DEFAULT_HANDOFF_TIMEOUT_SECS,
        }
    }
}

impl CoordinationTiming {
    fn from_overrides(
        heartbeat_secs: Option<u64>,
        leadership_timeout_secs: Option<u64>,
        handoff_timeout_secs: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            heartbeat_secs: heartbeat_secs
                .filter(|secs| *secs > 0)
                .map_or(defaults.heartbeat_secs, Seconds::from_secs),
            leadership_timeout_secs: leadership_timeout_secs
                .filter(|secs| *secs > 0)
                .map_or(defaults.leadership_timeout_secs, Seconds::from_secs),
            handoff_timeout_secs: handoff_timeout_secs
                .filter(|secs| *secs > 0)
                .map_or(defaults.handoff_timeout_secs, Seconds::from_secs),
        }
    }

    fn from_env() -> Self {
        Self::from_overrides(
            timing_override_from_env("SINEX_COORDINATION_HEARTBEAT"),
            timing_override_from_env("SINEX_COORDINATION_TIMEOUT"),
            timing_override_from_env("SINEX_COORDINATION_HANDOFF"),
        )
    }

    fn heartbeat_interval(self) -> Duration {
        self.heartbeat_secs.as_duration()
    }

    fn leadership_timeout(self) -> Duration {
        self.leadership_timeout_secs.as_duration()
    }

    fn handoff_timeout(self) -> Duration {
        self.handoff_timeout_secs.as_duration()
    }

    fn handoff_timeout_secs(self) -> Seconds {
        self.handoff_timeout_secs
    }

    fn instance_stale_timeout(self) -> Duration {
        self.leadership_timeout()
            .max(self.heartbeat_interval().saturating_mul(2))
    }
}

fn timing_override_from_env(key: &'static str) -> Option<u64> {
    match std::env::var(key) {
        Ok(raw) => match raw.parse::<u64>() {
            Ok(0) => {
                warn!(env_var = key, value = %raw, "Ignoring non-positive coordination timing override");
                None
            }
            Ok(value) => Some(value),
            Err(error) => {
                warn!(
                    env_var = key,
                    value = %raw,
                    error = %error,
                    "Ignoring invalid coordination timing override"
                );
                None
            }
        },
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(value)) => {
            warn!(
                env_var = key,
                value = ?value,
                "Ignoring non-unicode coordination timing override"
            );
            None
        }
    }
}

/// Client for interacting with the Coordination KV Store.
/// Handles node registration, heartbeats, and leader election.
#[derive(Clone)]
pub struct CoordinationKvClient {
    js: Context,
    service_name: String,
    instances_bucket: String,
    leadership_bucket: String,
    timing: CoordinationTiming,
}

/// Metadata for a registered service instance in the coordination KV store.
///
/// Tracks instance lifecycle information for health monitoring and multi-instance coordination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceMetadata {
    /// Unique identifier for this instance
    pub instance_id: String,
    /// Hostname where the instance is running
    pub hostname: String,
    /// Version of the service running on this instance
    pub version: String,
    /// Unix timestamp when the instance started
    pub started_at: i64,
    /// Unix timestamp of the last heartbeat
    pub last_heartbeat: i64,
}

impl CoordinationKvClient {
    fn parse_leader_id(raw: &[u8], context: &'static str) -> Result<String, SinexError> {
        let leader = std::str::from_utf8(raw).map_err(|error| {
            SinexError::serialization(format!("Invalid {context} leader ID encoding: {error}"))
        })?;
        if leader.trim().is_empty() {
            return Err(SinexError::serialization(format!(
                "Invalid {context} leader ID: value is empty"
            )));
        }
        Ok(leader.to_string())
    }

    async fn put_instance_metadata(
        &self,
        metadata: &InstanceMetadata,
        event_label: &'static str,
    ) -> Result<(), SinexError> {
        let bucket = self.instances_bucket().await?;
        let key = format!("{}.{}", self.service_name, metadata.instance_id);
        let value = serde_json::to_vec(metadata).map_err(|e| {
            SinexError::serialization(format!("Failed to serialize instance metadata: {e}"))
        })?;

        bucket
            .put(key, value.into())
            .await
            .map_err(|e| SinexError::kv(format!("Failed to persist instance metadata: {e}")))?;

        debug!(
            service = %self.service_name,
            instance = %metadata.instance_id,
            event = event_label,
            "Persisted coordination instance metadata"
        );
        Ok(())
    }

    #[must_use]
    pub fn new(js: Context, service_name: String) -> Self {
        let env = crate::environment::environment();
        let instances_bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_instances"));
        let leadership_bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_leadership"));
        Self {
            js,
            service_name,
            instances_bucket,
            leadership_bucket,
            timing: CoordinationTiming::from_env(),
        }
    }

    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        self.timing.heartbeat_interval()
    }

    #[must_use]
    pub fn leadership_timeout(&self) -> Duration {
        self.timing.leadership_timeout()
    }

    #[must_use]
    pub fn handoff_timeout(&self) -> Duration {
        self.timing.handoff_timeout()
    }

    #[must_use]
    pub fn instance_stale_timeout(&self) -> Duration {
        self.timing.instance_stale_timeout()
    }

    #[must_use]
    pub fn handoff_timeout_secs(&self) -> Seconds {
        self.timing.handoff_timeout_secs()
    }

    async fn instances_bucket(&self) -> Result<Store, SinexError> {
        create_or_open_kv_store(
            &self.js,
            async_nats::jetstream::kv::Config {
                bucket: self.instances_bucket.clone(),
                history: 5,
                max_age: self.timing.instance_stale_timeout(),
                ..Default::default()
            },
        )
        .await
    }

    async fn leadership_bucket(&self) -> Result<Store, SinexError> {
        create_or_open_kv_store(
            &self.js,
            async_nats::jetstream::kv::Config {
                bucket: self.leadership_bucket.clone(),
                history: 5,
                max_age: self.timing.leadership_timeout(),
                ..Default::default()
            },
        )
        .await
    }

    /// Register a node instance in the KV store.
    /// Key: `{service}.{instance}`
    pub async fn register_instance(&self, metadata: &InstanceMetadata) -> Result<(), SinexError> {
        self.put_instance_metadata(metadata, "register").await?;

        info!(
            service = %self.service_name,
            instance = %metadata.instance_id,
            "Registered instance in KV"
        );
        Ok(())
    }

    /// Update heartbeat for the instance.
    pub async fn heartbeat(&self, metadata: &InstanceMetadata) -> Result<(), SinexError> {
        self.put_instance_metadata(metadata, "heartbeat").await
    }

    /// Remove instance metadata from KV on graceful exit.
    pub async fn unregister_instance(&self, instance_id: &str) -> Result<(), SinexError> {
        let bucket = self.instances_bucket().await?;
        let key = format!("{}.{}", self.service_name, instance_id);
        bucket
            .delete(key)
            .await
            .map_err(|e| SinexError::kv(format!("Failed to unregister instance: {e}")))?;
        info!(
            service = %self.service_name,
            instance = %instance_id,
            "Unregistered instance from KV"
        );
        Ok(())
    }

    /// Attempt to acquire leadership for the service.
    /// Uses CAS on `leadership.{service}` in `KV_sinex_leadership`.
    /// Returns true if acquired (or already held), false if held by another.
    pub async fn acquire_leadership(&self, candidate_id: &str) -> Result<bool, SinexError> {
        let bucket = self.leadership_bucket().await?;
        let key = &self.service_name;

        // 1. Try to create if not exists using update with revision 0
        if bucket
            .update(key, candidate_id.to_string().into(), 0)
            .await
            .is_ok()
        {
            info!("Acquired leadership for {}", self.service_name);
            return Ok(true);
        }
        // Failed to create (exists or deleted with history)

        // 2. Check current state via entry()
        // We use entry() to get revision info and handle Tombstones
        let entry = bucket
            .entry(key)
            .await
            .map_err(|e| SinexError::kv(format!("Failed to get leadership key entry: {e}")))?;

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
            let current_leader =
                Self::parse_leader_id(&entry.value, "coordination leadership entry")?;
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
    ///
    /// Uses CAS `update` to atomically verify ownership before deleting,
    /// preventing a race where another instance acquires leadership between
    /// the ownership check and the delete.
    pub async fn release_leadership(&self, candidate_id: &str) -> Result<(), SinexError> {
        let bucket = self.leadership_bucket().await?;
        let key = &self.service_name;

        let entry = bucket
            .entry(key)
            .await
            .map_err(|e| SinexError::kv(format!("Failed to get leadership key entry: {e}")))?;

        if let Some(entry) = entry {
            use async_nats::jetstream::kv::Operation;

            // Only act on live entries (not already deleted/purged)
            if !matches!(entry.operation, Operation::Put) {
                return Ok(());
            }

            let current_leader =
                Self::parse_leader_id(&entry.value, "coordination leadership entry")?;
            if current_leader == candidate_id {
                // CAS update to prove we still own this key at this revision.
                // If another instance claimed leadership between our entry() read
                // and now, this fails safely with a revision conflict.
                match bucket
                    .update(key, candidate_id.to_string().into(), entry.revision)
                    .await
                {
                    Ok(_new_rev) => {
                        // Ownership verified atomically. Delete is now safe — the only
                        // possible race is a sub-millisecond window between sequential
                        // awaits in the same task.
                        bucket.delete(key).await.map_err(|e| {
                            SinexError::kv(format!("Failed to release leadership: {e}"))
                        })?;
                        info!("Released leadership for {}", self.service_name);
                    }
                    Err(err) => {
                        warn!(
                            error = %err,
                            "Leadership already transferred for {}, skipping release",
                            self.service_name
                        );
                    }
                }
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
            .map_err(|e| SinexError::kv(format!("Failed to list instance keys: {e}")))?;

        while let Some(key_result) = keys.next().await {
            let key = match key_result {
                Ok(key) => key,
                Err(error) => {
                    warn!(
                        service = %self.service_name,
                        error = %error,
                        "Failed to iterate coordination instance key"
                    );
                    continue;
                }
            };
            if !key.starts_with(&prefix) {
                continue;
            }
            let Some(entry) = bucket.get(&key).await.map_err(|e| {
                SinexError::kv(format!("Failed to read instance metadata for {key}: {e}"))
            })?
            else {
                continue;
            };
            let metadata = match serde_json::from_slice::<InstanceMetadata>(&entry) {
                Ok(metadata) => metadata,
                Err(error) => {
                    warn!(
                        service = %self.service_name,
                        key,
                        error = %error,
                        "Ignoring malformed coordination instance metadata"
                    );
                    continue;
                }
            };
            if self.instance_is_fresh(&metadata) {
                instances.push(metadata);
            } else {
                warn!(
                    service = %self.service_name,
                    instance = %metadata.instance_id,
                    last_heartbeat = metadata.last_heartbeat,
                    "Ignoring stale coordination instance metadata"
                );
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
            .map_err(|e| SinexError::kv(format!("Failed to get leadership key: {e}")))?;

        if let Some(entry) = entry {
            let leader = Self::parse_leader_id(&entry, "coordination leadership entry")?;
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
            .map_err(|e| SinexError::kv(format!("Failed to get instance: {e}")))?;

        if let Some(entry) = entry {
            let metadata = serde_json::from_slice::<InstanceMetadata>(&entry).map_err(|e| {
                SinexError::serialization(format!("Failed to deserialize instance metadata: {e}"))
            })?;
            if self.instance_is_fresh(&metadata) {
                Ok(Some(metadata))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn instance_is_fresh(&self, metadata: &InstanceMetadata) -> bool {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        now.saturating_sub(metadata.last_heartbeat)
            <= self.timing.instance_stale_timeout().as_secs() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::{EnvGuard, sinex_test};

    #[sinex_test]
    async fn coordination_timing_defaults_match_deployment_contract()
    -> ::xtask::sandbox::TestResult<()> {
        let timing = CoordinationTiming::from_overrides(None, None, None);
        assert_eq!(timing.heartbeat_secs, Seconds::from_secs(5));
        assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(30));
        assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(10));
        Ok(())
    }

    #[sinex_test]
    async fn coordination_timing_accepts_positive_overrides() -> ::xtask::sandbox::TestResult<()> {
        let timing = CoordinationTiming::from_overrides(Some(7), Some(31), Some(11));
        assert_eq!(timing.heartbeat_secs, Seconds::from_secs(7));
        assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(31));
        assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(11));
        Ok(())
    }

    #[sinex_test]
    async fn coordination_timing_rejects_zero_overrides() -> ::xtask::sandbox::TestResult<()> {
        let timing = CoordinationTiming::from_overrides(Some(0), Some(0), Some(0));
        assert_eq!(timing.heartbeat_secs, Seconds::from_secs(5));
        assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(30));
        assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(10));
        Ok(())
    }

    #[sinex_test(serial = true)]
    async fn coordination_timing_from_env_accepts_positive_overrides()
    -> ::xtask::sandbox::TestResult<()> {
        let _heartbeat = EnvGuard::set_single("SINEX_COORDINATION_HEARTBEAT", "7");
        let _timeout = EnvGuard::set_single("SINEX_COORDINATION_TIMEOUT", "31");
        let _handoff = EnvGuard::set_single("SINEX_COORDINATION_HANDOFF", "11");

        let timing = CoordinationTiming::from_env();

        assert_eq!(timing.heartbeat_secs, Seconds::from_secs(7));
        assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(31));
        assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(11));
        Ok(())
    }

    #[sinex_test(serial = true)]
    async fn coordination_timing_from_env_ignores_invalid_overrides()
    -> ::xtask::sandbox::TestResult<()> {
        let _heartbeat = EnvGuard::set_single("SINEX_COORDINATION_HEARTBEAT", "oops");
        let _timeout = EnvGuard::set_single("SINEX_COORDINATION_TIMEOUT", "0");
        let _handoff = EnvGuard::set_single("SINEX_COORDINATION_HANDOFF", "-5");

        let timing = CoordinationTiming::from_env();

        assert_eq!(timing.heartbeat_secs, Seconds::from_secs(5));
        assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(30));
        assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(10));
        Ok(())
    }

    #[sinex_test]
    async fn parse_leader_id_accepts_valid_utf8() -> ::xtask::sandbox::TestResult<()> {
        let leader =
            CoordinationKvClient::parse_leader_id(b"node-a", "coordination leadership entry")?;
        assert_eq!(leader, "node-a");
        Ok(())
    }

    #[sinex_test]
    async fn parse_leader_id_rejects_invalid_utf8() -> ::xtask::sandbox::TestResult<()> {
        let error =
            CoordinationKvClient::parse_leader_id(&[0xff, 0xfe], "coordination leadership entry")
                .expect_err("invalid leader bytes must fail honestly");
        assert!(
            error
                .to_string()
                .contains("Invalid coordination leadership entry leader ID encoding")
        );
        Ok(())
    }

    #[sinex_test]
    async fn parse_leader_id_rejects_empty_value() -> ::xtask::sandbox::TestResult<()> {
        let error = CoordinationKvClient::parse_leader_id(b"   ", "coordination leadership entry")
            .expect_err("empty leader bytes must fail honestly");
        assert!(
            error
                .to_string()
                .contains("Invalid coordination leadership entry leader ID: value is empty")
        );
        Ok(())
    }
}
