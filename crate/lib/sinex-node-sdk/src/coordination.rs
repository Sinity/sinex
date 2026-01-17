#![doc = include_str!("../docs/coordination.md")]

use crate::heartbeat::HeartbeatEmitter;
use crate::stream_processor::NodeRuntimeState;
use crate::version::{NodeInstance, NodeVersion};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::coordination::kv_client::{CoordinationKvClient, InstanceMetadata};
use sinex_core::types::utils::CoordinationPrimitive;
use sinex_core::types::{Result, Seconds, SinexError};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, instrument, warn};

use futures::StreamExt;

/// Instance mode determines node behavior
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceMode {
    /// Process all events (single leader)
    Leader,
    /// Do nothing, monitor for takeover opportunities
    Standby,
    /// Transitioning between modes
    Transitioning,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::CheckpointManager;
    use crate::event_processor::EventTransport;
    use crate::nats_publisher::NatsPublisher;
    use crate::stream_processor::{
        EventEmitter, NodeHandles, NodeRuntimeState, ServiceInfo,
    };
    use camino::Utf8PathBuf;
    use sinex_core::db::models::Event;
    use sinex_core::types::buffers::DEFAULT_EVENT_CHANNEL_SIZE;
    use sinex_core::types::ulid::Ulid;
    use sinex_core::JsonValue;
    use sinex_test_utils::{sinex_test, EphemeralNats, TestContext, TestResult};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    struct TestRuntimeHarness {
        runtime: NodeRuntimeState,
        _event_rx: mpsc::Receiver<Event<JsonValue>>,
        _nats: EphemeralNats,
    }

    async fn build_runtime(
        ctx: &TestContext,
        service_name: &str,
    ) -> TestResult<TestRuntimeHarness> {
        let nats = EphemeralNats::start().await?;
        let nats_client = nats.connect().await?;
        let publisher = Arc::new(NatsPublisher::new(nats_client.clone()));

        let (event_tx, event_rx) = mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
        let emitter = EventEmitter::new(event_tx, false);

        let js = async_nats::jetstream::new(nats_client);
        let kv = js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "sinex_checkpoints".to_string(),
                history: 1,
                ..Default::default()
            })
            .await?;

        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            service_name.to_string(),
            "test".to_string(),
            format!("{}-{}", service_name, Ulid::new()),
        ));

        let handles = NodeHandles::new(
            ctx.pool.clone(),
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );

        let work_dir = Utf8PathBuf::from_path_buf(sinex_core::environment().temp_dir())
            .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex-test"));

        let service_info = ServiceInfo::new(
            service_name.to_string(),
            gethostname::gethostname().to_string_lossy().to_string(),
            work_dir.clone().into_std_path_buf(),
            false,
        );

        let runtime = NodeRuntimeState::new(service_info, handles, HashMap::new(), work_dir);

        Ok(TestRuntimeHarness {
            runtime,
            _event_rx: event_rx,
            _nats: nats,
        })
    }

    #[sinex_test]
    async fn coordination_failure_counter_increments(ctx: TestContext) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-test").await?;
        let coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;

        let before = coordination.leadership_failures.get();
        coordination.record_coordination_failure("test", "simulated");
        let after = coordination.leadership_failures.get();

        assert_eq!(after, before + 1);
        Ok(())
    }
}

/// Handoff request from newer version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandoffRequest {
    pub from_instance: String,
    pub from_version: NodeVersion,
    pub to_version: NodeVersion,
    pub requested_at: SystemTime,
    pub timeout_seconds: Seconds,
}

impl Default for HandoffRequest {
    fn default() -> Self {
        Self {
            from_instance: "".to_string(),
            from_version: NodeVersion::current_or_default(),
            to_version: NodeVersion::current_or_default(),
            requested_at: SystemTime::now(),
            timeout_seconds: Seconds::from_secs(30),
        }
    }
}

/// Work tracking for graceful shutdown
#[derive(Debug, Clone)]
pub struct WorkTracker {
    /// Number of in-flight operations
    in_flight_operations: Arc<CoordinationPrimitive>,
    /// Signal to request graceful shutdown
    shutdown_requested: Arc<CoordinationPrimitive>,
    /// Heartbeat emitter for monitoring
    heartbeat_emitter: Option<Arc<HeartbeatEmitter>>,
}

impl WorkTracker {
    pub fn new() -> Self {
        Self {
            in_flight_operations: Arc::new(CoordinationPrimitive::event_counter(
                0,
                "in_flight_ops",
            )),
            shutdown_requested: Arc::new(CoordinationPrimitive::synchronizer("shutdown_signal")),
            heartbeat_emitter: None,
        }
    }

    pub fn with_heartbeat(mut self, heartbeat: Arc<HeartbeatEmitter>) -> Self {
        self.heartbeat_emitter = Some(heartbeat);
        self
    }

    /// Start a new operation (increments in-flight counter)
    pub fn start_operation(&self) {
        self.in_flight_operations.add(1);
        if let Some(heartbeat) = &self.heartbeat_emitter {
            heartbeat.increment_events_processed(1);
        }
    }

    /// Finish an operation (decrements in-flight counter)
    pub fn finish_operation(&self) {
        let current = self.in_flight_operations.get();
        if current > 0 {
            self.in_flight_operations.subtract(1);
        }
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.get() > 0
    }

    /// Request graceful shutdown
    pub fn request_shutdown(&self) {
        self.shutdown_requested.signal();
    }

    /// Get number of in-flight operations
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_operations.get()
    }

    /// Check if all work is complete
    pub fn is_work_complete(&self) -> bool {
        self.in_flight_operations.get() == 0
    }
}

impl Default for WorkTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&NodeInstance> for InstanceMetadata {
    fn from(instance: &NodeInstance) -> Self {
        let started_at = instance
            .start_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            instance_id: instance.instance_id.clone(),
            hostname: instance.host_name.clone(),
            version: instance.version.full_version.clone(),
            started_at,
            last_heartbeat: started_at,
        }
    }
}

/// Leadership coordination for a node service
pub struct NodeCoordination {
    instance: NodeInstance,
    kv_client: CoordinationKvClient,
    nats_client: async_nats::Client,
    current_mode: InstanceMode,
    handoff_receiver: Option<mpsc::Receiver<HandoffRequest>>,
    work_tracker: Arc<RwLock<WorkTracker>>,
    leadership_failures: CoordinationPrimitive,
}

impl NodeCoordination {
    fn current_metadata(&self) -> InstanceMetadata {
        let mut meta: InstanceMetadata = (&self.instance).into();
        meta.last_heartbeat = Utc::now().timestamp();
        meta
    }

    pub async fn new(
        service_name: String,
        instance_id: String,
        nats_client: async_nats::Client,
        _runtime_state: &NodeRuntimeState,
    ) -> crate::NodeResult<Self> {
        let instance = NodeInstance::new(instance_id.clone(), service_name.clone())?;

        // Initialize KV Client
        let js = async_nats::jetstream::new(nats_client.clone());
        let kv_client = CoordinationKvClient::new(js, service_name.clone());

        let work_tracker = Arc::new(RwLock::new(WorkTracker::new()));

        Ok(Self {
            instance,
            kv_client,
            nats_client,
            current_mode: InstanceMode::Standby,
            handoff_receiver: None,
            work_tracker,
            leadership_failures: CoordinationPrimitive::event_counter(
                0,
                "coordination_leadership_failures",
            ),
        })
    }

    pub async fn from_runtime(
        runtime: &NodeRuntimeState,
        instance_id: String,
    ) -> crate::NodeResult<Self> {
        let nats_client = runtime
            .nats_client()
            .ok_or_else(|| SinexError::configuration("NATS client missing"))?
            .clone();

        Self::new(
            runtime.service_info().service_name().to_string(),
            instance_id,
            nats_client,
            runtime,
        )
        .await
    }

    /// Run the coordination loop - main entry point
    pub async fn run_coordination_loop<F, Fut>(&mut self, process_events: F) -> Result<()>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        info!("Starting coordination loop for {}", self.instance.summary());

        // Register this instance for observability
        if let Err(e) = self
            .kv_client
            .register_instance(&self.current_metadata())
            .await
        {
            warn!("Failed to register instance: {}", e);
            self.record_coordination_failure("register_instance", &e);
            // Non-critical, continue
        }

        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            interval.tick().await;

            // Attempt to acquire leadership (CAS)
            let is_leader_check = self
                .kv_client
                .acquire_leadership(&self.instance.instance_id)
                .await;

            let desired_mode = match is_leader_check {
                Ok(true) => InstanceMode::Leader,
                Ok(false) => InstanceMode::Standby,
                Err(e) => {
                    error!("Failed to check leadership: {}", e);
                    self.record_coordination_failure("acquire_leadership", &e);
                    if self.current_mode == InstanceMode::Leader {
                        warn!("Cannot confirm leadership, degrading to Standby");
                        InstanceMode::Standby
                    } else {
                        InstanceMode::Standby
                    }
                }
            };

            // Send heartbeat regardless of mode
            if let Err(e) = self
                .kv_client
                .heartbeat(&self.instance.instance_id, &self.current_metadata())
                .await
            {
                warn!("Failed to send heartbeat: {}", e);
            }

            match desired_mode {
                InstanceMode::Leader => {
                    if self.current_mode != InstanceMode::Leader {
                        info!("Transitioning to LEADER mode");
                        self.current_mode = InstanceMode::Transitioning;

                        // 📊 COORDINATION EVENT: Leadership Acquired
                        info!(
                            event = "coordination.leadership_acquired",
                            service = %self.instance.service_name,
                            instance_id = %self.instance.instance_id,
                            version = %self.instance.version,
                            transition = "standby_to_leader",
                            "🏆 Leadership acquired successfully"
                        );

                        self.current_mode = InstanceMode::Leader;

                        let res = self.run_as_leader_with_maintenance(&process_events).await;
                        if let Err(e) = res {
                            error!("Error running as leader: {}", e);
                            self.current_mode = InstanceMode::Standby;
                        }
                    }
                }
                InstanceMode::Standby => {
                    if self.current_mode == InstanceMode::Leader {
                        // We lost leadership
                        info!("Lost leadership, transitioning to Standby");
                        self.current_mode = InstanceMode::Standby;
                    }
                    if self.current_mode != InstanceMode::Standby {
                        // 📊 COORDINATION EVENT: Standby Mode
                        info!(
                            event = "coordination.standby_mode_entered",
                            service = %self.instance.service_name,
                            instance_id = %self.instance.instance_id,
                            version = %self.instance.version,
                            previous_mode = ?self.current_mode,
                            "⏸️ Entering standby mode - monitoring for leadership opportunities"
                        );
                        self.current_mode = InstanceMode::Standby;
                    }
                    // Standby loop is just waiting.
                    // We just continue the outer loop which Ticks.
                }
                InstanceMode::Transitioning => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn run_as_leader_with_maintenance<F, Fut>(&mut self, process_events: &F) -> Result<()>
    where
        F: Fn() -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        // Start leader tasks
        let (handoff_sender, handoff_receiver) = mpsc::channel(10);
        self.handoff_receiver = Some(handoff_receiver);

        // Spawn Handoff Monitor
        let nats_clone = self.nats_client.clone();
        let service_name_clone = self.instance.service_name.clone();
        let handoff_sender_clone = handoff_sender.clone();

        let _monitor_handle = tokio::spawn(async move {
            let subject = format!("sinex.coordination.{}.handoff", service_name_clone);
            if let Ok(mut sub) = nats_clone.subscribe(subject.clone()).await {
                while let Some(msg) = sub.next().await {
                    if let Ok(req) = serde_json::from_slice::<HandoffRequest>(&msg.payload) {
                        let _ = handoff_sender_clone.send(req).await;
                    }
                }
            }
        });

        // Heartbeat/Lease Maintenance Interval
        let mut maintenance_interval = tokio::time::interval(Duration::from_secs(5));
        let handoff_rx = self
            .handoff_receiver
            .as_mut()
            .ok_or(SinexError::invalid_state("No handoff receiver"))?;

        loop {
            tokio::select! {
               // Maintenance
               _ = maintenance_interval.tick() => {
                   // Renew leadership / Heartbeat
                   if let Err(e) = self.kv_client.acquire_leadership(&self.instance.instance_id).await {
                       error!("Failed to maintain leadership: {}", e);
                        return Err(SinexError::service("Lost connection to coordination"));
                   }
                   if let Ok(false) = self.kv_client.acquire_leadership(&self.instance.instance_id).await {
                        error!("Lost leadership to another instance");
                        return Ok(()); // Clean exit to degrade
                   }
                    let _ = self.kv_client.heartbeat(&self.instance.instance_id, &(&self.instance).into()).await;
               }

               // Process Events
               result = process_events() => {
                   match result {
                       Ok(_) => info!("Event processing completed normally"),
                       Err(e) => {
                           error!("Critical failure in event processing: {}", e);
                           self.signal_critical_failure(&e.to_string()).await?;
                           return Err(e);
                       }
                   }
                   return Ok(());
               }

               // Handoffs
               Some(request) = handoff_rx.recv() => {
                   info!("Received handoff request");
                   self.handle_graceful_handoff(request).await?;
                   return Ok(()); // Exit after handoff
               }
            }
        }
    }

    /// Handle graceful handoff to newer version
    #[instrument(skip(self, request), fields(
        service = %self.instance.service_name,
        from_version = %request.from_version.version,
        to_version = %request.to_version.version
    ))]
    async fn handle_graceful_handoff(&self, request: HandoffRequest) -> Result<()> {
        // 📊 COORDINATION EVENT: Handoff Started
        info!(
            event = "coordination.handoff_started",
            service = %self.instance.service_name,
             current_instance = %self.instance.instance_id,
            target_instance = %request.from_instance,
            "🔄 Starting graceful handoff process"
        );

        // Finish current critical work
        self.finish_critical_work().await?;

        // Signal ready by releasing lease?
        let subject = format!(
            "sinex.coordination.{}.handoff_ready",
            self.instance.service_name
        );
        let payload = serde_json::to_vec(&request).unwrap_or_default();

        self.nats_client
            .publish(subject, payload.into())
            .await
            .map_err(|e| SinexError::network(format!("Failed to publish handoff ready: {}", e)))?;

        // Release lease explicitly
        if let Err(e) = self
            .kv_client
            .release_leadership(&self.instance.instance_id)
            .await
        {
            warn!("Failed to release lease during handoff: {}", e);
            self.record_coordination_failure("release_leadership", &e);
        }

        // 📊 COORDINATION EVENT: Handoff Ready
        info!(
            event = "coordination.handoff_ready",
            service = %self.instance.service_name,
            current_instance = %self.instance.instance_id,
            "✅ Signaled ready for handoff - released leadership"
        );

        Ok(())
    }

    /// Send handoff request to older version instance
    ///
    /// This method is called by a newer version to request an older version
    /// to gracefully drain its work and shut down.
    #[instrument(skip(self), fields(
        service = %self.instance.service_name,
        from_instance = %target_instance_id,
        current_version = %self.instance.version.version
    ))]
    pub async fn send_handoff_request(
        &self,
        target_instance_id: &str,
        target_version: NodeVersion,
    ) -> Result<()> {
        info!(
            event = "coordination.handoff_request_sent",
            target = %target_instance_id,
            "📤 Sending handoff request to older version"
        );

        let request = HandoffRequest {
            from_instance: target_instance_id.to_string(),
            from_version: target_version,
            to_version: self.instance.version.clone(),
            requested_at: SystemTime::now(),
            timeout_seconds: Seconds::from_secs(30),
        };

        let subject = format!("sinex.coordination.{}.handoff", self.instance.service_name);
        let payload = serde_json::to_vec(&request).map_err(|e| {
            SinexError::validation(format!("Failed to serialize handoff request: {}", e))
        })?;

        self.nats_client
            .publish(subject, payload.into())
            .await
            .map_err(|e| {
                SinexError::network(format!("Failed to publish handoff request: {}", e))
            })?;

        info!(
            event = "coordination.handoff_request_published",
            target = %target_instance_id,
            "✅ Handoff request sent, waiting for old version to drain"
        );

        Ok(())
    }

    /// Wait for handoff completion from target instance
    ///
    /// Subscribe to handoff_ready signal and wait for confirmation
    /// that the old instance has drained and is ready to shut down.
    pub async fn wait_for_handoff_ready(
        &self,
        target_instance_id: &str,
        timeout: Duration,
    ) -> Result<()> {
        let subject = format!(
            "sinex.coordination.{}.handoff_ready",
            self.instance.service_name
        );

        info!(
            event = "coordination.waiting_for_handoff_ready",
            target = %target_instance_id,
            timeout_secs = timeout.as_secs(),
            "⏳ Waiting for old instance to signal ready"
        );

        let mut sub = self
            .nats_client
            .subscribe(subject.clone())
            .await
            .map_err(|e| {
                SinexError::network(format!("Failed to subscribe to handoff_ready: {}", e))
            })?;

        // Wait for ready signal with timeout
        match tokio::time::timeout(timeout, sub.next()).await {
            Ok(Some(_msg)) => {
                info!(
                    event = "coordination.handoff_ready_received",
                    target = %target_instance_id,
                    "✅ Old instance signaled ready, proceeding with startup"
                );
                Ok(())
            }
            Ok(None) => {
                warn!(
                    "Handoff ready channel closed unexpectedly for {}",
                    target_instance_id
                );
                // Continue anyway - old instance may have crashed
                Ok(())
            }
            Err(_) => {
                warn!(
                    event = "coordination.handoff_timeout",
                    target = %target_instance_id,
                    timeout_secs = timeout.as_secs(),
                    "⚠️  Timeout waiting for handoff ready, proceeding anyway"
                );
                // Don't fail - proceed with startup even if old instance doesn't respond
                Ok(())
            }
        }
    }

    /// List all instances of this service currently registered
    ///
    /// Used to detect if older versions are running and need handoff.
    pub async fn list_instances(&self) -> Result<Vec<InstanceMetadata>> {
        self.kv_client
            .list_instances()
            .await
            .map_err(|e| SinexError::service(format!("Failed to list instances: {}", e)))
    }

    /// Initiate handoff from older version (if any) during startup
    ///
    /// This should be called during node startup to detect and request
    /// handoff from any older running versions. Returns true if handoff was
    /// initiated, false if no older version was found.
    ///
    /// # Example
    /// ```ignore
    /// let coordinator = NodeCoordination::from_runtime(&runtime, instance_id).await?;
    /// if coordinator.maybe_initiate_handoff().await? {
    ///     info!("Handoff completed, proceeding with startup");
    /// }
    /// ```
    pub async fn maybe_initiate_handoff(&self) -> Result<bool> {
        let instances = self.list_instances().await?;

        // Find older version of same service
        let my_version = &self.instance.version;

        for instance in instances {
            // Skip self
            if instance.instance_id == self.instance.instance_id {
                continue;
            }

            // Parse instance version
            if let Ok(instance_version) = instance.version.parse::<NodeVersion>() {
                // If we find an older version, request handoff
                if instance_version < *my_version {
                    info!(
                        event = "coordination.older_version_detected",
                        old_instance = %instance.instance_id,
                        old_version = %instance_version.version,
                        new_version = %my_version.version,
                        "🔄 Detected older version, initiating handoff"
                    );

                    // Send handoff request
                    self.send_handoff_request(&instance.instance_id, instance_version.clone())
                        .await?;

                    // Wait for old version to drain (30 second timeout)
                    self.wait_for_handoff_ready(&instance.instance_id, Duration::from_secs(30))
                        .await?;

                    return Ok(true);
                }
            }
        }

        // No older version found
        Ok(false)
    }

    /// Signal critical failure to other instances
    async fn signal_critical_failure(&self, error: &str) -> Result<()> {
        let subject = format!("sinex.coordination.{}.failure", self.instance.service_name);

        let payload = json!({
                "service": self.instance.service_name,
                "instance_id": self.instance.instance_id,
                "error": error
        });

        let bytes =
            serde_json::to_vec(&payload).map_err(|e| SinexError::validation(e.to_string()))?;

        self.nats_client
            .publish(subject, bytes.into())
            .await
            .map_err(|e| SinexError::network(format!("Failed to publish failure signal: {}", e)))?;

        error!("Signaled critical failure to standbys: {}", error);

        // Also force release lease if we can
        if let Err(e) = self
            .kv_client
            .release_leadership(&self.instance.instance_id)
            .await
        {
            self.record_coordination_failure("release_leadership", &e);
        }

        Ok(())
    }

    /// Finish current critical work before handoff
    async fn finish_critical_work(&self) -> Result<()> {
        info!("Finishing critical work before handoff");

        // Allow up to 30 seconds for graceful completion
        let timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();

        // Signal any running tasks to complete gracefully
        {
            let tracker = self.work_tracker.read().await;
            tracker.request_shutdown();
            info!(
                "Signaled shutdown to {} in-flight operations",
                tracker.in_flight_count()
            );
        }

        // Wait for in-flight operations to complete
        while start.elapsed() < timeout {
            // Check if any work is still in progress
            let work_complete = self.check_work_complete().await?;
            if work_complete {
                info!("All critical work completed");
                break;
            }

            // Brief sleep before checking again
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if start.elapsed() >= timeout {
            let tracker = self.work_tracker.read().await;
            warn!(
                "Graceful shutdown timeout reached, {} operations may not have completed",
                tracker.in_flight_count()
            );
        }

        Ok(())
    }

    /// Check if all critical work is complete
    async fn check_work_complete(&self) -> Result<bool> {
        let tracker = self.work_tracker.read().await;
        let is_complete = tracker.is_work_complete();

        if !is_complete {
            debug!(
                "Work still in progress: {} operations",
                tracker.in_flight_count()
            );
        }

        Ok(is_complete)
    }

    // Getters
    pub fn instance(&self) -> &NodeInstance {
        &self.instance
    }

    pub fn current_mode(&self) -> InstanceMode {
        self.current_mode.clone()
    }

    /// Get work tracker for external use
    pub fn work_tracker(&self) -> Arc<RwLock<WorkTracker>> {
        self.work_tracker.clone()
    }

    /// Get KV client for coordination queries (used by tests)
    pub fn kv_client(&self) -> &CoordinationKvClient {
        &self.kv_client
    }

    fn record_coordination_failure(&self, context: &str, error: impl std::fmt::Display) {
        let failures = self.leadership_failures.add(1);
        warn!(
            coordination_failures = failures,
            context,
            error = %error,
            "Coordination lease operation failed"
        );
    }
}
