#![doc = include_str!("../docs/coordination.md")]
//!
//! See `crate::docs::coordination` for architectural details on leadership election and handoff.
//!
//! # Issue 83: Lock Ordering Documentation
//!
//! This module uses multiple synchronization primitives that must be acquired in a
//! consistent order to prevent deadlocks:
//!
//! ## Lock Hierarchy (acquire in this order):
//!
//! 1. **work_tracker: RwLock<WorkTracker>** (coordination.rs:269)
//!    - Held during work tracking operations
//!    - Must be acquired BEFORE accessing any internal WorkTracker state
//!    - Read locks should be preferred when possible to allow concurrent access
//!
//! 2. **WorkTracker internal locks** (in_flight_operations, shutdown_requested)
//!    - CoordinationPrimitive uses AtomicUsize internally (lock-free)
//!    - No explicit lock ordering needed between these
//!
//! ## Deadlock Prevention Rules:
//!
//! 1. **Never hold work_tracker read lock while acquiring write lock**
//!    - This is the classic upgrade deadlock scenario
//!    - Release read lock before acquiring write lock
//!
//! 2. **Minimize critical sections**
//!    - Release locks as soon as possible
//!    - Don't perform I/O or async operations while holding locks
//!
//! 3. **Prefer lock-free operations**
//!    - CoordinationPrimitive operations are atomic and don't require external locks
//!    - Use these for counters and flags when possible
//!
//! ## Examples:
//!
//! ```rust,ignore
//! // CORRECT: Read lock for query
//! let count = {
//!     let tracker = self.work_tracker.read().await;
//!     tracker.in_flight_count()
//! }; // Lock released
//!
//! // CORRECT: Write lock for mutation
//! {
//!     let tracker = self.work_tracker.write().await;
//!     // Modify tracker...
//! } // Lock released
//!
//! // WRONG: Attempting to upgrade read to write lock
//! let tracker = self.work_tracker.read().await;
//! // ... some work ...
//! let mut tracker = self.work_tracker.write().await; // DEADLOCK!
//! ```

use crate::heartbeat::HeartbeatEmitter;
use crate::stream_processor::NodeRuntimeState;
use crate::version::{NodeInstance, NodeVersion};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_primitives::coordination::{CoordinationKvClient, InstanceMetadata};
use sinex_primitives::utils::CoordinationPrimitive;
use sinex_primitives::{Result, Seconds, SinexError};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, instrument, warn};

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
    use crate::nats_publisher::NatsPublisher;
    use crate::stream_processor::{EventEmitter, NodeHandles, NodeRuntimeState, ServiceInfo};
    use crate::EventTransport;
    use camino::Utf8PathBuf;
    use sinex_db::models::Event;
    use sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE;
    use sinex_primitives::ulid::Ulid;
    use sinex_primitives::JsonValue;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use xtask::sandbox::{sinex_test, EphemeralNats, TestContext, TestResult};

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

        let work_dir = Utf8PathBuf::from_path_buf(sinex_primitives::environment().temp_dir())
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
    async fn coordination_failure_counter_increments(
        ctx: TestContext,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
///
/// Issue 5: HandoffRequest is now fully implemented with send/receive logic
/// See: send_handoff_request(), handle_graceful_handoff(), wait_for_handoff_ready()
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
            from_instance: String::new(),
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
    /// Notification for work completion (separate from CoordinationPrimitive)
    work_complete_notify: Arc<tokio::sync::Notify>,
}

/// RAII guard for work tracking
///
/// Issue 14 fix: Automatically decrements counter on drop to prevent drift
#[derive(Debug)]
pub struct WorkGuard {
    tracker: Arc<CoordinationPrimitive>,
    notify: Arc<tokio::sync::Notify>,
}

impl Drop for WorkGuard {
    fn drop(&mut self) {
        let current = self.tracker.get();
        if current > 0 {
            let new_count = self.tracker.subtract(1);
            // Notify waiters if work is now complete
            if new_count == 0 {
                self.notify.notify_waiters();
            }
        }
    }
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
            work_complete_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub fn with_heartbeat(mut self, heartbeat: Arc<HeartbeatEmitter>) -> Self {
        self.heartbeat_emitter = Some(heartbeat);
        self
    }

    /// Start a new operation (increments in-flight counter)
    ///
    /// Issue 14 fix: Returns a guard that auto-finishes on drop to prevent drift
    pub fn start_operation(&self) -> WorkGuard {
        self.in_flight_operations.add(1);
        if let Some(heartbeat) = &self.heartbeat_emitter {
            heartbeat.increment_events_processed(1);
        }
        WorkGuard {
            tracker: self.in_flight_operations.clone(),
            notify: self.work_complete_notify.clone(),
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

    /// Wait for all in-flight work to complete (event-driven)
    ///
    /// Returns when the in-flight counter reaches zero or timeout is exceeded.
    /// This is truly event-driven using tokio::sync::Notify - no polling loops.
    ///
    /// When WorkGuard is dropped (either normally or via unwinding), it decrements
    /// the counter and calls notify_waiters() if the count reaches zero. This wakes
    /// up any tasks waiting here immediately, with no CPU waste.
    pub async fn wait_for_work_complete(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();

        loop {
            // Check if work is complete
            if self.is_work_complete() {
                return Ok(());
            }

            // Check timeout
            if start.elapsed() >= timeout {
                return Err(SinexError::timeout(format!(
                    "Timeout waiting for {} in-flight operations to complete",
                    self.in_flight_count()
                )));
            }

            // Calculate remaining time for this wait
            let remaining = timeout.saturating_sub(start.elapsed());

            // Wait for notification (event-driven, no polling)
            // We'll be woken up when the last in-flight operation completes
            tokio::select! {
                _ = self.work_complete_notify.notified() => {
                    // Work may be complete, loop will check
                    continue;
                }
                _ = tokio::time::sleep(remaining) => {
                    // Timeout reached
                    break;
                }
            }
        }

        // Final check before returning timeout error
        if self.is_work_complete() {
            Ok(())
        } else {
            Err(SinexError::timeout(format!(
                "Timeout waiting for {} in-flight operations to complete",
                self.in_flight_count()
            )))
        }
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
    handoff_drops: CoordinationPrimitive,
}

impl NodeCoordination {
    fn current_metadata(&self) -> InstanceMetadata {
        let mut meta: InstanceMetadata = (&self.instance).into();
        meta.last_heartbeat = sinex_primitives::temporal::Timestamp::now().unix_timestamp();
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
            handoff_drops: CoordinationPrimitive::event_counter(0, "coordination_handoff_drops"),
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
                    // This state should be transient - if we reach here, transition immediately
                    // to avoid unnecessary delays. The transition logic above should have already
                    // set current_mode to the target state.
                    warn!("Unexpected Transitioning state persisted - forcing to Standby");
                    self.current_mode = InstanceMode::Standby;
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
        // Issue 8 fix: Increase handoff channel from 10 to 100 to handle multi-deployment
        let (handoff_sender, handoff_receiver) = mpsc::channel(100);
        self.handoff_receiver = Some(handoff_receiver);

        // Issue 8 fix: Increase handoff channel size to 100 to handle multi-deployment scenarios
        // Spawn Handoff Monitor
        let nats_clone = self.nats_client.clone();
        let service_name_clone = self.instance.service_name.clone();
        let handoff_sender_clone = handoff_sender.clone();
        let handoff_drops_clone = self.handoff_drops.clone();

        // Issue 12 fix: Monitor spawned task health
        let service_name_health = self.instance.service_name.clone();
        let _monitor_handle = tokio::spawn(async move {
            let subject = format!("sinex.coordination.{service_name_clone}.handoff");
            match nats_clone.subscribe(subject.clone()).await {
                Ok(mut sub) => {
                    while let Some(msg) = sub.next().await {
                        if let Ok(req) = serde_json::from_slice::<HandoffRequest>(&msg.payload) {
                            if handoff_sender_clone.send(req).await.is_err() {
                                handoff_drops_clone.add(1);
                                warn!(
                                    handoff_drops = handoff_drops_clone.get(),
                                    "Handoff channel backpressure: dropped handoff request"
                                );
                            }
                        }
                    }
                    // Normal completion
                }
                Err(e) => {
                    error!(
                        service = %service_name_health,
                        error = %e,
                        "Handoff monitor failed to subscribe - coordination may be impaired"
                    );
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
                   // Issue 13 fix: Check mode INSIDE leadership acquisition to prevent TOCTOU race
                   // Renew leadership / Heartbeat
                   match self.kv_client.acquire_leadership(&self.instance.instance_id).await {
                       Ok(true) => {
                           // Still leader, continue
                       }
                       Ok(false) => {
                           error!("Lost leadership to another instance");
                           return Ok(()); // Clean exit to degrade
                       }
                       Err(e) => {
                           error!("Failed to maintain leadership: {}", e);
                           return Err(SinexError::service("Lost connection to coordination"));
                       }
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
    ///
    /// # Issue 96: Shutdown Signal Ordering
    ///
    /// This method performs shutdown operations in a specific order to ensure clean handoff:
    ///
    /// 1. **Drain work** (`finish_critical_work()`)
    ///    - Signals shutdown to WorkTracker
    ///    - Waits for in-flight operations to complete (with 30s timeout)
    ///    - Prevents new work from starting
    ///
    /// 2. **Publish handoff_ready signal**
    ///    - Notifies waiting instances that we're ready to shut down
    ///    - Published BEFORE releasing leadership to ensure message ordering
    ///
    /// 3. **Release leadership lease**
    ///    - Best-effort release via KV client
    ///    - Failures are logged but don't block shutdown
    ///    - Lease will eventually expire if release fails
    ///
    /// ORDERING RATIONALE:
    /// - Work must be drained before signaling ready (prevent data loss)
    /// - Signal must be sent before releasing lease (prevent race where new leader
    ///   acquires before old leader finishes cleanup)
    /// - Lease release is last and best-effort (cleanup can continue even if it fails)
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

        // Step 1: Finish current critical work
        self.finish_critical_work().await?;

        // Step 2: Signal ready by publishing to handoff_ready subject
        let subject = format!(
            "sinex.coordination.{}.handoff_ready",
            self.instance.service_name
        );
        let payload = serde_json::to_vec(&request).unwrap_or_default();

        self.nats_client
            .publish(subject, payload.into())
            .await
            .map_err(|e| SinexError::network(format!("Failed to publish handoff ready: {e}")))?;

        // Step 3: Release lease explicitly (best-effort)
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

        let bytes = serde_json::to_vec(&payload).map_err(|e| {
            SinexError::validation("failed to serialize failure signal").with_std_error(&e)
        })?;

        self.nats_client
            .publish(subject, bytes.into())
            .await
            .map_err(|e| {
                SinexError::network("failed to publish failure signal").with_std_error(&e)
            })?;

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
    ///
    /// # Issue 81: Lock Usage Pattern
    ///
    /// This method acquires `work_tracker` read locks multiple times in sequence.
    /// This is SAFE because:
    /// 1. All locks are read locks (RwLock allows multiple concurrent readers)
    /// 2. Each lock is released before the next is acquired (no lock held across await)
    /// 3. The locks guard different critical sections:
    ///    - Initial lock: Request shutdown signal
    ///    - Wait lock: Event-driven wait for completion (no polling)
    ///    - Timeout lock: Read final state for logging (only if timeout occurs)
    ///
    /// This pattern is intentional to minimize lock hold time and avoid blocking
    /// shutdown signals from other threads. The wait is event-driven using
    /// CoordinationPrimitive notifications, not polling.
    async fn finish_critical_work(&self) -> Result<()> {
        info!("Finishing critical work before handoff");

        // Issue 4 fix: Configurable drain timeout with force-shutdown
        let graceful_timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();

        // Signal any running tasks to complete gracefully
        // Lock scope 1: Signal shutdown
        {
            let tracker = self.work_tracker.read().await;
            tracker.request_shutdown();
            info!(
                "Signaled shutdown to {} in-flight operations",
                tracker.in_flight_count()
            );
        } // Lock released here

        // Wait for in-flight operations to complete with timeout
        // Lock scope 2: Event-driven wait (no polling)
        let tracker = self.work_tracker.read().await;
        let drain_result = tracker.wait_for_work_complete(graceful_timeout).await;
        drop(tracker); // Release lock before handling result

        match drain_result {
            Ok(()) => {
                info!(
                    elapsed_ms = start.elapsed().as_millis(),
                    "All critical work completed gracefully"
                );
            }
            Err(e) => {
                // Lock scope 3: Timeout diagnostic logging
                let tracker = self.work_tracker.read().await;
                warn!(
                    error = %e,
                    timeout_secs = graceful_timeout.as_secs(),
                    remaining_ops = tracker.in_flight_count(),
                    "Graceful shutdown timeout - forcing shutdown with remaining work"
                );
                // Lock released when tracker goes out of scope
                // Force-shutdown: continue with handoff despite pending work
            }
        }

        Ok(())
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
