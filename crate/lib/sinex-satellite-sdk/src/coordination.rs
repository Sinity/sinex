#![doc = include_str!("../docs/coordination.md")]

use crate::heartbeat::HeartbeatEmitter;
use crate::stream_processor::ProcessorRuntimeState;
use crate::version::{SatelliteInstance, SatelliteVersion};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sinex_core::db::distributed_locking::{DistributedCoordination, LeadershipGuard};
use sinex_core::types::utils::CoordinationPrimitive;
use sinex_core::types::{DbPool, Result, SinexError};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, instrument, warn};

/// Instance mode determines satellite behavior
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceMode {
    /// Process all events (single leader)
    Leader,
    /// Do nothing, monitor for takeover opportunities
    Standby,
    /// Transitioning between modes
    Transitioning,
}

/// Handoff request from newer version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandoffRequest {
    pub from_instance: String,
    pub from_version: SatelliteVersion,
    pub to_version: SatelliteVersion,
    pub requested_at: SystemTime,
    pub timeout_seconds: u64,
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

/// Leadership coordination for a satellite service
pub struct SatelliteCoordination {
    instance: SatelliteInstance,
    pool: DbPool,
    coordination: DistributedCoordination,
    current_mode: InstanceMode,
    handoff_receiver: Option<mpsc::Receiver<HandoffRequest>>,
    failure_coordinator: CoordinationPrimitive,
    work_tracker: Arc<RwLock<WorkTracker>>,
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
}

impl SatelliteCoordination {
    pub fn new(
        service_name: String,
        instance_id: String,
        pool: DbPool,
    ) -> crate::SatelliteResult<Self> {
        let instance = SatelliteInstance::new(instance_id, service_name)?;
        let coordination = DistributedCoordination::new(pool.clone());
        let failure_coordinator = CoordinationPrimitive::synchronizer(format!(
            "failure_detection_{}",
            instance.service_name
        ));

        let work_tracker = Arc::new(RwLock::new(WorkTracker::new()));

        Ok(Self {
            instance,
            pool,
            coordination,
            current_mode: InstanceMode::Standby,
            handoff_receiver: None,
            failure_coordinator,
            work_tracker,
            heartbeat_handle: None,
        })
    }

    pub fn from_runtime(
        runtime: &ProcessorRuntimeState,
        instance_id: String,
    ) -> crate::SatelliteResult<Self> {
        Self::new(
            runtime.service_info().service_name().to_string(),
            instance_id,
            runtime.db_pool().clone(),
        )
    }

    /// Run the coordination loop - main entry point
    pub async fn run_coordination_loop<F, Fut>(&mut self, process_events: F) -> Result<()>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        info!("Starting coordination loop for {}", self.instance.summary());

        loop {
            match self.determine_desired_mode().await? {
                InstanceMode::Leader => {
                    if self.current_mode != InstanceMode::Leader {
                        info!("Transitioning to LEADER mode");
                        self.current_mode = InstanceMode::Transitioning;

                        if let Some(leadership) = self.try_acquire_leadership().await? {
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
                            self.run_as_leader(leadership, &process_events).await?;
                        } else {
                            // 📊 COORDINATION EVENT: Leadership Acquisition Failed
                            warn!(
                                event = "coordination.leadership_acquisition_failed",
                                service = %self.instance.service_name,
                                instance_id = %self.instance.instance_id,
                                version = %self.instance.version,
                                reason = "advisory_lock_unavailable",
                                "⚠️ Failed to acquire leadership - reverting to standby"
                            );
                            self.current_mode = InstanceMode::Standby;
                        }
                    }
                }
                InstanceMode::Standby => {
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
                    self.run_as_standby().await?;
                }
                InstanceMode::Transitioning => {
                    // Should not happen from determine_desired_mode
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Determine what mode this instance should be in
    async fn determine_desired_mode(&self) -> Result<InstanceMode> {
        let all_instances = self.get_all_active_instances().await?;

        if all_instances.is_empty() {
            return Ok(InstanceMode::Leader); // Only instance
        }

        // Find the instance that should be leader
        let leader_candidate = all_instances.iter().max_by(|a, b| {
            // Version first, then start time for tie-breaking
            match a.version.cmp(&b.version) {
                std::cmp::Ordering::Equal => b.start_time.cmp(&a.start_time), // Earlier start wins
                other => other,
            }
        });

        match leader_candidate {
            Some(leader) if leader.instance_id == self.instance.instance_id => {
                Ok(InstanceMode::Leader)
            }
            _ => Ok(InstanceMode::Standby),
        }
    }

    /// Try to acquire leadership with preflight checks
    async fn try_acquire_leadership(&self) -> Result<Option<LeadershipGuard>> {
        // First, verify we're ready to be leader
        if !self.verify_preflight_checks().await? {
            info!("Skipping leadership attempt - preflight checks failed");
            return Ok(None);
        }

        // Try to acquire the advisory lock
        if let Some(lock_guard) = self
            .coordination
            .try_become_leader(&self.instance.service_name)
            .await?
        {
            let instance_uuid = uuid::Uuid::parse_str(&self.instance.instance_id)
                .map_err(|e| SinexError::validation(format!("Invalid instance UUID: {}", e)))?;
            let leadership = LeadershipGuard::new(
                lock_guard,
                self.instance.service_name.clone(),
                instance_uuid,
            );

            // Record leadership in database
            leadership.record_leadership(&self.pool).await?;

            info!("Acquired leadership for {}", self.instance.service_name);
            Ok(Some(leadership))
        } else {
            debug!("Leadership already held by another instance");
            Ok(None)
        }
    }

    /// Run as leader with event processing and handoff monitoring
    #[instrument(skip(self, leadership, process_events), fields(service = %self.instance.service_name, instance = %self.instance.instance_id))]
    async fn run_as_leader<F, Fut>(
        &mut self,
        leadership: LeadershipGuard,
        process_events: &F,
    ) -> Result<()>
    where
        F: Fn() -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let (handoff_sender, handoff_receiver) = mpsc::channel(10);
        self.handoff_receiver = Some(handoff_receiver);

        // Start handoff monitoring
        let handoff_monitor = self.monitor_handoff_requests(handoff_sender);

        // Start failure monitoring
        let failure_monitor = self.monitor_for_critical_failures();

        // Start heartbeat
        let heartbeat_task = self.run_leadership_heartbeat(&leadership);

        tokio::select! {
            // Run main event processing
            result = process_events() => {
                match result {
                    Ok(_) => info!("Event processing completed normally"),
                    Err(e) => {
                        error!("Critical failure in event processing: {}", e);
                        self.signal_critical_failure(&e.to_string()).await?;
                        return Err(e);
                    }
                }
            }

            // Handle handoff requests
            handoff_result = handoff_monitor => {
                if let Ok(request) = handoff_result {
                    // 📊 COORDINATION EVENT: Handoff Request Received
                    info!(
                        event = "coordination.handoff_request_received",
                        service = %self.instance.service_name,
                        current_instance = %self.instance.instance_id,
                        requesting_instance = %request.from_instance,
                        current_version = %self.instance.version,
                        requesting_version = %request.to_version.version,
                        "🔄 Received handoff request - initiating graceful transfer"
                    );
                    self.handle_graceful_handoff(request).await?;
                }
            }

            // Handle critical failures
            _ = failure_monitor => {
                // 📊 COORDINATION EVENT: Critical Failure
                error!(
                    event = "coordination.critical_failure_detected",
                    service = %self.instance.service_name,
                    instance_id = %self.instance.instance_id,
                    mode = "leader",
                    action = "immediate_takeover_signal",
                    "🚨 Critical failure detected - signaling for immediate takeover"
                );
                self.signal_critical_failure("Critical system failure").await?;
                return Err(SinexError::invalid_state("Critical failure detected"));
            }

            // Heartbeat failure
            _ = heartbeat_task => {
                warn!("Leadership heartbeat failed - releasing leadership");
            }
        }

        Ok(())
    }

    /// Run as standby, monitoring for leadership opportunities
    #[instrument(skip(self), fields(service = %self.instance.service_name, instance = %self.instance.instance_id))]
    async fn run_as_standby(&self) -> Result<()> {
        debug!("Running in STANDBY mode");

        tokio::select! {
            // Check for leadership opportunity every 30 seconds
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                // Re-evaluate leadership in main loop
            }

            // Watch for leader failure signals
            _ = self.watch_for_leader_failure() => {
                warn!("Leader failure detected - will attempt takeover");
            }

            // Monitor for handoff opportunities (newer version challenging us)
            _ = self.monitor_version_challenges() => {
                debug!("Version challenge detected - re-evaluating leadership");
            }
        }

        Ok(())
    }

    /// Verify preflight checks before becoming leader
    #[cfg(feature = "preflight")]
    #[instrument(skip(self), fields(service = %self.instance.service_name))]
    async fn verify_preflight_checks(&self) -> Result<bool> {
        match crate::preflight::services::verify_service_dependencies().await {
            Ok((status, _details, messages)) => {
                match status {
                    crate::preflight::VerificationStatus::Pass => {
                        debug!("Preflight checks passed for {}", self.instance.service_name);
                        Ok(true)
                    }
                    crate::preflight::VerificationStatus::Warning => {
                        warn!(
                            "Preflight warnings for {}: {:?}",
                            self.instance.service_name, messages
                        );
                        Ok(true) // Warnings still allow leadership
                    }
                    crate::preflight::VerificationStatus::Fail => {
                        error!(
                            "Preflight failed for {}: {:?}",
                            self.instance.service_name, messages
                        );
                        Ok(false)
                    }
                }
            }
            Err(e) => {
                error!("Preflight check error: {}", e);
                Ok(false) // Fail safe
            }
        }
    }

    #[cfg(not(feature = "preflight"))]
    #[instrument(skip(self), fields(service = %self.instance.service_name))]
    async fn verify_preflight_checks(&self) -> Result<bool> {
        debug!(
            "Preflight checks skipped for {} because feature 'preflight' is disabled",
            self.instance.service_name
        );
        Ok(true)
    }

    /// Get all active instances from database
    async fn get_all_active_instances(&self) -> Result<Vec<SatelliteInstance>> {
        // Query active instances from database (would need to implement this table)
        // For now, return just this instance
        Ok(vec![self.instance.clone()])
    }

    /// Monitor for handoff requests from newer versions
    async fn monitor_handoff_requests(
        &self,
        sender: mpsc::Sender<HandoffRequest>,
    ) -> Result<HandoffRequest> {
        loop {
            let signals = sqlx::query!(
                r#"
                SELECT id, message, payload
                FROM core.satellite_signals
                WHERE (target_instance = $1 OR target_instance = 'ALL')
                  AND signal_type = 'handoff_request'
                  AND processed_at IS NULL
                  AND created_at > NOW() - INTERVAL '1 minute'
                ORDER BY created_at DESC
                "#,
                self.instance.instance_id.to_string()
            )
            .fetch_all(&self.pool)
            .await?;

            if let Some(signal) = signals.into_iter().next() {
                let payload = match signal.payload {
                    Some(value) => value,
                    None => {
                        warn!(
                            "handoff request without payload for {}",
                            self.instance.service_name
                        );
                        sqlx::query!(
                            "UPDATE core.satellite_signals SET processed_at = NOW() WHERE id = $1",
                            signal.id
                        )
                        .execute(&self.pool)
                        .await?;
                        continue;
                    }
                };
                let request = Self::parse_handoff_request(payload, signal.message)?;

                sqlx::query!(
                    "UPDATE core.satellite_signals SET processed_at = NOW() WHERE id = $1",
                    signal.id
                )
                .execute(&self.pool)
                .await?;

                let _ = sender.send(request.clone()).await;
                return Ok(request);
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
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
            from_version = %request.from_version.version,
            to_version = %request.to_version.version,
            "🔄 Starting graceful handoff process"
        );

        // Begin transaction to ensure atomicity between work completion and signaling
        let mut tx = self.pool.begin().await?;

        // Finish current critical work
        self.finish_critical_work().await?;

        // Signal ready for handoff - within transaction
        let payload = serde_json::to_value(&request).map_err(|e| {
            SinexError::validation(format!("Failed to encode handoff request: {e}"))
        })?;

        sqlx::query!(
            "INSERT INTO core.satellite_signals (target_instance, signal_type, message, payload)
             VALUES ($1, 'handoff_ready', $2, $3)",
            request.from_instance,
            "Ready for leadership transfer",
            payload
        )
        .execute(tx.as_mut())
        .await?;

        // Commit transaction - makes work completion and signaling atomic
        tx.commit().await?;

        // 📊 COORDINATION EVENT: Handoff Ready
        info!(
            event = "coordination.handoff_ready",
            service = %self.instance.service_name,
            current_instance = %self.instance.instance_id,
            target_instance = %request.from_instance,
            "✅ Signaled ready for handoff - releasing leadership"
        );

        Ok(())
    }

    /// Monitor for critical failures
    async fn monitor_for_critical_failures(&self) -> Result<()> {
        // This would monitor system health, memory usage, etc.
        // For now, just wait indefinitely
        tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
        Ok(())
    }

    /// Signal critical failure to other instances
    async fn signal_critical_failure(&self, error: &str) -> Result<()> {
        // Begin transaction to ensure atomicity between database signal and coordinator signal
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            "INSERT INTO core.satellite_signals (target_instance, signal_type, message, payload)
             VALUES ('ALL', 'leader_failure', $1, $2)",
            error,
            json!({
                "service": self.instance.service_name,
                "instance_id": self.instance.instance_id,
                "error": error
            })
        )
        .execute(tx.as_mut())
        .await?;

        // Commit the database signal first
        tx.commit().await?;

        // Only signal the coordinator after successful database commit
        error!("Signaled critical failure to standbys: {}", error);
        self.failure_coordinator.signal();
        Ok(())
    }

    /// Watch for leader failure signals
    async fn watch_for_leader_failure(&self) -> Result<()> {
        loop {
            // Check for failure signals
            let failures = sqlx::query!(
                r#"
                SELECT id
                FROM core.satellite_signals
                WHERE (target_instance = $1 OR target_instance = 'ALL')
                  AND signal_type = 'leader_failure'
                  AND processed_at IS NULL
                  AND created_at > NOW() - INTERVAL '30 seconds'
                "#,
                self.instance.instance_id.to_string()
            )
            .fetch_all(&self.pool)
            .await?;

            if !failures.is_empty() {
                for failure in &failures {
                    sqlx::query!(
                        "UPDATE core.satellite_signals SET processed_at = NOW() WHERE id = $1",
                        failure.id
                    )
                    .execute(&self.pool)
                    .await?;
                }

                warn!("Leader failure signals detected");
                return Ok(());
            }

            // Check if current leader is still healthy via heartbeat
            let leader_health = sqlx::query!(
                "SELECT last_heartbeat FROM core.service_leadership 
                 WHERE service_name = $1 
                 AND last_heartbeat > NOW() - INTERVAL '30 seconds'",
                self.instance.service_name
            )
            .fetch_optional(&self.pool)
            .await?;

            if leader_health.is_none() {
                warn!("Leader heartbeat timeout detected");
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    /// Monitor for version challenges
    async fn monitor_version_challenges(&self) -> Result<()> {
        // Check if there are newer versions challenging leadership
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(())
    }

    /// Run leadership heartbeat
    async fn run_leadership_heartbeat(&self, leadership: &LeadershipGuard) -> Result<()> {
        loop {
            if let Err(e) = leadership.heartbeat(&self.pool).await {
                error!("Failed to update leadership heartbeat: {}", e);
                return Err(e);
            }

            tokio::time::sleep(Duration::from_secs(15)).await;
        }
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

        // Stop heartbeat if running
        if let Some(handle) = self.heartbeat_handle.as_ref() {
            handle.abort();
            info!("Stopped heartbeat task");
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

    #[allow(clippy::result_large_err)]
    fn parse_handoff_request(
        payload: JsonValue,
        message: Option<String>,
    ) -> Result<HandoffRequest> {
        serde_json::from_value::<HandoffRequest>(payload.clone()).or_else(|payload_err| {
            if let Some(message) = message {
                serde_json::from_str::<HandoffRequest>(&message).map_err(|msg_err| {
                    SinexError::validation(format!(
                        "Failed to parse handoff request payload ({payload_err}) and message ({msg_err})"
                    ))
                })
            } else {
                Err(SinexError::validation(format!(
                    "Failed to parse handoff request payload: {payload_err}"
                )))
            }
        })
    }

    // Getters
    pub fn instance(&self) -> &SatelliteInstance {
        &self.instance
    }

    pub fn current_mode(&self) -> InstanceMode {
        self.current_mode.clone()
    }

    /// Get work tracker for external use
    pub fn work_tracker(&self) -> Arc<RwLock<WorkTracker>> {
        self.work_tracker.clone()
    }

    /// Initialize heartbeat monitoring
    pub async fn start_heartbeat(&mut self, interval_seconds: u64) -> Result<()> {
        let heartbeat_emitter = Arc::new(HeartbeatEmitter::new(
            self.instance.service_name.clone(),
            interval_seconds,
        ));

        // Update work tracker with heartbeat
        {
            let mut tracker = self.work_tracker.write().await;
            *tracker = tracker.clone().with_heartbeat(heartbeat_emitter.clone());
        }

        // Start heartbeat background task
        let heartbeat_clone = heartbeat_emitter.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
            loop {
                interval.tick().await;

                let metrics = heartbeat_clone.create_heartbeat_metrics(None);

                // Emit structured JSON log to stdout for journald capture
                let heartbeat_json = serde_json::to_string(&metrics).unwrap_or_else(|_| {
                    "{\"error\":\"failed_to_serialize_heartbeat\"}".to_string()
                });

                info!(target: "heartbeat", "{}", heartbeat_json);
            }
        });

        self.heartbeat_handle = Some(handle);
        info!(
            "Started heartbeat monitoring with {}-second interval",
            interval_seconds
        );
        Ok(())
    }

    /// Stop heartbeat monitoring
    pub fn stop_heartbeat(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
            info!("Stopped heartbeat monitoring");
        }
    }
}
