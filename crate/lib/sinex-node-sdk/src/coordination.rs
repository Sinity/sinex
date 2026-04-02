#![doc = include_str!("../docs/coordination.md")]
//!
//! See `crate::docs::coordination` for architectural details on leadership election and handoff.
//!
//! # Lock Ordering Documentation
//!
//! This module uses multiple synchronization primitives that must be acquired in a
//! consistent order to prevent deadlocks:
//!
//! ## Lock Hierarchy (acquire in this order):
//!
//! 1. **`work_tracker`: `RwLock`<WorkTracker>** (coordination.rs:269)
//!    - Held during work tracking operations
//!    - Must be acquired BEFORE accessing any internal `WorkTracker` state
//!    - Read locks should be preferred when possible to allow concurrent access
//!
//! 2. **`WorkTracker` internal locks** (`in_flight_operations`, `shutdown_requested`)
//!    - `CoordinationPrimitive` uses `AtomicUsize` internally (lock-free)
//!    - No explicit lock ordering needed between these
//!
//! ## Deadlock Prevention Rules:
//!
//! 1. **Never hold `work_tracker` read lock while acquiring write lock**
//!    - This is the classic upgrade deadlock scenario
//!    - Release read lock before acquiring write lock
//!
//! 2. **Minimize critical sections**
//!    - Release locks as soon as possible
//!    - Don't perform I/O or async operations while holding locks
//!
//! 3. **Prefer lock-free operations**
//!    - `CoordinationPrimitive` operations are atomic and don't require external locks
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
use crate::error_helpers::unix_timestamp_secs_with_warning;
use crate::runtime::stream::NodeRuntimeState;
use crate::version::{NodeInstance, NodeVersion};

use async_nats::Subscriber;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_primitives::coordination::{CoordinationKvClient, InstanceMetadata};
use sinex_primitives::utils::CoordinationPrimitive;
use sinex_primitives::{Result, Seconds, SinexError};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, mpsc};
use tracing::{error, info, instrument, warn};

use futures::{Stream, StreamExt};
use std::future::Future;

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

enum CoordinationLoopDirective {
    Continue,
    Exit,
}

enum LeaderLoopOutcome {
    LeadershipLost,
    Exit,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventTransport;
    use crate::checkpoint::CheckpointManager;
    use crate::nats_publisher::NatsPublisher;
    use crate::runtime::stream::{EventEmitter, NodeHandles, NodeRuntimeState, ServiceInfo};
    use camino::Utf8PathBuf;
    use sinex_db::models::Event;
    use sinex_primitives::JsonValue;
    use sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;
    use uuid::Uuid;
    use xtask::sandbox::{EphemeralNats, TestContext, TestResult, sinex_test};

    struct TestRuntimeHarness {
        runtime: NodeRuntimeState,
        _event_rx: mpsc::Receiver<Event<JsonValue>>,
        _nats: Arc<EphemeralNats>,
    }

    async fn build_runtime(
        ctx: &TestContext,
        service_name: &str,
    ) -> TestResult<TestRuntimeHarness> {
        let nats_client = ctx.ensure_nats().await?;
        let nats = ctx.nats_handle()?;
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
            format!(
                "{service_name}-{}",
                Uuid::now_v7().to_string().to_lowercase()
            ),
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
            service_name.to_string(),
            sinex_primitives::events::builder::get_hostname(),
            work_dir.clone().into_std_path_buf(),
            false,
            format!("test-instance-{}", Uuid::now_v7().simple()),
            env!("CARGO_PKG_VERSION").to_string(),
            None,
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

    #[sinex_test]
    async fn current_metadata_refreshes_last_heartbeat(ctx: TestContext) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-heartbeat").await?;
        let coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;

        let first = coordination.current_metadata();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let second = coordination.current_metadata();

        assert!(
            second.last_heartbeat > first.last_heartbeat,
            "current_metadata should refresh last_heartbeat"
        );
        Ok(())
    }

    #[sinex_test]
    async fn serialize_handoff_request_round_trips(_ctx: TestContext) -> TestResult<()> {
        let request = HandoffRequest {
            requester_instance_id: "requester-1".to_string(),
            requester_version: NodeVersion::current()?,
            target_instance_id: "target-1".to_string(),
            target_version: NodeVersion::current()?,
            requested_at: SystemTime::now(),
            timeout_seconds: Seconds::from_secs(30),
        };

        let payload = NodeCoordination::serialize_handoff_request(&request)?;
        let decoded: HandoffRequest = serde_json::from_slice(&payload)?;

        assert_eq!(decoded.requester_instance_id, request.requester_instance_id);
        assert_eq!(decoded.target_instance_id, request.target_instance_id);
        assert_eq!(decoded.timeout_seconds, request.timeout_seconds);
        Ok(())
    }

    #[sinex_test]
    async fn decode_handoff_request_reports_malformed_payload(_ctx: TestContext) -> TestResult<()> {
        let err = NodeCoordination::decode_handoff_request(b"{not-json", "handoff request")
            .expect_err("malformed handoff payload should be rejected");
        assert!(
            err.to_string()
                .contains("Failed to decode handoff request"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn forward_handoff_requests_closes_channel_when_subscription_ends(
        _ctx: TestContext,
    ) -> TestResult<()> {
        let (handoff_sender, mut handoff_receiver) = mpsc::channel(1);
        let handoff_drops = CoordinationPrimitive::event_counter(0, "coordination_handoff_drops");

        NodeCoordination::forward_handoff_requests(
            futures::stream::empty::<async_nats::Message>(),
            "target-instance".to_string(),
            handoff_sender,
            handoff_drops.clone(),
            "coordination-test".to_string(),
        )
        .await;

        assert!(
            handoff_receiver.recv().await.is_none(),
            "monitor shutdown should close the handoff channel"
        );
        assert_eq!(
            handoff_drops.get(),
            1,
            "subscription shutdown should increment the handoff drop counter"
        );
        Ok(())
    }

    #[sinex_test]
    async fn list_instances_filters_stale_metadata(ctx: TestContext) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-filter").await?;
        let coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let fresh = coordination.current_metadata();
        coordination.kv_client.register_instance(&fresh).await?;

        let stale = InstanceMetadata {
            instance_id: "stale-instance".to_string(),
            hostname: fresh.hostname.clone(),
            version: fresh.version.clone(),
            started_at: fresh.started_at,
            last_heartbeat: fresh.last_heartbeat - 600,
        };
        coordination.kv_client.register_instance(&stale).await?;

        let listed = coordination.kv_client.list_instances().await?;
        assert!(
            listed
                .iter()
                .any(|meta| meta.instance_id == fresh.instance_id),
            "fresh instance should remain visible"
        );
        assert!(
            listed
                .iter()
                .all(|meta| meta.instance_id != stale.instance_id),
            "stale instance should be filtered out"
        );
        assert!(
            coordination
                .kv_client
                .get_instance(&stale.instance_id)
                .await?
                .is_none(),
            "stale instance lookup should behave as missing"
        );
        Ok(())
    }

    #[sinex_test]
    async fn maybe_initiate_handoff_ignores_stale_older_version(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-filter").await?;
        let coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let fresh = coordination.current_metadata();

        coordination
            .kv_client
            .register_instance(&InstanceMetadata {
                instance_id: "stale-old-version".to_string(),
                hostname: fresh.hostname.clone(),
                version: "0.0.0".to_string(),
                started_at: fresh.started_at - 600,
                last_heartbeat: fresh.last_heartbeat - 600,
            })
            .await?;
        coordination
            .kv_client
            .acquire_leadership("stale-old-version")
            .await?;

        assert!(
            !coordination.maybe_initiate_handoff().await?,
            "stale older instances must not trigger startup handoff"
        );
        Ok(())
    }

    #[sinex_test]
    async fn maybe_initiate_handoff_ignores_older_standby_when_self_is_leader(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-leader-only-handoff").await?;
        let coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let fresh = coordination.current_metadata();
        coordination.kv_client.register_instance(&fresh).await?;
        coordination
            .kv_client
            .register_instance(&InstanceMetadata {
                instance_id: "older-standby".to_string(),
                hostname: fresh.hostname.clone(),
                version: "0.0.0".to_string(),
                started_at: fresh.started_at,
                last_heartbeat: fresh.last_heartbeat,
            })
            .await?;
        coordination
            .kv_client
            .acquire_leadership(&fresh.instance_id)
            .await?;

        assert!(
            !coordination.maybe_initiate_handoff().await?,
            "only the current leader should be considered for startup handoff"
        );
        Ok(())
    }

    #[sinex_test]
    async fn send_handoff_request_publishes_explicit_requester_and_target(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-payload").await?;
        let coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let subject = format!(
            "sinex.coordination.{}.handoff",
            coordination.instance.service_name
        );
        let mut sub = coordination.nats_client.subscribe(subject).await?;

        coordination
            .send_handoff_request("older-leader", "0.0.0".parse()?)
            .await?;

        let message = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("handoff request not published"))?;
        let request: HandoffRequest = serde_json::from_slice(&message.payload)?;
        assert_eq!(
            request.requester_instance_id,
            coordination.instance.instance_id
        );
        assert_eq!(request.requester_version, coordination.instance.version);
        assert_eq!(request.target_instance_id, "older-leader");
        assert_eq!(request.target_version, "0.0.0".parse()?);
        Ok(())
    }

    #[sinex_test]
    async fn wait_for_handoff_ready_ignores_unrelated_messages(ctx: TestContext) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-ready-filter").await?;
        let coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let service = coordination.instance.service_name.clone();
        let requester = coordination.instance.instance_id.clone();
        let nats = coordination.nats_client.clone();
        let mut sub = coordination.subscribe_handoff_ready().await?;

        let publisher = tokio::spawn(async move {
            let ready_subject = format!("sinex.coordination.{service}.handoff_ready");

            nats.publish(ready_subject.clone(), "not-json".into())
                .await
                .expect("publish malformed ready");

            let unrelated = HandoffRequest {
                requester_instance_id: "other-requester".to_string(),
                requester_version: "9.9.9".parse().expect("valid version"),
                target_instance_id: "other-target".to_string(),
                target_version: "0.0.1".parse().expect("valid version"),
                requested_at: SystemTime::now(),
                timeout_seconds: Seconds::from_secs(30),
            };
            nats.publish(
                ready_subject.clone(),
                serde_json::to_vec(&unrelated)
                    .expect("serialize unrelated")
                    .into(),
            )
            .await
            .expect("publish unrelated ready");

            tokio::time::sleep(Duration::from_millis(50)).await;

            let matching = HandoffRequest {
                requester_instance_id: requester,
                requester_version: "1.0.0".parse().expect("valid version"),
                target_instance_id: "older-leader".to_string(),
                target_version: "0.0.1".parse().expect("valid version"),
                requested_at: SystemTime::now(),
                timeout_seconds: Seconds::from_secs(30),
            };
            nats.publish(
                ready_subject,
                serde_json::to_vec(&matching)
                    .expect("serialize matching")
                    .into(),
            )
            .await
            .expect("publish matching ready");
        });

        coordination
            .wait_for_handoff_ready_with_subscription(
                &mut sub,
                "older-leader",
                Duration::from_secs(5),
            )
            .await?;
        publisher.await?;
        Ok(())
    }

    #[sinex_test]
    async fn wait_for_handoff_ready_times_out_honestly(ctx: TestContext) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-timeout").await?;
        let coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let mut sub = coordination.subscribe_handoff_ready().await?;

        let err = coordination
            .wait_for_handoff_ready_with_subscription(
                &mut sub,
                "older-leader",
                Duration::from_millis(50),
            )
            .await
            .expect_err("missing handoff_ready should surface as an error");
        assert!(
            err.to_string()
                .contains("Timed out waiting for handoff_ready"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn maybe_initiate_handoff_targets_current_leader_and_waits_for_ready(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-roundtrip").await?;
        let coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let self_metadata = coordination.current_metadata();
        coordination
            .kv_client
            .register_instance(&self_metadata)
            .await?;

        let older_leader = InstanceMetadata {
            instance_id: "older-leader".to_string(),
            hostname: self_metadata.hostname.clone(),
            version: "0.0.0".to_string(),
            started_at: self_metadata.started_at,
            last_heartbeat: self_metadata.last_heartbeat,
        };
        coordination
            .kv_client
            .register_instance(&older_leader)
            .await?;
        coordination
            .kv_client
            .acquire_leadership(&older_leader.instance_id)
            .await?;

        let requester = coordination.instance.instance_id.clone();
        let service = coordination.instance.service_name.clone();
        let nats = coordination.nats_client.clone();
        let responder = tokio::spawn(async move {
            let handoff_subject = format!("sinex.coordination.{service}.handoff");
            let ready_subject = format!("sinex.coordination.{service}.handoff_ready");
            let mut sub = nats
                .subscribe(handoff_subject)
                .await
                .expect("subscribe handoff");
            let message = tokio::time::timeout(Duration::from_secs(5), sub.next())
                .await
                .expect("handoff timeout")
                .expect("handoff message missing");
            let request: HandoffRequest =
                serde_json::from_slice(&message.payload).expect("decode handoff request");
            assert_eq!(request.requester_instance_id, requester);
            assert_eq!(request.target_instance_id, "older-leader");
            nats.publish(
                ready_subject,
                serde_json::to_vec(&request)
                    .expect("serialize ready")
                    .into(),
            )
            .await
            .expect("publish handoff ready");
        });

        assert!(
            coordination.maybe_initiate_handoff().await?,
            "older current leader should trigger startup handoff"
        );
        responder.await?;
        Ok(())
    }

    #[sinex_test]
    async fn leader_maintenance_heartbeat_refreshes_registered_metadata(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-leader-heartbeat").await?;
        let mut coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let initial_last_heartbeat = coordination.current_metadata().last_heartbeat;
        let instance_id = coordination.instance.instance_id.clone();
        let kv_client = coordination.kv_client.clone();

        let run_handle = tokio::spawn(async move {
            let _ = tokio::time::timeout(
                Duration::from_secs(14),
                coordination.run_coordination_loop(|| async {
                    tokio::time::sleep(Duration::from_secs(14)).await;
                    Ok::<(), SinexError>(())
                }),
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(11)).await;
        let metadata = kv_client
            .get_instance(&instance_id)
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("instance metadata missing from KV"))?;
        assert!(
            metadata.last_heartbeat >= initial_last_heartbeat + 5,
            "leader maintenance should keep refreshing last_heartbeat beyond startup registration"
        );

        run_handle.abort();
        let _ = run_handle.await;
        Ok(())
    }

    #[sinex_test]
    async fn leader_maintenance_does_not_restart_process_events_future(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-single-process-future").await?;
        let mut coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let starts = Arc::new(AtomicUsize::new(0));
        let starts_for_task = starts.clone();

        let run_handle = tokio::spawn(async move {
            let _ = tokio::time::timeout(
                Duration::from_secs(14),
                coordination.run_coordination_loop(move || {
                    let starts = starts_for_task.clone();
                    async move {
                        starts.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_secs(14)).await;
                        Ok::<(), SinexError>(())
                    }
                }),
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(11)).await;
        assert_eq!(
            starts.load(Ordering::SeqCst),
            1,
            "maintenance ticks must not recreate the leader process future"
        );

        run_handle.abort();
        let _ = run_handle.await;
        Ok(())
    }

    #[sinex_test]
    async fn run_coordination_loop_unregisters_instance_after_clean_exit(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-clean-exit").await?;
        let mut coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let instance_id = coordination.instance.instance_id.clone();
        let kv_client = coordination.kv_client.clone();

        coordination
            .run_coordination_loop(|| async { Ok::<(), SinexError>(()) })
            .await?;

        assert!(
            kv_client.get_instance(&instance_id).await?.is_none(),
            "clean loop exit must remove the instance registration"
        );
        Ok(())
    }

    #[sinex_test]
    async fn run_coordination_loop_propagates_leader_failures_and_unregisters(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-fatal-exit").await?;
        let mut coordination =
            NodeCoordination::from_runtime(&harness.runtime, "coord-test".to_string()).await?;
        let instance_id = coordination.instance.instance_id.clone();
        let kv_client = coordination.kv_client.clone();

        let error = coordination
            .run_coordination_loop(|| async {
                Err::<(), _>(SinexError::service("fatal leader failure"))
            })
            .await
            .expect_err("fatal leader failure must terminate the coordination loop");
        assert!(
            error.to_string().contains("fatal leader failure"),
            "unexpected error: {error}"
        );
        assert!(
            kv_client.get_instance(&instance_id).await?.is_none(),
            "fatal loop exit must remove the instance registration"
        );
        Ok(())
    }
}

/// Handoff request from newer version
///
/// Handoff request payload used by send/receive coordination paths.
/// See: `send_handoff_request()`, `handle_graceful_handoff()`, `wait_for_handoff_ready()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandoffRequest {
    pub requester_instance_id: String,
    pub requester_version: NodeVersion,
    pub target_instance_id: String,
    pub target_version: NodeVersion,
    pub requested_at: SystemTime,
    pub timeout_seconds: Seconds,
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
    /// Notification for work completion (separate from `CoordinationPrimitive`)
    work_complete_notify: Arc<tokio::sync::Notify>,
}

/// RAII guard for work tracking
///
/// Automatically decrements the in-flight counter on drop.
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
    #[must_use]
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

    #[must_use]
    pub fn with_heartbeat(mut self, heartbeat: Arc<HeartbeatEmitter>) -> Self {
        self.heartbeat_emitter = Some(heartbeat);
        self
    }

    /// Start a new operation (increments in-flight counter)
    ///
    /// Returns a guard that auto-finishes on drop.
    #[must_use]
    pub fn start_operation(&self) -> WorkGuard {
        let _ = self.in_flight_operations.add(1);
        if let Some(heartbeat) = &self.heartbeat_emitter {
            heartbeat.increment_events_processed(1);
        }
        WorkGuard {
            tracker: self.in_flight_operations.clone(),
            notify: self.work_complete_notify.clone(),
        }
    }

    /// Check if shutdown has been requested
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.get() > 0
    }

    /// Request graceful shutdown
    pub fn request_shutdown(&self) {
        let _ = self.shutdown_requested.signal();
    }

    /// Get number of in-flight operations
    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_operations.get()
    }

    /// Check if all work is complete
    #[must_use]
    pub fn is_work_complete(&self) -> bool {
        self.in_flight_operations.get() == 0
    }

    /// Wait for all in-flight work to complete (event-driven)
    ///
    /// Returns when the in-flight counter reaches zero or timeout is exceeded.
    /// This is truly event-driven using `tokio::sync::Notify` - no polling loops.
    ///
    /// When `WorkGuard` is dropped (either normally or via unwinding), it decrements
    /// the counter and calls `notify_waiters()` if the count reaches zero. This wakes
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
                () = self.work_complete_notify.notified() => {
                    // Work may be complete, loop will check
                }
                () = tokio::time::sleep(remaining) => {
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
        instance_metadata_at(instance, None)
    }
}

fn instance_started_at(instance: &NodeInstance) -> i64 {
    unix_timestamp_secs_with_warning(instance.start_time, "coordination instance start time") as i64
}

fn instance_metadata_at(instance: &NodeInstance, last_heartbeat: Option<i64>) -> InstanceMetadata {
    let started_at = instance_started_at(instance);
    InstanceMetadata {
        instance_id: instance.instance_id.clone(),
        hostname: instance.host_name.clone(),
        version: instance.version.full_version.clone(),
        started_at,
        last_heartbeat: last_heartbeat.unwrap_or(started_at),
    }
}

/// Leadership coordination for a node service
pub struct NodeCoordination {
    instance: NodeInstance,
    kv_client: CoordinationKvClient,
    nats_client: async_nats::Client,
    current_mode: InstanceMode,
    work_tracker: Arc<RwLock<WorkTracker>>,
    leadership_failures: CoordinationPrimitive,
    handoff_drops: CoordinationPrimitive,
}

impl NodeCoordination {
    fn serialize_handoff_request(request: &HandoffRequest) -> Result<Vec<u8>> {
        serde_json::to_vec(request).map_err(|error| {
            SinexError::validation(format!("Failed to serialize handoff request: {error}"))
        })
    }

    fn decode_handoff_request(payload: &[u8], context: &'static str) -> Result<HandoffRequest> {
        serde_json::from_slice(payload).map_err(|error| {
            SinexError::validation(format!("Failed to decode {context}: {error}"))
        })
    }

    fn current_metadata(&self) -> InstanceMetadata {
        instance_metadata_at(
            &self.instance,
            Some(sinex_primitives::temporal::Timestamp::now().unix_timestamp()),
        )
    }

    async fn forward_handoff_requests<S>(
        mut sub: S,
        target_instance_id: String,
        handoff_sender: mpsc::Sender<HandoffRequest>,
        handoff_drops: CoordinationPrimitive,
        service_name: String,
    ) where
        S: Stream<Item = async_nats::Message> + Unpin,
    {
        while let Some(message) = sub.next().await {
            match Self::decode_handoff_request(&message.payload, "handoff request") {
                Ok(request) => {
                    if request.target_instance_id != target_instance_id {
                        continue;
                    }

                    if handoff_sender.send(request).await.is_err() {
                        let _ = handoff_drops.add(1);
                        warn!(
                            service = %service_name,
                            handoff_drops = handoff_drops.get(),
                            "Handoff monitor channel closed while delivering request"
                        );
                        return;
                    }
                }
                Err(error) => {
                    warn!(
                        service = %service_name,
                        error = %error,
                        "Ignoring malformed handoff request"
                    );
                }
            }
        }

        let _ = handoff_drops.add(1);
        warn!(
            service = %service_name,
            "Handoff subscription closed while monitoring; leader can no longer receive handoff requests"
        );
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
        Fut: Future<Output = Result<()>> + Send,
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

        // If an older version is still active, request graceful handoff before
        // entering the steady-state election loop.
        match self.maybe_initiate_handoff().await {
            Ok(true) => info!("Version handoff completed; entering coordination loop"),
            Ok(false) => {}
            Err(e) => {
                warn!("Failed to run startup handoff check: {}", e);
                self.record_coordination_failure("startup_handoff_check", &e);
            }
        }

        let mut interval = tokio::time::interval(self.kv_client.heartbeat_interval());

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
                    }
                    InstanceMode::Standby
                }
            };

            // Send heartbeat regardless of mode
            if let Err(e) = self.kv_client.heartbeat(&self.current_metadata()).await {
                self.record_coordination_failure("instance_heartbeat", &e);
            }

            match self.apply_mode_transition(desired_mode, &process_events).await {
                Ok(CoordinationLoopDirective::Continue) => {}
                Ok(CoordinationLoopDirective::Exit) => {
                    self.unregister_current_instance("coordination loop exited")
                        .await;
                    return Ok(());
                }
                Err(error) => {
                    self.unregister_current_instance("coordination loop failed")
                        .await;
                    return Err(error);
                }
            }
        }
    }

    /// Transition to the desired coordination mode and run leader duties if promoted.
    async fn apply_mode_transition<F, Fut>(
        &mut self,
        desired_mode: InstanceMode,
        process_events: &F,
    ) -> Result<CoordinationLoopDirective>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: Future<Output = Result<()>> + Send,
    {
        match desired_mode {
            InstanceMode::Leader if self.current_mode != InstanceMode::Leader => {
                info!(
                    event = "coordination.leadership_acquired",
                    service = %self.instance.service_name,
                    instance_id = %self.instance.instance_id,
                    version = %self.instance.version,
                    transition = "standby_to_leader",
                    "🏆 Leadership acquired successfully"
                );
                self.current_mode = InstanceMode::Leader;

                match self.run_as_leader_with_maintenance(process_events).await {
                    Ok(LeaderLoopOutcome::LeadershipLost) => {
                        self.current_mode = InstanceMode::Standby;
                    }
                    Ok(LeaderLoopOutcome::Exit) => {
                        self.current_mode = InstanceMode::Standby;
                        return Ok(CoordinationLoopDirective::Exit);
                    }
                    Err(e) => {
                        error!("Error running as leader: {}", e);
                        self.current_mode = InstanceMode::Standby;
                        return Err(e);
                    }
                }
            }
            InstanceMode::Leader => {} // already leader, no-op
            InstanceMode::Standby => {
                if self.current_mode == InstanceMode::Leader {
                    info!("Lost leadership, transitioning to Standby");
                }
                if self.current_mode != InstanceMode::Standby {
                    info!(
                        event = "coordination.standby_mode_entered",
                        service = %self.instance.service_name,
                        instance_id = %self.instance.instance_id,
                        version = %self.instance.version,
                        previous_mode = ?self.current_mode,
                        "⏸️ Entering standby mode"
                    );
                }
                self.current_mode = InstanceMode::Standby;
            }
            InstanceMode::Transitioning => {
                warn!("Unexpected Transitioning state persisted - forcing to Standby");
                self.current_mode = InstanceMode::Standby;
            }
        }
        Ok(CoordinationLoopDirective::Continue)
    }

    async fn run_as_leader_with_maintenance<F, Fut>(
        &mut self,
        process_events: &F,
    ) -> Result<LeaderLoopOutcome>
    where
        F: Fn() -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);

        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                if let Some(handle) = self.0.take() {
                    handle.abort();
                }
            }
        }

        // Start leader tasks
        // Use a larger channel to absorb handoff bursts.
        let (handoff_sender, mut handoff_rx) = mpsc::channel(100);

        // Spawn handoff monitor.
        let nats_clone = self.nats_client.clone();
        let service_name_clone = self.instance.service_name.clone();
        let instance_id_clone = self.instance.instance_id.clone();
        let handoff_drops_clone = self.handoff_drops.clone();

        // Monitor spawned task health.
        let service_name_health = self.instance.service_name.clone();
        let _monitor_handle = AbortOnDrop(Some(tokio::spawn(async move {
            let subject = format!("sinex.coordination.{service_name_clone}.handoff");
            match nats_clone.subscribe(subject.clone()).await {
                Ok(sub) => {
                    Self::forward_handoff_requests(
                        sub,
                        instance_id_clone,
                        handoff_sender,
                        handoff_drops_clone,
                        service_name_health,
                    )
                    .await;
                }
                Err(e) => {
                    let _ = handoff_drops_clone.add(1);
                    error!(
                        service = %service_name_health,
                        error = %e,
                        "Handoff monitor failed to subscribe - coordination may be impaired"
                    );
                }
            }
        })));

        // Heartbeat/Lease Maintenance Interval
        let mut maintenance_interval = tokio::time::interval(self.kv_client.heartbeat_interval());
        let kv_client = self.kv_client.clone();
        let instance_id = self.instance.instance_id.clone();
        let instance = self.instance.clone();
        let process_events_future = process_events();
        tokio::pin!(process_events_future);

        loop {
            tokio::select! {
               // Maintenance
               _ = maintenance_interval.tick() => {
                   // Check leadership inside the maintenance loop to avoid TOCTOU races.
                   // Renew leadership / Heartbeat
                   match kv_client.acquire_leadership(&instance_id).await {
                       Ok(true) => {
                           // Still leader, continue
                       }
                       Ok(false) => {
                           error!("Lost leadership to another instance");
                           return Ok(LeaderLoopOutcome::LeadershipLost);
                       }
                       Err(e) => {
                           error!("Failed to maintain leadership: {}", e);
                           return Err(SinexError::service("Lost connection to coordination"));
                       }
                   }
                   let heartbeat_metadata = instance_metadata_at(
                       &instance,
                       Some(sinex_primitives::temporal::Timestamp::now().unix_timestamp()),
                   );
                   if let Err(error) = kv_client.heartbeat(&heartbeat_metadata).await {
                       self.record_coordination_failure("leader_instance_heartbeat", &error);
                   }
               }

               // Process Events
               result = &mut process_events_future => {
                   match result {
                       Ok(()) => {
                           info!("Leader event processing completed; exiting coordination loop");
                           return Ok(LeaderLoopOutcome::Exit);
                       }
                       Err(e) => {
                           error!("Critical failure in event processing: {}", e);
                           self.signal_critical_failure(&e.to_string()).await?;
                           return Err(e);
                       }
                   }
               }

               // Handoffs
               request = handoff_rx.recv() => {
                   match request {
                       Some(request) => {
                           info!("Received handoff request");
                           self.handle_graceful_handoff(request).await?;
                           return Ok(LeaderLoopOutcome::Exit); // Exit after handoff
                       }
                       None => {
                           warn!(service = %self.instance.service_name,
                               instance_id = %self.instance.instance_id,
                               "Handoff monitor terminated unexpectedly; cannot receive handoff requests"
                           );
                           return Err(SinexError::channel_receive(
                               "Handoff monitor channel closed while leader is running",
                           ));
                       }
                   }
               }
            }
        }
    }

    /// Handle graceful handoff to newer version
    ///
    /// This method performs shutdown operations in a specific order to ensure clean handoff:
    ///
    /// 1. **Drain work** (`finish_critical_work()`)
    ///    - Signals shutdown to `WorkTracker`
    ///    - Waits for in-flight operations to complete (with 30s timeout)
    ///    - Prevents new work from starting
    ///
    /// 2. **Publish `handoff_ready` signal**
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
        requester_version = %request.requester_version.version,
        target_version = %request.target_version.version
    ))]
    async fn handle_graceful_handoff(&self, request: HandoffRequest) -> Result<()> {
        // 📊 COORDINATION EVENT: Handoff Started
        info!(
            event = "coordination.handoff_started",
            service = %self.instance.service_name,
            current_instance = %self.instance.instance_id,
            requester_instance = %request.requester_instance_id,
            "🔄 Starting graceful handoff process"
        );

        // Step 1: Finish current critical work
        self.finish_critical_work().await?;

        // Step 2: Signal ready by publishing to handoff_ready subject
        let subject = format!(
            "sinex.coordination.{}.handoff_ready",
            self.instance.service_name
        );
        let payload = Self::serialize_handoff_request(&request)?;

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
        requester_instance = %self.instance.instance_id,
        target_instance = %target_instance_id,
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
            requester_instance_id: self.instance.instance_id.clone(),
            requester_version: self.instance.version.clone(),
            target_instance_id: target_instance_id.to_string(),
            target_version,
            requested_at: SystemTime::now(),
            timeout_seconds: self.kv_client.handoff_timeout_secs(),
        };

        let subject = format!("sinex.coordination.{}.handoff", self.instance.service_name);
        let payload = Self::serialize_handoff_request(&request)?;

        self.nats_client
            .publish(subject, payload.into())
            .await
            .map_err(|e| SinexError::network(format!("Failed to publish handoff request: {e}")))?;

        info!(
            event = "coordination.handoff_request_published",
            target = %target_instance_id,
            "✅ Handoff request sent, waiting for old version to drain"
        );

        Ok(())
    }

    async fn subscribe_handoff_ready(&self) -> Result<Subscriber> {
        let subject = format!(
            "sinex.coordination.{}.handoff_ready",
            self.instance.service_name
        );

        self.nats_client
            .subscribe(subject)
            .await
            .map_err(|e| SinexError::network(format!("Failed to subscribe to handoff_ready: {e}")))
    }

    /// Wait for handoff completion from target instance.
    ///
    /// The caller is responsible for subscribing before the request is published so
    /// a fast responder cannot win the race and emit `handoff_ready` before we are listening.
    async fn wait_for_handoff_ready_with_subscription(
        &self,
        sub: &mut Subscriber,
        target_instance_id: &str,
        timeout: Duration,
    ) -> Result<()> {
        info!(
            event = "coordination.waiting_for_handoff_ready",
            requester = %self.instance.instance_id,
            target = %target_instance_id,
            timeout_secs = timeout.as_secs(),
            "⏳ Waiting for old instance to signal ready"
        );

        // Wait for ready signal with timeout
        let wait_deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = wait_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let error = SinexError::timeout(format!(
                    "Timed out waiting for handoff_ready from {target_instance_id} after {}s",
                    timeout.as_secs()
                ));
                warn!(
                    event = "coordination.handoff_timeout",
                    requester = %self.instance.instance_id,
                    target = %target_instance_id,
                    timeout_secs = timeout.as_secs(),
                    error = %error,
                    "Timed out waiting for old instance to signal ready"
                );
                return Err(error);
            }

            match tokio::time::timeout(remaining, sub.next()).await {
                Ok(Some(message)) => {
                    let ready = match Self::decode_handoff_request(
                        &message.payload,
                        "handoff_ready payload",
                    ) {
                        Ok(ready) => ready,
                        Err(error) => {
                            warn!(
                                event = "coordination.handoff_ready_decode_failed",
                                requester = %self.instance.instance_id,
                                target = %target_instance_id,
                                error = %error,
                                "Ignoring malformed handoff_ready payload"
                            );
                            continue;
                        }
                    };
                    if ready.requester_instance_id == self.instance.instance_id
                        && ready.target_instance_id == target_instance_id
                    {
                        info!(
                            event = "coordination.handoff_ready_received",
                            requester = %self.instance.instance_id,
                            target = %target_instance_id,
                            "✅ Old instance signaled ready, proceeding with startup"
                        );
                        return Ok(());
                    }

                    info!(
                        event = "coordination.handoff_ready_ignored",
                        requester = %self.instance.instance_id,
                        target = %target_instance_id,
                        received_requester = %ready.requester_instance_id,
                        received_target = %ready.target_instance_id,
                        "Ignoring unrelated handoff_ready signal"
                    );
                }
                Ok(None) => {
                    let error = SinexError::channel_receive(format!(
                        "handoff_ready subscription closed while waiting for {target_instance_id}"
                    ));
                    warn!(
                        requester = %self.instance.instance_id,
                        target = %target_instance_id,
                        error = %error,
                        "Handoff ready channel closed unexpectedly"
                    );
                    return Err(error);
                }
                Err(_) => {
                    let error = SinexError::timeout(format!(
                        "Timed out waiting for handoff_ready from {target_instance_id} after {}s",
                        timeout.as_secs()
                    ));
                    warn!(
                        event = "coordination.handoff_timeout",
                        requester = %self.instance.instance_id,
                        target = %target_instance_id,
                        timeout_secs = timeout.as_secs(),
                        error = %error,
                        "Timed out waiting for old instance to signal ready"
                    );
                    return Err(error);
                }
            }
        }
    }

    /// Wait for handoff completion from target instance.
    ///
    /// Subscribes to `handoff_ready` and filters signals to the current requester/target pair.
    pub async fn wait_for_handoff_ready(
        &self,
        target_instance_id: &str,
        timeout: Duration,
    ) -> Result<()> {
        let mut sub = self.subscribe_handoff_ready().await?;
        self.wait_for_handoff_ready_with_subscription(&mut sub, target_instance_id, timeout)
            .await
    }

    /// List all instances of this service currently registered
    ///
    /// Used to detect if older versions are running and need handoff.
    pub async fn list_instances(&self) -> Result<Vec<InstanceMetadata>> {
        self.kv_client
            .list_instances()
            .await
            .map_err(|e| SinexError::service(format!("Failed to list instances: {e}")))
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
        let Some(leader_instance_id) = self.kv_client.get_leader().await? else {
            return Ok(false);
        };
        if leader_instance_id == self.instance.instance_id {
            return Ok(false);
        }

        let instances = self.list_instances().await?;
        let Some(leader_metadata) = instances
            .into_iter()
            .find(|instance| instance.instance_id == leader_instance_id)
        else {
            warn!(
                leader_instance = %leader_instance_id,
                "Leader lease exists but instance metadata is missing or stale; skipping startup handoff"
            );
            return Ok(false);
        };

        let my_version = &self.instance.version;

        let Ok(leader_version) = leader_metadata.version.parse::<NodeVersion>() else {
            warn!(
                leader_instance = %leader_metadata.instance_id,
                leader_version = %leader_metadata.version,
                "Leader version is not parseable; skipping startup handoff"
            );
            return Ok(false);
        };

        if leader_version < *my_version {
            info!(
                event = "coordination.older_leader_detected",
                leader_instance = %leader_metadata.instance_id,
                old_version = %leader_version.version,
                new_version = %my_version.version,
                "🔄 Detected older leader, initiating handoff"
            );

            let mut handoff_ready = self.subscribe_handoff_ready().await?;
            self.send_handoff_request(&leader_metadata.instance_id, leader_version)
                .await?;
            self.wait_for_handoff_ready_with_subscription(
                &mut handoff_ready,
                &leader_metadata.instance_id,
                self.kv_client.handoff_timeout(),
            )
            .await?;

            return Ok(true);
        }

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
            SinexError::validation("failed to serialize failure signal").with_source(e)
        })?;

        self.nats_client
            .publish(subject, bytes.into())
            .await
            .map_err(|e| SinexError::network("failed to publish failure signal").with_source(e))?;

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
    /// This method acquires `work_tracker` read locks multiple times in sequence.
    /// This is SAFE because:
    /// 1. All locks are read locks (`RwLock` allows multiple concurrent readers)
    /// 2. Each lock is released before the next is acquired (no lock held across await)
    /// 3. The locks guard different critical sections:
    ///    - Initial lock: Request shutdown signal
    ///    - Wait lock: Event-driven wait for completion (no polling)
    ///    - Timeout lock: Read final state for logging (only if timeout occurs)
    ///
    /// This pattern is intentional to minimize lock hold time and avoid blocking
    /// shutdown signals from other threads. The wait is event-driven using
    /// `CoordinationPrimitive` notifications, not polling.
    async fn finish_critical_work(&self) -> Result<()> {
        info!("Finishing critical work before handoff");

        // Use a bounded drain timeout before forcing shutdown.
        let graceful_timeout = self.kv_client.handoff_timeout();
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
    #[must_use]
    pub fn instance(&self) -> &NodeInstance {
        &self.instance
    }

    #[must_use]
    pub fn current_mode(&self) -> InstanceMode {
        self.current_mode.clone()
    }

    /// Get work tracker for external use
    #[must_use]
    pub fn work_tracker(&self) -> Arc<RwLock<WorkTracker>> {
        self.work_tracker.clone()
    }

    /// Get KV client for coordination queries (used by tests)
    #[must_use]
    pub fn kv_client(&self) -> &CoordinationKvClient {
        &self.kv_client
    }

    fn record_coordination_failure(&self, context: &str, error: impl std::fmt::Display) {
        let _ = self.leadership_failures.add(1);
        warn!(
            coordination_failures = self.leadership_failures.get(),
            context,
            error = %error,
            "Coordination lease operation failed"
        );
    }

    async fn unregister_current_instance(&self, reason: &str) {
        if let Err(error) = self
            .kv_client
            .unregister_instance(&self.instance.instance_id)
            .await
        {
            self.record_coordination_failure("unregister_instance", &error);
            return;
        }
        info!(
            service = %self.instance.service_name,
            instance_id = %self.instance.instance_id,
            reason,
            "Removed coordination instance registration"
        );
    }
}
