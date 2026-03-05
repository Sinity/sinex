//! Service lifecycle management for node services

use crate::health_reporter::{HealthReporter, HealthThresholds};
use crate::heartbeat::{HeartbeatCounterHandle, HeartbeatEmitter};
use crate::runtime::stream::NodeRuntimeState;
use crate::{NodeResult, SinexError};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sinex_primitives::Seconds;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

/// Service status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl std::fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceStatus::Starting => write!(f, "starting"),
            ServiceStatus::Running => write!(f, "running"),
            ServiceStatus::Stopping => write!(f, "stopping"),
            ServiceStatus::Stopped => write!(f, "stopped"),
            ServiceStatus::Failed => write!(f, "failed"),
        }
    }
}

/// Lifecycle manager for node services
///
/// Uses `parking_lot::Mutex` for non-poisoning status updates.
pub struct LifecycleManager {
    service_name: String,
    status: Arc<Mutex<ServiceStatus>>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
    shutdown_watch_tx: watch::Sender<bool>,
    shutdown_watch_rx: watch::Receiver<bool>,
    health_check_interval: tokio::time::Duration,
    heartbeat_emitter: Option<HeartbeatEmitter>,
    heartbeat_interval_seconds: Seconds,
    health_reporter: Option<Arc<HealthReporter>>,
    health_thresholds: Option<HealthThresholds>,
    started_at: std::time::Instant,
    shutdown_grace_period: tokio::time::Duration,
}

impl LifecycleManager {
    /// Create a new lifecycle manager
    pub fn new(service_name: String) -> Self {
        let (shutdown_watch_tx, shutdown_watch_rx) = watch::channel(false);
        Self {
            service_name,
            status: Arc::new(Mutex::new(ServiceStatus::Starting)),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_sender: None,
            shutdown_watch_tx,
            shutdown_watch_rx,
            health_check_interval: tokio::time::Duration::from_secs(30),
            heartbeat_emitter: None,
            heartbeat_interval_seconds: Seconds::from_secs(30), // Default 30 second heartbeats
            health_reporter: None,
            health_thresholds: None,
            started_at: std::time::Instant::now(),
            shutdown_grace_period: tokio::time::Duration::from_secs(5),
        }
    }

    /// Construct a lifecycle manager for a given runtime, hydrating heartbeat handles
    pub fn from_runtime(runtime: &NodeRuntimeState) -> Self {
        let mut manager = Self::new(runtime.service_info().service_name().to_string());
        manager.hydrate_heartbeat(runtime);
        manager
    }

    /// Set health check interval
    pub fn with_health_check_interval(mut self, interval: tokio::time::Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    /// Enable heartbeat emission with custom interval
    pub fn with_heartbeat(mut self, interval_seconds: Seconds) -> Self {
        self.heartbeat_interval_seconds = interval_seconds;
        self
    }

    /// Enable health monitoring with custom thresholds
    pub fn with_health_monitoring(mut self, thresholds: HealthThresholds) -> Self {
        self.health_thresholds = Some(thresholds);
        self
    }

    /// Set the shutdown grace period (default: 5s)
    pub fn with_shutdown_grace_period(mut self, duration: tokio::time::Duration) -> Self {
        self.shutdown_grace_period = duration;
        self
    }

    /// Hydrate heartbeat configuration once runtime handles are available
    pub fn hydrate_heartbeat(&mut self, runtime: &NodeRuntimeState) {
        self.heartbeat_emitter = Some(runtime.heartbeat_emitter(self.heartbeat_interval_seconds));
    }

    /// Hydrate the health reporter with runtime components
    ///
    /// This method must be called after the lifecycle manager has access to runtime state.
    /// It creates the HealthReporter with a fully configured SelfObserver.
    #[cfg(feature = "messaging")]
    pub fn hydrate_health_reporter(&mut self, runtime: &NodeRuntimeState) {
        use crate::self_observation::{SelfObserver, SelfObserverConfig};
        use std::time::Duration;

        // Only create if thresholds were configured and NATS is available
        if let (Some(thresholds), Some(nats_client)) =
            (&self.health_thresholds, runtime.nats_client())
        {
            let config = SelfObserverConfig {
                component: runtime.service_info().service_name().to_string(),
                subject_prefix: "sinex.telemetry".to_string(),
                enabled: true,
                min_emission_interval: Duration::from_secs(1),
            };

            let observer = std::sync::Arc::new(SelfObserver::new(nats_client, config));

            self.health_reporter = Some(std::sync::Arc::new(
                crate::health_reporter::HealthReporter::new(
                    runtime.service_info().service_name().to_string(),
                    observer,
                    thresholds.clone(),
                ),
            ));

            tracing::info!(
                component = %runtime.service_info().service_name(),
                "Health monitoring enabled with HealthReporter"
            );
        }
    }

    /// Hydrate health reporter (no-op without messaging feature)
    #[cfg(not(feature = "messaging"))]
    pub fn hydrate_health_reporter(&mut self, _runtime: &NodeRuntimeState) {
        // No-op when messaging feature is disabled
    }

    /// Get heartbeat counter handle for tracking metrics
    pub fn get_heartbeat_handle(&self) -> Option<HeartbeatCounterHandle> {
        self.heartbeat_emitter
            .as_ref()
            .map(|emitter| emitter.get_counter_handle())
    }

    /// Get health reporter for tracking component health
    pub fn health_reporter(&self) -> Option<&Arc<HealthReporter>> {
        self.health_reporter.as_ref()
    }

    /// Get current status
    pub fn status(&self) -> ServiceStatus {
        *self.status.lock()
    }

    /// Set status
    ///
    /// This method performs status updates in the following order:
    /// 1. Update internal status (guarded by parking_lot::Mutex)
    /// 2. Log status change via tracing::info
    /// 3. Notify systemd via sd_notify (best-effort, may fail)
    ///
    /// IMPORTANT: The systemd notification is best-effort and non-blocking.
    /// Failures to notify systemd do not affect internal state transitions.
    /// This design ensures that systemd communication issues cannot prevent
    /// the service from transitioning states or block critical operations.
    ///
    /// The status mutex uses parking_lot which doesn't poison on panic,
    /// ensuring status updates remain available even after thread panics.
    pub fn set_status(&self, status: ServiceStatus) {
        *self.status.lock() = status;
        info!(
            service = %self.service_name,
            status = %status,
            "Service status changed"
        );

        // Notify systemd of status change (best-effort, may fail)
        let sd_status = match status {
            ServiceStatus::Starting => "Starting up",
            ServiceStatus::Running => "Running",
            ServiceStatus::Stopping => "Stopping",
            ServiceStatus::Stopped => "Stopped",
            ServiceStatus::Failed => "Failed",
        };

        if let Err(e) = sd_notify::notify(false, &[sd_notify::NotifyState::Status(sd_status)]) {
            warn!(
                service = %self.service_name,
                error = %e,
                "Failed to notify systemd of status change (best-effort notification, continuing)"
            );
        }
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_flag.load(Ordering::Relaxed)
    }

    /// Initialize signal handlers and lifecycle management
    pub fn initialize(&mut self) -> NodeResult<()> {
        info!(service = %self.service_name, "Initializing lifecycle management");

        // Drop old sender before creating a new one to prevent accumulation.
        if let Some(old_sender) = self.shutdown_sender.take() {
            drop(old_sender);
        }

        // Create shutdown channel
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        self.shutdown_sender = Some(shutdown_sender);

        // Set up signal handlers
        let shutdown_flag = self.shutdown_flag.clone();
        let shutdown_notify = self.shutdown_watch_tx.clone();
        let service_name = self.service_name.clone();

        tokio::spawn(async move {
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(e) => {
                    error!(
                        service = %service_name,
                        error = %e,
                        "Failed to set up SIGTERM handler"
                    );
                    return;
                }
            };

            let mut sigint = match signal(SignalKind::interrupt()) {
                Ok(signal) => signal,
                Err(e) => {
                    error!(
                        service = %service_name,
                        error = %e,
                        "Failed to set up SIGINT handler"
                    );
                    return;
                }
            };

            tokio::select! {
                _ = sigterm.recv() => {
                    info!(service = %service_name, "Received SIGTERM, initiating shutdown");
                }
                _ = sigint.recv() => {
                    info!(service = %service_name, "Received SIGINT, initiating shutdown");
                }
                _ = shutdown_receiver => {
                    info!(service = %service_name, "Received internal shutdown signal");
                }
            }

            shutdown_flag.store(true, Ordering::Relaxed);
            let _ = shutdown_notify.send(true);
        });

        // Notify systemd that we're ready
        if let Err(e) = sd_notify::notify(true, &[sd_notify::NotifyState::Ready]) {
            warn!(
                service = %self.service_name,
                error = %e,
                "Failed to notify systemd ready state"
            );
        }

        Ok(())
    }

    /// Start the service and run main loop with health checks
    pub async fn run_with_health_check<F, Fut, H, HFut>(
        &self,
        main_task: F,
        health_check: H,
    ) -> NodeResult<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = NodeResult<()>>,
        H: Fn() -> HFut + Send + Sync + 'static,
        HFut: std::future::Future<Output = bool> + Send,
    {
        self.set_status(ServiceStatus::Running);

        let shutdown_flag = self.shutdown_flag.clone();
        let status = self.status.clone();
        let service_name = self.service_name.clone();
        let health_interval = self.health_check_interval;

        let mut join_set = JoinSet::new();

        // Start health check task
        let shutdown_flag_health = shutdown_flag.clone();
        let shutdown_watch_health = self.shutdown_watch_tx.clone();
        join_set.spawn(async move {
            let mut interval = tokio::time::interval(health_interval);

            loop {
                interval.tick().await;

                if shutdown_flag_health.load(Ordering::Relaxed) {
                    break;
                }

                let healthy = health_check().await;
                if !healthy {
                    error!(service = %service_name, "Health check failed, triggering shutdown");
                    *status.lock() = ServiceStatus::Failed;

                    // Notify systemd of failure
                    let _ = sd_notify::notify(
                        false,
                        &[sd_notify::NotifyState::Status("Health check failed")],
                    );

                    // Trigger shutdown so the service doesn't continue as a zombie
                    shutdown_flag_health.store(true, Ordering::Relaxed);
                    let _ = shutdown_watch_health.send(true);
                    break;
                }
            }

            "health"
        });

        // Start heartbeat task if heartbeat emitter is configured
        if let Some(emitter) = &self.heartbeat_emitter {
            let emitter_clone = emitter.clone(); // We need to implement Clone for HeartbeatEmitter
            let shutdown_flag_clone = shutdown_flag.clone();
            let service_name_clone = self.service_name.clone();

            join_set.spawn(async move {
                info!(service = %service_name_clone, "Starting heartbeat emission");

                // Create a metadata provider that includes current status
                let _metadata_provider: Box<dyn Fn() -> Option<serde_json::Value> + Send> =
                    Box::new(move || {
                        Some(serde_json::json!({
                            "service_type": "node",
                            "heartbeat_source": "lifecycle_manager"
                        }))
                    });

                // Start periodic heartbeat emission
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                    emitter_clone.interval_seconds.as_secs(),
                ));

                loop {
                    interval.tick().await;

                    if shutdown_flag_clone.load(Ordering::Relaxed) {
                        break;
                    }

                    let _ = emitter_clone
                        .emit_heartbeat(Some(serde_json::json!({
                            "service_type": "node",
                            "heartbeat_source": "lifecycle_manager"
                        })))
                        .await;
                }

                info!(service = %service_name_clone, "Heartbeat emission stopped");
                "heartbeat"
            });
        }

        // Run main task
        let mut main_task = Box::pin(main_task());
        let main_result = tokio::select! {
            result = &mut main_task => result,
            result = join_set.join_next() => {
                match result {
                    Some(Ok(task_name)) => {
                        if shutdown_flag.load(Ordering::Relaxed) {
                            Ok(())
                        } else {
                            Err(SinexError::lifecycle(format!(
                                "{task_name} task exited unexpectedly"
                            )))
                        }
                    }
                    Some(Err(join_err)) => Err(SinexError::lifecycle(format!(
                        "Background task failed: {join_err}"
                    ))),
                    None => Ok(()),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!(service = %self.service_name, "Received Ctrl+C");
                self.shutdown_flag.store(true, Ordering::Relaxed);
                let _ = self.shutdown_watch_tx.send(true);
                Ok(())
            }
        };

        // Emit final heartbeat on any shutdown path (SIGTERM, SIGINT, Ctrl+C, health failure)
        // so production monitoring always sees shutdown events
        if let Some(emitter) = &self.heartbeat_emitter {
            let shutdown_reason = if self.status() == ServiceStatus::Failed {
                "health_check_failure"
            } else {
                "shutdown"
            };
            let _ = emitter
                .emit_heartbeat(Some(serde_json::json!({
                    "shutdown_reason": shutdown_reason,
                    "final_heartbeat": true
                })))
                .await;
        }

        // Cancel background tasks
        join_set.shutdown().await;

        match main_result {
            Ok(()) => {
                info!(service = %self.service_name, "Service completed successfully");
                self.set_status(ServiceStatus::Stopped);
            }
            Err(e) => {
                error!(service = %self.service_name, error = %e, "Service failed");
                self.set_status(ServiceStatus::Failed);
                return Err(e);
            }
        }

        Ok(())
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> NodeResult<()> {
        info!(service = %self.service_name, "Initiating graceful shutdown");

        self.set_status(ServiceStatus::Stopping);
        self.shutdown_flag.store(true, Ordering::Relaxed);
        let _ = self.shutdown_watch_tx.send(true);

        // Send shutdown signal if sender is available
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }

        // Notify systemd we're stopping
        if let Err(e) = sd_notify::notify(false, &[sd_notify::NotifyState::Stopping]) {
            warn!(
                service = %self.service_name,
                error = %e,
                "Failed to notify systemd stopping state"
            );
        }

        // Give tasks time to complete gracefully
        tokio::time::sleep(self.shutdown_grace_period).await;

        self.set_status(ServiceStatus::Stopped);

        info!(service = %self.service_name, "Graceful shutdown completed");

        Ok(())
    }

    /// Create a shutdown future that completes when shutdown is requested
    pub fn shutdown_future(&self) -> impl std::future::Future<Output = ()> {
        let mut receiver = self.shutdown_watch_rx.clone();
        async move {
            if *receiver.borrow() {
                return;
            }
            let _ = receiver.changed().await;
        }
    }

    /// Get service metrics for monitoring
    pub fn get_metrics(&self) -> ServiceMetrics {
        ServiceMetrics {
            service_name: self.service_name.clone(),
            status: self.status(),
            uptime: self.started_at.elapsed(),
            shutdown_requested: self.is_shutdown_requested(),
        }
    }
}

/// Service metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceMetrics {
    pub service_name: String,
    pub status: ServiceStatus,
    pub uptime: std::time::Duration,
    pub shutdown_requested: bool,
}

/// Helper macro for creating a main function with lifecycle management
#[macro_export]
macro_rules! node_main {
    ($service_name:expr, $runner:expr) => {
        #[tokio::main]
        async fn main() -> Result<(), Box<dyn std::error::Error>> {
            use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
            use $crate::lifecycle::LifecycleManager;

            // Initialize logging
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                )
                .with(tracing_subscriber::fmt::layer())
                .init();

            // Create lifecycle manager with heartbeat enabled
            let mut lifecycle = LifecycleManager::new($service_name.to_string())
                .with_heartbeat(sinex_primitives::Seconds::from_secs(30)); // 30 second heartbeat interval
            lifecycle.initialize()?;

            // Run service with lifecycle management
            let result = lifecycle
                .run_with_health_check(
                    || async { $runner.await },
                    || async { true }, // Default health check always returns true
                )
                .await;

            // Graceful shutdown
            lifecycle.shutdown().await?;

            result.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        }
    };

    ($service_name:expr, $runner:expr, $health_check:expr) => {
        #[tokio::main]
        async fn main() -> Result<(), Box<dyn std::error::Error>> {
            use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
            use $crate::lifecycle::LifecycleManager;

            // Initialize logging
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                )
                .with(tracing_subscriber::fmt::layer())
                .init();

            // Create lifecycle manager with heartbeat enabled
            let mut lifecycle = LifecycleManager::new($service_name.to_string())
                .with_heartbeat(sinex_primitives::Seconds::from_secs(30)); // 30 second heartbeat interval
            lifecycle.initialize()?;

            // Run service with lifecycle management
            let result = lifecycle
                .run_with_health_check(|| async { $runner.await }, $health_check)
                .await;

            // Graceful shutdown
            lifecycle.shutdown().await?;

            result.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        }
    };

    ($service_name:expr, $runner:expr, $heartbeat_interval:expr) => {
        #[tokio::main]
        async fn main() -> Result<(), Box<dyn std::error::Error>> {
            use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
            use $crate::lifecycle::LifecycleManager;

            // Initialize logging
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                )
                .with(tracing_subscriber::fmt::layer())
                .init();

            // Create lifecycle manager with custom heartbeat interval
            let mut lifecycle = LifecycleManager::new($service_name.to_string())
                .with_heartbeat($heartbeat_interval);
            lifecycle.initialize()?;

            // Run service with lifecycle management
            let result = lifecycle
                .run_with_health_check(
                    || async { $runner.await },
                    || async { true }, // Default health check always returns true
                )
                .await;

            // Graceful shutdown
            lifecycle.shutdown().await?;

            result.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic;
    use std::time::Duration;
    use tokio::time::timeout;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn shutdown_future_notifies_without_polling() -> Result<(), Box<dyn std::error::Error>> {
        let manager = LifecycleManager::new("test-service".to_string());
        let mut wait_future = Box::pin(manager.shutdown_future());

        manager.shutdown_flag.store(true, Ordering::Relaxed);
        manager
            .shutdown_watch_tx
            .send(true)
            .expect("send shutdown signal");

        timeout(Duration::from_millis(10), &mut wait_future)
            .await
            .expect("shutdown future should resolve immediately");
        Ok(())
    }

    #[sinex_test]
    async fn status_lock_survives_panics() -> Result<(), Box<dyn std::error::Error>> {
        let manager = LifecycleManager::new("test-service".to_string());
        let status_handle = manager.status.clone();

        let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let _guard = status_handle.lock();
            panic!("poison status mutex");
        }));

        assert_eq!(manager.status(), ServiceStatus::Starting);
        manager.set_status(ServiceStatus::Running);
        assert_eq!(manager.status(), ServiceStatus::Running);
        Ok(())
    }
}
