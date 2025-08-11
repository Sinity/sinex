//! Service lifecycle management for satellite services

use crate::heartbeat::{HeartbeatCounterHandle, HeartbeatEmitter};
use crate::SatelliteResult;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
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

/// Lifecycle manager for satellite services
pub struct LifecycleManager {
    service_name: String,
    status: Arc<std::sync::Mutex<ServiceStatus>>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
    health_check_interval: tokio::time::Duration,
    heartbeat_emitter: Option<HeartbeatEmitter>,
    heartbeat_interval_seconds: u64,
}

impl LifecycleManager {
    /// Create a new lifecycle manager
    pub fn new(service_name: String) -> Self {
        Self {
            service_name,
            status: Arc::new(std::sync::Mutex::new(ServiceStatus::Starting)),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_sender: None,
            health_check_interval: tokio::time::Duration::from_secs(30),
            heartbeat_emitter: None,
            heartbeat_interval_seconds: 30, // Default 30 second heartbeats
        }
    }

    /// Set health check interval
    pub fn with_health_check_interval(mut self, interval: tokio::time::Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    /// Enable heartbeat emission with custom interval
    pub fn with_heartbeat(mut self, interval_seconds: u64) -> Self {
        self.heartbeat_interval_seconds = interval_seconds;
        self.heartbeat_emitter = Some(HeartbeatEmitter::new(
            self.service_name.clone(),
            interval_seconds,
        ));
        self
    }

    /// Get heartbeat counter handle for tracking metrics
    pub fn get_heartbeat_handle(&self) -> Option<HeartbeatCounterHandle> {
        self.heartbeat_emitter
            .as_ref()
            .map(|emitter| emitter.get_counter_handle())
    }

    /// Get current status
    pub fn status(&self) -> ServiceStatus {
        match self.status.lock() {
            Ok(guard) => *guard,
            Err(poisoned) => {
                warn!(
                    service = %self.service_name,
                    "Status mutex was poisoned, recovering from guarded data"
                );
                *poisoned.into_inner()
            }
        }
    }

    /// Set status
    pub fn set_status(&self, status: ServiceStatus) {
        match self.status.lock() {
            Ok(mut guard) => {
                *guard = status;
            }
            Err(mut poisoned) => {
                warn!(
                    service = %self.service_name,
                    "Status mutex was poisoned, recovering and setting status"
                );
                *poisoned.get_mut() = status;
            }
        }
        info!(
            service = %self.service_name,
            status = %status,
            "Service status changed"
        );

        // Notify systemd of status change
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
                "Failed to notify systemd of status change"
            );
        }
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_flag.load(Ordering::Relaxed)
    }

    /// Initialize signal handlers and lifecycle management
    pub async fn initialize(&mut self) -> SatelliteResult<()> {
        info!(service = %self.service_name, "Initializing lifecycle management");

        // Create shutdown channel
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        self.shutdown_sender = Some(shutdown_sender);

        // Set up signal handlers
        let shutdown_flag = self.shutdown_flag.clone();
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
    ) -> SatelliteResult<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = SatelliteResult<()>>,
        H: Fn() -> HFut + Send + Sync + 'static,
        HFut: std::future::Future<Output = bool> + Send,
    {
        self.set_status(ServiceStatus::Running);

        let shutdown_flag = self.shutdown_flag.clone();
        let status = self.status.clone();
        let service_name = self.service_name.clone();
        let health_interval = self.health_check_interval;

        // Start health check task
        let health_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(health_interval);

            loop {
                interval.tick().await;

                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }

                let healthy = health_check().await;
                if !healthy {
                    error!(service = %service_name, "Health check failed");
                    match status.lock() {
                        Ok(mut guard) => {
                            *guard = ServiceStatus::Failed;
                        }
                        Err(mut poisoned) => {
                            warn!(
                                service = %service_name,
                                "Status mutex was poisoned during health check failure, recovering"
                            );
                            *poisoned.get_mut() = ServiceStatus::Failed;
                        }
                    }

                    // Notify systemd of failure
                    let _ = sd_notify::notify(
                        false,
                        &[sd_notify::NotifyState::Status("Health check failed")],
                    );
                }
            }
        });

        // Start heartbeat task if heartbeat emitter is configured
        let heartbeat_task = if let Some(emitter) = &self.heartbeat_emitter {
            let emitter_clone = emitter.clone(); // We need to implement Clone for HeartbeatEmitter
            let shutdown_flag_clone = self.shutdown_flag.clone();
            let service_name_clone = self.service_name.clone();

            Some(tokio::spawn(async move {
                info!(service = %service_name_clone, "Starting heartbeat emission");

                // Create a metadata provider that includes current status
                let _metadata_provider: Box<dyn Fn() -> Option<serde_json::Value> + Send> =
                    Box::new(move || {
                        Some(serde_json::json!({
                            "service_type": "satellite",
                            "heartbeat_source": "lifecycle_manager"
                        }))
                    });

                // Start periodic heartbeat emission
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                    emitter_clone.interval_seconds,
                ));

                loop {
                    interval.tick().await;

                    if shutdown_flag_clone.load(Ordering::Relaxed) {
                        break;
                    }

                    emitter_clone.emit_heartbeat(Some(serde_json::json!({
                        "service_type": "satellite",
                        "heartbeat_source": "lifecycle_manager"
                    })));
                }

                info!(service = %service_name_clone, "Heartbeat emission stopped");
            }))
        } else {
            None
        };

        // Run main task
        let main_result = tokio::select! {
            result = main_task() => result,
            _ = tokio::signal::ctrl_c() => {
                info!(service = %self.service_name, "Received Ctrl+C");
                self.shutdown_flag.store(true, Ordering::Relaxed);
                Ok(())
            }
        };

        // Cancel background tasks
        health_task.abort();
        if let Some(heartbeat_task) = heartbeat_task {
            heartbeat_task.abort();
        }

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
    pub async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!(service = %self.service_name, "Initiating graceful shutdown");

        self.set_status(ServiceStatus::Stopping);
        self.shutdown_flag.store(true, Ordering::Relaxed);

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
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        self.set_status(ServiceStatus::Stopped);

        info!(service = %self.service_name, "Graceful shutdown completed");

        Ok(())
    }

    /// Create a shutdown future that completes when shutdown is requested
    pub fn shutdown_future(&self) -> impl std::future::Future<Output = ()> + '_ {
        let shutdown_flag = self.shutdown_flag.clone();
        async move {
            loop {
                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    /// Get service metrics for monitoring
    pub fn get_metrics(&self) -> ServiceMetrics {
        ServiceMetrics {
            service_name: self.service_name.clone(),
            status: self.status(),
            uptime: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default(),
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
macro_rules! satellite_main {
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
            let mut lifecycle = LifecycleManager::new($service_name.to_string()).with_heartbeat(30); // 30 second heartbeat interval
            lifecycle.initialize().await?;

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
            let mut lifecycle = LifecycleManager::new($service_name.to_string()).with_heartbeat(30); // 30 second heartbeat interval
            lifecycle.initialize().await?;

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
            lifecycle.initialize().await?;

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
