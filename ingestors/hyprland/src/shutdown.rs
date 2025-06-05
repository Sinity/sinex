#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{broadcast, RwLock};
use tracing::{info, warn, error};

use crate::error::{IngestorError, Result};

/// Shutdown coordinator for graceful application termination
#[derive(Clone)]
pub struct ShutdownCoordinator {
    shutdown_tx: broadcast::Sender<ShutdownSignal>,
    is_shutting_down: Arc<RwLock<bool>>,
    timeout: Duration,
}

/// Shutdown signal with optional reason
#[derive(Debug, Clone)]
pub struct ShutdownSignal {
    pub reason: String,
    pub force: bool,
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator
    pub fn new(timeout_secs: u64) -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);
        
        Self {
            shutdown_tx,
            is_shutting_down: Arc::new(RwLock::new(false)),
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Get a receiver for shutdown signals
    pub fn subscribe(&self) -> broadcast::Receiver<ShutdownSignal> {
        self.shutdown_tx.subscribe()
    }

    /// Check if shutdown has been initiated
    pub async fn is_shutting_down(&self) -> bool {
        *self.is_shutting_down.read().await
    }

    /// Initiate graceful shutdown
    pub async fn shutdown(&self, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        info!("Initiating graceful shutdown: {}", reason);

        {
            let mut is_shutting_down = self.is_shutting_down.write().await;
            if *is_shutting_down {
                warn!("Shutdown already in progress");
                return Ok(());
            }
            *is_shutting_down = true;
        }

        let signal = ShutdownSignal {
            reason,
            force: false,
        };

        if let Err(e) = self.shutdown_tx.send(signal) {
            error!("Failed to send shutdown signal: {}", e);
            return Err(IngestorError::application(format!(
                "Failed to send shutdown signal: {}", e
            )));
        }

        Ok(())
    }

    /// Initiate forced shutdown
    pub async fn force_shutdown(&self, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        warn!("Initiating forced shutdown: {}", reason);

        {
            let mut is_shutting_down = self.is_shutting_down.write().await;
            *is_shutting_down = true;
        }

        let signal = ShutdownSignal {
            reason,
            force: true,
        };

        if let Err(e) = self.shutdown_tx.send(signal) {
            error!("Failed to send forced shutdown signal: {}", e);
            return Err(IngestorError::application(format!(
                "Failed to send forced shutdown signal: {}", e
            )));
        }

        Ok(())
    }

    /// Wait for system signals and initiate shutdown
    pub async fn wait_for_signal(&self) {
        let coordinator = self.clone();
        
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
                    .expect("Failed to register SIGINT handler");
                let mut sighup = signal::unix::signal(signal::unix::SignalKind::hangup())
                    .expect("Failed to register SIGHUP handler");

                tokio::select! {
                    _ = sigterm.recv() => {
                        info!("Received SIGTERM");
                        let _ = coordinator.shutdown("SIGTERM received").await;
                    }
                    _ = sigint.recv() => {
                        info!("Received SIGINT");
                        let _ = coordinator.shutdown("SIGINT received").await;
                    }
                    _ = sighup.recv() => {
                        info!("Received SIGHUP");
                        let _ = coordinator.shutdown("SIGHUP received").await;
                    }
                }
            }

            #[cfg(windows)]
            {
                let _ = signal::ctrl_c().await;
                info!("Received Ctrl+C");
                let _ = coordinator.shutdown("Ctrl+C received").await;
            }
        });
    }

    /// Wait for shutdown with timeout
    pub async fn wait_for_shutdown(&self) -> Result<ShutdownSignal> {
        let mut receiver = self.subscribe();
        
        tokio::select! {
            result = receiver.recv() => {
                match result {
                    Ok(signal) => {
                        info!("Shutdown signal received: {}", signal.reason);
                        Ok(signal)
                    }
                    Err(e) => {
                        error!("Failed to receive shutdown signal: {}", e);
                        Err(IngestorError::application(format!(
                            "Failed to receive shutdown signal: {}", e
                        )))
                    }
                }
            }
            _ = tokio::time::sleep(self.timeout) => {
                warn!("Shutdown timeout reached, forcing shutdown");
                let signal = ShutdownSignal {
                    reason: "Shutdown timeout".to_string(),
                    force: true,
                };
                Ok(signal)
            }
        }
    }

    /// Get the configured timeout duration
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Shutdown manager for coordinating component shutdowns
pub struct ShutdownManager {
    coordinator: ShutdownCoordinator,
    components: Vec<Box<dyn ShutdownComponent + Send + Sync>>,
}

/// Trait for components that need graceful shutdown
#[async_trait::async_trait]
pub trait ShutdownComponent {
    /// Component name for logging
    fn name(&self) -> &str;
    
    /// Gracefully shutdown the component
    async fn shutdown(&mut self) -> Result<()>;
    
    /// Force shutdown the component (should be quick)
    async fn force_shutdown(&mut self) -> Result<()> {
        self.shutdown().await
    }
}

impl ShutdownManager {
    /// Create a new shutdown manager
    pub fn new(coordinator: ShutdownCoordinator) -> Self {
        Self {
            coordinator,
            components: Vec::new(),
        }
    }

    /// Register a component for graceful shutdown
    pub fn register_component(&mut self, component: Box<dyn ShutdownComponent + Send + Sync>) {
        info!("Registering component for shutdown: {}", component.name());
        self.components.push(component);
    }

    /// Execute graceful shutdown of all components
    pub async fn execute_shutdown(&mut self, signal: ShutdownSignal) -> Result<()> {
        info!(
            "Executing {} shutdown of {} components: {}",
            if signal.force { "forced" } else { "graceful" },
            self.components.len(),
            signal.reason
        );

        let timeout = if signal.force {
            Duration::from_secs(5) // Short timeout for forced shutdown
        } else {
            self.coordinator.timeout()
        };

        // Shutdown components in reverse order of registration
        for component in self.components.iter_mut().rev() {
            let component_name = component.name().to_string();
            info!("Shutting down component: {}", component_name);

            let shutdown_result = if signal.force {
                tokio::time::timeout(timeout, component.force_shutdown()).await
            } else {
                tokio::time::timeout(timeout, component.shutdown()).await
            };

            match shutdown_result {
                Ok(Ok(())) => {
                    info!("Component shutdown completed: {}", component_name);
                }
                Ok(Err(e)) => {
                    error!("Component shutdown failed: {}: {}", component_name, e);
                    if !signal.force {
                        warn!("Continuing with shutdown despite component failure");
                    }
                }
                Err(_) => {
                    error!("Component shutdown timed out: {}", component_name);
                    if !signal.force {
                        warn!("Continuing with shutdown despite timeout");
                    }
                }
            }
        }

        info!("All components shutdown completed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestComponent {
        name: String,
        shutdown_called: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ShutdownComponent for TestComponent {
        fn name(&self) -> &str {
            &self.name
        }

        async fn shutdown(&mut self) -> Result<()> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_shutdown_coordinator() {
        let coordinator = ShutdownCoordinator::new(10);
        
        assert!(!coordinator.is_shutting_down().await);
        
        coordinator.shutdown("test").await.unwrap();
        
        assert!(coordinator.is_shutting_down().await);
    }

    #[tokio::test]
    async fn test_shutdown_manager() {
        let coordinator = ShutdownCoordinator::new(10);
        let mut manager = ShutdownManager::new(coordinator);

        let shutdown_called = Arc::new(AtomicBool::new(false));
        let component = TestComponent {
            name: "test-component".to_string(),
            shutdown_called: Arc::clone(&shutdown_called),
        };

        manager.register_component(Box::new(component));

        let signal = ShutdownSignal {
            reason: "test shutdown".to_string(),
            force: false,
        };

        manager.execute_shutdown(signal).await.unwrap();
        
        assert!(shutdown_called.load(Ordering::SeqCst));
    }
}