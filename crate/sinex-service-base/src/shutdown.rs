//! Graceful Shutdown Management
//!
//! Provides utilities for handling graceful shutdown of services including
//! signal handling, shutdown coordination, and timeout management.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock, Notify};
use tokio::time::timeout;
use tracing::{info, warn, error};

use crate::{ServiceResult, ServiceError};

/// Type alias for shutdown function
type ShutdownFunction = Box<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ServiceResult<()>> + Send>> + Send + Sync>;

/// Shutdown signal types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShutdownSignal {
    /// Graceful shutdown requested (SIGTERM)
    Graceful,
    /// Immediate shutdown requested (SIGINT/Ctrl+C)
    Immediate,
    /// Force shutdown requested (SIGKILL equivalent)
    Force,
    /// Custom shutdown signal
    Custom(String),
}

impl ShutdownSignal {
    /// Check if this is a graceful shutdown
    pub fn is_graceful(&self) -> bool {
        matches!(self, ShutdownSignal::Graceful)
    }
    
    /// Check if this requires immediate action
    pub fn is_immediate(&self) -> bool {
        matches!(self, ShutdownSignal::Immediate | ShutdownSignal::Force)
    }
}

/// Shutdown reason and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownRequest {
    /// The shutdown signal that triggered this request
    pub signal: ShutdownSignal,
    /// Human-readable reason for shutdown
    pub reason: String,
    /// When the shutdown was requested
    pub requested_at: chrono::DateTime<chrono::Utc>,
    /// Who or what requested the shutdown
    pub requested_by: String,
    /// Additional metadata
    pub metadata: std::collections::HashMap<String, serde_json::Value>,
}

impl ShutdownRequest {
    /// Create a new shutdown request
    pub fn new(signal: ShutdownSignal, reason: impl Into<String>) -> Self {
        Self {
            signal,
            reason: reason.into(),
            requested_at: chrono::Utc::now(),
            requested_by: "system".to_string(),
            metadata: std::collections::HashMap::new(),
        }
    }
    
    /// Set who requested the shutdown
    pub fn requested_by(mut self, requester: impl Into<String>) -> Self {
        self.requested_by = requester.into();
        self
    }
    
    /// Add metadata to the shutdown request
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

/// Trait for components that need to handle graceful shutdown
#[async_trait::async_trait]
pub trait GracefulShutdown: Send + Sync {
    /// Component name for logging
    fn component_name(&self) -> &str;
    
    /// Gracefully shutdown this component
    async fn graceful_shutdown(&self) -> ServiceResult<()>;
    
    /// Get the timeout for graceful shutdown
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }
    
    /// Priority for shutdown order (lower numbers shut down first)
    fn shutdown_priority(&self) -> u32 {
        100
    }
}

/// Manages graceful shutdown for multiple components
pub struct ShutdownManager {
    components: Arc<RwLock<Vec<Box<dyn GracefulShutdown>>>>,
    shutdown_sender: broadcast::Sender<ShutdownRequest>,
    #[allow(dead_code)]
    shutdown_receiver: broadcast::Receiver<ShutdownRequest>,
    shutdown_complete: Arc<Notify>,
    is_shutting_down: Arc<RwLock<bool>>,
    shutdown_timeout: Duration,
}

impl ShutdownManager {
    /// Create a new shutdown manager
    pub fn new() -> Self {
        let (shutdown_sender, shutdown_receiver) = broadcast::channel(16);
        
        Self {
            components: Arc::new(RwLock::new(Vec::new())),
            shutdown_sender,
            shutdown_receiver,
            shutdown_complete: Arc::new(Notify::new()),
            is_shutting_down: Arc::new(RwLock::new(false)),
            shutdown_timeout: Duration::from_secs(60),
        }
    }
    
    /// Set global shutdown timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }
    
    /// Register a component for graceful shutdown
    pub async fn register_component(&self, component: Box<dyn GracefulShutdown>) {
        let mut components = self.components.write().await;
        components.push(component);
        
        // Sort by shutdown priority (lower priority shuts down first)
        components.sort_by_key(|c| c.shutdown_priority());
    }
    
    /// Get a shutdown receiver for listening to shutdown signals
    pub fn subscribe(&self) -> broadcast::Receiver<ShutdownRequest> {
        self.shutdown_sender.subscribe()
    }
    
    /// Check if shutdown is in progress
    pub async fn is_shutting_down(&self) -> bool {
        *self.is_shutting_down.read().await
    }
    
    /// Request graceful shutdown
    pub async fn request_shutdown(&self, request: ShutdownRequest) -> ServiceResult<()> {
        // Check if already shutting down
        {
            let mut shutting_down = self.is_shutting_down.write().await;
            if *shutting_down {
                warn!("Shutdown already in progress, ignoring new request");
                return Ok(());
            }
            *shutting_down = true;
        }
        
        info!(
            signal = ?request.signal,
            reason = %request.reason,
            requested_by = %request.requested_by,
            "Shutdown requested"
        );
        
        // Broadcast shutdown request
        if let Err(e) = self.shutdown_sender.send(request.clone()) {
            error!("Failed to broadcast shutdown request: {}", e);
        }
        
        // Perform the actual shutdown
        self.perform_shutdown(request).await
    }
    
    /// Wait for shutdown to complete
    pub async fn wait_for_shutdown(&self) {
        if self.is_shutting_down().await {
            self.shutdown_complete.notified().await;
        }
    }
    
    /// Setup signal handlers for common shutdown signals
    pub async fn setup_signal_handlers(&self) -> ServiceResult<()> {
        let shutdown_manager = self.clone();
        
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                
                let mut sigterm = signal(SignalKind::terminate())
                    .map_err(|e| ServiceError::Runtime(format!("Failed to setup SIGTERM handler: {}", e)))?;
                let mut sigint = signal(SignalKind::interrupt())
                    .map_err(|e| ServiceError::Runtime(format!("Failed to setup SIGINT handler: {}", e)))?;
                
                tokio::select! {
                    _ = sigterm.recv() => {
                        let request = ShutdownRequest::new(ShutdownSignal::Graceful, "SIGTERM received")
                            .requested_by("signal_handler");
                        let _ = shutdown_manager.request_shutdown(request).await;
                    }
                    _ = sigint.recv() => {
                        let request = ShutdownRequest::new(ShutdownSignal::Immediate, "SIGINT received")
                            .requested_by("signal_handler");
                        let _ = shutdown_manager.request_shutdown(request).await;
                    }
                }
            }
            
            #[cfg(windows)]
            {
                use tokio::signal::ctrl_c;
                
                if let Ok(_) = ctrl_c().await {
                    let request = ShutdownRequest::new(ShutdownSignal::Immediate, "Ctrl+C received")
                        .requested_by("signal_handler");
                    let _ = shutdown_manager.request_shutdown(request).await;
                }
            }
            
            ServiceResult::<()>::Ok(())
        });
        
        Ok(())
    }
    
    async fn perform_shutdown(&self, request: ShutdownRequest) -> ServiceResult<()> {
        let components = self.components.read().await;
        let component_count = components.len();
        
        info!(
            component_count = component_count,
            timeout_seconds = self.shutdown_timeout.as_secs(),
            "Starting graceful shutdown of {} components",
            component_count
        );
        
        let shutdown_start = std::time::Instant::now();
        let is_graceful = request.signal.is_graceful();
        
        // Shutdown components in priority order
        for component in components.iter() {
            let component_name = component.component_name();
            let component_timeout = if is_graceful {
                component.shutdown_timeout()
            } else {
                Duration::from_secs(5) // Shorter timeout for immediate shutdown
            };
            
            info!(
                component = component_name,
                timeout_seconds = component_timeout.as_secs(),
                "Shutting down component"
            );
            
            let component_start = std::time::Instant::now();
            
            match timeout(component_timeout, component.graceful_shutdown()).await {
                Ok(Ok(())) => {
                    let duration = component_start.elapsed();
                    info!(
                        component = component_name,
                        duration_ms = duration.as_millis(),
                        "Component shutdown completed"
                    );
                }
                Ok(Err(e)) => {
                    warn!(
                        component = component_name,
                        error = %e,
                        "Component shutdown failed"
                    );
                }
                Err(_) => {
                    warn!(
                        component = component_name,
                        timeout_seconds = component_timeout.as_secs(),
                        "Component shutdown timed out"
                    );
                }
            }
        }
        
        let total_duration = shutdown_start.elapsed();
        info!(
            component_count = component_count,
            total_duration_ms = total_duration.as_millis(),
            "Graceful shutdown completed"
        );
        
        // Notify that shutdown is complete
        self.shutdown_complete.notify_waiters();
        
        Ok(())
    }
}

impl Clone for ShutdownManager {
    fn clone(&self) -> Self {
        Self {
            components: Arc::clone(&self.components),
            shutdown_sender: self.shutdown_sender.clone(),
            shutdown_receiver: self.shutdown_sender.subscribe(),
            shutdown_complete: Arc::clone(&self.shutdown_complete),
            is_shutting_down: Arc::clone(&self.is_shutting_down),
            shutdown_timeout: self.shutdown_timeout,
        }
    }
}

impl Default for ShutdownManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple graceful shutdown implementation for functions
pub struct FunctionShutdown {
    name: String,
    shutdown_fn: ShutdownFunction,
    timeout: Duration,
    priority: u32,
}

impl FunctionShutdown {
    /// Create a new function-based shutdown handler
    pub fn new<F, Fut>(name: impl Into<String>, shutdown_fn: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ServiceResult<()>> + Send + 'static,
    {
        Self {
            name: name.into(),
            shutdown_fn: Box::new(move || Box::pin(shutdown_fn())),
            timeout: Duration::from_secs(30),
            priority: 100,
        }
    }
    
    /// Set shutdown timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    
    /// Set shutdown priority
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }
}

#[async_trait::async_trait]
impl GracefulShutdown for FunctionShutdown {
    fn component_name(&self) -> &str {
        &self.name
    }
    
    async fn graceful_shutdown(&self) -> ServiceResult<()> {
        (self.shutdown_fn)().await
    }
    
    fn shutdown_timeout(&self) -> Duration {
        self.timeout
    }
    
    fn shutdown_priority(&self) -> u32 {
        self.priority
    }
}

/// Utility for creating shutdown handlers
pub mod util {
    use super::*;
    
    /// Create a shutdown handler that just logs
    pub fn log_shutdown(component_name: impl Into<String>) -> FunctionShutdown {
        let name = component_name.into();
        let name_clone = name.clone();
        
        FunctionShutdown::new(name, move || {
            let name = name_clone.clone();
            async move {
                info!("Shutting down component: {}", name);
                Ok(())
            }
        })
    }
    
    /// Create a shutdown handler that sends a message to a channel
    pub fn channel_shutdown<T>(
        component_name: impl Into<String>,
        sender: tokio::sync::mpsc::Sender<T>,
        message: T,
    ) -> FunctionShutdown
    where
        T: Send + Sync + Clone + 'static,
    {
        let name = component_name.into();
        
        FunctionShutdown::new(name, move || {
            let sender = sender.clone();
            let message = message.clone();
            async move {
                sender.send(message).await
                    .map_err(|_| ServiceError::Runtime("Failed to send shutdown message".to_string()))?;
                Ok(())
            }
        })
    }
    
    /// Create a shutdown handler that notifies a notification
    pub fn notify_shutdown(
        component_name: impl Into<String>,
        notify: Arc<tokio::sync::Notify>,
    ) -> FunctionShutdown {
        let name = component_name.into();
        
        FunctionShutdown::new(name, move || {
            let notify = notify.clone();
            async move {
                notify.notify_waiters();
                Ok(())
            }
        })
    }
}