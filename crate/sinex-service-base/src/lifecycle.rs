//! Service Lifecycle Management
//!
//! Provides centralized lifecycle management for services including state transitions,
//! dependency resolution, and coordinated startup/shutdown sequences.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, Notify};
use tracing::{info, warn, error};

use crate::{Service, ServiceContext, ServiceError, ServiceResult, ServiceId, ServiceName};

/// Current lifecycle state of a service
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleState {
    /// Service is not yet initialized
    Created,
    /// Service is being initialized
    Initializing,
    /// Service is initialized but not started
    Initialized,
    /// Service is starting up
    Starting,
    /// Service is running normally
    Running,
    /// Service is stopping
    Stopping,
    /// Service is stopped
    Stopped,
    /// Service is in error state
    Error(String),
}

impl LifecycleState {
    /// Check if service is in a healthy running state
    pub fn is_healthy(&self) -> bool {
        matches!(self, LifecycleState::Running)
    }
    
    /// Check if service is available for operations
    pub fn is_available(&self) -> bool {
        matches!(self, LifecycleState::Initialized | LifecycleState::Running)
    }
    
    /// Check if service is transitioning between states
    pub fn is_transitioning(&self) -> bool {
        matches!(self, LifecycleState::Initializing | LifecycleState::Starting | LifecycleState::Stopping)
    }
}

/// Lifecycle events that can occur during service management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LifecycleEvent {
    /// Service initialization started
    InitializationStarted {
        service_id: ServiceId,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Service initialization completed
    InitializationCompleted {
        service_id: ServiceId,
        timestamp: chrono::DateTime<chrono::Utc>,
        duration_ms: u64,
    },
    /// Service startup started
    StartupStarted {
        service_id: ServiceId,
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Service startup completed
    StartupCompleted {
        service_id: ServiceId,
        timestamp: chrono::DateTime<chrono::Utc>,
        duration_ms: u64,
    },
    /// Service shutdown started
    ShutdownStarted {
        service_id: ServiceId,
        timestamp: chrono::DateTime<chrono::Utc>,
        graceful: bool,
    },
    /// Service shutdown completed
    ShutdownCompleted {
        service_id: ServiceId,
        timestamp: chrono::DateTime<chrono::Utc>,
        duration_ms: u64,
    },
    /// Service encountered an error
    ErrorOccurred {
        service_id: ServiceId,
        timestamp: chrono::DateTime<chrono::Utc>,
        error: String,
        previous_state: LifecycleState,
    },
    /// Service state changed
    StateChanged {
        service_id: ServiceId,
        timestamp: chrono::DateTime<chrono::Utc>,
        from_state: LifecycleState,
        to_state: LifecycleState,
    },
}

/// Trait for listening to lifecycle events
#[async_trait]
pub trait LifecycleListener: Send + Sync {
    /// Handle a lifecycle event
    async fn on_event(&self, event: LifecycleEvent);
}

/// Manages the lifecycle of a single service
pub struct ServiceLifecycle {
    service_id: ServiceId,
    service_name: ServiceName,
    service: Box<dyn Service>,
    state: Arc<RwLock<LifecycleState>>,
    context: Option<ServiceContext>,
    listeners: Vec<Box<dyn LifecycleListener>>,
    state_change_notify: Arc<Notify>,
}

impl ServiceLifecycle {
    /// Create a new service lifecycle manager
    pub fn new(service_id: ServiceId, service_name: ServiceName, service: Box<dyn Service>) -> Self {
        Self {
            service_id,
            service_name,
            service,
            state: Arc::new(RwLock::new(LifecycleState::Created)),
            context: None,
            listeners: Vec::new(),
            state_change_notify: Arc::new(Notify::new()),
        }
    }
    
    /// Add a lifecycle listener
    pub fn add_listener(&mut self, listener: Box<dyn LifecycleListener>) {
        self.listeners.push(listener);
    }
    
    /// Get current service state
    pub async fn state(&self) -> LifecycleState {
        self.state.read().await.clone()
    }
    
    /// Wait for service to reach a specific state
    pub async fn wait_for_state(&self, target_state: LifecycleState) -> ServiceResult<()> {
        loop {
            let current_state = self.state().await;
            if current_state == target_state {
                return Ok(());
            }
            
            // If service is in error state and we're not waiting for error, fail
            if matches!(current_state, LifecycleState::Error(_)) && !matches!(target_state, LifecycleState::Error(_)) {
                return Err(ServiceError::Runtime(format!("Service entered error state: {:?}", current_state)));
            }
            
            // Wait for state change notification
            self.state_change_notify.notified().await;
        }
    }
    
    /// Initialize the service
    pub async fn initialize(&mut self, context: ServiceContext) -> ServiceResult<()> {
        let current_state = self.state().await;
        if !matches!(current_state, LifecycleState::Created) {
            return Err(ServiceError::Initialization(format!(
                "Cannot initialize service in state: {:?}", current_state
            )));
        }
        
        self.change_state(LifecycleState::Initializing).await;
        self.emit_event(LifecycleEvent::InitializationStarted {
            service_id: self.service_id.clone(),
            timestamp: chrono::Utc::now(),
        }).await;
        
        let start_time = std::time::Instant::now();
        
        match self.service.initialize(context.clone()).await {
            Ok(()) => {
                self.context = Some(context);
                self.change_state(LifecycleState::Initialized).await;
                
                let duration_ms = start_time.elapsed().as_millis() as u64;
                self.emit_event(LifecycleEvent::InitializationCompleted {
                    service_id: self.service_id.clone(),
                    timestamp: chrono::Utc::now(),
                    duration_ms,
                }).await;
                
                info!(
                    service_id = %self.service_id,
                    service_name = %self.service_name,
                    duration_ms = duration_ms,
                    "Service initialized successfully"
                );
                
                Ok(())
            }
            Err(e) => {
                let error_msg = e.to_string();
                self.change_state(LifecycleState::Error(error_msg.clone())).await;
                
                self.emit_event(LifecycleEvent::ErrorOccurred {
                    service_id: self.service_id.clone(),
                    timestamp: chrono::Utc::now(),
                    error: error_msg.clone(),
                    previous_state: LifecycleState::Initializing,
                }).await;
                
                error!(
                    service_id = %self.service_id,
                    service_name = %self.service_name,
                    error = %error_msg,
                    "Service initialization failed"
                );
                
                Err(e)
            }
        }
    }
    
    /// Start the service
    pub async fn start(&mut self) -> ServiceResult<()> {
        let current_state = self.state().await;
        if !matches!(current_state, LifecycleState::Initialized) {
            return Err(ServiceError::Startup(format!(
                "Cannot start service in state: {:?}", current_state
            )));
        }
        
        self.change_state(LifecycleState::Starting).await;
        self.emit_event(LifecycleEvent::StartupStarted {
            service_id: self.service_id.clone(),
            timestamp: chrono::Utc::now(),
        }).await;
        
        let start_time = std::time::Instant::now();
        
        match self.service.start().await {
            Ok(()) => {
                self.change_state(LifecycleState::Running).await;
                
                let duration_ms = start_time.elapsed().as_millis() as u64;
                self.emit_event(LifecycleEvent::StartupCompleted {
                    service_id: self.service_id.clone(),
                    timestamp: chrono::Utc::now(),
                    duration_ms,
                }).await;
                
                info!(
                    service_id = %self.service_id,
                    service_name = %self.service_name,
                    duration_ms = duration_ms,
                    "Service started successfully"
                );
                
                Ok(())
            }
            Err(e) => {
                let error_msg = e.to_string();
                self.change_state(LifecycleState::Error(error_msg.clone())).await;
                
                self.emit_event(LifecycleEvent::ErrorOccurred {
                    service_id: self.service_id.clone(),
                    timestamp: chrono::Utc::now(),
                    error: error_msg.clone(),
                    previous_state: LifecycleState::Starting,
                }).await;
                
                error!(
                    service_id = %self.service_id,
                    service_name = %self.service_name,
                    error = %error_msg,
                    "Service startup failed"
                );
                
                Err(e)
            }
        }
    }
    
    /// Stop the service gracefully
    pub async fn stop(&mut self, graceful: bool) -> ServiceResult<()> {
        let current_state = self.state().await;
        if matches!(current_state, LifecycleState::Stopped | LifecycleState::Stopping) {
            return Ok(());
        }
        
        self.change_state(LifecycleState::Stopping).await;
        self.emit_event(LifecycleEvent::ShutdownStarted {
            service_id: self.service_id.clone(),
            timestamp: chrono::Utc::now(),
            graceful,
        }).await;
        
        let start_time = std::time::Instant::now();
        
        match self.service.stop().await {
            Ok(()) => {
                self.change_state(LifecycleState::Stopped).await;
                
                let duration_ms = start_time.elapsed().as_millis() as u64;
                self.emit_event(LifecycleEvent::ShutdownCompleted {
                    service_id: self.service_id.clone(),
                    timestamp: chrono::Utc::now(),
                    duration_ms,
                }).await;
                
                info!(
                    service_id = %self.service_id,
                    service_name = %self.service_name,
                    duration_ms = duration_ms,
                    graceful = graceful,
                    "Service stopped successfully"
                );
                
                Ok(())
            }
            Err(e) => {
                let error_msg = e.to_string();
                warn!(
                    service_id = %self.service_id,
                    service_name = %self.service_name,
                    error = %error_msg,
                    "Service shutdown encountered error, marking as stopped anyway"
                );
                
                // Mark as stopped even if shutdown had errors
                self.change_state(LifecycleState::Stopped).await;
                
                let duration_ms = start_time.elapsed().as_millis() as u64;
                self.emit_event(LifecycleEvent::ShutdownCompleted {
                    service_id: self.service_id.clone(),
                    timestamp: chrono::Utc::now(),
                    duration_ms,
                }).await;
                
                Ok(())
            }
        }
    }
    
    /// Get service reference
    pub fn service(&self) -> &dyn Service {
        self.service.as_ref()
    }
    
    /// Get service context
    pub fn context(&self) -> Option<&ServiceContext> {
        self.context.as_ref()
    }
    
    async fn change_state(&self, new_state: LifecycleState) {
        let old_state = {
            let mut state = self.state.write().await;
            let old_state = state.clone();
            *state = new_state.clone();
            old_state
        };
        
        if old_state != new_state {
            self.emit_event(LifecycleEvent::StateChanged {
                service_id: self.service_id.clone(),
                timestamp: chrono::Utc::now(),
                from_state: old_state,
                to_state: new_state,
            }).await;
            
            self.state_change_notify.notify_waiters();
        }
    }
    
    async fn emit_event(&self, event: LifecycleEvent) {
        for listener in &self.listeners {
            listener.on_event(event.clone()).await;
        }
    }
}

/// Manages multiple service lifecycles with dependency resolution
pub struct LifecycleManager {
    services: HashMap<ServiceId, ServiceLifecycle>,
    dependencies: HashMap<ServiceId, HashSet<ServiceId>>,
    listeners: Vec<Box<dyn LifecycleListener>>,
}

impl LifecycleManager {
    /// Create a new lifecycle manager
    pub fn new() -> Self {
        Self {
            services: HashMap::new(),
            dependencies: HashMap::new(),
            listeners: Vec::new(),
        }
    }
    
    /// Add a service to the manager
    pub fn add_service(
        &mut self,
        service_id: ServiceId,
        service_name: ServiceName,
        service: Box<dyn Service>,
        dependencies: Vec<ServiceId>,
    ) -> ServiceResult<()> {
        if self.services.contains_key(&service_id) {
            return Err(ServiceError::Configuration(format!(
                "Service {} already exists", service_id
            )));
        }
        
        let lifecycle = ServiceLifecycle::new(service_id.clone(), service_name, service);
        
        // Add existing listeners to the new service
        for _listener in &self.listeners {
            // Note: We can't clone Box<dyn LifecycleListener> directly
            // In a real implementation, you might need a different approach
            // such as Arc<dyn LifecycleListener> or a listener registry
        }
        
        self.services.insert(service_id.clone(), lifecycle);
        self.dependencies.insert(service_id, dependencies.into_iter().collect());
        
        Ok(())
    }
    
    /// Add a global lifecycle listener
    pub fn add_listener(&mut self, listener: Box<dyn LifecycleListener>) {
        // Add to existing services
        for _service in self.services.values_mut() {
            // Note: Same issue as above - can't clone Box<dyn LifecycleListener>
        }
        
        self.listeners.push(listener);
    }
    
    /// Initialize all services in dependency order
    pub async fn initialize_all(&mut self) -> ServiceResult<()> {
        let startup_order = self.calculate_startup_order()?;
        
        for service_id in startup_order {
            if let Some(service) = self.services.get_mut(&service_id) {
                let context = ServiceContext::new(service.service_name.clone());
                service.initialize(context).await?;
            }
        }
        
        Ok(())
    }
    
    /// Start all services in dependency order
    pub async fn start_all(&mut self) -> ServiceResult<()> {
        let startup_order = self.calculate_startup_order()?;
        
        for service_id in startup_order {
            if let Some(service) = self.services.get_mut(&service_id) {
                service.start().await?;
            }
        }
        
        Ok(())
    }
    
    /// Stop all services in reverse dependency order
    pub async fn stop_all(&mut self, graceful: bool) -> ServiceResult<()> {
        let mut startup_order = self.calculate_startup_order()?;
        startup_order.reverse(); // Stop in reverse order
        
        for service_id in startup_order {
            if let Some(service) = self.services.get_mut(&service_id) {
                let _ = service.stop(graceful).await; // Continue even if stop fails
            }
        }
        
        Ok(())
    }
    
    /// Get service state
    pub async fn service_state(&self, service_id: &ServiceId) -> Option<LifecycleState> {
        self.services.get(service_id)?.state().await.into()
    }
    
    /// Wait for service to reach specific state
    pub async fn wait_for_service_state(&self, service_id: &ServiceId, state: LifecycleState) -> ServiceResult<()> {
        match self.services.get(service_id) {
            Some(service) => service.wait_for_state(state).await,
            None => Err(ServiceError::Configuration(format!("Service {} not found", service_id))),
        }
    }
    
    /// Get all service states
    pub async fn all_service_states(&self) -> HashMap<ServiceId, LifecycleState> {
        let mut states = HashMap::new();
        for (service_id, service) in &self.services {
            states.insert(service_id.clone(), service.state().await);
        }
        states
    }
    
    fn calculate_startup_order(&self) -> ServiceResult<Vec<ServiceId>> {
        let mut order = Vec::new();
        let mut visited = HashSet::new();
        let mut visiting = HashSet::new();
        
        for service_id in self.services.keys() {
            if !visited.contains(service_id) {
                self.visit_service(service_id, &mut order, &mut visited, &mut visiting)?;
            }
        }
        
        Ok(order)
    }
    
    fn visit_service(
        &self,
        service_id: &ServiceId,
        order: &mut Vec<ServiceId>,
        visited: &mut HashSet<ServiceId>,
        visiting: &mut HashSet<ServiceId>,
    ) -> ServiceResult<()> {
        if visiting.contains(service_id) {
            return Err(ServiceError::Dependency(format!(
                "Circular dependency detected involving service: {}", service_id
            )));
        }
        
        if visited.contains(service_id) {
            return Ok(());
        }
        
        visiting.insert(service_id.clone());
        
        if let Some(dependencies) = self.dependencies.get(service_id) {
            for dep_id in dependencies {
                if !self.services.contains_key(dep_id) {
                    return Err(ServiceError::Dependency(format!(
                        "Service {} depends on unknown service: {}", service_id, dep_id
                    )));
                }
                self.visit_service(dep_id, order, visited, visiting)?;
            }
        }
        
        visiting.remove(service_id);
        visited.insert(service_id.clone());
        order.push(service_id.clone());
        
        Ok(())
    }
}

impl Default for LifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}