//! Shell Integration Library
//!
//! This crate provides high-level utilities for integrating Sinex with various
//! shell environments, building on the foundation provided by sinex-events-shell.
//!
//! Key features:
//! - Shell detection and configuration
//! - Hook installation and management
//! - Session tracking and correlation
//! - Environment setup and teardown

pub mod shell_detector;
pub mod hook_manager;
pub mod session_tracker;
pub mod environment_setup;

// Re-export key types from sinex-events-shell
pub use sinex_events_shell::{
    ShellCommandInfo, ShellConfig, AtuinCommandExecuted, ShellHistoryCommand,
    CommandExecuted, CommandCompleted,
};

// Re-export integration utilities
pub use shell_detector::{ShellType, ShellDetector, ShellInfo};
pub use hook_manager::{HookManager, HookType, HookInstallation, IntegrationConfig};
pub use session_tracker::{SessionTracker, SessionInfo, SessionEvent};
pub use environment_setup::EnvironmentSetup;

/// Main integration facade providing a unified interface for shell integration
pub struct ShellIntegration {
    shell_info: ShellInfo,
    hook_manager: HookManager,
    session_tracker: SessionTracker,
    environment_setup: EnvironmentSetup,
}

impl ShellIntegration {
    /// Create a new shell integration instance for the current environment
    pub async fn new() -> sinex_core::Result<Self> {
        let shell_info = ShellDetector::detect_current_shell()?;
        let hook_manager = HookManager::new(&shell_info)?;
        let session_tracker = SessionTracker::new();
        let environment_setup = EnvironmentSetup::new(&shell_info)?;
        
        Ok(Self {
            shell_info,
            hook_manager,
            session_tracker,
            environment_setup,
        })
    }
    
    /// Install shell hooks for event capture
    pub async fn install_hooks(&mut self, config: &IntegrationConfig) -> sinex_core::Result<()> {
        self.hook_manager.install_all_hooks(config).await?;
        self.environment_setup.configure_environment(config).await?;
        Ok(())
    }
    
    /// Uninstall shell hooks
    pub async fn uninstall_hooks(&mut self) -> sinex_core::Result<()> {
        self.hook_manager.uninstall_all_hooks().await?;
        self.environment_setup.cleanup_environment().await?;
        Ok(())
    }
    
    /// Start a new shell session
    pub async fn start_session(&mut self, session_id: Option<String>) -> sinex_core::Result<String> {
        self.session_tracker.start_session(session_id, &self.shell_info).await
    }
    
    /// End the current shell session
    pub async fn end_session(&mut self, session_id: &str) -> sinex_core::Result<()> {
        self.session_tracker.end_session(session_id).await
    }
    
    /// Get information about the current shell
    pub fn shell_info(&self) -> &ShellInfo {
        &self.shell_info
    }
    
    /// Check if hooks are properly installed
    pub async fn verify_installation(&self) -> sinex_core::Result<bool> {
        self.hook_manager.verify_hooks().await
    }
}