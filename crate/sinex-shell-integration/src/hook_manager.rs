//! Shell Hook Management
//!
//! This module provides utilities for installing and managing shell hooks
//! that enable real-time command tracking and integration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;
use tracing::{debug, info, warn};

use crate::shell_detector::{ShellInfo, ShellType};

/// Types of shell hooks that can be installed
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HookType {
    /// Hook executed before each command
    PreCommand,
    /// Hook executed after each command
    PostCommand,
    /// Hook executed when changing directories
    ChangeDirectory,
    /// Hook executed when starting a new session
    SessionStart,
    /// Hook executed when ending a session
    SessionEnd,
    /// Hook for command completion
    CommandCompletion,
}

/// Information about an installed hook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookInstallation {
    pub hook_type: HookType,
    pub shell_type: ShellType,
    pub installed_at: chrono::DateTime<chrono::Utc>,
    pub config_file: PathBuf,
    pub backup_file: Option<PathBuf>,
    pub hook_content: String,
}

/// Configuration for shell integration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationConfig {
    /// Enable pre-command hooks
    pub enable_pre_command: bool,
    /// Enable post-command hooks  
    pub enable_post_command: bool,
    /// Enable directory change tracking
    pub enable_cd_tracking: bool,
    /// Enable session tracking
    pub enable_session_tracking: bool,
    /// Sinex host endpoint for reporting events
    pub sinex_host_endpoint: String,
    /// Session ID for this shell instance
    pub session_id: Option<String>,
    /// Additional environment variables to set
    pub environment_vars: HashMap<String, String>,
    /// Whether to create backups before modifying config files
    pub create_backups: bool,
}

impl Default for IntegrationConfig {
    fn default() -> Self {
        Self {
            enable_pre_command: true,
            enable_post_command: true,
            enable_cd_tracking: true,
            enable_session_tracking: true,
            sinex_host_endpoint: "127.0.0.1:9999".to_string(),
            session_id: None,
            environment_vars: HashMap::new(),
            create_backups: true,
        }
    }
}

/// Manages shell hook installation and lifecycle
pub struct HookManager {
    shell_info: ShellInfo,
    installed_hooks: HashMap<HookType, HookInstallation>,
}

impl HookManager {
    /// Create a new hook manager for the given shell
    pub fn new(shell_info: &ShellInfo) -> sinex_core::Result<Self> {
        Ok(Self {
            shell_info: shell_info.clone(),
            installed_hooks: HashMap::new(),
        })
    }

    /// Install all supported hooks for the current shell
    pub async fn install_all_hooks(
        &mut self,
        config: &IntegrationConfig,
    ) -> sinex_core::Result<()> {
        if !self.shell_info.capabilities.supports_hooks {
            return Err(sinex_core::CoreError::Configuration(format!(
                "Shell {} does not support hooks",
                self.shell_info.shell_type.name()
            )));
        }

        let hooks_to_install = self.determine_hooks_to_install(config);

        for hook_type in hooks_to_install {
            if let Err(e) = self.install_hook(hook_type.clone(), config).await {
                warn!("Failed to install {:?} hook: {}", hook_type, e);
            }
        }

        info!(
            "Installed {} hooks for {}",
            self.installed_hooks.len(),
            self.shell_info.shell_type.name()
        );
        Ok(())
    }

    /// Uninstall all hooks
    pub async fn uninstall_all_hooks(&mut self) -> sinex_core::Result<()> {
        let hook_types: Vec<HookType> = self.installed_hooks.keys().cloned().collect();

        for hook_type in hook_types {
            if let Err(e) = self.uninstall_hook(&hook_type).await {
                warn!("Failed to uninstall {:?} hook: {}", hook_type, e);
            }
        }

        Ok(())
    }

    /// Install a specific hook
    pub async fn install_hook(
        &mut self,
        hook_type: HookType,
        config: &IntegrationConfig,
    ) -> sinex_core::Result<()> {
        let config_file = self.shell_info.config_path.as_ref().ok_or_else(|| {
            sinex_core::CoreError::Configuration("No config file found for shell".to_string())
        })?;

        // Create backup if requested
        let backup_file = if config.create_backups {
            Some(self.create_backup(config_file).await?)
        } else {
            None
        };

        // Generate hook content
        let hook_content = self.generate_hook_content(&hook_type, config)?;

        // Read existing config
        let existing_content = if config_file.exists() {
            fs::read_to_string(config_file).await.map_err(|e| {
                sinex_core::CoreError::Io(format!("Failed to read config file: {}", e))
            })?
        } else {
            String::new()
        };

        // Check if hook is already installed
        if self.is_hook_already_installed(&existing_content, &hook_type) {
            debug!("Hook {:?} already installed", hook_type);
            return Ok(());
        }

        // Append hook content
        let updated_content = format!("{}\n\n{}\n", existing_content, hook_content);

        // Write updated config
        fs::write(config_file, updated_content).await.map_err(|e| {
            sinex_core::CoreError::Io(format!("Failed to write config file: {}", e))
        })?;

        // Record installation
        let installation = HookInstallation {
            hook_type: hook_type.clone(),
            shell_type: self.shell_info.shell_type.clone(),
            installed_at: chrono::Utc::now(),
            config_file: config_file.clone(),
            backup_file,
            hook_content,
        };

        self.installed_hooks.insert(hook_type, installation);
        Ok(())
    }

    /// Uninstall a specific hook
    pub async fn uninstall_hook(&mut self, hook_type: &HookType) -> sinex_core::Result<()> {
        let installation = self.installed_hooks.remove(hook_type).ok_or_else(|| {
            sinex_core::CoreError::Configuration("Hook not installed".to_string())
        })?;

        // Read current config
        let current_content = fs::read_to_string(&installation.config_file)
            .await
            .map_err(|e| sinex_core::CoreError::Io(format!("Failed to read config file: {}", e)))?;

        // Remove hook content
        let updated_content = current_content.replace(&installation.hook_content, "");

        // Write updated config
        fs::write(&installation.config_file, updated_content)
            .await
            .map_err(|e| {
                sinex_core::CoreError::Io(format!("Failed to write config file: {}", e))
            })?;

        info!("Uninstalled {:?} hook", hook_type);
        Ok(())
    }

    /// Verify that hooks are properly installed
    pub async fn verify_hooks(&self) -> sinex_core::Result<bool> {
        let config_file = self.shell_info.config_path.as_ref().ok_or_else(|| {
            sinex_core::CoreError::Configuration("No config file found".to_string())
        })?;

        if !config_file.exists() {
            return Ok(false);
        }

        let content = fs::read_to_string(config_file)
            .await
            .map_err(|e| sinex_core::CoreError::Io(format!("Failed to read config file: {}", e)))?;

        // Check if all installed hooks are present
        for installation in self.installed_hooks.values() {
            if !content.contains(&installation.hook_content) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn determine_hooks_to_install(&self, config: &IntegrationConfig) -> Vec<HookType> {
        let mut hooks = Vec::new();

        if config.enable_pre_command {
            hooks.push(HookType::PreCommand);
        }
        if config.enable_post_command {
            hooks.push(HookType::PostCommand);
        }
        if config.enable_cd_tracking {
            hooks.push(HookType::ChangeDirectory);
        }
        if config.enable_session_tracking {
            hooks.extend([HookType::SessionStart, HookType::SessionEnd]);
        }

        hooks
    }

    fn generate_hook_content(
        &self,
        hook_type: &HookType,
        config: &IntegrationConfig,
    ) -> sinex_core::Result<String> {
        let sinex_notify_cmd = format!(
            "sinex-shell-notify --endpoint {} --session-id {}",
            config.sinex_host_endpoint,
            config.session_id.as_deref().unwrap_or("unknown")
        );

        match (&self.shell_info.shell_type, hook_type) {
            (ShellType::Bash, HookType::PreCommand) => Ok(format!(
                r#"
# Sinex pre-command hook
_sinex_preexec() {{
    if [[ -n "$BASH_COMMAND" ]]; then
        {} --event precommand --command "$BASH_COMMAND" --pwd "$PWD" &
    fi
}}
trap '_sinex_preexec' DEBUG"#,
                sinex_notify_cmd
            )),

            (ShellType::Bash, HookType::PostCommand) => Ok(format!(
                r#"
# Sinex post-command hook  
_sinex_postexec() {{
    local exit_code=$?
    {} --event postcommand --exit-code $exit_code --pwd "$PWD" &
}}
PROMPT_COMMAND="_sinex_postexec;$PROMPT_COMMAND""#,
                sinex_notify_cmd
            )),

            (ShellType::Zsh, HookType::PreCommand) => Ok(format!(
                r#"
# Sinex pre-command hook
preexec() {{
    {} --event precommand --command "$1" --pwd "$PWD" &
}}"#,
                sinex_notify_cmd
            )),

            (ShellType::Zsh, HookType::PostCommand) => Ok(format!(
                r#"
# Sinex post-command hook
precmd() {{
    local exit_code=$?
    {} --event postcommand --exit-code $exit_code --pwd "$PWD" &
}}"#,
                sinex_notify_cmd
            )),

            (ShellType::Fish, HookType::PreCommand) => Ok(format!(
                r#"
# Sinex pre-command hook
function _sinex_preexec --on-event fish_preexec
    {} --event precommand --command "$argv[1]" --pwd "$PWD" &
end"#,
                sinex_notify_cmd
            )),

            (ShellType::Fish, HookType::PostCommand) => Ok(format!(
                r#"
# Sinex post-command hook
function _sinex_postexec --on-event fish_postexec
    {} --event postcommand --exit-code $status --pwd "$PWD" &
end"#,
                sinex_notify_cmd
            )),

            _ => Err(sinex_core::CoreError::Configuration(format!(
                "Hook {:?} not supported for shell {}",
                hook_type,
                self.shell_info.shell_type.name()
            ))),
        }
    }

    async fn create_backup(&self, config_file: &PathBuf) -> sinex_core::Result<PathBuf> {
        let backup_file = config_file.with_extension(format!(
            "{}.sinex-backup.{}",
            config_file
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or(""),
            chrono::Utc::now().format("%Y%m%d-%H%M%S")
        ));

        fs::copy(config_file, &backup_file)
            .await
            .map_err(|e| sinex_core::CoreError::Io(format!("Failed to create backup: {}", e)))?;

        info!("Created backup at {}", backup_file.display());
        Ok(backup_file)
    }

    fn is_hook_already_installed(&self, content: &str, hook_type: &HookType) -> bool {
        let marker = format!("Sinex {:?} hook", hook_type).to_lowercase();
        content.to_lowercase().contains(&marker)
    }
}
