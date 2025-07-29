//! Environment Setup and Configuration
//!
//! This module handles the setup and teardown of the shell environment
//! for Sinex integration, including environment variables and PATH modifications.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use tokio::fs;
use tracing::{debug, info};

use crate::hook_manager::IntegrationConfig;
use crate::shell_detector::{ShellInfo, ShellType};

/// Environment setup configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentConfig {
    /// Additional PATH entries to add
    pub path_additions: Vec<String>,
    /// Environment variables to set
    pub environment_vars: HashMap<String, String>,
    /// Whether to modify the shell profile
    pub modify_profile: bool,
    /// Custom profile modifications
    pub profile_additions: Vec<String>,
}

impl Default for EnvironmentConfig {
    fn default() -> Self {
        Self {
            path_additions: Vec::new(),
            environment_vars: HashMap::new(),
            modify_profile: true,
            profile_additions: Vec::new(),
        }
    }
}

/// Manages environment setup for shell integration
pub struct EnvironmentSetup {
    shell_info: ShellInfo,
    current_config: Option<EnvironmentConfig>,
    original_env: HashMap<String, String>,
}

impl EnvironmentSetup {
    /// Create a new environment setup manager
    pub fn new(shell_info: &ShellInfo) -> sinex_core_types::Result<Self> {
        let original_env = Self::capture_current_env();

        Ok(Self {
            shell_info: shell_info.clone(),
            current_config: None,
            original_env,
        })
    }

    /// Configure the environment for Sinex integration
    pub async fn configure_environment(
        &mut self,
        config: &IntegrationConfig,
    ) -> sinex_core_types::Result<()> {
        let env_config = self.create_environment_config(config)?;

        // Set environment variables
        self.set_environment_variables(&env_config).await?;

        // Modify shell profile if requested
        if env_config.modify_profile {
            self.modify_shell_profile(&env_config).await?;
        }

        self.current_config = Some(env_config);

        info!("Configured environment for Sinex integration");
        Ok(())
    }

    /// Clean up environment modifications
    pub async fn cleanup_environment(&mut self) -> sinex_core_types::Result<()> {
        if let Some(config) = &self.current_config {
            // Restore original environment variables
            self.restore_environment_variables(config).await?;

            // Clean up profile modifications if needed
            if config.modify_profile {
                self.cleanup_shell_profile(config).await?;
            }
        }

        self.current_config = None;

        info!("Cleaned up Sinex environment configuration");
        Ok(())
    }

    /// Check if environment is properly configured
    pub fn verify_environment(&self) -> bool {
        // Check if required environment variables are set
        let required_vars = ["SINEX_SESSION_ID", "SINEX_HOST_ENDPOINT"];

        for var in &required_vars {
            if env::var(var).is_err() {
                debug!("Missing required environment variable: {}", var);
                return false;
            }
        }

        true
    }

    /// Get the current environment configuration
    pub fn current_config(&self) -> Option<&EnvironmentConfig> {
        self.current_config.as_ref()
    }

    fn create_environment_config(
        &self,
        config: &IntegrationConfig,
    ) -> sinex_core_types::Result<EnvironmentConfig> {
        let mut env_config = EnvironmentConfig::default();

        // Set Sinex-specific environment variables
        env_config.environment_vars.insert(
            "SINEX_SESSION_ID".to_string(),
            config
                .session_id
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
        );

        env_config.environment_vars.insert(
            "SINEX_HOST_ENDPOINT".to_string(),
            config.sinex_host_endpoint.clone(),
        );

        env_config
            .environment_vars
            .insert("SINEX_SHELL_INTEGRATION".to_string(), "enabled".to_string());

        env_config.environment_vars.insert(
            "SINEX_SHELL_TYPE".to_string(),
            self.shell_info.shell_type.name().to_string(),
        );

        // Add user-specified environment variables
        for (key, value) in &config.environment_vars {
            env_config
                .environment_vars
                .insert(key.clone(), value.clone());
        }

        // Add PATH entries for Sinex tools if they exist
        if let Some(sinex_bin) = self.find_sinex_bin_path() {
            env_config.path_additions.push(sinex_bin);
        }

        // Add shell-specific profile additions
        env_config.profile_additions = self.generate_profile_additions(config)?;

        Ok(env_config)
    }

    async fn set_environment_variables(
        &self,
        config: &EnvironmentConfig,
    ) -> sinex_core_types::Result<()> {
        for (key, value) in &config.environment_vars {
            env::set_var(key, value);
            debug!("Set environment variable: {}={}", key, value);
        }

        // Update PATH if needed
        if !config.path_additions.is_empty() {
            let current_path = env::var("PATH").unwrap_or_default();
            let mut path_components: Vec<String> = config.path_additions.clone();

            if !current_path.is_empty() {
                path_components.push(current_path);
            }

            let new_path = path_components.join(":");
            env::set_var("PATH", &new_path);
            debug!("Updated PATH: {}", new_path);
        }

        Ok(())
    }

    async fn restore_environment_variables(
        &self,
        config: &EnvironmentConfig,
    ) -> sinex_core_types::Result<()> {
        // Remove Sinex-specific variables
        for key in config.environment_vars.keys() {
            if key.starts_with("SINEX_") {
                env::remove_var(key);
                debug!("Removed environment variable: {}", key);
            }
        }

        // Restore original PATH
        if let Some(original_path) = self.original_env.get("PATH") {
            env::set_var("PATH", original_path);
            debug!("Restored original PATH");
        }

        Ok(())
    }

    async fn modify_shell_profile(&self, config: &EnvironmentConfig) -> sinex_core_types::Result<()> {
        let profile_file = self.get_profile_file()?;

        if config.profile_additions.is_empty() {
            return Ok(());
        }

        // Read existing profile
        let existing_content = if profile_file.exists() {
            fs::read_to_string(&profile_file).await.map_err(|e| {
                sinex_core_types::SinexError::io(format!("Failed to read profile file: {}", e))
            })?
        } else {
            String::new()
        };

        // Check if modifications are already present
        let marker = "# Sinex integration";
        if existing_content.contains(marker) {
            debug!("Profile already contains Sinex modifications");
            return Ok(());
        }

        // Append Sinex configuration
        let mut additions = vec![
            format!("\n{}", marker),
            "# This section was added by Sinex shell integration".to_string(),
        ];
        additions.extend(config.profile_additions.clone());
        additions.push("# End Sinex integration\n".to_string());

        let updated_content = format!("{}\n{}", existing_content, additions.join("\n"));

        // Write updated profile
        fs::write(&profile_file, updated_content)
            .await
            .map_err(|e| {
                sinex_core_types::SinexError::io(format!("Failed to write profile file: {}", e))
            })?;

        info!("Modified shell profile: {}", profile_file.display());
        Ok(())
    }

    async fn cleanup_shell_profile(&self, _config: &EnvironmentConfig) -> sinex_core_types::Result<()> {
        let profile_file = self.get_profile_file()?;

        if !profile_file.exists() {
            return Ok(());
        }

        // Read current profile
        let current_content = fs::read_to_string(&profile_file).await.map_err(|e| {
            sinex_core_types::SinexError::io(format!("Failed to read profile file: {}", e))
        })?;

        // Remove Sinex section
        let lines: Vec<&str> = current_content.lines().collect();
        let mut filtered_lines = Vec::new();
        let mut in_sinex_section = false;

        for line in lines {
            if line.contains("# Sinex integration") {
                in_sinex_section = true;
                continue;
            }
            if line.contains("# End Sinex integration") {
                in_sinex_section = false;
                continue;
            }
            if !in_sinex_section {
                filtered_lines.push(line);
            }
        }

        let cleaned_content = filtered_lines.join("\n");

        // Write cleaned profile
        fs::write(&profile_file, cleaned_content)
            .await
            .map_err(|e| {
                sinex_core_types::SinexError::io(format!("Failed to write profile file: {}", e))
            })?;

        info!("Cleaned up shell profile: {}", profile_file.display());
        Ok(())
    }

    fn get_profile_file(&self) -> sinex_core_types::Result<PathBuf> {
        match &self.shell_info.shell_type {
            ShellType::Bash => {
                // Try .bash_profile first, then .bashrc
                let home = dirs::home_dir().ok_or_else(|| {
                    sinex_core_types::SinexError::configuration("Cannot find home directory".to_string())
                })?;

                let bash_profile = home.join(".bash_profile");
                if bash_profile.exists() {
                    Ok(bash_profile)
                } else {
                    Ok(home.join(".bashrc"))
                }
            }
            ShellType::Zsh => {
                let home = dirs::home_dir().ok_or_else(|| {
                    sinex_core_types::SinexError::configuration("Cannot find home directory".to_string())
                })?;
                Ok(home.join(".zshrc"))
            }
            ShellType::Fish => {
                let home = dirs::home_dir().ok_or_else(|| {
                    sinex_core_types::SinexError::configuration("Cannot find home directory".to_string())
                })?;
                Ok(home.join(".config/fish/config.fish"))
            }
            _ => Err(sinex_core_types::SinexError::configuration(format!(
                "Profile modification not supported for {}",
                self.shell_info.shell_type.name()
            ))),
        }
    }

    fn generate_profile_additions(
        &self,
        config: &IntegrationConfig,
    ) -> sinex_core_types::Result<Vec<String>> {
        let mut additions = Vec::new();

        // Add environment variable exports
        for (key, value) in &config.environment_vars {
            additions.push(format!("export {}=\"{}\"", key, value));
        }

        // Add session initialization
        additions.push("# Initialize Sinex session".to_string());
        additions.push("if command -v sinex-shell-init >/dev/null 2>&1; then".to_string());
        additions.push("    eval \"$(sinex-shell-init)\"".to_string());
        additions.push("fi".to_string());

        Ok(additions)
    }

    fn find_sinex_bin_path(&self) -> Option<String> {
        // Try to find Sinex binaries in common locations
        let possible_paths = [
            "/usr/local/bin",
            "/usr/bin",
            "/opt/sinex/bin",
            "~/.local/bin",
            "~/.cargo/bin",
        ];

        for path_str in &possible_paths {
            let path = if path_str.starts_with('~') {
                if let Some(home) = dirs::home_dir() {
                    home.join(&path_str[2..])
                } else {
                    continue;
                }
            } else {
                PathBuf::from(path_str)
            };

            if path.join("sinex-gateway").exists() || path.join("sinex-shell-notify").exists() {
                return Some(path.to_string_lossy().to_string());
            }
        }

        None
    }

    fn capture_current_env() -> HashMap<String, String> {
        env::vars().collect()
    }
}
