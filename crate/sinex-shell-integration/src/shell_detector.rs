//! Shell Detection Utilities
//!
//! This module provides utilities for detecting the current shell environment
//! and extracting shell-specific configuration and capabilities.

use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;
use tracing::{debug, info};

/// Supported shell types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShellType {
    Bash,
    Zsh,
    Fish,
    Nushell,
    Elvish,
    PowerShell,
    Unknown(String),
}

impl ShellType {
    /// Get the shell name as a string
    pub fn name(&self) -> &str {
        match self {
            ShellType::Bash => "bash",
            ShellType::Zsh => "zsh",
            ShellType::Fish => "fish",
            ShellType::Nushell => "nushell",
            ShellType::Elvish => "elvish",
            ShellType::PowerShell => "powershell",
            ShellType::Unknown(name) => name,
        }
    }

    /// Check if this shell supports hooks
    pub fn supports_hooks(&self) -> bool {
        matches!(self, ShellType::Bash | ShellType::Zsh | ShellType::Fish)
    }

    /// Get the default configuration file path for this shell
    pub fn default_config_path(&self) -> Option<PathBuf> {
        let home = dirs::home_dir()?;

        match self {
            ShellType::Bash => Some(home.join(".bashrc")),
            ShellType::Zsh => Some(home.join(".zshrc")),
            ShellType::Fish => Some(home.join(".config/fish/config.fish")),
            ShellType::Nushell => Some(home.join(".config/nushell/config.nu")),
            ShellType::Elvish => Some(home.join(".config/elvish/rc.elv")),
            ShellType::PowerShell => None, // Platform-specific
            ShellType::Unknown(_) => None,
        }
    }

    /// Get the history file path for this shell
    pub fn default_history_path(&self) -> Option<PathBuf> {
        let home = dirs::home_dir()?;

        match self {
            ShellType::Bash => Some(home.join(".bash_history")),
            ShellType::Zsh => Some(home.join(".zsh_history")),
            ShellType::Fish => Some(home.join(".local/share/fish/fish_history")),
            ShellType::Nushell => Some(home.join(".config/nushell/history.txt")),
            ShellType::Elvish => Some(home.join(".config/elvish/db")),
            ShellType::PowerShell => None,
            ShellType::Unknown(_) => None,
        }
    }
}

/// Information about the detected shell environment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShellInfo {
    pub shell_type: ShellType,
    pub executable_path: Option<PathBuf>,
    pub version: Option<String>,
    pub config_path: Option<PathBuf>,
    pub history_path: Option<PathBuf>,
    pub session_id: Option<String>,
    pub pid: Option<u32>,
    pub parent_pid: Option<u32>,
    pub terminal: Option<String>,
    pub capabilities: ShellCapabilities,
}

/// Shell capabilities and features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShellCapabilities {
    pub supports_hooks: bool,
    pub supports_functions: bool,
    pub supports_aliases: bool,
    pub supports_completion: bool,
    pub supports_job_control: bool,
    pub has_atuin: bool,
    pub has_starship: bool,
}

/// Shell detector utility
pub struct ShellDetector;

impl ShellDetector {
    /// Detect the current shell environment
    pub fn detect_current_shell() -> sinex_core::Result<ShellInfo> {
        let shell_type = Self::detect_shell_type();
        let executable_path = Self::detect_executable_path(&shell_type);
        let version = Self::detect_version(&executable_path);
        let config_path = Self::detect_config_path(&shell_type);
        let history_path = Self::detect_history_path(&shell_type);
        let session_id = Self::detect_session_id();
        let pid = Self::detect_shell_pid();
        let parent_pid = Self::detect_parent_pid();
        let terminal = Self::detect_terminal();
        let capabilities = Self::detect_capabilities(&shell_type);

        info!(
            shell_type = ?shell_type,
            executable_path = ?executable_path,
            version = ?version,
            "Detected shell environment"
        );

        Ok(ShellInfo {
            shell_type,
            executable_path,
            version,
            config_path,
            history_path,
            session_id,
            pid,
            parent_pid,
            terminal,
            capabilities,
        })
    }

    fn detect_shell_type() -> ShellType {
        // Try SHELL environment variable first
        if let Ok(shell_env) = env::var("SHELL") {
            if let Some(shell_name) = PathBuf::from(&shell_env).file_name() {
                if let Some(name_str) = shell_name.to_str() {
                    return Self::parse_shell_type(name_str);
                }
            }
        }

        // Try 0 argument (process name)
        if let Ok(arg0) = env::var("0") {
            return Self::parse_shell_type(&arg0);
        }

        // Try parent process detection
        if let Some(parent_name) = Self::get_parent_process_name() {
            let shell_type = Self::parse_shell_type(&parent_name);
            if !matches!(shell_type, ShellType::Unknown(_)) {
                return shell_type;
            }
        }

        debug!("Could not detect shell type, defaulting to bash");
        ShellType::Bash
    }

    fn parse_shell_type(name: &str) -> ShellType {
        let name_lower = name.to_lowercase();

        if name_lower.contains("bash") {
            ShellType::Bash
        } else if name_lower.contains("zsh") {
            ShellType::Zsh
        } else if name_lower.contains("fish") {
            ShellType::Fish
        } else if name_lower.contains("nu") || name_lower.contains("nushell") {
            ShellType::Nushell
        } else if name_lower.contains("elvish") {
            ShellType::Elvish
        } else if name_lower.contains("pwsh") || name_lower.contains("powershell") {
            ShellType::PowerShell
        } else {
            ShellType::Unknown(name.to_string())
        }
    }

    fn detect_executable_path(shell_type: &ShellType) -> Option<PathBuf> {
        // First try SHELL environment variable
        if let Ok(shell_path) = env::var("SHELL") {
            let path = PathBuf::from(shell_path);
            if path.exists() {
                return Some(path);
            }
        }

        // Try to find in PATH
        if let Ok(path_env) = env::var("PATH") {
            for path_dir in path_env.split(':') {
                let shell_path = PathBuf::from(path_dir).join(shell_type.name());
                if shell_path.exists() {
                    return Some(shell_path);
                }
            }
        }

        None
    }

    fn detect_version(executable_path: &Option<PathBuf>) -> Option<String> {
        let path = executable_path.as_ref()?;

        // Try to get version using --version flag
        let output = std::process::Command::new(path)
            .arg("--version")
            .output()
            .ok()?;

        if output.status.success() {
            let version_text = String::from_utf8_lossy(&output.stdout);
            // Extract first line which usually contains version info
            version_text.lines().next().map(|s| s.trim().to_string())
        } else {
            None
        }
    }

    fn detect_config_path(shell_type: &ShellType) -> Option<PathBuf> {
        // Check environment variables first
        match shell_type {
            ShellType::Bash => env::var("BASH_ENV")
                .ok()
                .map(PathBuf::from)
                .or_else(|| shell_type.default_config_path()),
            ShellType::Zsh => env::var("ZDOTDIR")
                .ok()
                .map(|dir| PathBuf::from(dir).join(".zshrc"))
                .or_else(|| shell_type.default_config_path()),
            _ => shell_type.default_config_path(),
        }
    }

    fn detect_history_path(shell_type: &ShellType) -> Option<PathBuf> {
        // Check environment variables first
        match shell_type {
            ShellType::Bash => env::var("HISTFILE")
                .ok()
                .map(PathBuf::from)
                .or_else(|| shell_type.default_history_path()),
            ShellType::Zsh => env::var("HISTFILE")
                .ok()
                .map(PathBuf::from)
                .or_else(|| shell_type.default_history_path()),
            _ => shell_type.default_history_path(),
        }
    }

    fn detect_session_id() -> Option<String> {
        // Try various session identifiers
        env::var("SINEX_SESSION_ID")
            .ok()
            .or_else(|| env::var("TMUX_PANE").ok())
            .or_else(|| env::var("STY").ok()) // screen session
            .or_else(|| env::var("TERM_SESSION_ID").ok())
    }

    fn detect_shell_pid() -> Option<u32> {
        env::var("PPID")
            .ok()
            .and_then(|pid_str| pid_str.parse().ok())
            .or_else(|| std::process::id().into())
    }

    fn detect_parent_pid() -> Option<u32> {
        // This would require platform-specific code
        // For now, return None
        None
    }

    fn detect_terminal() -> Option<String> {
        env::var("TERM_PROGRAM")
            .ok()
            .or_else(|| env::var("TERMINAL_EMULATOR").ok())
            .or_else(|| env::var("TERM").ok())
    }

    fn detect_capabilities(shell_type: &ShellType) -> ShellCapabilities {
        ShellCapabilities {
            supports_hooks: shell_type.supports_hooks(),
            supports_functions: !matches!(shell_type, ShellType::Unknown(_)),
            supports_aliases: !matches!(shell_type, ShellType::Unknown(_)),
            supports_completion: !matches!(shell_type, ShellType::Unknown(_)),
            supports_job_control: !matches!(shell_type, ShellType::Unknown(_)),
            has_atuin: Self::check_command_available("atuin"),
            has_starship: Self::check_command_available("starship"),
        }
    }

    fn check_command_available(command: &str) -> bool {
        std::process::Command::new("which")
            .arg(command)
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn get_parent_process_name() -> Option<String> {
        // This would require platform-specific process introspection
        // For now, return None
        None
    }
}
