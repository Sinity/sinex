//! Shell detection utilities for terminal satellite
//!
//! This module provides functionality to detect the current shell environment
//! and its capabilities, extracted from sinex-shell-integration.

use camino::Utf8PathBuf;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, sync::RwLock};
use tracing::info;

/// Cache for command existence checks to avoid repeated which::which() calls
static COMMAND_CACHE: Lazy<RwLock<HashMap<String, bool>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

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
    pub fn default_config_path(&self) -> Option<Utf8PathBuf> {
        let home = get_home_dir()?;

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
    pub fn default_history_path(&self) -> Option<Utf8PathBuf> {
        let home = get_home_dir()?;

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
    pub executable_path: Option<Utf8PathBuf>,
    pub version: Option<String>,
    pub config_path: Option<Utf8PathBuf>,
    pub history_path: Option<Utf8PathBuf>,
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

/// Detect the current shell environment
pub fn detect_current_shell() -> Result<ShellInfo, sinex_satellite_sdk::SatelliteError> {
    // Get shell from environment
    let shell_env = env::var("SHELL").unwrap_or_default();
    let shell_type = detect_shell_type(&shell_env);

    // Detect capabilities
    let capabilities = detect_capabilities(&shell_type);

    // Get process info
    let pid = std::process::id();
    let parent_pid = get_parent_pid();

    // Get session ID from environment
    let session_id = env::var("SINEX_SESSION_ID")
        .ok()
        .or_else(|| env::var("TERM_SESSION_ID").ok());

    // Get terminal info
    let terminal = env::var("TERM").ok();

    // Build shell info
    let shell_info = ShellInfo {
        shell_type: shell_type.clone(),
        executable_path: if shell_env.is_empty() {
            None
        } else {
            Some(Utf8PathBuf::from(&shell_env))
        },
        version: get_shell_version(&shell_type),
        config_path: shell_type.default_config_path(),
        history_path: shell_type.default_history_path(),
        session_id,
        pid: Some(pid),
        parent_pid,
        terminal,
        capabilities,
    };

    info!("Detected shell environment: {:?}", shell_info.shell_type);
    Ok(shell_info)
}

/// Detect shell type from path or name
pub fn detect_shell_type(shell_path: &str) -> ShellType {
    let shell_name = shell_path
        .split('/')
        .last()
        .unwrap_or(shell_path)
        .to_lowercase();

    match shell_name.as_str() {
        "bash" => ShellType::Bash,
        "zsh" => ShellType::Zsh,
        "fish" => ShellType::Fish,
        "nu" | "nushell" => ShellType::Nushell,
        "elvish" => ShellType::Elvish,
        "pwsh" | "powershell" => ShellType::PowerShell,
        _ => ShellType::Unknown(shell_name),
    }
}

/// Detect shell capabilities
pub fn detect_capabilities(shell_type: &ShellType) -> ShellCapabilities {
    ShellCapabilities {
        supports_hooks: shell_type.supports_hooks(),
        supports_functions: matches!(
            shell_type,
            ShellType::Bash | ShellType::Zsh | ShellType::Fish | ShellType::Nushell
        ),
        supports_aliases: !matches!(shell_type, ShellType::Nushell),
        supports_completion: true, // Most modern shells support this
        supports_job_control: !matches!(shell_type, ShellType::PowerShell),
        has_atuin: check_command_exists("atuin"),
        has_starship: check_command_exists("starship"),
    }
}

/// Check if a command exists in PATH with caching
fn check_command_exists(cmd: &str) -> bool {
    // Check cache first (read lock)
    if let Ok(cache) = COMMAND_CACHE.read() {
        if let Some(&exists) = cache.get(cmd) {
            return exists;
        }
    }

    // Cache miss - check command existence
    let exists = which::which(cmd).is_ok();

    // Update cache (write lock)
    if let Ok(mut cache) = COMMAND_CACHE.write() {
        cache.insert(cmd.to_string(), exists);
    }

    exists
}

/// Get shell version
fn get_shell_version(shell_type: &ShellType) -> Option<String> {
    get_shell_version_impl(shell_type).ok()
}

/// Helper function that uses ? operator for cleaner error handling
fn get_shell_version_impl(shell_type: &ShellType) -> Result<String, Box<dyn std::error::Error>> {
    use std::process::Command;

    let version_flag = match shell_type {
        ShellType::PowerShell => "-Version",
        _ => "--version",
    };

    let output = Command::new(shell_type.name()).arg(version_flag).output()?;

    let stdout = String::from_utf8(output.stdout)?;
    let version = stdout.lines().next().unwrap_or("").to_string();
    Ok(version)
}

/// Get parent process ID using sysinfo crate for cross-platform compatibility
fn get_parent_pid() -> Option<u32> {
    let mut system = sysinfo::System::new();
    system.refresh_processes();

    let current_pid = std::process::id();
    system
        .process(sysinfo::Pid::from_u32(current_pid))?
        .parent()
        .map(|pid| pid.as_u32())
}

/// Helper function to get home directory as Utf8PathBuf
fn get_home_dir() -> Option<Utf8PathBuf> {
    dirs::home_dir().and_then(|p| Utf8PathBuf::from_path_buf(p).ok())
}
