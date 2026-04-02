//! Shell detection utilities for terminal ingestor
//!
//! This module provides functionality to detect the current shell environment
//! and its capabilities, extracted from sinex-shell-integration.

use camino::Utf8PathBuf;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, path::PathBuf};
use tracing::{info, warn};

/// Cache for command existence checks to avoid repeated `which::which()` calls
static COMMAND_CACHE: std::sync::LazyLock<RwLock<HashMap<String, bool>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

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
    #[must_use]
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

    /// Get the executable name used to invoke this shell.
    #[must_use]
    pub fn executable_name(&self) -> &str {
        match self {
            ShellType::PowerShell => "pwsh",
            ShellType::Nushell => "nu",
            _ => self.name(),
        }
    }

    /// Check if this shell supports hooks
    #[must_use]
    pub fn supports_hooks(&self) -> bool {
        matches!(self, ShellType::Bash | ShellType::Zsh | ShellType::Fish)
    }

    /// Get the default configuration file path for this shell
    #[must_use]
    pub fn default_config_path(&self) -> Option<Utf8PathBuf> {
        let home = get_home_dir()?;
        self.default_config_path_from(Some(home))
    }

    #[must_use]
    fn default_config_path_from(&self, home: Option<Utf8PathBuf>) -> Option<Utf8PathBuf> {
        let home = home?;

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
    ///
    /// # Shell-Specific Notes
    ///
    /// ## Fish
    /// Fish stores its native history in a YAML-like text file at
    /// `~/.local/share/fish/fish_history`. The terminal ingestor does not treat that
    /// format as generic line-oriented text; only explicitly SQLite-backed Fish history
    /// sources are accepted for ingestion.
    ///
    /// ## Elvish
    /// Elvish uses a custom binary format at `~/.config/elvish/db`. This format is not
    /// currently supported by the terminal ingestor. Consider exporting Elvish history
    /// to a text format if ingestion is required.
    #[must_use]
    pub fn default_history_path(&self) -> Option<Utf8PathBuf> {
        let home = get_home_dir()?;
        self.default_history_path_from(Some(home))
    }

    #[must_use]
    fn default_history_path_from(&self, home: Option<Utf8PathBuf>) -> Option<Utf8PathBuf> {
        let home = home?;

        match self {
            ShellType::Bash => Some(home.join(".bash_history")),
            ShellType::Zsh => Some(home.join(".zsh_history")),
            // Native fish history is not an ingestible text or SQLite source by default.
            ShellType::Fish => None,
            ShellType::Nushell => Some(home.join(".config/nushell/history.txt")),
            // Native Elvish history is a custom binary database and not ingestible by default.
            ShellType::Elvish => None,
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
pub fn detect_current_shell() -> Result<ShellInfo, sinex_node_sdk::SinexError> {
    // Get shell from environment
    let shell_env = read_optional_env_var("SHELL", "detecting current shell").unwrap_or_default();
    let shell_type = detect_shell_type(&shell_env);

    // Detect capabilities
    let capabilities = detect_capabilities(&shell_type);

    // Get process info
    let pid = std::process::id();
    let parent_pid = get_parent_pid();

    // Get session ID from environment
    let session_id = read_optional_env_var("SINEX_SESSION_ID", "detecting shell session id")
        .or_else(|| read_optional_env_var("TERM_SESSION_ID", "detecting terminal session id"));

    // Get terminal info
    let terminal = read_optional_env_var("TERM", "detecting terminal type");

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
#[must_use]
pub fn detect_shell_type(shell_path: &str) -> ShellType {
    let shell_name = shell_path
        .split('/')
        .next_back()
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
#[must_use]
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

fn read_cached_command_exists(
    cmd: &str,
    cache: &RwLock<HashMap<String, bool>>,
) -> Option<bool> {
    cache.read().get(cmd).copied()
}

fn write_cached_command_exists(cmd: &str, exists: bool, cache: &RwLock<HashMap<String, bool>>) {
    cache.write().insert(cmd.to_string(), exists);
}

/// Check if a command exists in PATH with caching
fn check_command_exists(cmd: &str) -> bool {
    // Check cache first (read lock)
    if let Some(exists) = read_cached_command_exists(cmd, &COMMAND_CACHE) {
        return exists;
    }

    // Cache miss - check command existence
    let exists = which::which(cmd).is_ok();

    // Update cache (write lock)
    write_cached_command_exists(cmd, exists, &COMMAND_CACHE);

    exists
}

/// Get shell version
fn get_shell_version(shell_type: &ShellType) -> Option<String> {
    match get_shell_version_impl(shell_type) {
        Ok(version) => Some(version),
        Err(error) => {
            warn!(
                shell = shell_type.name(),
                %error,
                "Failed to determine shell version"
            );
            None
        }
    }
}

/// Helper function that uses ? operator for cleaner error handling
fn get_shell_version_impl(shell_type: &ShellType) -> std::io::Result<String> {
    use std::process::Command;

    let version_flag = match shell_type {
        ShellType::PowerShell => "-Version",
        _ => "--version",
    };

    let output = Command::new(shell_type.executable_name())
        .arg(version_flag)
        .output()
        .map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!(
                    "failed to execute {} {}: {error}",
                    shell_type.executable_name(),
                    version_flag
                ),
            )
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() {
            stderr
        } else {
            stdout
        };
        let message = if detail.is_empty() {
            format!(
                "failed to determine {} version: command exited with {}",
                shell_type.name(),
                output.status
            )
        } else {
            format!(
                "failed to determine {} version: command exited with {}: {}",
                shell_type.name(),
                output.status,
                detail
            )
        };
        return Err(std::io::Error::other(message));
    }

    let stdout = String::from_utf8(output.stdout).map_err(std::io::Error::other)?;
    let version = stdout
        .lines()
        .find(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            std::io::Error::other(format!(
                "failed to determine {} version: command produced empty stdout",
                shell_type.name()
            ))
        })?;
    Ok(version)
}

fn read_optional_env_var(var: &str, context: &str) -> Option<String> {
    match env::var(var) {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            warn!(
                variable = var,
                context,
                "Environment variable is not valid UTF-8; ignoring value"
            );
            None
        }
    }
}

/// Get parent process ID using sysinfo crate for cross-platform compatibility
fn get_parent_pid() -> Option<u32> {
    let mut system = sysinfo::System::new();
    system.refresh_processes();

    let current_pid = std::process::id();
    system
        .process(sysinfo::Pid::from_u32(current_pid))?
        .parent()
        .map(sysinfo::Pid::as_u32)
}

pub(crate) fn utf8_home_dir(context: &'static str) -> Option<Utf8PathBuf> {
    utf8_home_dir_from(dirs::home_dir(), context)
}

fn utf8_home_dir_from(path: Option<PathBuf>, context: &'static str) -> Option<Utf8PathBuf> {
    let path = path?;
    match Utf8PathBuf::from_path_buf(path.clone()) {
        Ok(path) => Some(path),
        Err(_) => {
            warn!(
                path = %path.display(),
                context,
                "Home directory path is not valid UTF-8; shell defaults are unavailable"
            );
            None
        }
    }
}

/// Helper function to get home directory as `Utf8PathBuf`
fn get_home_dir() -> Option<Utf8PathBuf> {
    utf8_home_dir("detecting shell home directory")
}

#[cfg(test)]
mod tests {
    // Inline because this covers local env/cache/version failure semantics.
    use super::{
        ShellType, Utf8PathBuf, get_shell_version, get_shell_version_impl, read_optional_env_var,
        utf8_home_dir_from,
    };
    use xtask::sandbox::{EnvGuard, sinex_serial_test};


    #[sinex_serial_test]
    async fn read_optional_env_var_returns_none_without_value() -> xtask::sandbox::TestResult<()> {
        let _env = EnvGuard::new();
        assert_eq!(
            read_optional_env_var("SINEX_UNUSED_OPTIONAL_ENV", "test context"),
            None
        );
        Ok(())
    }

    #[test]
    fn get_shell_version_impl_rejects_unknown_shell() {
        let error = get_shell_version_impl(&ShellType::Unknown(
            "__sinex_nonexistent_shell__".to_string(),
        ))
        .expect_err("unknown shell should fail");
        assert!(
            error.to_string().contains("__sinex_nonexistent_shell__"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn get_shell_version_surfaces_failure_as_none() {
        assert_eq!(
            get_shell_version(&ShellType::Unknown("__sinex_nonexistent_shell__".to_string())),
            None
        );
    }

    #[test]
    fn executable_names_use_real_shell_binaries() {
        assert_eq!(ShellType::Nushell.executable_name(), "nu");
        assert_eq!(ShellType::PowerShell.executable_name(), "pwsh");
        assert_eq!(ShellType::Bash.executable_name(), "bash");
    }

    #[test]
    fn unsupported_native_history_stores_do_not_advertise_default_paths() {
        let home = Some(Utf8PathBuf::from("/tmp/home"));

        assert_eq!(ShellType::Fish.default_history_path_from(home.clone()), None);
        assert_eq!(ShellType::Elvish.default_history_path_from(home), None);
        assert_eq!(
            ShellType::Nushell.default_history_path_from(Some(Utf8PathBuf::from("/tmp/home"))),
            Some(Utf8PathBuf::from("/tmp/home/.config/nushell/history.txt"))
        );
    }

    #[sinex_serial_test]
    async fn session_id_falls_back_to_term_session_id() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("TERM_SESSION_ID", "term-session");

        let session_id = read_optional_env_var("SINEX_SESSION_ID", "test context")
            .or_else(|| read_optional_env_var("TERM_SESSION_ID", "test context"));

        assert_eq!(session_id.as_deref(), Some("term-session"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn utf8_home_dir_from_rejects_non_utf8_paths() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        use std::path::PathBuf;

        let non_utf8 = PathBuf::from(OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]));
        assert!(utf8_home_dir_from(Some(non_utf8), "test").is_none());
    }
}
