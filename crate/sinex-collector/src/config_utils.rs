//! Configuration utilities shared across collector modules

use std::env;
use tracing::warn;

/// Resolve a path that may contain ~ to a system-safe alternative
/// 
/// This function handles path resolution for system services that may not have
/// access to user home directories in the traditional way.
pub fn resolve_system_safe_path(default_path: &str, env_var: Option<&str>, fallback_dir: &str) -> String {
    // First try environment variable if provided
    if let Some(var_name) = env_var {
        if let Ok(path) = env::var(var_name) {
            return path;
        }
    }
    
    // If path starts with ~, resolve to system-safe alternatives
    if let Some(relative_path) = default_path.strip_prefix("~/") {
        // Remove ~/
        
        // Try XDG directories first (most appropriate for system services)
        if let Ok(data_dir) = env::var("XDG_DATA_HOME") {
            return format!("{}/{}", data_dir, relative_path);
        }
        
        // Try HOME as last resort
        if let Ok(home) = env::var("HOME") {
            warn!("Using HOME directory for system service - consider setting XDG_DATA_HOME");
            return format!("{}/.local/share/{}", home, relative_path);
        }
        
        // Fall back to /var/lib or /tmp for system services
        warn!("No HOME or XDG_DATA_HOME available, using fallback: {}/{}", fallback_dir, relative_path);
        return format!("{}/{}", fallback_dir, relative_path);
    }
    
    // Return path as-is if not home directory based
    default_path.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_resolve_absolute_path() {
        let result = resolve_system_safe_path("/absolute/path", None, "/fallback");
        assert_eq!(result, "/absolute/path");
    }

    #[test]
    fn test_resolve_relative_path() {
        let result = resolve_system_safe_path("relative/path", None, "/fallback");
        assert_eq!(result, "relative/path");
    }

    #[test]
    fn test_resolve_home_path_with_xdg() {
        env::set_var("XDG_DATA_HOME", "/test/xdg");
        let result = resolve_system_safe_path("~/config/test", None, "/fallback");
        assert_eq!(result, "/test/xdg/config/test");
        env::remove_var("XDG_DATA_HOME");
    }

    #[test]
    fn test_resolve_home_path_fallback() {
        env::remove_var("XDG_DATA_HOME");
        env::remove_var("HOME");
        let result = resolve_system_safe_path("~/config/test", None, "/var/lib");
        assert_eq!(result, "/var/lib/config/test");
    }

    #[test]
    fn test_resolve_with_env_var() {
        env::set_var("TEST_CONFIG_PATH", "/custom/path");
        let result = resolve_system_safe_path("~/default", Some("TEST_CONFIG_PATH"), "/fallback");
        assert_eq!(result, "/custom/path");
        env::remove_var("TEST_CONFIG_PATH");
    }
}