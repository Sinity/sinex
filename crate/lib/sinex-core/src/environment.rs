//! Environment namespacing for Sinex
//!
//! This module provides centralized environment-aware resource naming for proper isolation
//! between development, staging, and production environments. All resources (database names,
//! NATS subjects, socket paths, work directories) are namespaced based on SINEX_ENVIRONMENT.

use color_eyre::eyre::{eyre, Result};
use std::env;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Default environment when SINEX_ENVIRONMENT is not set
const DEFAULT_ENVIRONMENT: &str = "dev";

/// Valid environment names
const VALID_ENVIRONMENTS: &[&str] = &["dev", "staging", "prod"];

/// Environment context providing namespaced resource access
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinexEnvironment {
    /// Environment name (dev, staging, prod)
    name: String,
}

impl SinexEnvironment {
    /// Get the current environment from SINEX_ENVIRONMENT variable
    pub fn current() -> Result<Self> {
        let name = env::var("SINEX_ENVIRONMENT").unwrap_or_else(|_| {
            warn!(
                "SINEX_ENVIRONMENT not set, defaulting to '{}'",
                DEFAULT_ENVIRONMENT
            );
            DEFAULT_ENVIRONMENT.to_string()
        });

        Self::new(&name)
    }

    /// Create a new environment context with validation
    pub fn new(name: &str) -> Result<Self> {
        let name = name.trim().to_lowercase();

        if name.is_empty() {
            return Err(eyre!("Environment name cannot be empty"));
        }

        if !VALID_ENVIRONMENTS.contains(&name.as_str()) {
            return Err(eyre!(
                "Invalid environment '{}'. Valid environments: {}",
                name,
                VALID_ENVIRONMENTS.join(", ")
            ));
        }

        info!("Initialized Sinex environment: {}", name);
        Ok(Self { name })
    }

    /// Get the environment name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if this is the development environment
    pub fn is_dev(&self) -> bool {
        self.name == "dev"
    }

    /// Check if this is the staging environment
    pub fn is_staging(&self) -> bool {
        self.name == "staging"
    }

    /// Check if this is the production environment
    pub fn is_prod(&self) -> bool {
        self.name == "prod"
    }

    /// Get environment-namespaced database name
    ///
    /// Transforms base database name into environment-specific name:
    /// - sinex -> sinex_dev, sinex_staging, sinex_prod
    pub fn database_name(&self, base_name: &str) -> String {
        format!("{}_{}", base_name, self.name)
    }

    /// Get environment-namespaced database URL
    ///
    /// Modifies the database URL to use environment-specific database name
    pub fn database_url(&self, base_url: &str) -> Result<String> {
        if base_url.is_empty() {
            return Err(eyre!("Database URL cannot be empty"));
        }

        // Separate query from main part first so we don't mis-detect slashes inside the query
        let (main, query) = match base_url.split_once('?') {
            Some((m, q)) => (m, format!("?{q}")),
            None => (base_url, String::new()),
        };

        // Find the database name as the segment after the last '/' in the main part
        let last_slash = main
            .rfind('/')
            .ok_or_else(|| eyre!("Invalid database URL format: {}", base_url))?;
        let (prefix, db_name) = main.split_at(last_slash + 1);

        // Skip namespacing if already present
        if db_name.contains(&format!("_{}", self.name)) {
            debug!(
                "Database URL already namespaced for environment {}",
                self.name
            );
            return Ok(base_url.to_string());
        }

        let namespaced_db = self.database_name(db_name);
        Ok(format!("{prefix}{namespaced_db}{query}"))
    }

    /// Get environment-namespaced NATS subject
    ///
    /// Prefixes NATS subjects with environment:
    /// - sinex.events.raw.> -> dev.sinex.events.raw.>
    pub fn nats_subject(&self, base_subject: &str) -> String {
        let env_prefix = &self.name;
        if base_subject.starts_with(&format!("{env_prefix}.")) {
            debug!(
                "NATS subject already namespaced for environment {}",
                self.name
            );
            base_subject.to_string()
        } else {
            format!("{}.{}", self.name, base_subject)
        }
    }

    /// Get environment-namespaced NATS stream name
    ///
    /// Prefixes stream names with environment:
    /// - SINEX_RAW_EVENTS -> DEV_SINEX_RAW_EVENTS
    pub fn nats_stream_name(&self, base_name: &str) -> String {
        let env_prefix = self.name.to_uppercase();
        if base_name.starts_with(&format!("{env_prefix}_")) {
            debug!(
                "NATS stream name already namespaced for environment {}",
                self.name
            );
            base_name.to_string()
        } else {
            format!("{env_prefix}_{base_name}")
        }
    }

    /// Get environment-namespaced socket path
    ///
    /// Modifies socket paths to include environment:
    /// - /run/sinex/ingest.sock -> /run/sinex-dev/ingest.sock
    pub fn socket_path<P: AsRef<Path>>(&self, base_path: P) -> PathBuf {
        let path = base_path.as_ref();

        if let Some(parent) = path.parent() {
            if let Some(filename) = path.file_name() {
                // Check if path is already namespaced
                if parent
                    .to_string_lossy()
                    .contains(&format!("-{}", self.name))
                {
                    debug!(
                        "Socket path already namespaced for environment {}",
                        self.name
                    );
                    return path.to_path_buf();
                }

                // Transform /run/sinex/file -> /run/sinex-env/file
                let parent_str = parent.to_string_lossy();
                let namespaced_parent = if parent_str.ends_with("sinex") {
                    format!("{}-{}", parent_str, self.name)
                } else {
                    // For other patterns, append environment to the last component
                    if let Some(parent_name) = parent.file_name() {
                        let parent_name_str = parent_name.to_string_lossy();
                        let namespaced_parent_name = format!("{}-{}", parent_name_str, self.name);
                        parent
                            .with_file_name(namespaced_parent_name)
                            .to_string_lossy()
                            .to_string()
                    } else {
                        parent_str.to_string()
                    }
                };

                PathBuf::from(namespaced_parent).join(filename)
            } else {
                path.to_path_buf()
            }
        } else {
            path.to_path_buf()
        }
    }

    /// Get environment-namespaced work directory
    ///
    /// Modifies work directories to include environment:
    /// - /tmp/sinex -> /tmp/sinex-dev
    pub fn work_directory<P: AsRef<Path>>(&self, base_path: P) -> PathBuf {
        let path = base_path.as_ref();

        // Check if already namespaced
        if path.to_string_lossy().contains(&format!("-{}", self.name)) {
            debug!(
                "Work directory already namespaced for environment {}",
                self.name
            );
            return path.to_path_buf();
        }

        // Append environment suffix to the path
        let path_str = path.to_string_lossy();
        PathBuf::from(format!("{}-{}", path_str, self.name))
    }

    /// Get environment-aware configuration prefix for figment
    ///
    /// Returns the environment variable prefix for configuration:
    /// - dev: SINEX_DEV_
    /// - staging: SINEX_STAGING_
    /// - prod: SINEX_PROD_
    pub fn config_prefix(&self) -> String {
        format!("SINEX_{}_", self.name.to_uppercase())
    }

    /// Get environment-specific temporary directory
    pub fn temp_dir(&self) -> PathBuf {
        self.work_directory("/tmp/sinex")
    }

    /// Get environment-specific runtime directory
    pub fn runtime_dir(&self) -> PathBuf {
        Path::new("/run").join(format!("sinex-{}", self.name))
    }

    /// Validate that all environment resources are properly isolated
    pub async fn validate_isolation(&self) -> Result<()> {
        info!("Validating environment isolation for '{}'", self.name);

        // Check database isolation
        if let Ok(db_url) = env::var("DATABASE_URL") {
            let namespaced = self.database_url(&db_url)?;
            if namespaced == db_url {
                warn!("DATABASE_URL is not environment-namespaced");
            }
        }

        // Check that critical paths are namespaced
        let socket_path = self.socket_path("/run/sinex/ingest.sock");
        if !socket_path.to_string_lossy().contains(&self.name) {
            return Err(eyre!("Socket path isolation failed"));
        }

        let work_dir = self.work_directory("/tmp/sinex");
        if !work_dir.to_string_lossy().contains(&self.name) {
            return Err(eyre!("Work directory isolation failed"));
        }

        info!(
            "Environment isolation validation passed for '{}'",
            self.name
        );
        Ok(())
    }
}

impl Default for SinexEnvironment {
    fn default() -> Self {
        Self::current().unwrap_or_else(|_| {
            warn!("Failed to get current environment, using dev");
            Self {
                name: "dev".to_string(),
            }
        })
    }
}

/// Global environment instance (lazy initialized)
static ENVIRONMENT: std::sync::OnceLock<SinexEnvironment> = std::sync::OnceLock::new();

/// Get the global environment instance
pub fn environment() -> &'static SinexEnvironment {
    ENVIRONMENT.get_or_init(|| {
        SinexEnvironment::current().unwrap_or_else(|e| {
            warn!("Failed to initialize environment: {}, using dev", e);
            SinexEnvironment {
                name: "dev".to_string(),
            }
        })
    })
}

/// Set the global environment (for testing)
#[cfg(test)]
pub fn set_test_environment(env: SinexEnvironment) {
    // This only works if environment hasn't been initialized yet
    let _ = ENVIRONMENT.set(env);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_environment_creation() {
        let env = SinexEnvironment::new("dev").unwrap();
        assert_eq!(env.name(), "dev");
        assert!(env.is_dev());
        assert!(!env.is_prod());

        let env = SinexEnvironment::new("prod").unwrap();
        assert_eq!(env.name(), "prod");
        assert!(env.is_prod());
        assert!(!env.is_dev());
    }

    #[test]
    fn test_invalid_environment() {
        let result = SinexEnvironment::new("invalid");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid environment"));
    }

    #[test]
    fn test_database_name_namespacing() {
        let env = SinexEnvironment::new("dev").unwrap();
        assert_eq!(env.database_name("sinex"), "sinex_dev");

        let env = SinexEnvironment::new("prod").unwrap();
        assert_eq!(env.database_name("sinex"), "sinex_prod");
    }

    #[test]
    fn test_database_url_namespacing() {
        let env = SinexEnvironment::new("dev").unwrap();

        let base_url = "postgresql:///sinex?host=/run/postgresql";
        let result = env.database_url(base_url).unwrap();
        assert_eq!(result, "postgresql:///sinex_dev?host=/run/postgresql");

        // Test already namespaced URL
        let result2 = env.database_url(&result).unwrap();
        assert_eq!(result2, result); // Should be unchanged
    }

    #[test]
    fn test_nats_subject_namespacing() {
        let env = SinexEnvironment::new("dev").unwrap();

        let subject = env.nats_subject("sinex.events.raw.>");
        assert_eq!(subject, "dev.sinex.events.raw.>");

        // Test already namespaced
        let subject2 = env.nats_subject(&subject);
        assert_eq!(subject2, subject); // Should be unchanged
    }

    #[test]
    fn test_nats_stream_name_namespacing() {
        let env = SinexEnvironment::new("dev").unwrap();

        let stream = env.nats_stream_name("SINEX_RAW_EVENTS");
        assert_eq!(stream, "DEV_SINEX_RAW_EVENTS");

        // Test already namespaced
        let stream2 = env.nats_stream_name(&stream);
        assert_eq!(stream2, stream); // Should be unchanged
    }

    #[test]
    fn test_socket_path_namespacing() {
        let env = SinexEnvironment::new("dev").unwrap();

        let path = env.socket_path("/run/sinex/ingest.sock");
        assert_eq!(path, PathBuf::from("/run/sinex-dev/ingest.sock"));

        // Test already namespaced
        let path2 = env.socket_path(&path);
        assert_eq!(path2, path); // Should be unchanged
    }

    #[test]
    fn test_work_directory_namespacing() {
        let env = SinexEnvironment::new("staging").unwrap();

        let dir = env.work_directory("/tmp/sinex");
        assert_eq!(dir, PathBuf::from("/tmp/sinex-staging"));

        // Test already namespaced
        let dir2 = env.work_directory(&dir);
        assert_eq!(dir2, dir); // Should be unchanged
    }

    #[test]
    fn test_config_prefix() {
        let env = SinexEnvironment::new("dev").unwrap();
        assert_eq!(env.config_prefix(), "SINEX_DEV_");

        let env = SinexEnvironment::new("prod").unwrap();
        assert_eq!(env.config_prefix(), "SINEX_PROD_");
    }

    #[test]
    fn test_environment_from_var() {
        // Test with environment variable set
        env::set_var("SINEX_ENVIRONMENT", "staging");
        let env = SinexEnvironment::current().unwrap();
        assert_eq!(env.name(), "staging");

        // Clean up
        env::remove_var("SINEX_ENVIRONMENT");
    }

    #[test]
    fn test_global_environment() {
        // This test may interfere with others since it uses a global
        let env = environment();
        assert!(["dev", "staging", "prod"].contains(&env.name()));
    }
}
