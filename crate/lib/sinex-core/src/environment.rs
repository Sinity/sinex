//! Environment namespacing for Sinex
//!
//! This module provides centralized environment-aware resource naming for proper isolation
//! between development, staging, and production environments. All resources (database names,
//! NATS subjects, socket paths, work directories) are namespaced based on SINEX_ENVIRONMENT.

use color_eyre::eyre::{eyre, Result};
use std::env;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};
use url::{form_urlencoded, Url};

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
    fn path_is_namespaced<P: AsRef<Path>>(&self, path: P) -> bool {
        let suffix = format!("-{}", self.name);
        path.as_ref()
            .components()
            .any(|component| component.as_os_str().to_string_lossy().ends_with(&suffix))
    }

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
        if base_url.trim().is_empty() {
            return Err(eyre!("Database URL cannot be empty"));
        }

        let mut url =
            Url::parse(base_url).map_err(|e| eyre!("Invalid database URL format: {e}"))?;
        let mut query_pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();

        if let Some(idx) = query_pairs
            .iter_mut()
            .position(|(key, _)| Self::is_dbname_param(key))
        {
            let (_, value) = &mut query_pairs[idx];
            if value.is_empty() {
                return Err(eyre!(
                    "dbname query parameter is empty in database URL: {base_url}"
                ));
            }

            if self.is_already_namespaced(value) {
                debug!(
                    "Database URL already namespaced for environment {} via dbname/database parameter",
                    self.name
                );
                return Ok(base_url.to_string());
            }

            *value = self.database_name(value);
            Self::rewrite_query(&mut url, &query_pairs);
            return Ok(url.into());
        }

        let mut segments: Vec<String> = url
            .path_segments()
            .map(|segments| segments.map(|s| s.to_string()).collect())
            .unwrap_or_default();

        if segments.is_empty() || segments.last().map_or(true, |s| s.is_empty()) {
            return Err(eyre!(
                "Database name missing from URL and no dbname query parameter provided: {}",
                base_url
            ));
        }

        if segments
            .last()
            .is_some_and(|segment| segment.ends_with(&format!("_{}", self.name)))
        {
            debug!(
                "Database URL already namespaced for environment {} via path",
                self.name
            );
            return Ok(base_url.to_string());
        }

        if let Some(last) = segments.last_mut() {
            *last = self.database_name(last);
        }

        let mut new_path = String::new();
        for segment in &segments {
            new_path.push('/');
            new_path.push_str(segment);
        }
        url.set_path(&new_path);

        Ok(url.into())
    }

    fn is_dbname_param(key: &str) -> bool {
        key.eq_ignore_ascii_case("dbname") || key.eq_ignore_ascii_case("database")
    }

    fn is_already_namespaced(&self, value: &str) -> bool {
        value.ends_with(&format!("_{}", self.name))
    }

    fn rewrite_query(url: &mut Url, pairs: &[(String, String)]) {
        if pairs.is_empty() {
            url.set_query(None);
            return;
        }

        let mut serializer = form_urlencoded::Serializer::new(String::new());
        for (key, val) in pairs {
            if val.is_empty() {
                serializer.append_key_only(key);
            } else {
                serializer.append_pair(key, val);
            }
        }
        url.set_query(Some(&serializer.finish()));
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
                if self.path_is_namespaced(parent) {
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
        if self.path_is_namespaced(path) {
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
