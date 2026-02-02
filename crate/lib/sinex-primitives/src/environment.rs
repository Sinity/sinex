//! Environment namespacing for Sinex
//!
//! This module provides centralized environment-aware resource naming for proper isolation
//! between development, staging, and production environments. All resources (database names,
//! NATS subjects, socket paths, work directories) are namespaced based on `SINEX_ENVIRONMENT`.

use crate::error::{Result, SinexError};
use std::env;
use std::path::{Component, Path, PathBuf};
use tracing::{debug, info, warn};
use url::{form_urlencoded, Url};

/// Default environment when `SINEX_ENVIRONMENT` is not set
const DEFAULT_ENVIRONMENT: &str = "dev";

/// Max environment name length.
const MAX_ENVIRONMENT_LEN: usize = 64;

fn allow_default_environment() -> bool {
    cfg!(debug_assertions) || cfg!(test)
}

fn is_valid_environment_name(name: &str) -> bool {
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

/// Environment context providing namespaced resource access
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinexEnvironment {
    /// Environment name (dev, staging, prod)
    name: String,
}

impl SinexEnvironment {
    fn normalize_path<P: AsRef<Path>>(path: P) -> PathBuf {
        let mut normalized = PathBuf::new();
        for component in path.as_ref().components() {
            match component {
                Component::CurDir => {}
                Component::ParentDir => {
                    normalized.pop();
                }
                other => normalized.push(other.as_os_str()),
            }
        }
        normalized
    }

    fn path_is_namespaced<P: AsRef<Path>>(&self, path: P) -> bool {
        let suffix = format!("-{}", self.name);
        path.as_ref()
            .components()
            .any(|component| component.as_os_str().to_string_lossy().ends_with(&suffix))
    }

    /// Get the current environment from `SINEX_ENVIRONMENT` variable
    pub fn current() -> Result<Self> {
        match env::var("SINEX_ENVIRONMENT") {
            Ok(name) => Self::new(&name),
            Err(_) if allow_default_environment() => {
                warn!(
                    "SINEX_ENVIRONMENT not set, defaulting to '{}'",
                    DEFAULT_ENVIRONMENT
                );
                Self::new(DEFAULT_ENVIRONMENT)
            }
            Err(_) => Err(SinexError::configuration(
                "SINEX_ENVIRONMENT must be set for non-dev builds",
            )),
        }
    }

    /// Create a new environment context with validation
    pub fn new(name: &str) -> Result<Self> {
        let name = name.trim().to_lowercase();

        if name.is_empty() {
            return Err(SinexError::configuration(
                "Environment name cannot be empty",
            ));
        }

        if name.len() > MAX_ENVIRONMENT_LEN {
            return Err(SinexError::configuration(format!(
                "Environment name cannot exceed {MAX_ENVIRONMENT_LEN} characters"
            )));
        }

        if !is_valid_environment_name(&name) {
            return Err(SinexError::configuration(format!(
                "Invalid environment '{name}'. Use [a-z0-9_-]+"
            )));
        }

        info!("Initialized Sinex environment: {}", name);
        Ok(Self { name })
    }

    /// Get the environment name
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if this is the development environment
    #[must_use]
    pub fn is_dev(&self) -> bool {
        self.name == "dev"
    }

    /// Check if this is the staging environment
    #[must_use]
    pub fn is_staging(&self) -> bool {
        self.name == "staging"
    }

    /// Check if this is the production environment
    #[must_use]
    pub fn is_prod(&self) -> bool {
        self.name == "prod"
    }

    /// Get environment-namespaced database name
    ///
    /// Transforms base database name into environment-specific name:
    /// - sinex -> `sinex_dev`, `sinex_staging`, `sinex_prod`
    #[must_use]
    pub fn database_name(&self, base_name: &str) -> String {
        format!("{}_{}", base_name, self.name)
    }

    /// Get environment-namespaced database URL
    ///
    /// Modifies the database URL to use environment-specific database name
    pub fn database_url(&self, base_url: &str) -> Result<String> {
        if base_url.trim().is_empty() {
            return Err(SinexError::configuration("Database URL cannot be empty"));
        }

        let mut url = Url::parse(base_url)
            .map_err(|e| SinexError::configuration(format!("Invalid database URL format: {e}")))?;
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
                return Err(SinexError::configuration(format!(
                    "dbname query parameter is empty in database URL: {base_url}"
                )));
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
            .map(|segments| segments.map(std::string::ToString::to_string).collect())
            .unwrap_or_default();

        if segments.is_empty() || segments.last().is_none_or(std::string::String::is_empty) {
            return Err(SinexError::configuration(format!(
                "Database name missing from URL and no dbname query parameter provided: {base_url}"
            )));
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
    /// - `SINEX_RAW_EVENTS` -> `DEV_SINEX_RAW_EVENTS`
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

    /// Get NATS credentials file path if authentication is enabled
    ///
    /// Reads from `NATS_CREDS` environment variable or falls back to
    /// namespaced path in runtime directory if not implicitly set.
    #[must_use]
    pub fn nats_creds_path(&self) -> Option<PathBuf> {
        if let Ok(creds) = env::var("NATS_CREDS") {
            return Some(PathBuf::from(creds));
        }

        let runtime_creds = self.runtime_dir().join("nats.creds");
        if runtime_creds.exists() {
            return Some(runtime_creds);
        }

        None
    }

    /// Get an environment-namespaced NATS subject with an additional test namespace.
    #[must_use]
    pub fn nats_subject_with_namespace(
        &self,
        namespace: Option<&str>,
        base_subject: &str,
    ) -> String {
        let trimmed = base_subject.trim_start_matches('.');
        if let Some(ns) = namespace {
            let ns = ns.trim_matches('.');
            if ns.is_empty() {
                return self.nats_subject(trimmed);
            }
            self.nats_subject(&format!("{ns}.{trimmed}"))
        } else {
            self.nats_subject(trimmed)
        }
    }

    /// Get an environment-namespaced stream name with an additional namespace suffix.
    #[must_use]
    pub fn nats_stream_name_with_namespace(
        &self,
        namespace: Option<&str>,
        base_name: &str,
    ) -> String {
        if let Some(ns) = namespace {
            let suffix = ns
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() {
                        c.to_ascii_uppercase()
                    } else {
                        '_'
                    }
                })
                .collect::<String>();
            self.nats_stream_name(&format!("{base_name}_{suffix}"))
        } else {
            self.nats_stream_name(base_name)
        }
    }

    /// Get environment-namespaced NATS KV bucket name
    ///
    /// Prefixes KV bucket names with environment:
    /// - `sinex_checkpoints` -> `dev_sinex_checkpoints`
    pub fn nats_kv_bucket_name(&self, base_name: &str) -> String {
        let env_prefix = self.name.to_lowercase();
        if base_name.starts_with(&format!("{env_prefix}_")) {
            debug!(
                "NATS KV bucket name already namespaced for environment {}",
                self.name
            );
            base_name.to_string()
        } else {
            format!("{env_prefix}_{base_name}")
        }
    }

    /// Get an environment-namespaced KV bucket name with an additional namespace suffix.
    #[must_use]
    pub fn nats_kv_bucket_with_namespace(
        &self,
        namespace: Option<&str>,
        base_name: &str,
    ) -> String {
        if let Some(ns) = namespace {
            let suffix = ns
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() {
                        c.to_ascii_lowercase()
                    } else {
                        '_'
                    }
                })
                .collect::<String>();
            self.nats_kv_bucket_name(&format!("{base_name}_{suffix}"))
        } else {
            self.nats_kv_bucket_name(base_name)
        }
    }

    /// Get environment-namespaced socket path
    ///
    /// Modifies socket paths to include environment:
    /// - /tmp/sinex-host.sock -> /tmp-dev/sinex-host.sock
    pub fn socket_path<P: AsRef<Path>>(&self, base_path: P) -> PathBuf {
        let path = Self::normalize_path(base_path);

        if let Some(parent) = path.parent() {
            if let Some(filename) = path.file_name() {
                // Check if path is already namespaced
                if self.path_is_namespaced(parent) {
                    debug!(
                        "Socket path already namespaced for environment {}",
                        self.name
                    );
                    return path.clone();
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
                path.clone()
            }
        } else {
            path.clone()
        }
    }

    /// Get environment-namespaced work directory
    ///
    /// Modifies work directories to include environment:
    /// - /tmp/sinex -> /tmp/sinex-dev
    pub fn work_directory<P: AsRef<Path>>(&self, base_path: P) -> PathBuf {
        let path = Self::normalize_path(base_path);

        // Check if already namespaced
        if self.path_is_namespaced(&path) {
            debug!(
                "Work directory already namespaced for environment {}",
                self.name
            );
            return path;
        }

        // Append environment suffix to the path
        let path_str = path.to_string_lossy();
        PathBuf::from(format!("{}-{}", path_str, self.name))
    }

    /// Get environment-aware configuration prefix for figment
    ///
    /// Returns the environment variable prefix for configuration:
    /// - dev: `SINEX_DEV`_
    /// - staging: `SINEX_STAGING`_
    /// - prod: `SINEX_PROD`_
    #[must_use]
    pub fn config_prefix(&self) -> String {
        format!("SINEX_{}_", self.name.to_uppercase())
    }

    /// Get environment-specific temporary directory
    #[must_use]
    pub fn temp_dir(&self) -> PathBuf {
        self.work_directory("/tmp/sinex")
    }

    /// Get environment-specific runtime directory
    #[must_use]
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
        let socket_path = self.socket_path("/tmp/sinex-host.sock");
        if !socket_path.to_string_lossy().contains(&self.name) {
            return Err(SinexError::configuration("Socket path isolation failed"));
        }

        let work_dir = self.work_directory("/tmp/sinex");
        if !work_dir.to_string_lossy().contains(&self.name) {
            return Err(SinexError::configuration("Work directory isolation failed"));
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

#[cfg(any(test, feature = "testing"))]
static ENVIRONMENT_OVERRIDE: std::sync::OnceLock<std::sync::RwLock<Option<SinexEnvironment>>> =
    std::sync::OnceLock::new();

#[cfg(any(test, feature = "testing"))]
pub struct EnvironmentOverrideGuard {
    previous: Option<SinexEnvironment>,
}

#[cfg(any(test, feature = "testing"))]
impl Drop for EnvironmentOverrideGuard {
    fn drop(&mut self) {
        let lock = ENVIRONMENT_OVERRIDE.get_or_init(|| std::sync::RwLock::new(None));
        if let Ok(mut guard) = lock.write() {
            *guard = self.previous.take();
        }
    }
}

#[cfg(any(test, feature = "testing"))]
pub fn override_environment_for_tests(name: &str) -> Result<EnvironmentOverrideGuard> {
    let env = SinexEnvironment::new(name)?;
    let lock = ENVIRONMENT_OVERRIDE.get_or_init(|| std::sync::RwLock::new(None));
    let mut guard = lock
        .write()
        .map_err(|_| SinexError::invalid_state("Failed to acquire environment override lock"))?;
    let previous = guard.clone();
    *guard = Some(env);
    Ok(EnvironmentOverrideGuard { previous })
}

#[cfg(any(test, feature = "testing"))]
fn environment_override() -> Option<SinexEnvironment> {
    ENVIRONMENT_OVERRIDE
        .get()
        .and_then(|lock| lock.read().ok().and_then(|guard| guard.as_ref().cloned()))
}

/// Get the global environment instance
pub fn environment() -> SinexEnvironment {
    #[cfg(any(test, feature = "testing"))]
    if let Some(env) = environment_override() {
        return env;
    }

    ENVIRONMENT
        .get_or_init(|| {
            SinexEnvironment::current().unwrap_or_else(|e| {
                if allow_default_environment() {
                    warn!("Failed to initialize environment: {}, using dev", e);
                    SinexEnvironment {
                        name: "dev".to_string(),
                    }
                } else {
                    panic!("Failed to initialize environment: {e}");
                }
            })
        })
        .clone()
}
