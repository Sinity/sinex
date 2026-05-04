#![doc = include_str!("../docs/native_messaging.md")]

use crate::config::GatewayConfig;
use sinex_primitives::env as shared_env;
use color_eyre::eyre::{Context, Result, bail, eyre};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_primitives::SinexError;
use std::collections::HashSet;
use std::io::{self};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

use crate::service_container::ServiceContainer;

/// Environment variable used to configure trusted native-messaging extensions.
const TRUSTED_EXTENSION_ENV: &str = "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS";
/// Environment variable used to configure trusted native-messaging hosts.
const TRUSTED_HOSTS_ENV: &str = "SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS";
/// Environment variable used to enforce a protocol version for native messaging.
const PROTOCOL_VERSION_ENV: &str = "SINEX_NATIVE_MESSAGING_PROTOCOL_VERSION";
/// Environment variable for maximum native messaging payload size in bytes.
const MAX_MESSAGE_SIZE_ENV: &str = "SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES";
/// Environment variable for read timeout in seconds (default: 30)
const READ_TIMEOUT_ENV: &str = "SINEX_NATIVE_MESSAGING_READ_TIMEOUT_SECS";
/// Default read timeout for native messaging reads (30 seconds)
const DEFAULT_READ_TIMEOUT_SECS: u64 = 30;
/// Default maximum native messaging payload size (1 MiB).
const DEFAULT_MAX_MESSAGE_SIZE_BYTES: usize = 1024 * 1024;
/// Environment variable for capability-based access control (JSON map: `extension_id` -> capabilities)
const CAPABILITIES_ENV: &str = "SINEX_NATIVE_MESSAGING_CAPABILITIES";
/// Environment variable for per-extension role mapping (JSON map: `extension_id` -> role)
const EXTENSION_ROLES_ENV: &str = "SINEX_NATIVE_MESSAGING_EXTENSION_ROLES";

/// Capability-based access control for native messaging extensions.
///
/// When configured via `SINEX_NATIVE_MESSAGING_CAPABILITIES`, each extension
/// can be restricted to specific methods and rate limits.
#[derive(Debug, Clone, Deserialize)]
pub struct ExtensionCapabilities {
    /// Set of RPC method names this extension is allowed to call.
    pub allowed_methods: HashSet<String>,
    /// Maximum requests per minute. `None` means unlimited.
    pub rate_limit_per_minute: Option<u32>,
}

/// Simple sliding-window rate limiter for native messaging.
///
/// Tracks request timestamps per extension and enforces a per-minute cap.
#[derive(Debug)]
struct RateLimiter {
    /// Map of `extension_id` -> recent request timestamps
    windows: std::sync::Mutex<std::collections::HashMap<String, Vec<std::time::Instant>>>,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            windows: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Check if a request is allowed and record it. Returns `Err` if rate limit exceeded.
    fn check_and_record(&self, extension_id: &str, limit_per_minute: u32) -> Result<()> {
        let now = std::time::Instant::now();
        let window = std::time::Duration::from_mins(1);
        let mut windows = match self.windows.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // Mutex poisoned by a previous panic. Recover by clearing all rate limit
                // history — this resets windows for all extensions but keeps rate limiting
                // operational. Failing open here would silently disable rate limiting.
                warn!("Rate limiter mutex poisoned; recovering by clearing all rate limit state");
                let mut guard = poisoned.into_inner();
                guard.clear();
                guard
            }
        };
        let timestamps = windows.entry(extension_id.to_string()).or_default();

        // Prune timestamps older than the window
        timestamps.retain(|ts| now.duration_since(*ts) < window);

        if timestamps.len() >= limit_per_minute as usize {
            warn!(
                event = "native_messaging.rate_limit",
                extension_id = extension_id,
                limit = limit_per_minute,
                "Rate limit exceeded for extension"
            );
            return Err(eyre!(
                "Rate limit exceeded for extension '{extension_id}': {limit_per_minute} requests/minute"
            ));
        }

        timestamps.push(now);
        Ok(())
    }
}

/// Configuration knobs for the native messaging server.
#[derive(Debug, Clone, Default)]
pub struct NativeMessagingConfig {
    trusted_extensions: Vec<TrustedExtension>,
    trusted_extensions_config_error: Option<String>,
    trusted_hosts: Vec<String>,
    trusted_hosts_config_error: Option<String>,
    expected_protocol_version: Option<String>,
    /// Per-extension capability restrictions. Key: `extension_id`, Value: capabilities.
    capabilities: std::collections::HashMap<String, ExtensionCapabilities>,
    /// Configuration parse error for `SINEX_NATIVE_MESSAGING_CAPABILITIES`.
    capabilities_config_error: Option<String>,
    /// Shared rate limiter state (wrapped in Arc for Clone)
    rate_limiter: Option<Arc<RateLimiter>>,
    /// Per-extension role mapping. Key: `extension_id`, Value: auth role.
    /// Loaded from `SINEX_NATIVE_MESSAGING_EXTENSION_ROLES` env var.
    extension_roles: std::collections::HashMap<String, crate::auth::Role>,
    /// Configuration parse error for `SINEX_NATIVE_MESSAGING_EXTENSION_ROLES`.
    extension_roles_config_error: Option<String>,
    max_message_size: usize,
    read_timeout: std::time::Duration,
}

#[derive(Debug)]
struct ParsedConfigMap<T> {
    values: std::collections::HashMap<String, T>,
    error: Option<String>,
}

impl<T> Default for ParsedConfigMap<T> {
    fn default() -> Self {
        Self {
            values: std::collections::HashMap::new(),
            error: None,
        }
    }
}

#[derive(Debug)]
struct ParsedConfigList<T> {
    values: Vec<T>,
    error: Option<String>,
}

impl<T> Default for ParsedConfigList<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            error: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct TrustedExtension {
    id: String,
    secret: Option<String>,
}

#[cfg(test)]
static SECRET_COMPARE_CALLS: AtomicUsize = AtomicUsize::new(0);

fn secrets_match(expected: &str, provided: &str) -> bool {
    #[cfg(test)]
    SECRET_COMPARE_CALLS.fetch_add(1, Ordering::Relaxed);

    bool::from(expected.as_bytes().ct_eq(provided.as_bytes()))
}

impl NativeMessagingConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Result<Self> {
        Ok(Self::from_raw(
            shared_env::strict_var(TRUSTED_EXTENSION_ENV)?.as_ref(),
            shared_env::strict_var(TRUSTED_HOSTS_ENV)?.as_ref(),
            shared_env::strict_var(PROTOCOL_VERSION_ENV)?.as_ref(),
            shared_env::strict_var(CAPABILITIES_ENV)?.as_ref(),
            shared_env::strict_var(EXTENSION_ROLES_ENV)?.as_ref(),
            env_positive_usize_with_default(MAX_MESSAGE_SIZE_ENV, DEFAULT_MAX_MESSAGE_SIZE_BYTES),
            std::time::Duration::from_secs(env_positive_u64_with_default(
                READ_TIMEOUT_ENV,
                DEFAULT_READ_TIMEOUT_SECS,
            )),
        ))
    }

    #[must_use]
    pub fn from_gateway_config(config: &GatewayConfig) -> Self {
        Self::from_raw(
            config.native_messaging_trusted_extensions.as_ref(),
            config.native_messaging_trusted_hosts.as_ref(),
            config.native_messaging_protocol_version.as_ref(),
            config.native_messaging_capabilities.as_ref(),
            config.native_messaging_extension_roles.as_ref(),
            config.native_messaging_max_size_bytes,
            std::time::Duration::from_secs(config.native_messaging_read_timeout_secs),
        )
    }

    fn from_raw(
        trusted_extensions_raw: Option<&String>,
        trusted_hosts_raw: Option<&String>,
        expected_protocol_version_raw: Option<&String>,
        capabilities_raw: Option<&String>,
        extension_roles_raw: Option<&String>,
        max_message_size: usize,
        read_timeout: std::time::Duration,
    ) -> Self {
        let parsed_trusted_extensions =
            parse_trusted_entries(trusted_extensions_raw.map(String::as_str));
        let parsed_trusted_hosts = parse_csv_entries(trusted_hosts_raw.map(String::as_str));
        let expected_protocol_version =
            expected_protocol_version_raw.and_then(|raw| normalize_optional_string(raw));
        let parsed_capabilities = parse_capabilities(capabilities_raw.map(String::as_str));
        let rate_limiter = if parsed_capabilities
            .values
            .values()
            .any(|c| c.rate_limit_per_minute.is_some())
        {
            Some(Arc::new(RateLimiter::new()))
        } else {
            None
        };
        let parsed_extension_roles = parse_extension_roles(extension_roles_raw.map(String::as_str));

        Self {
            trusted_extensions: parsed_trusted_extensions.values,
            trusted_extensions_config_error: parsed_trusted_extensions.error,
            trusted_hosts: parsed_trusted_hosts.values,
            trusted_hosts_config_error: parsed_trusted_hosts.error,
            expected_protocol_version,
            capabilities: parsed_capabilities.values,
            capabilities_config_error: parsed_capabilities.error,
            rate_limiter,
            extension_roles: parsed_extension_roles.values,
            extension_roles_config_error: parsed_extension_roles.error,
            max_message_size,
            read_timeout,
        }
    }

    fn enforce_metadata(&self, message: &NativeMessage) -> Result<()> {
        self.enforce_extension(message)?;
        self.enforce_capabilities(message)?;
        self.enforce_host(message)?;
        self.enforce_protocol_version(message)?;
        Ok(())
    }

    fn enforce_extension(&self, message: &NativeMessage) -> Result<()> {
        if let Some(err) = &self.trusted_extensions_config_error {
            return Err(eyre!(
                "Invalid {TRUSTED_EXTENSION_ENV} configuration: {err}"
            ));
        }

        // Issue 138: Fail closed - require explicit allowlist
        if self.trusted_extensions.is_empty() {
            warn!(
                event = "native_messaging.auth",
                reason = "no_trusted_extensions_configured",
                "Rejected native messaging call: no trusted extensions configured (set SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS)"
            );
            return Err(eyre!(
                "No trusted extensions configured. Set SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS environment variable."
            ));
        }

        let Some(incoming_id) = message.extension_id.as_deref() else {
            warn!(
                event = "native_messaging.auth",
                reason = "missing_extension_id",
                "Rejected native messaging call: extension metadata missing"
            );
            return Err(eyre!("Missing extension_id"));
        };

        let trusted = self
            .trusted_extensions
            .iter()
            .find(|ext| ext.id == incoming_id)
            .ok_or_else(|| {
                warn!(
                    event = "native_messaging.auth",
                    extension_id = incoming_id,
                    reason = "not_trusted",
                    "Extension is not in the trusted allow-list"
                );
                eyre!("Extension '{incoming_id}' is not in the trusted allow-list")
            })?;

        if let Some(expected_secret) = &trusted.secret {
            let Some(provided) = message.extension_secret.as_deref() else {
                warn!(
                    event = "native_messaging.auth",
                    extension_id = incoming_id,
                    reason = "missing_secret",
                    "Trusted extension omitted the required secret"
                );
                return Err(eyre!("Missing extension_secret"));
            };
            if !secrets_match(expected_secret, provided) {
                warn!(
                    event = "native_messaging.auth",
                    extension_id = incoming_id,
                    reason = "invalid_secret",
                    "Extension provided an invalid secret"
                );
                bail!("Invalid secret for extension '{incoming_id}'");
            }
        }

        debug!(
            event = "native_messaging.auth",
            extension_id = incoming_id,
            has_secret = trusted.secret.is_some(),
            "Native messaging request authorized"
        );
        Ok(())
    }

    /// Enforce capability-based access control for the extension.
    ///
    /// If capabilities are configured for this extension, the requested method
    /// must be in `allowed_methods` and rate limits are enforced.
    /// Capability configuration is mandatory (fail-closed).
    fn enforce_capabilities(&self, message: &NativeMessage) -> Result<()> {
        if let Some(err) = &self.capabilities_config_error {
            return Err(eyre!("Invalid {CAPABILITIES_ENV} configuration: {err}"));
        }

        // Explicit capability map is required for native messaging.
        if self.capabilities.is_empty() {
            return Err(eyre!(
                "Native messaging capabilities are not configured; set {CAPABILITIES_ENV}"
            ));
        }

        let Some(extension_id) = message.extension_id.as_deref() else {
            // Already handled in enforce_extension
            return Ok(());
        };

        let Some(caps) = self.capabilities.get(extension_id) else {
            return Err(eyre!(
                "No capability profile configured for extension '{extension_id}'"
            ));
        };

        // Enforce method allowlist
        if let Some(method) = message.method.as_deref()
            && !caps.allowed_methods.contains(method)
        {
            warn!(
                event = "native_messaging.capability",
                extension_id = extension_id,
                method = method,
                reason = "method_not_allowed",
                "Extension attempted to call disallowed method"
            );
            return Err(eyre!(
                "Extension '{extension_id}' is not allowed to call method '{method}'"
            ));
        }

        // Enforce rate limiting
        if let Some(limit) = caps.rate_limit_per_minute
            && let Some(ref limiter) = self.rate_limiter
        {
            limiter.check_and_record(extension_id, limit)?;
        }

        debug!(
            event = "native_messaging.capability",
            extension_id = extension_id,
            method = message.method.as_deref().unwrap_or("none"),
            "Capability check passed"
        );
        Ok(())
    }

    fn enforce_host(&self, message: &NativeMessage) -> Result<()> {
        if let Some(err) = &self.trusted_hosts_config_error {
            return Err(eyre!("Invalid {TRUSTED_HOSTS_ENV} configuration: {err}"));
        }

        if self.trusted_hosts.is_empty() {
            return Ok(());
        }

        let Some(host) = message.host.as_deref() else {
            warn!(
                event = "native_messaging.auth",
                reason = "missing_host",
                "Rejected native messaging call: host metadata missing"
            );
            return Err(eyre!("Missing host"));
        };

        if !self.trusted_hosts.iter().any(|allowed| allowed == host) {
            warn!(
                event = "native_messaging.auth",
                host = host,
                reason = "host_not_trusted",
                "Host is not in the trusted allow-list"
            );
            return Err(eyre!("Host '{host}' is not in the trusted allow-list"));
        }

        debug!(
            event = "native_messaging.auth",
            host = host,
            "Native messaging host authorized"
        );
        Ok(())
    }

    fn enforce_protocol_version(&self, message: &NativeMessage) -> Result<()> {
        let Some(expected) = self.expected_protocol_version.as_deref() else {
            return Ok(());
        };

        let Some(provided) = message.protocol_version.as_deref() else {
            warn!(
                event = "native_messaging.auth",
                expected_version = expected,
                reason = "missing_protocol_version",
                "Rejected native messaging call: protocol version missing"
            );
            return Err(eyre!("Missing protocol_version"));
        };

        if provided != expected {
            warn!(
                event = "native_messaging.auth",
                expected_version = expected,
                provided_version = provided,
                reason = "protocol_version_mismatch",
                "Rejected native messaging call: protocol version mismatch"
            );
            return Err(eyre!(
                "Protocol version mismatch (expected '{expected}', got '{provided}')"
            ));
        }

        debug!(
            event = "native_messaging.auth",
            protocol_version = provided,
            "Native messaging protocol version authorized"
        );
        Ok(())
    }

    /// Resolve the auth role for a given extension ID.
    /// Returns the configured role if found, or `ReadOnly` as the default.
    fn resolve_extension_role(&self, extension_id: Option<&str>) -> Result<crate::auth::Role> {
        if let Some(err) = &self.extension_roles_config_error {
            return Err(eyre!("Invalid {EXTENSION_ROLES_ENV} configuration: {err}"));
        }

        Ok(extension_id
            .and_then(|id| self.extension_roles.get(id))
            .copied()
            .unwrap_or(crate::auth::Role::ReadOnly))
    }
}

fn env_positive_usize_with_default(var: &str, default: usize) -> usize {
    match std::env::var(var) {
        Ok(raw) => match raw.parse::<usize>() {
            Ok(value) if value > 0 => value,
            Ok(_) => {
                warn!(
                    variable = var,
                    value = %raw,
                    default,
                    "Invalid native messaging override; expected a positive integer, using default"
                );
                default
            }
            Err(error) => {
                warn!(
                    variable = var,
                    value = %raw,
                    %error,
                    default,
                    "Invalid native messaging override; using default"
                );
                default
            }
        },
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                variable = var,
                default, "Native messaging override is not valid UTF-8; using default"
            );
            default
        }
    }
}

fn env_positive_u64_with_default(var: &str, default: u64) -> u64 {
    match std::env::var(var) {
        Ok(raw) => match raw.parse::<u64>() {
            Ok(value) if value > 0 => value,
            Ok(_) => {
                warn!(
                    variable = var,
                    value = %raw,
                    default,
                    "Invalid native messaging override; expected a positive integer, using default"
                );
                default
            }
            Err(error) => {
                warn!(
                    variable = var,
                    value = %raw,
                    %error,
                    default,
                    "Invalid native messaging override; using default"
                );
                default
            }
        },
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                variable = var,
                default, "Native messaging override is not valid UTF-8; using default"
            );
            default
        }
    }
}

fn normalize_optional_string(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_capabilities(raw: Option<&str>) -> ParsedConfigMap<ExtensionCapabilities> {
    let Some(raw) = raw else {
        return ParsedConfigMap::default();
    };

    match serde_json::from_str::<std::collections::HashMap<String, ExtensionCapabilities>>(raw) {
        Ok(caps) => {
            info!(
                extensions = caps.len(),
                "Loaded native messaging capabilities"
            );
            ParsedConfigMap {
                values: caps,
                error: None,
            }
        }
        Err(error) => {
            warn!(
                error = %error,
                "Failed to parse SINEX_NATIVE_MESSAGING_CAPABILITIES; requests will be denied with an explicit configuration error"
            );
            ParsedConfigMap {
                values: std::collections::HashMap::new(),
                error: Some(error.to_string()),
            }
        }
    }
}

fn parse_extension_roles(raw: Option<&str>) -> ParsedConfigMap<crate::auth::Role> {
    let Some(raw) = raw else {
        return ParsedConfigMap::default();
    };

    match serde_json::from_str::<std::collections::HashMap<String, crate::auth::Role>>(raw) {
        Ok(roles) => {
            info!(
                extensions = roles.len(),
                "Loaded native messaging extension roles"
            );
            ParsedConfigMap {
                values: roles,
                error: None,
            }
        }
        Err(error) => {
            warn!(
                error = %error,
                "Failed to parse SINEX_NATIVE_MESSAGING_EXTENSION_ROLES; requests will be denied with an explicit configuration error"
            );
            ParsedConfigMap {
                values: std::collections::HashMap::new(),
                error: Some(error.to_string()),
            }
        }
    }
}

fn parse_trusted_entries(raw: Option<&str>) -> ParsedConfigList<TrustedExtension> {
    let Some(raw) = raw else {
        return ParsedConfigList::default();
    };

    let mut values = Vec::new();
    let mut seen_ids = HashSet::new();

    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (id, secret) = match entry.split_once('#') {
            Some((id, secret)) => (id.trim(), Some(secret.trim().to_string())),
            None => (entry, None),
        };
        if id.is_empty() {
            return ParsedConfigList {
                values: Vec::new(),
                error: Some(format!(
                    "entry '{entry}' is missing an extension id before '#'"
                )),
            };
        }
        if !seen_ids.insert(id.to_string()) {
            return ParsedConfigList {
                values: Vec::new(),
                error: Some(format!("duplicate trusted extension id '{id}'")),
            };
        }
        values.push(TrustedExtension {
            id: id.to_string(),
            secret: secret.filter(|s| !s.is_empty()),
        });
    }

    ParsedConfigList {
        values,
        error: None,
    }
}

fn parse_csv_entries(raw: Option<&str>) -> ParsedConfigList<String> {
    let Some(raw) = raw else {
        return ParsedConfigList::default();
    };

    let values = raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();

    if !raw.trim().is_empty() && values.is_empty() {
        return ParsedConfigList {
            values,
            error: Some("no host entries could be parsed".to_string()),
        };
    }

    ParsedConfigList {
        values,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    fn trusted_message(secret: &str) -> NativeMessage {
        NativeMessage {
            msg_type: "request".to_string(),
            method: None,
            params: None,
            id: None,
            extension_id: Some("ext-1".to_string()),
            extension_secret: Some(secret.to_string()),
            host: None,
            protocol_version: None,
        }
    }

    #[sinex_test]
    async fn secret_comparison_is_routed_through_constant_time_helper() -> TestResult<()> {
        SECRET_COMPARE_CALLS.store(0, Ordering::Relaxed);

        let config = NativeMessagingConfig {
            trusted_extensions: vec![TrustedExtension {
                id: "ext-1".to_string(),
                secret: Some("topsecret".to_string()),
            }],
            trusted_extensions_config_error: None,
            trusted_hosts: Vec::new(),
            trusted_hosts_config_error: None,
            expected_protocol_version: None,
            capabilities: std::collections::HashMap::new(),
            capabilities_config_error: None,
            rate_limiter: None,
            extension_roles: std::collections::HashMap::new(),
            extension_roles_config_error: None,
            max_message_size: 1024 * 1024,
            read_timeout: std::time::Duration::from_secs(DEFAULT_READ_TIMEOUT_SECS),
        };

        // Successful path still calls the constant-time helper
        config
            .enforce_extension(&trusted_message("topsecret"))
            .expect("trusted secret should pass");

        // Failure path also uses the same helper
        assert!(
            config
                .enforce_extension(&trusted_message("wrongsecret"))
                .is_err()
        );

        assert!(SECRET_COMPARE_CALLS.load(Ordering::Relaxed) >= 2);
        Ok(())
    }

    #[sinex_test]
    async fn extension_roles_env_uses_typed_role_values() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(
            EXTENSION_ROLES_ENV,
            r#"{"ext-read":"readonly","ext-write":"write","ext-admin":"admin"}"#,
        );

        let config = NativeMessagingConfig::from_env()?;

        assert_eq!(
            config.resolve_extension_role(Some("ext-read"))?,
            crate::auth::Role::ReadOnly
        );
        assert_eq!(
            config.resolve_extension_role(Some("ext-write"))?,
            crate::auth::Role::Write
        );
        assert_eq!(
            config.resolve_extension_role(Some("ext-admin"))?,
            crate::auth::Role::Admin
        );
        Ok(())
    }

    #[sinex_test]
    async fn invalid_extension_role_env_entry_surfaces_parse_error() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(EXTENSION_ROLES_ENV, r#"{"ext-write":"superuser"}"#);

        let config = NativeMessagingConfig::from_env()?;

        let error = config
            .resolve_extension_role(Some("ext-write"))
            .expect_err("invalid role config should be surfaced");
        assert!(error.to_string().contains(EXTENSION_ROLES_ENV));
        Ok(())
    }

    #[sinex_test]
    async fn invalid_capabilities_env_entry_surfaces_parse_error() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(
            CAPABILITIES_ENV,
            r#"{"ext-1":{"allowed_methods":"system.health","rate_limit_per_minute":null}}"#,
        );

        let config = NativeMessagingConfig::from_env()?;
        let message = NativeMessage {
            msg_type: "rpc".to_string(),
            method: Some("system.health".to_string()),
            params: Some(serde_json::json!({})),
            id: None,
            extension_id: Some("ext-1".to_string()),
            extension_secret: None,
            host: None,
            protocol_version: None,
        };

        let error = config
            .enforce_capabilities(&message)
            .expect_err("invalid capabilities config should be surfaced");
        assert!(error.to_string().contains(CAPABILITIES_ENV));
        Ok(())
    }

    #[sinex_test]
    async fn invalid_trusted_extensions_env_entry_surfaces_parse_error() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(TRUSTED_EXTENSION_ENV, "#missing-id");

        let config = NativeMessagingConfig::from_env()?;
        let error = config
            .enforce_extension(&trusted_message("anything"))
            .expect_err("malformed trusted extension config should be surfaced");
        assert!(error.to_string().contains(TRUSTED_EXTENSION_ENV));
        assert!(error.to_string().contains("missing an extension id"));
        Ok(())
    }

    #[sinex_test]
    async fn duplicate_trusted_extensions_env_entry_surfaces_parse_error() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(TRUSTED_EXTENSION_ENV, "ext-1#alpha,ext-1#beta");

        let config = NativeMessagingConfig::from_env()?;
        let error = config
            .enforce_extension(&trusted_message("alpha"))
            .expect_err("duplicate trusted extension ids should be surfaced");
        assert!(error.to_string().contains(TRUSTED_EXTENSION_ENV));
        assert!(error.to_string().contains("duplicate trusted extension id"));
        Ok(())
    }

    #[sinex_test]
    async fn invalid_trusted_hosts_env_entry_surfaces_parse_error() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(TRUSTED_HOSTS_ENV, " , ");

        let config = NativeMessagingConfig::from_env()?;
        let error = config
            .enforce_host(&NativeMessage {
                msg_type: "request".to_string(),
                method: None,
                params: None,
                id: None,
                extension_id: None,
                extension_secret: None,
                host: Some("localhost".to_string()),
                protocol_version: None,
            })
            .expect_err("malformed trusted hosts config should be surfaced");
        assert!(error.to_string().contains(TRUSTED_HOSTS_ENV));
        assert!(
            error
                .to_string()
                .contains("no host entries could be parsed")
        );
        Ok(())
    }

    #[sinex_test]
    async fn native_messaging_numeric_env_overrides_apply_valid_values() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(MAX_MESSAGE_SIZE_ENV, "2048");
        env.set(READ_TIMEOUT_ENV, "12");

        let config = NativeMessagingConfig::from_env()?;

        assert_eq!(config.max_message_size, 2048);
        assert_eq!(config.read_timeout, std::time::Duration::from_secs(12));
        Ok(())
    }

    #[sinex_test]
    async fn native_messaging_numeric_env_overrides_reject_invalid_values() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(MAX_MESSAGE_SIZE_ENV, "0");
        env.set(READ_TIMEOUT_ENV, "forever");

        let config = NativeMessagingConfig::from_env()?;

        assert_eq!(config.max_message_size, DEFAULT_MAX_MESSAGE_SIZE_BYTES);
        assert_eq!(
            config.read_timeout,
            std::time::Duration::from_secs(DEFAULT_READ_TIMEOUT_SECS)
        );
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn native_messaging_env_rejects_non_utf8_trusted_extensions() -> TestResult<()> {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let mut env = EnvGuard::new();
        env.set(
            TRUSTED_EXTENSION_ENV,
            OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]),
        );

        let error = NativeMessagingConfig::from_env()
            .expect_err("non-UTF-8 native messaging env should be rejected");
        assert!(error.to_string().contains(
            "Environment variable SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS is not valid UTF-8"
        ));
        Ok(())
    }

    #[sinex_test]
    async fn native_messaging_read_rejects_header_timeout() -> TestResult<()> {
        let (_writer, mut reader) = tokio::io::duplex(64);

        let error = read_message_from(
            &mut reader,
            DEFAULT_MAX_MESSAGE_SIZE_BYTES,
            std::time::Duration::from_millis(10),
        )
        .await
        .expect_err("header timeout should be surfaced");

        let message = error.to_string();
        assert!(message.contains("header"));
        assert!(message.contains("timed out"));
        Ok(())
    }

    #[sinex_test]
    async fn native_messaging_read_rejects_body_timeout() -> TestResult<()> {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer.write_all(&(4_u32).to_le_bytes()).await?;
        writer.write_all(&[0x7b, 0x7d]).await?;

        let error = read_message_from(
            &mut reader,
            DEFAULT_MAX_MESSAGE_SIZE_BYTES,
            std::time::Duration::from_millis(10),
        )
        .await
        .expect_err("body timeout should be surfaced");

        let message = error.to_string();
        assert!(message.contains("body"));
        assert!(message.contains("expected_bytes"));
        Ok(())
    }
}

/// Transport abstraction so tests can drive the native messaging loop without stdin/stdout.
#[allow(async_fn_in_trait)]
pub trait NativeMessagingTransport: Send {
    async fn read_message(&mut self) -> Result<Option<NativeMessage>>;
    async fn write_message(&mut self, response: &NativeResponse) -> Result<()>;
}

#[derive(Debug, Clone, Deserialize)]
pub struct NativeMessage {
    #[serde(rename = "type")]
    msg_type: String,
    method: Option<String>,
    params: Option<Value>,
    id: Option<String>,
    #[serde(default)]
    extension_id: Option<String>,
    #[serde(default)]
    extension_secret: Option<String>,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    protocol_version: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NativeResponse {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
}

impl NativeResponse {
    fn success(id: Option<String>, result: Value) -> Self {
        Self {
            msg_type: "response".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    fn error(id: Option<String>, error: String) -> Self {
        Self {
            msg_type: "error".to_string(),
            result: None,
            error: Some(error),
            id,
        }
    }
}

/// Read a message from stdin using native messaging protocol (async)
async fn read_message_async(
    max_message_size: usize,
    read_timeout: std::time::Duration,
) -> Result<Option<NativeMessage>> {
    let mut stdin = tokio::io::stdin();
    read_message_from(&mut stdin, max_message_size, read_timeout).await
}

fn native_messaging_read_timeout(
    phase: &'static str,
    read_timeout: std::time::Duration,
    expected_bytes: Option<usize>,
) -> color_eyre::Report {
    let mut error = SinexError::network(format!(
        "Native messaging {phase} read timed out after {read_timeout:?}"
    ))
    .with_context("phase", phase)
    .with_context("timeout_secs", read_timeout.as_secs_f64().to_string());

    if let Some(expected_bytes) = expected_bytes {
        error = error.with_context("expected_bytes", expected_bytes.to_string());
    }

    error.into()
}

async fn read_message_from<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_message_size: usize,
    read_timeout: std::time::Duration,
) -> Result<Option<NativeMessage>> {
    // Read message length (4 bytes, little-endian)
    let mut len_bytes = [0u8; 4];

    // Wrap read in timeout to prevent indefinite blocking if browser crashes
    match tokio::time::timeout(read_timeout, reader.read_exact(&mut len_bytes)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Err(native_messaging_read_timeout("header", read_timeout, None)),
    }
    let length = u32::from_le_bytes(len_bytes) as usize;

    if length > max_message_size {
        bail!(
            "Message too large: {} bytes (limit: {})",
            length,
            max_message_size
        );
    }

    let mut buffer = vec![0u8; length];
    match tokio::time::timeout(read_timeout, reader.read_exact(&mut buffer)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => {
            return Err(native_messaging_read_timeout(
                "body",
                read_timeout,
                Some(length),
            ));
        }
    }

    let message: NativeMessage =
        serde_json::from_slice(&buffer).wrap_err("Failed to parse native message")?;

    Ok(Some(message))
}

async fn write_message_async(response: &NativeResponse) -> Result<()> {
    let mut stdout = tokio::io::stdout();
    let json = serde_json::to_vec(response)?;
    let len_bytes = (json.len() as u32).to_le_bytes();

    stdout.write_all(&len_bytes).await?;
    stdout.write_all(&json).await?;
    stdout.flush().await?;

    Ok(())
}

struct StdioNativeMessagingTransport {
    max_message_size: usize,
    read_timeout: std::time::Duration,
}

impl NativeMessagingTransport for StdioNativeMessagingTransport {
    async fn read_message(&mut self) -> Result<Option<NativeMessage>> {
        read_message_async(self.max_message_size, self.read_timeout).await
    }

    async fn write_message(&mut self, response: &NativeResponse) -> Result<()> {
        write_message_async(response).await
    }
}

/// Process a single message and return response
async fn process_message(
    services: &ServiceContainer,
    config: &NativeMessagingConfig,
    message: NativeMessage,
) -> NativeResponse {
    let message_id = message.id.clone();
    let span = tracing::info_span!(
        "native_messaging.request",
        extension_id = message
            .extension_id
            .as_deref()
            .unwrap_or("unknown_extension"),
        host = message.host.as_deref().unwrap_or("unknown_host"),
        protocol_version = message
            .protocol_version
            .as_deref()
            .unwrap_or("unknown_version")
    );
    let _guard = span.enter();

    if let Err(err) = config.enforce_metadata(&message) {
        return NativeResponse::error(message_id, format!("Native messaging rejected: {err}"));
    }

    // Handle different message types
    match message.msg_type.as_str() {
        "ping" => NativeResponse::success(message.id, serde_json::json!({ "pong": true })),

        "rpc" => match (message.method, message.params) {
            (Some(method), Some(params)) => {
                match dispatch_method(
                    services,
                    config,
                    &method,
                    params,
                    message.extension_id.as_deref(),
                )
                .await
                {
                    Ok(result) => NativeResponse::success(message.id, result),
                    Err(err) => NativeResponse::error(message.id, err.to_string()),
                }
            }
            _ => NativeResponse::error(
                message.id,
                "RPC message must include method and params".to_string(),
            ),
        },

        _ => NativeResponse::error(
            message.id,
            format!("Unknown message type: {}", message.msg_type),
        ),
    }
}

/// Dispatch RPC method to appropriate handler (shared with `rpc_server`)
async fn dispatch_method(
    services: &ServiceContainer,
    config: &NativeMessagingConfig,
    method: &str,
    params: Value,
    extension_id: Option<&str>,
) -> Result<Value> {
    // Resolve per-extension auth role from configuration
    let role = config.resolve_extension_role(extension_id)?;
    let auth = match extension_id {
        Some(id) => crate::rpc_server::RpcAuthContext::extension(id, role),
        None => crate::rpc_server::RpcAuthContext::system(),
    };

    // Use shared dispatch table from rpc_server
    crate::rpc_server::dispatch_rpc_method("native", services, method, params, &auth)
        .await
        .map_err(Into::into)
}

/// Run the native messaging loop using stdin/stdout transport.
pub async fn run(
    services: ServiceContainer,
    gateway_config: &GatewayConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let config = NativeMessagingConfig::from_gateway_config(gateway_config);
    let transport = StdioNativeMessagingTransport {
        max_message_size: config.max_message_size,
        read_timeout: config.read_timeout,
    };
    run_with_transport(services, config, transport, shutdown).await
}

/// Run the native messaging loop with a custom transport and configuration.
pub async fn run_with_transport<T: NativeMessagingTransport>(
    services: ServiceContainer,
    config: NativeMessagingConfig,
    mut transport: T,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    info!("Starting native messaging mode");

    loop {
        tokio::select! {
            message_result = transport.read_message() => {
                if let Some(message) = message_result? {
                    debug!("Received message: {:?}", message);

                    let response = process_message(&services, &config, message).await;

                    if let Err(e) = transport.write_message(&response).await {
                        error!("Failed to write response: {}", e);
                        break;
                    }
                } else {
                    info!("EOF reached, shutting down");
                    break;
                }
            }
            shutdown_result = shutdown.changed() => {
                if shutdown_result.is_err() {
                    warn!("Native messaging shutdown channel dropped before explicit shutdown");
                }
                if shutdown_result.is_err() || *shutdown.borrow() {
                    info!("Shutdown signal received, stopping native messaging");
                    break;
                }
            }
        }
    }

    info!("Native messaging shutdown complete");
    Ok(())
}
