//! Unified privacy engine for Sinex.
//!
//! All sensitive-data handling — secret redaction, PII detection, encryption,
//! hashing — flows through a single [`PrivacyEngine`] instance obtained via
//! [`engine()`].
//!
//! # Quick start
//!
//! ```
//! use sinex_primitives::privacy::{self, ProcessingContext};
//!
//! let result = privacy::process("export TOKEN=ghp_abc123", ProcessingContext::Command)?;
//! assert!(!result.matched_rules.is_empty());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod catalog;
mod config;
mod detector;
mod engine;
mod envelope;

pub use config::{PrivacyConfig, PrivacyConfigError};
pub use engine::PrivacyEngine;

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::OnceLock;

// ─── Global engine ───────────────────────────────────────────

static ENGINE: OnceLock<Result<PrivacyEngine, PrivacyError>> = OnceLock::new();

fn build_engine_from_env() -> Result<PrivacyEngine, PrivacyError> {
    let config = PrivacyConfig::from_env().map_err(PrivacyError::Config)?;
    PrivacyEngine::new(config)
}

/// Get the process-wide privacy engine.
///
/// On first call, initializes from `PrivacyConfig::from_env()`.
///
/// Returns the same cached initialization error on every call if privacy
/// configuration or built-in rule compilation fails.
pub fn engine() -> Result<&'static PrivacyEngine, &'static PrivacyError> {
    ENGINE.get_or_init(build_engine_from_env).as_ref()
}

/// Process text with the global privacy engine.
pub fn process(
    text: &str,
    context: ProcessingContext,
) -> Result<Processed<'_>, &'static PrivacyError> {
    Ok(engine()?.process(text, context))
}

/// Process JSON with the global privacy engine.
pub fn process_json(
    value: &serde_json::Value,
    context: ProcessingContext,
) -> Result<serde_json::Value, &'static PrivacyError> {
    Ok(engine()?.process_json(value, context))
}

// ─── Processing context ──────────────────────────────────────

/// What kind of content is being processed.
///
/// Different contexts activate different rule subsets and have different
/// false-positive tolerances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingContext {
    /// Shell commands, command-line arguments.
    Command,
    /// Clipboard text content.
    Clipboard,
    /// Window and tab titles.
    WindowTitle,
    /// Systemd journal messages and fields.
    Journal,
    /// D-Bus method arguments, signal payloads.
    Dbus,
    /// Notification body text.
    Notification,
    /// File / document body text.
    Document,
    /// Structured metadata fields (hostnames, PIDs, paths).
    Metadata,
}

// ─── Strategy ────────────────────────────────────────────────

/// What to do when a rule matches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Strategy {
    /// Replace matched text with a fixed label. Lossy, non-reversible.
    Redact {
        /// Replacement label. Supports `$1`, `$2` capture group refs for regex
        /// matchers. If `None`, uses `<RULE_NAME>`.
        label: Option<String>,
    },
    /// Encrypt using XChaCha20-Poly1305 with the system privacy key.
    /// Output: `⌜enc:v1:<b64url>⌝`. Reversible with the correct key.
    Encrypt,
    /// Replace with a keyed BLAKE3 MAC. Deterministic for the same input+key.
    /// Output: `⌜hash:<hex>⌝`. Not reversible but allows correlation.
    Hash,
    /// Drop the containing field entirely.
    Suppress,
    /// Partially obscure matched text, keeping some characters visible.
    ///
    /// Example: `4111111111111111` with `keep_prefix: 4, keep_suffix: 4, char: '*'`
    /// produces `4111********1111`.
    Mask {
        /// Character to use for masking. Defaults to `'*'`.
        char: Option<char>,
        /// Number of characters to keep visible at the start.
        keep_prefix: Option<usize>,
        /// Number of characters to keep visible at the end.
        keep_suffix: Option<usize>,
    },
}

impl Default for Strategy {
    fn default() -> Self {
        Self::Redact { label: None }
    }
}

// ─── Matcher ─────────────────────────────────────────────────

/// How a rule identifies sensitive content.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Matcher {
    /// Regular expression with optional capture groups.
    Regex { pattern: String },
    /// Structural validator using checksums / format rules.
    Structural { detector: StructuralDetector },
    /// Exact literal match.
    Literal {
        text: String,
        #[serde(default)]
        case_sensitive: bool,
    },
    /// All sub-matchers must match (AND logic).
    All(Vec<Matcher>),
    /// Any sub-matcher must match (OR logic).
    Any(Vec<Matcher>),
}

/// Structural detectors that use domain knowledge beyond regex.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StructuralDetector {
    /// Payment card numbers with Luhn check-digit validation.
    CreditCard,
    /// Email addresses (simplified RFC 5322).
    Email,
    /// Phone numbers requiring area/country code prefix.
    PhoneNumber,
    /// International Bank Account Numbers with mod-97 validation.
    Iban,
    /// IPv4 addresses.
    Ipv4,
    /// IPv6 addresses (full, compressed, or mixed notation).
    Ipv6,
    /// MAC addresses (colon-separated, dash-separated, or dot-separated pairs).
    MacAddress,
    /// Paths under the current user's home directory (`/home/USER/` or `/Users/USER/`).
    UserHomePath,
    /// The local machine hostname.
    LocalHostname,
    /// US Social Security Numbers (format-validated, excludes invalid area/group/serial).
    Ssn,
    /// Polish national identification number (PESEL) — 11 digits with checksum validation.
    Pesel,
    /// Polish tax identification number (NIP) — 10 digits with checksum validation.
    Nip,
    /// Polish business registry number (REGON) — 9 or 14 digits.
    Regon,
}

// ─── Rule ────────────────────────────────────────────────────

/// Functional category for a privacy rule.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuleCategory {
    /// Authentication secrets: API keys, tokens, passwords, private keys.
    Secret,
    /// Personally identifiable information: emails, phones, card numbers.
    Pii,
    /// Privacy-relevant metadata: window titles revealing activity.
    Privacy,
    /// User-defined rules.
    Custom,
}

/// A single privacy rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternRule {
    /// Unique identifier (used in overrides, stats, diagnostics).
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Functional category.
    pub category: RuleCategory,
    /// How to find matches.
    pub matcher: Matcher,
    /// What to do with matches.
    pub strategy: Strategy,
    /// Contexts where this rule is active. Empty = all contexts.
    #[serde(default)]
    pub contexts: Vec<ProcessingContext>,
    /// Whether this rule is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// Override for a built-in rule.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuleOverride {
    /// Set to false to disable a built-in rule.
    pub enabled: Option<bool>,
    /// Override the strategy.
    pub strategy: Option<Strategy>,
    /// Override the context list.
    pub contexts: Option<Vec<ProcessingContext>>,
}

// ─── Processing result ───────────────────────────────────────

/// Result of processing a string through the privacy engine.
pub struct Processed<'a> {
    /// The output string. Borrowed if unchanged, owned if modified.
    pub text: Cow<'a, str>,
    /// Names of rules that matched, in application order.
    pub matched_rules: Vec<String>,
    /// Whether a Suppress rule matched (caller should drop the field).
    pub suppressed: bool,
}

impl<'a> Processed<'a> {
    /// No rules matched — zero-allocation fast path.
    pub(crate) fn unchanged(input: &'a str) -> Self {
        Self {
            text: Cow::Borrowed(input),
            matched_rules: Vec::new(),
            suppressed: false,
        }
    }

    /// A Suppress rule matched.
    pub(crate) fn suppressed(rule_name: &str) -> Self {
        Self {
            text: Cow::Borrowed(""),
            matched_rules: vec![rule_name.to_string()],
            suppressed: true,
        }
    }

    /// Whether any rule matched.
    #[must_use]
    pub fn any_matched(&self) -> bool {
        !self.matched_rules.is_empty()
    }
}

// ─── Material path classification ────────────────────────────

/// Contract class for a path-bearing metadata field.
///
/// Classifying a path before it enters durable material metadata makes the
/// intended privacy treatment explicit and prevents accidental raw-home-path
/// leakage into public/export artifacts.
///
/// # Policy table
///
/// | Class | Example | Durable storage | Display / export |
/// |---|---|---|---|
/// `DurableIdentifier` | `/home/sinity/projects/sinex/Cargo.toml` | Raw (local truth) | Tilde-collapsed (`~/projects/sinex/Cargo.toml`) |
/// `SystemPath` | `/etc/nixos/configuration.nix`, `/run/user/1000/hypr/` | Raw | Raw |
/// `ApplicationData` | `~/.local/share/atuin/history.db` | Raw (local truth) | Tilde-collapsed |
/// `Temporary` | `/tmp/sinex-clipboard-abc123` | Suppress / omit | Omit |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterialPathClass {
    /// A path under the user's home directory that identifies a durable file
    /// (project source, document, config). Raw in local storage; tilde-collapsed
    /// for display and export.
    DurableIdentifier,
    /// A system-wide path with no personal-data sensitivity (`/etc/`, `/run/`,
    /// `/nix/`, `/proc/`, `/sys/`, `/usr/`, `/lib/`, `/bin/`, `/sbin/`,
    /// `/var/`, `/boot/`). Kept raw in all contexts.
    SystemPath,
    /// A path under the user's home directory that identifies application state
    /// (`~/.local/`, `~/.config/`, `~/.cache/`). Raw in local storage;
    /// tilde-collapsed for display and export.
    ApplicationData,
    /// An ephemeral path (`/tmp/`, `/var/tmp/`, `/dev/shm/`, `%TEMP%`).
    /// Suppressed from display and export; may be omitted from material metadata.
    Temporary,
}

/// Classify a raw path string and return the class plus a display-safe projection.
///
/// The raw value is never modified — callers store the raw path as local
/// truth and use the returned projection only for display or export surfaces.
///
/// ```
/// use sinex_primitives::privacy::{MaterialPathClass, classify_material_path};
///
/// let (class, display) = classify_material_path("/home/alice/projects/sinex/Cargo.toml");
/// assert_eq!(class, MaterialPathClass::DurableIdentifier);
/// assert!(display.starts_with("~/"));
///
/// let (class, _display) = classify_material_path("/etc/nixos/configuration.nix");
/// assert_eq!(class, MaterialPathClass::SystemPath);
///
/// let (class, _display) = classify_material_path("/tmp/sinex-clipboard-abc123");
/// assert_eq!(class, MaterialPathClass::Temporary);
/// ```
pub fn classify_material_path(path: &str) -> (MaterialPathClass, String) {
    // Temporary paths: suppress from export.
    if is_temporary_path(path) {
        return (MaterialPathClass::Temporary, String::new());
    }

    // System paths: no personal data, keep raw.
    if is_system_path(path) {
        return (MaterialPathClass::SystemPath, path.to_string());
    }

    // Home-relative paths: tilde-collapse for display/export.
    if let Some(home_suffix) = home_suffix(path) {
        // Distinguish application data (dot-prefixed components under home)
        // from user project/document paths.
        let class = if is_application_data_suffix(home_suffix) {
            MaterialPathClass::ApplicationData
        } else {
            MaterialPathClass::DurableIdentifier
        };
        let display = format!("~/{home_suffix}");
        return (class, display);
    }

    // Fallback: treat unknown paths as durable identifiers; keep raw.
    (MaterialPathClass::DurableIdentifier, path.to_string())
}

fn is_temporary_path(path: &str) -> bool {
    path.starts_with("/tmp/")
        || path.starts_with("/var/tmp/")
        || path.starts_with("/dev/shm/")
        || path == "/tmp"
        || path == "/var/tmp"
        || path == "/dev/shm"
}

fn is_system_path(path: &str) -> bool {
    const SYSTEM_PREFIXES: &[&str] = &[
        "/etc/", "/run/", "/nix/", "/proc/", "/sys/", "/usr/", "/lib/",
        "/lib64/", "/bin/", "/sbin/", "/var/", "/boot/", "/opt/", "/srv/",
    ];
    SYSTEM_PREFIXES.iter().any(|p| path.starts_with(p))
        || matches!(
            path,
            "/etc" | "/run" | "/nix" | "/proc" | "/sys" | "/usr" | "/lib"
                | "/lib64" | "/bin" | "/sbin" | "/var" | "/boot" | "/opt"
                | "/srv"
        )
}

/// Returns the suffix after `/home/<user>/` (or `/Users/<user>/` on macOS) if
/// the path is rooted under a home directory, or `None` otherwise.
fn home_suffix(path: &str) -> Option<&str> {
    // Check live HOME env var first for accuracy.
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            let home_slash = if home.ends_with('/') {
                home.clone()
            } else {
                format!("{home}/")
            };
            if let Some(suffix) = path.strip_prefix(home_slash.as_str()) {
                return Some(suffix);
            }
            // Exact home dir itself
            if path == home {
                return Some("");
            }
        }
    }

    // Heuristic fallback: /home/<user>/ or /Users/<user>/
    for prefix in ["/home/", "/Users/"] {
        if let Some(rest) = path.strip_prefix(prefix) {
            if let Some(slash) = rest.find('/') {
                return Some(&rest[slash + 1..]);
            }
        }
    }

    None
}

/// Returns true if the home-relative suffix looks like application state
/// (starts with a hidden directory, i.e. `.local/`, `.config/`, `.cache/`, etc.).
fn is_application_data_suffix(suffix: &str) -> bool {
    suffix.starts_with('.') || suffix.is_empty()
}

// ─── Error ───────────────────────────────────────────────────

/// Errors from the privacy engine.
#[derive(Debug, thiserror::Error)]
pub enum PrivacyError {
    #[error(transparent)]
    Config(#[from] PrivacyConfigError),
    #[error("invalid regex pattern in rule '{rule}': {source}")]
    InvalidPattern { rule: String, source: regex::Error },
    #[error("encryption failed: {0}")]
    EncryptionFailed(String),
    #[error("decryption failed: {0}")]
    DecryptionFailed(String),
    #[error("no privacy key configured")]
    NoKey,
    #[error("invalid token format: {0}")]
    InvalidToken(String),
    #[error("invalid key: {0}")]
    InvalidKey(String),
}

impl From<PrivacyError> for crate::error::SinexError {
    fn from(err: PrivacyError) -> Self {
        match err {
            PrivacyError::Config(inner) => {
                crate::error::SinexError::from(inner)
                    .with_context("privacy_component", "config")
            }
            PrivacyError::InvalidPattern { ref rule, ref source } => {
                crate::error::SinexError::configuration("invalid regex pattern in privacy rule")
                    .with_context("rule", rule)
                    .with_source(source)
            }
            PrivacyError::EncryptionFailed(ref msg) => {
                crate::error::SinexError::processing("privacy encryption failed")
                    .with_context("detail", msg)
            }
            PrivacyError::DecryptionFailed(ref msg) => {
                crate::error::SinexError::processing("privacy decryption failed")
                    .with_context("detail", msg)
            }
            PrivacyError::NoKey => {
                crate::error::SinexError::configuration("no privacy key configured")
            }
            PrivacyError::InvalidToken(ref msg) => {
                crate::error::SinexError::parse("invalid privacy token format")
                    .with_context("detail", msg)
            }
            PrivacyError::InvalidKey(ref msg) => {
                crate::error::SinexError::configuration("invalid privacy key")
                    .with_context("detail", msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Exception to per-crate tests/: this exercises private privacy-engine
    // initialization helpers without widening the public API.
    use super::*;
    use std::ffi::OsString;
    use std::sync::LazyLock;
    use xtask::sandbox::sinex_test;

    static ENV_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    fn restore_var(key: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    #[sinex_test]
    async fn build_engine_from_env_propagates_config_errors() -> ::xtask::sandbox::TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        let old_extra_rules = std::env::var_os("SINEX_PRIVACY_EXTRA_RULES");
        unsafe { std::env::set_var("SINEX_PRIVACY_EXTRA_RULES", "{not-json") };

        let result = build_engine_from_env();

        restore_var("SINEX_PRIVACY_EXTRA_RULES", old_extra_rules);

        let Err(err) = result else {
            panic!("invalid privacy env override should fail honestly")
        };
        assert!(matches!(err, PrivacyError::Config(_)));
        assert!(
            err.to_string()
                .contains("invalid privacy environment override SINEX_PRIVACY_EXTRA_RULES")
        );
        Ok(())
    }

    #[sinex_test]
    async fn build_engine_from_env_accepts_default_configuration()
    -> ::xtask::sandbox::TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        let old_extra_rules = std::env::var_os("SINEX_PRIVACY_EXTRA_RULES");
        unsafe { std::env::remove_var("SINEX_PRIVACY_EXTRA_RULES") };

        let engine = build_engine_from_env()?;
        let processed = engine.process("token=abc", ProcessingContext::Command);

        restore_var("SINEX_PRIVACY_EXTRA_RULES", old_extra_rules);

        assert!(processed.any_matched());
        Ok(())
    }
}
