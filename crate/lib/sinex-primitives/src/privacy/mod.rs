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
//! let result = privacy::engine().process("export TOKEN=ghp_abc123", ProcessingContext::Command);
//! assert!(!result.matched_rules.is_empty());
//! ```

mod catalog;
mod config;
mod detector;
mod engine;
mod envelope;

pub use config::PrivacyConfig;
pub use engine::PrivacyEngine;

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::OnceLock;

// ─── Global engine ───────────────────────────────────────────

static ENGINE: OnceLock<PrivacyEngine> = OnceLock::new();

/// Get the process-wide privacy engine.
///
/// On first call, initializes from `PrivacyConfig::from_env()`.
/// Panics only if built-in pattern compilation fails (build-time bug).
#[allow(clippy::expect_used)] // Compile-time constant patterns
pub fn engine() -> &'static PrivacyEngine {
    ENGINE.get_or_init(|| {
        let config = PrivacyConfig::from_env();
        PrivacyEngine::new(config).expect("built-in privacy patterns must compile")
    })
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
    pub fn any_matched(&self) -> bool {
        !self.matched_rules.is_empty()
    }
}

// ─── Error ───────────────────────────────────────────────────

/// Errors from the privacy engine.
#[derive(Debug, thiserror::Error)]
pub enum PrivacyError {
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
