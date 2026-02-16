//! Configuration types for redaction patterns.
//!
//! Provides [`RedactionConfig`] for configuring which patterns the
//! [`ConfigurableRedactor`](crate::secret_redaction::ConfigurableRedactor) uses
//! at runtime.  Patterns can come from the built-in reference set, from
//! environment variables, or from application-level configuration (e.g. Figment).

use serde::{Deserialize, Serialize};

/// A single redaction pattern: a named regex with a replacement template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionPattern {
    /// Human-readable name for diagnostics / stats tracking.
    pub name: String,
    /// Regex pattern (will be compiled with `regex::Regex`).
    pub regex: String,
    /// Replacement template (may reference capture groups like `$1`).
    pub replacement: String,
}

impl RedactionPattern {
    fn new(name: &str, regex: &str, replacement: &str) -> Self {
        Self {
            name: name.to_owned(),
            regex: regex.to_owned(),
            replacement: replacement.to_owned(),
        }
    }
}

/// Top-level redaction configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionConfig {
    /// Master switch: when `false` the redactor becomes a no-op.
    pub enabled: bool,
    /// Patterns applied to general content (commands, clipboard text, log
    /// messages, etc.).
    pub patterns: Vec<RedactionPattern>,
    /// Patterns applied specifically to window/tab titles.
    pub title_patterns: Vec<RedactionPattern>,
    /// Whether the redactor should collect per-pattern match statistics.
    pub track_stats: bool,
}

impl Default for RedactionConfig {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl RedactionConfig {
    /// All hardcoded content patterns from the original `SecretRedactor` plus
    /// the desktop-ingestor `PrivacyFilter` content patterns.
    ///
    /// This is the *union* of both sets, de-duplicated by name.
    #[must_use]
    pub fn reference_patterns() -> Vec<RedactionPattern> {
        vec![
            // --- From SecretRedactor ---
            RedactionPattern::new(
                "aws_access_key",
                r"(?i)\b(AKIA|ASIA|ABIA|ACCA)[0-9A-Z]{16}\b",
                "<AWS_ACCESS_KEY>",
            ),
            RedactionPattern::new(
                "aws_secret_key",
                r"(?i)(aws_secret_access_key|secret_access_key|aws_secret)\s*[:=]\s*([A-Za-z0-9/+=]{40})",
                "$1=<AWS_SECRET_KEY>",
            ),
            RedactionPattern::new(
                "url_credentials",
                r"(?i)([a-z]+://)([^:/]+):([^@]+)@",
                "${1}${2}:<REDACTED>@",
            ),
            RedactionPattern::new(
                "private_key_header",
                r"(?i)-----BEGIN[ A-Z]+PRIVATE KEY-----",
                "<PRIVATE_KEY_HEADER>",
            ),
            RedactionPattern::new(
                "github_pat",
                r"\b(gh[pousr]_[A-Za-z0-9]{36,})\b",
                "<GITHUB_TOKEN>",
            ),
            RedactionPattern::new(
                "generic_api_key",
                r"(?i)(sk[-_]live[-_]|sk[-_]test[-_]|pk[-_]live[-_]|pk[-_]test[-_])[A-Za-z0-9]{24,}",
                "<API_KEY>",
            ),
            RedactionPattern::new(
                "slack_token",
                r"\b(xox[bpsar]-[A-Za-z0-9-]{10,})\b",
                "<SLACK_TOKEN>",
            ),
            RedactionPattern::new(
                "jwt_token",
                r"\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b",
                "<JWT_TOKEN>",
            ),
            RedactionPattern::new(
                "google_api_key",
                r"\bAIza[A-Za-z0-9_-]{35}\b",
                "<GOOGLE_API_KEY>",
            ),
            RedactionPattern::new(
                "azure_connection_string",
                r"(?i)AccountKey=[A-Za-z0-9/+=]{44,}",
                "AccountKey=<REDACTED>",
            ),
            RedactionPattern::new(
                "generic_secret_assignment",
                r#"(?i)\b(password|passwd|secret|token|api_key|apikey|auth_token|access_token)\s*[:=]\s*([^\s;'"]+)"#,
                "$1=<REDACTED>",
            ),
            RedactionPattern::new(
                "cli_flag_secret",
                r"(?i)(--password|--secret|--token|--key|--api-key)\s+([^\s]+)",
                "$1 <REDACTED>",
            ),
            // --- Additional patterns from PrivacyFilter (desktop) ---
            RedactionPattern::new(
                "bearer_token",
                r"(?i)\bBearer\s+([A-Za-z0-9._~+/=-]+)",
                "Bearer <REDACTED>",
            ),
            RedactionPattern::new(
                "credit_card",
                r"\b([0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4})\b",
                "<CARD_NUMBER>",
            ),
            RedactionPattern::new(
                "ssn",
                r"\b([0-9]{3}[-\s]?[0-9]{2}[-\s]?[0-9]{4})\b",
                "<SSN>",
            ),
        ]
    }

    /// Reference title patterns (from the desktop `PrivacyFilter`).
    #[must_use]
    pub fn reference_title_patterns() -> Vec<RedactionPattern> {
        vec![
            RedactionPattern::new(
                "password_entry",
                r"(?i)(password\s*(for|:|\s)|unlock\s*(for|:|\s)|master\s*password)",
                "<PASSWORD_ENTRY>",
            ),
            RedactionPattern::new(
                "login_window",
                r"(?i)(sign\s*in|log\s*in|enter\s*password|authentication)",
                "<LOGIN_WINDOW>",
            ),
            RedactionPattern::new(
                "password_manager",
                r"(?i)(keepass|bitwarden|1password|lastpass|dashlane|enpass)",
                "<PASSWORD_MANAGER>",
            ),
            RedactionPattern::new(
                "sensitive_file",
                r"(?i)(\.env|credentials|secrets?\.ya?ml|\.pem|\.key|id_rsa|\.netrc)",
                "<SENSITIVE_FILE>",
            ),
        ]
    }

    /// Load configuration from environment variables.
    ///
    /// | Variable | Default |
    /// |----------|---------|
    /// | `SINEX_REDACTION_ENABLED` | `true` |
    /// | `SINEX_REDACTION_PATTERNS` | JSON array of `RedactionPattern` |
    /// | `SINEX_REDACTION_TITLE_PATTERNS` | JSON array of `RedactionPattern` |
    /// | `SINEX_REDACTION_TRACK_STATS` | `false` |
    ///
    /// When a `*_PATTERNS` variable is absent the corresponding reference set
    /// is used as the default.
    pub fn from_env() -> Result<Self, String> {
        let enabled = std::env::var("SINEX_REDACTION_ENABLED")
            .map_or(true, |v| v.eq_ignore_ascii_case("true") || v == "1");

        let patterns = match std::env::var("SINEX_REDACTION_PATTERNS") {
            Ok(json) => {
                serde_json::from_str(&json).map_err(|e| format!("SINEX_REDACTION_PATTERNS: {e}"))?
            }
            Err(_) => Self::reference_patterns(),
        };

        let title_patterns = match std::env::var("SINEX_REDACTION_TITLE_PATTERNS") {
            Ok(json) => serde_json::from_str(&json)
                .map_err(|e| format!("SINEX_REDACTION_TITLE_PATTERNS: {e}"))?,
            Err(_) => Self::reference_title_patterns(),
        };

        let track_stats = std::env::var("SINEX_REDACTION_TRACK_STATS")
            .is_ok_and(|v| v.eq_ignore_ascii_case("true") || v == "1");

        Ok(Self {
            enabled,
            patterns,
            title_patterns,
            track_stats,
        })
    }

    /// Default config with all reference patterns enabled and stats off.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self {
            enabled: true,
            patterns: Self::reference_patterns(),
            title_patterns: Self::reference_title_patterns(),
            track_stats: false,
        }
    }
}
