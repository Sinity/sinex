//! Secret redaction for sensitive information in captured text.
//!
//! Two redaction interfaces are provided:
//!
//! - [`SecretRedactor`] — zero-sized struct with **static** methods and
//!   hardcoded `lazy_static!` patterns.  All existing call-sites continue
//!   to work unchanged.
//!
//! - [`ConfigurableRedactor`] — **instance-based** redactor constructed from
//!   a [`RedactionConfig`](crate::redaction_config::RedactionConfig).  Allows
//!   runtime customisation of patterns, title-vs-content separation, and
//!   optional statistics tracking.
#![allow(clippy::expect_used)] // All expects are on compile-time constant regex patterns

use lazy_static::lazy_static;
use regex::Regex;
use std::borrow::Cow;

use crate::redaction_config::RedactionConfig;

// ---------------------------------------------------------------------------
// RedactionStats
// ---------------------------------------------------------------------------

/// Statistics from a redaction pass, tracking which patterns matched.
#[derive(Debug, Default, Clone)]
pub struct RedactionStats {
    /// Names of patterns that matched during redaction.
    pub matched_patterns: Vec<&'static str>,
}

impl RedactionStats {
    /// Returns true if any secrets were redacted.
    pub fn any_redacted(&self) -> bool {
        !self.matched_patterns.is_empty()
    }
}

/// Owned variant of [`RedactionStats`] used by [`ConfigurableRedactor`]
/// where pattern names are not `&'static str`.
#[derive(Debug, Default, Clone)]
pub struct OwnedRedactionStats {
    /// Names of patterns that matched during redaction.
    pub matched_patterns: Vec<String>,
}

impl OwnedRedactionStats {
    /// Returns true if any secrets were redacted.
    pub fn any_redacted(&self) -> bool {
        !self.matched_patterns.is_empty()
    }
}

// ---------------------------------------------------------------------------
// SecretRedactor (static API — unchanged)
// ---------------------------------------------------------------------------

/// Redactor for sensitive information in captured text (commands, log messages, etc.)
///
/// All methods are **static** (associated functions) and use hardcoded
/// `lazy_static!` patterns.  This API exists for backward-compatibility; new
/// consumers should prefer [`ConfigurableRedactor`].
pub struct SecretRedactor;

struct RedactionPattern {
    name: &'static str,
    regex: Regex,
    placeholder: &'static str,
}

lazy_static! {
    static ref PATTERNS: Vec<RedactionPattern> = vec![
        // AWS Access Key ID (AKIA/ASIA...)
        RedactionPattern {
            name: "aws_access_key",
            regex: Regex::new(r"(?i)\b(AKIA|ASIA|ABIA|ACCA)[0-9A-Z]{16}\b")
                .expect("AWS access key regex pattern is valid at compile-time"),
            placeholder: "<AWS_ACCESS_KEY>",
        },
        // AWS Secret Access Key - context-aware pattern (only matches after known variable names)
        // This avoids false positives on git hashes and UUIDs
        RedactionPattern {
            name: "aws_secret_key",
            regex: Regex::new(r"(?i)(aws_secret_access_key|secret_access_key|aws_secret)\s*[:=]\s*([A-Za-z0-9/+=]{40})")
                .expect("AWS secret key regex pattern is valid at compile-time"),
            placeholder: "$1=<AWS_SECRET_KEY>",
        },
        // URLs with credentials
        RedactionPattern {
            name: "url_credentials",
            regex: Regex::new(r"(?i)([a-z]+://)([^:/]+):([^@]+)@")
                .expect("URL credentials regex pattern is valid at compile-time"),
            placeholder: "${1}${2}:<REDACTED>@",
        },
        // Private Key Headers
        RedactionPattern {
            name: "private_key_header",
            regex: Regex::new(r"(?i)-----BEGIN[ A-Z]+PRIVATE KEY-----")
                .expect("Private key header regex pattern is valid at compile-time"),
            placeholder: "<PRIVATE_KEY_HEADER>",
        },
        // GitHub Personal Access Tokens (ghp_, gho_, ghu_, ghs_, ghr_)
        RedactionPattern {
            name: "github_pat",
            regex: Regex::new(r"\b(gh[pousr]_[A-Za-z0-9]{36,})\b")
                .expect("GitHub PAT regex pattern is valid at compile-time"),
            placeholder: "<GITHUB_TOKEN>",
        },
        // Generic API keys (Stripe, etc.)
        RedactionPattern {
            name: "generic_api_key",
            regex: Regex::new(r"(?i)(sk[-_]live[-_]|sk[-_]test[-_]|pk[-_]live[-_]|pk[-_]test[-_])[A-Za-z0-9]{24,}")
                .expect("Generic API key regex pattern is valid at compile-time"),
            placeholder: "<API_KEY>",
        },
        // Slack tokens (xoxb-, xoxp-, xoxs-, xoxa-, xoxr-)
        RedactionPattern {
            name: "slack_token",
            regex: Regex::new(r"\b(xox[bpsar]-[A-Za-z0-9-]{10,})\b")
                .expect("Slack token regex pattern is valid at compile-time"),
            placeholder: "<SLACK_TOKEN>",
        },
        // JWT tokens (three base64url-encoded segments separated by dots)
        RedactionPattern {
            name: "jwt_token",
            regex: Regex::new(r"\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b")
                .expect("JWT token regex pattern is valid at compile-time"),
            placeholder: "<JWT_TOKEN>",
        },
        // Google API keys (AIza...)
        RedactionPattern {
            name: "google_api_key",
            regex: Regex::new(r"\bAIza[A-Za-z0-9_-]{35}\b")
                .expect("Google API key regex pattern is valid at compile-time"),
            placeholder: "<GOOGLE_API_KEY>",
        },
        // Azure connection strings
        RedactionPattern {
            name: "azure_connection_string",
            regex: Regex::new(r"(?i)AccountKey=[A-Za-z0-9/+=]{44,}")
                .expect("Azure connection string regex pattern is valid at compile-time"),
            placeholder: "AccountKey=<REDACTED>",
        },
        // Generic assignment patterns for common secret variable names
        // Uses word boundary \b to avoid matching substrings like "database_password"
        RedactionPattern {
            name: "generic_secret_assignment",
            regex: Regex::new(r#"(?i)\b(password|passwd|secret|token|api_key|apikey|auth_token|access_token)\s*[:=]\s*([^\s;'"]+)"#)
                .expect("Generic secret assignment regex pattern is valid at compile-time"),
            placeholder: "$1=<REDACTED>",
        },
    ];

    // CLI flag patterns for common secret flags
    static ref CLI_FLAG_SECRET: Regex = Regex::new(r"(?i)(--password|--secret|--token|--key|--api-key)\s+([^\s]+)")
        .expect("CLI flag secret regex pattern is valid at compile-time");
}

impl SecretRedactor {
    /// Redact sensitive information from the input string
    #[must_use]
    pub fn redact(input: &str) -> Cow<'_, str> {
        Self::redact_with_stats(input).0
    }

    /// Redact sensitive information and return statistics about what was matched
    #[must_use]
    pub fn redact_with_stats(input: &str) -> (Cow<'_, str>, RedactionStats) {
        let mut result = Cow::Borrowed(input);
        let mut stats = RedactionStats::default();

        // Apply global patterns
        for pattern in PATTERNS.iter() {
            if pattern.regex.is_match(&result) {
                let redacted = pattern.regex.replace_all(&result, pattern.placeholder);
                result = Cow::Owned(redacted.into_owned());
                stats.matched_patterns.push(pattern.name);
            }
        }

        // Apply CLI flag redaction
        if CLI_FLAG_SECRET.is_match(&result) {
            let redacted = CLI_FLAG_SECRET.replace_all(&result, "$1 <REDACTED>");
            result = Cow::Owned(redacted.into_owned());
            stats.matched_patterns.push("cli_flag_secret");
        }

        (result, stats)
    }
}

// ---------------------------------------------------------------------------
// ConfigurableRedactor (instance-based API)
// ---------------------------------------------------------------------------

/// A compiled redaction pattern for use by [`ConfigurableRedactor`].
struct CompiledPattern {
    name: String,
    regex: Regex,
    replacement: String,
}

/// Instance-based redactor that uses patterns from a [`RedactionConfig`].
///
/// Unlike [`SecretRedactor`], this struct holds its own compiled pattern set
/// and can be configured at runtime.
pub struct ConfigurableRedactor {
    enabled: bool,
    content_patterns: Vec<CompiledPattern>,
    title_patterns: Vec<CompiledPattern>,
    track_stats: bool,
}

impl ConfigurableRedactor {
    /// Create a new redactor from the given configuration.
    ///
    /// All regex patterns in the config are compiled eagerly.  Returns an error
    /// if any pattern fails to compile.
    pub fn new(config: &RedactionConfig) -> Result<Self, String> {
        let content_patterns = Self::compile_patterns(&config.patterns)?;
        let title_patterns = Self::compile_patterns(&config.title_patterns)?;
        Ok(Self {
            enabled: config.enabled,
            content_patterns,
            title_patterns,
            track_stats: config.track_stats,
        })
    }

    /// Create a no-op redactor that passes all input through unchanged.
    #[must_use]
    pub fn noop() -> Self {
        Self {
            enabled: false,
            content_patterns: Vec::new(),
            title_patterns: Vec::new(),
            track_stats: false,
        }
    }

    /// Create a redactor with all reference/default patterns.
    ///
    /// # Panics
    ///
    /// Panics if any reference pattern fails to compile (which would be a bug
    /// in the hardcoded patterns).
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(&RedactionConfig::with_defaults())
            .expect("reference redaction patterns must compile")
    }

    /// Redact sensitive content using the content pattern set.
    #[must_use]
    pub fn redact_content<'a>(&self, input: &'a str) -> Cow<'a, str> {
        self.redact_content_with_stats(input).0
    }

    /// Redact content and return owned statistics about which patterns matched.
    #[must_use]
    pub fn redact_content_with_stats<'a>(
        &self,
        input: &'a str,
    ) -> (Cow<'a, str>, OwnedRedactionStats) {
        self.apply_patterns(input, &self.content_patterns)
    }

    /// Redact a window/tab title using the title pattern set.
    #[must_use]
    pub fn redact_title<'a>(&self, input: &'a str) -> Cow<'a, str> {
        self.apply_patterns(input, &self.title_patterns).0
    }

    /// Heuristic check for highly sensitive content (private keys, credential
    /// JSON, etc.).
    #[must_use]
    pub fn is_highly_sensitive(&self, input: &str) -> bool {
        if !self.enabled {
            return false;
        }
        // Check for private key content
        if input.contains("-----BEGIN") && input.contains("PRIVATE KEY") {
            return true;
        }
        // Check for JSON with obvious credential fields
        if input.contains("\"password\"") || input.contains("\"secret\"") {
            return true;
        }
        false
    }

    // -- private helpers --

    fn compile_patterns(
        patterns: &[crate::redaction_config::RedactionPattern],
    ) -> Result<Vec<CompiledPattern>, String> {
        patterns
            .iter()
            .map(|p| {
                let regex =
                    Regex::new(&p.regex).map_err(|e| format!("pattern '{}': {e}", p.name))?;
                Ok(CompiledPattern {
                    name: p.name.clone(),
                    regex,
                    replacement: p.replacement.clone(),
                })
            })
            .collect()
    }

    fn apply_patterns<'a>(
        &self,
        input: &'a str,
        patterns: &[CompiledPattern],
    ) -> (Cow<'a, str>, OwnedRedactionStats) {
        if !self.enabled {
            return (Cow::Borrowed(input), OwnedRedactionStats::default());
        }

        let mut result = Cow::Borrowed(input);
        let mut stats = OwnedRedactionStats::default();

        for pattern in patterns {
            if pattern.regex.is_match(&result) {
                let redacted = pattern
                    .regex
                    .replace_all(&result, pattern.replacement.as_str());
                result = Cow::Owned(redacted.into_owned());
                if self.track_stats {
                    stats.matched_patterns.push(pattern.name.clone());
                }
            }
        }

        (result, stats)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn configurable_redactor_with_defaults_matches_static() -> TestResult<()> {
        let cr = ConfigurableRedactor::with_defaults();
        let inputs = [
            "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
            "git clone https://user:password123@github.com/repo.git",
            "GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "./deploy --token abcdef123456 --verbose",
        ];
        for input in inputs {
            // ConfigurableRedactor content redaction should produce the same
            // result as the static SecretRedactor for overlapping patterns.
            let static_result = SecretRedactor::redact(input);
            let instance_result = cr.redact_content(input);
            assert_eq!(
                static_result, instance_result,
                "mismatch for input: {input}"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn configurable_redactor_noop_passes_through() -> TestResult<()> {
        let cr = ConfigurableRedactor::noop();
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        assert_eq!(cr.redact_content(input), input);
        assert!(!cr.is_highly_sensitive("-----BEGIN RSA PRIVATE KEY-----"));
        Ok(())
    }

    #[sinex_test]
    async fn configurable_redactor_redacts_titles() -> TestResult<()> {
        let cr = ConfigurableRedactor::with_defaults();
        let input = "KeePassXC - Password for bank.example.com";
        let result = cr.redact_title(input);
        assert!(
            result.contains("<PASSWORD_MANAGER>") || result.contains("<PASSWORD_ENTRY>"),
            "expected title redaction, got: {result}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn configurable_redactor_custom_patterns() -> TestResult<()> {
        let config = RedactionConfig {
            enabled: true,
            patterns: vec![crate::redaction_config::RedactionPattern {
                name: "custom".into(),
                regex: r"\bfoo\b".into(),
                replacement: "<BAR>".into(),
            }],
            title_patterns: Vec::new(),
            track_stats: true,
        };
        let cr = ConfigurableRedactor::new(&config).unwrap();
        let (result, stats) = cr.redact_content_with_stats("hello foo world");
        assert_eq!(result, "hello <BAR> world");
        assert!(stats.any_redacted());
        assert_eq!(stats.matched_patterns, vec!["custom"]);
        Ok(())
    }

    #[sinex_test]
    async fn configurable_redactor_disabled_config() -> TestResult<()> {
        let config = RedactionConfig {
            enabled: false,
            patterns: RedactionConfig::reference_patterns(),
            title_patterns: RedactionConfig::reference_title_patterns(),
            track_stats: false,
        };
        let cr = ConfigurableRedactor::new(&config).unwrap();
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        assert_eq!(cr.redact_content(input), input);
        Ok(())
    }

    #[sinex_test]
    async fn configurable_redactor_is_highly_sensitive() -> TestResult<()> {
        let cr = ConfigurableRedactor::with_defaults();
        assert!(cr.is_highly_sensitive("-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA..."));
        assert!(cr.is_highly_sensitive(r#"{"password": "hunter2"}"#));
        assert!(!cr.is_highly_sensitive("normal text"));
        Ok(())
    }

    #[sinex_test]
    async fn configurable_redactor_invalid_regex() -> TestResult<()> {
        let config = RedactionConfig {
            enabled: true,
            patterns: vec![crate::redaction_config::RedactionPattern {
                name: "bad".into(),
                regex: r"[invalid".into(),
                replacement: "x".into(),
            }],
            title_patterns: Vec::new(),
            track_stats: false,
        };
        assert!(ConfigurableRedactor::new(&config).is_err());
        Ok(())
    }
}
