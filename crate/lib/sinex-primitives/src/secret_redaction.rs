//! Secret redaction for sensitive information in captured text.
//!
//! The single public interface is [`ConfigurableRedactor`], which compiles
//! patterns from a [`RedactionConfig`](crate::redaction_config::RedactionConfig)
//! and exposes separate content and title redaction paths.
//!
//! A process-wide default instance is available as [`GLOBAL_REDACTOR`]:
//!
//! ```ignore
//! // Command / log / D-Bus payload redaction
//! let safe = GLOBAL_REDACTOR.redact_content(raw_text);
//!
//! // Window / tab title redaction
//! let title = GLOBAL_REDACTOR.redact_title(window_title);
//!
//! // With statistics
//! let (safe, stats) = GLOBAL_REDACTOR.redact_content_with_stats(raw_text);
//! if stats.any_redacted() { /* log which patterns fired */ }
//! ```
//!
//! For node-specific configuration (per-user patterns, disabled redaction, etc.)
//! construct a local [`ConfigurableRedactor`] from a [`RedactionConfig`].
#![allow(clippy::expect_used)] // Expects are on compile-time constant regex patterns

use lazy_static::lazy_static;
use regex::Regex;
use std::borrow::Cow;

use crate::redaction_config::RedactionConfig;

// ---------------------------------------------------------------------------
// OwnedRedactionStats
// ---------------------------------------------------------------------------

/// Statistics from a [`ConfigurableRedactor`] pass, tracking which patterns fired.
#[derive(Debug, Default, Clone)]
pub struct OwnedRedactionStats {
    /// Names of patterns that matched during redaction.
    pub matched_patterns: Vec<String>,
}

impl OwnedRedactionStats {
    /// Returns `true` if any secrets were redacted.
    #[must_use]
    pub fn any_redacted(&self) -> bool {
        !self.matched_patterns.is_empty()
    }
}

// ---------------------------------------------------------------------------
// GLOBAL_REDACTOR
// ---------------------------------------------------------------------------

lazy_static! {
    /// Process-wide [`ConfigurableRedactor`] initialised with the default
    /// reference patterns.  Use this for content and title redaction unless
    /// you need node-specific pattern overrides.
    pub static ref GLOBAL_REDACTOR: ConfigurableRedactor =
        ConfigurableRedactor::with_defaults();
}

// ---------------------------------------------------------------------------
// ConfigurableRedactor (instance-based API)
// ---------------------------------------------------------------------------

/// A compiled redaction pattern.
struct CompiledPattern {
    name: String,
    regex: Regex,
    replacement: String,
}

/// Instance-based redactor built from a [`RedactionConfig`].
///
/// Separates content patterns (commands, log messages, D-Bus payloads) from
/// title patterns (window/tab titles), supports optional statistics, and can
/// be disabled entirely via `config.enabled = false`.
pub struct ConfigurableRedactor {
    enabled: bool,
    content_patterns: Vec<CompiledPattern>,
    title_patterns: Vec<CompiledPattern>,
    track_stats: bool,
}

impl ConfigurableRedactor {
    /// Build a redactor from the given configuration, compiling all regex
    /// patterns eagerly.  Returns an error if any pattern fails to compile.
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

    /// Create a redactor with the built-in reference patterns.
    ///
    /// # Panics
    ///
    /// Panics if any reference pattern fails to compile (which would be a bug
    /// in the hardcoded pattern set).
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

    /// Redact content and return statistics about which patterns matched.
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

    /// Heuristic check for highly sensitive content (private keys, credential JSON, etc.).
    #[must_use]
    pub fn is_highly_sensitive(&self, input: &str) -> bool {
        if !self.enabled {
            return false;
        }
        if input.contains("-----BEGIN") && input.contains("PRIVATE KEY") {
            return true;
        }
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
    use crate::redaction_config::RedactionPattern as ConfigPattern;
    use xtask::sandbox::prelude::*;

    // --- GLOBAL_REDACTOR / with_defaults ---

    #[sinex_test]
    fn global_redactor_redacts_aws_access_key() -> TestResult<()> {
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<AWS_ACCESS_KEY>"), "got: {result}");
        assert!(!result.contains("AKIAIOSFODNN7EXAMPLE"));
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_url_credentials() -> TestResult<()> {
        let input = "git clone https://user:password123@github.com/repo.git";
        let expected = "git clone https://user:<REDACTED>@github.com/repo.git";
        assert_eq!(GLOBAL_REDACTOR.redact_content(input), expected);
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_github_pat() -> TestResult<()> {
        let input = "GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<GITHUB_TOKEN>"), "got: {result}");
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_cli_flag() -> TestResult<()> {
        let input = "./deploy --token abcdef123456 --verbose";
        let expected = "./deploy --token <REDACTED> --verbose";
        assert_eq!(GLOBAL_REDACTOR.redact_content(input), expected);
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_no_false_positive_on_git_hash() -> TestResult<()> {
        let input = "git show a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
        assert_eq!(GLOBAL_REDACTOR.redact_content(input), input);
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_no_false_positive_on_uuid() -> TestResult<()> {
        let input = "resource-id: 123e4567-e89b-12d3-a456-426614174000";
        assert_eq!(GLOBAL_REDACTOR.redact_content(input), input);
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_aws_secret_key_with_context() -> TestResult<()> {
        let input = "export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<AWS_SECRET_KEY>"), "got: {result}");
        assert!(!result.contains("wJalrXUtnFEMI"));
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_stripe_key() -> TestResult<()> {
        let input = "stripe_key=sk_live_abcdefghijklmnopqrstuvwxyz";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<API_KEY>"), "got: {result}");
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_slack_token() -> TestResult<()> {
        let input = "SLACK_TOKEN=xoxb-123456789012-1234567890123-abcdefghijklmnopqrstuvwx";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<SLACK_TOKEN>"), "got: {result}");
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_jwt_token() -> TestResult<()> {
        let input = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<JWT_TOKEN>"), "got: {result}");
        assert!(!result.contains("eyJhbGci"));
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_google_api_key() -> TestResult<()> {
        let input = "GOOGLE_KEY=AIzaSyA1234567890abcdefghijklmnopqrstuv";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<GOOGLE_API_KEY>"), "got: {result}");
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_azure_connection_string() -> TestResult<()> {
        let input = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=abc123def456ghi789jkl012mno345pqr678stu901vwxyz+A==";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("AccountKey=<REDACTED>"), "got: {result}");
        assert!(!result.contains("abc123def456"));
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_redacts_generic_password() -> TestResult<()> {
        let input = "export PASSWORD=mysecretpassword";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<REDACTED>"), "got: {result}");
        assert!(!result.contains("mysecretpassword"));
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_word_boundary_prevents_substring_match() -> TestResult<()> {
        // "database_password" should NOT be redacted — word boundary blocks it
        let input = "export database_password=myvalue";
        assert_eq!(GLOBAL_REDACTOR.redact_content(input), input);
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_with_stats_tracks_patterns() -> TestResult<()> {
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        // stats only populated when track_stats is true; GLOBAL_REDACTOR may not have it
        // Use a local redactor with stats enabled
        use crate::redaction_config::RedactionConfig;
        let config = RedactionConfig {
            track_stats: true,
            ..RedactionConfig::with_defaults()
        };
        let cr = ConfigurableRedactor::new(&config).unwrap();
        let (result, stats) = cr.redact_content_with_stats(input);
        assert!(result.contains("<AWS_ACCESS_KEY>"), "got: {result}");
        assert!(stats.any_redacted());
        assert!(stats.matched_patterns.iter().any(|p| p == "aws_access_key"));
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_with_stats_empty_on_clean_input() -> TestResult<()> {
        use crate::redaction_config::RedactionConfig;
        let config = RedactionConfig {
            track_stats: true,
            ..RedactionConfig::with_defaults()
        };
        let cr = ConfigurableRedactor::new(&config).unwrap();
        let input = "ls -la /home/user";
        let (result, stats) = cr.redact_content_with_stats(input);
        assert_eq!(result, input);
        assert!(!stats.any_redacted());
        Ok(())
    }

    #[sinex_test]
    fn global_redactor_multiple_pattern_hits() -> TestResult<()> {
        use crate::redaction_config::RedactionConfig;
        let config = RedactionConfig {
            track_stats: true,
            ..RedactionConfig::with_defaults()
        };
        let cr = ConfigurableRedactor::new(&config).unwrap();
        let input = "curl --password hunter2 https://user:pass@example.com";
        let (_result, stats) = cr.redact_content_with_stats(input);
        assert!(stats.matched_patterns.len() >= 2);
        Ok(())
    }

    // --- noop ---

    #[sinex_test]
    fn noop_passes_through_all_input() -> TestResult<()> {
        let cr = ConfigurableRedactor::noop();
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        assert_eq!(cr.redact_content(input), input);
        assert!(!cr.is_highly_sensitive("-----BEGIN RSA PRIVATE KEY-----"));
        Ok(())
    }

    // --- title redaction ---

    #[sinex_test]
    fn title_redaction_matches_password_manager() -> TestResult<()> {
        let cr = ConfigurableRedactor::with_defaults();
        let input = "KeePassXC - Password for bank.example.com";
        let result = cr.redact_title(input);
        assert!(
            result.contains("<PASSWORD_MANAGER>") || result.contains("<PASSWORD_ENTRY>"),
            "expected title redaction, got: {result}"
        );
        Ok(())
    }

    // --- custom patterns ---

    #[sinex_test]
    fn custom_pattern_replaces_correctly() -> TestResult<()> {
        let config = RedactionConfig {
            enabled: true,
            patterns: vec![ConfigPattern {
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
    fn disabled_config_passes_input_through() -> TestResult<()> {
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
    fn invalid_regex_returns_error() -> TestResult<()> {
        let config = RedactionConfig {
            enabled: true,
            patterns: vec![ConfigPattern {
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

    // --- is_highly_sensitive ---

    #[sinex_test]
    fn is_highly_sensitive_detects_private_keys() -> TestResult<()> {
        let cr = ConfigurableRedactor::with_defaults();
        assert!(cr.is_highly_sensitive("-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA..."));
        assert!(cr.is_highly_sensitive(r#"{"password": "hunter2"}"#));
        assert!(!cr.is_highly_sensitive("normal text"));
        Ok(())
    }
}
