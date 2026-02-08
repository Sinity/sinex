//! Secret redaction for sensitive information in captured text.
//!
//! Provides regex-based pattern matching to redact credentials, API keys, tokens,
//! and other secrets from strings before they are stored as events. Used by both
//! terminal-ingestor (command history) and system-ingestor (journal entries).

use lazy_static::lazy_static;
use regex::Regex;
use std::borrow::Cow;

/// Statistics from a redaction pass, tracking which patterns matched
#[derive(Debug, Default)]
pub struct RedactionStats {
    /// Names of patterns that matched during redaction
    pub matched_patterns: Vec<&'static str>,
}

impl RedactionStats {
    /// Returns true if any secrets were redacted
    pub fn any_redacted(&self) -> bool {
        !self.matched_patterns.is_empty()
    }
}

/// Redactor for sensitive information in captured text (commands, log messages, etc.)
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
