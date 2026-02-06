//! Privacy filter for clipboard and window content
//!
//! Redacts sensitive information from clipboard content and window titles
//! before storage. This protects against accidental capture of credentials,
//! API keys, and other sensitive data.

use lazy_static::lazy_static;
use regex::Regex;
use std::borrow::Cow;

/// Privacy filter for desktop ingestor content
pub struct PrivacyFilter;

struct RedactionPattern {
    _name: &'static str,
    regex: Regex,
    placeholder: &'static str,
}

lazy_static! {
    /// Patterns for redacting sensitive content in clipboard text
    static ref CONTENT_PATTERNS: Vec<RedactionPattern> = vec![
        // AWS Access Key ID (AKIA/ASIA/ABIA/ACCA...)
        RedactionPattern {
            _name: "aws_access_key",
            regex: Regex::new(r"(?i)\b(AKIA|ASIA|ABIA|ACCA)[0-9A-Z]{16}\b")
                .expect("AWS access key regex valid"),
            placeholder: "<AWS_ACCESS_KEY>",
        },
        // AWS Secret Key (40 char base64 after known assignment)
        RedactionPattern {
            _name: "aws_secret_key_assignment",
            regex: Regex::new(r"(?i)(aws_secret_access_key|secret_access_key)\s*[:=]\s*([A-Za-z0-9/+=]{40})")
                .expect("AWS secret key regex valid"),
            placeholder: "$1=<AWS_SECRET_KEY>",
        },
        // URLs with credentials (user:pass@host)
        RedactionPattern {
            _name: "url_credentials",
            regex: Regex::new(r"(?i)([a-z]+://)([^:/]+):([^@]+)@")
                .expect("URL credentials regex valid"),
            placeholder: "${1}${2}:<REDACTED>@",
        },
        // Private Key Headers
        RedactionPattern {
            _name: "private_key_header",
            regex: Regex::new(r"-----BEGIN[ A-Z]+PRIVATE KEY-----")
                .expect("Private key regex valid"),
            placeholder: "<PRIVATE_KEY_REDACTED>",
        },
        // Generic secret assignments (password=X, api_key=X, token=X, secret=X)
        RedactionPattern {
            _name: "secret_assignment",
            regex: Regex::new(r#"(?i)\b(password|api_key|api[-_]?key|secret|token|auth[-_]?token|access[-_]?token|bearer)\s*[:=]\s*([^\s;,"'\n]+)"#)
                .expect("Secret assignment regex valid"),
            placeholder: "$1=<REDACTED>",
        },
        // Bearer tokens
        RedactionPattern {
            _name: "bearer_token",
            regex: Regex::new(r"(?i)\bBearer\s+([A-Za-z0-9._~+/=-]+)")
                .expect("Bearer token regex valid"),
            placeholder: "Bearer <REDACTED>",
        },
        // GitHub Personal Access Tokens (ghp_, gho_, ghu_, ghs_, ghr_)
        RedactionPattern {
            _name: "github_pat",
            regex: Regex::new(r"\b(gh[pousr]_[A-Za-z0-9]{36,})\b")
                .expect("GitHub PAT regex valid"),
            placeholder: "<GITHUB_TOKEN>",
        },
        // Generic API keys (common formats)
        RedactionPattern {
            _name: "generic_api_key",
            regex: Regex::new(r"(?i)(sk[-_]live[-_]|sk[-_]test[-_]|pk[-_]live[-_]|pk[-_]test[-_])[A-Za-z0-9]{24,}")
                .expect("Generic API key regex valid"),
            placeholder: "<API_KEY>",
        },
        // Credit card numbers (basic Luhn-eligible 16 digit patterns)
        RedactionPattern {
            _name: "credit_card",
            regex: Regex::new(r"\b([0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4})\b")
                .expect("Credit card regex valid"),
            placeholder: "<CARD_NUMBER>",
        },
        // Social Security Numbers (US format)
        RedactionPattern {
            _name: "ssn",
            regex: Regex::new(r"\b([0-9]{3}[-\s]?[0-9]{2}[-\s]?[0-9]{4})\b")
                .expect("SSN regex valid"),
            placeholder: "<SSN>",
        },
    ];

    /// Patterns for redacting sensitive window titles
    static ref TITLE_PATTERNS: Vec<RedactionPattern> = vec![
        // Password manager patterns
        RedactionPattern {
            _name: "password_entry",
            regex: Regex::new(r"(?i)(password\s*(for|:|\s)|unlock\s*(for|:|\s)|master\s*password)")
                .expect("Password entry regex valid"),
            placeholder: "<PASSWORD_ENTRY>",
        },
        // Login/credential windows
        RedactionPattern {
            _name: "login_window",
            regex: Regex::new(r"(?i)(sign\s*in|log\s*in|enter\s*password|authentication)")
                .expect("Login window regex valid"),
            placeholder: "<LOGIN_WINDOW>",
        },
        // Known password manager app titles
        RedactionPattern {
            _name: "password_manager",
            regex: Regex::new(r"(?i)(keepass|bitwarden|1password|lastpass|dashlane|enpass)")
                .expect("Password manager regex valid"),
            placeholder: "<PASSWORD_MANAGER>",
        },
        // Sensitive file names in editor titles
        RedactionPattern {
            _name: "sensitive_file",
            regex: Regex::new(r"(?i)(\.env|credentials|secrets?\.ya?ml|\.pem|\.key|id_rsa|\.netrc)")
                .expect("Sensitive file regex valid"),
            placeholder: "<SENSITIVE_FILE>",
        },
    ];
}

impl PrivacyFilter {
    /// Redact sensitive information from clipboard content
    #[must_use]
    pub fn redact_content(input: &str) -> Cow<'_, str> {
        let mut result = Cow::Borrowed(input);

        for pattern in CONTENT_PATTERNS.iter() {
            if pattern.regex.is_match(&result) {
                let redacted = pattern.regex.replace_all(&result, pattern.placeholder);
                result = Cow::Owned(redacted.into_owned());
            }
        }

        result
    }

    /// Redact sensitive information from window titles
    #[must_use]
    pub fn redact_title(input: &str) -> Cow<'_, str> {
        let mut result = Cow::Borrowed(input);

        for pattern in TITLE_PATTERNS.iter() {
            if pattern.regex.is_match(&result) {
                let redacted = pattern.regex.replace_all(&result, pattern.placeholder);
                result = Cow::Owned(redacted.into_owned());
            }
        }

        result
    }

    /// Check if content appears to be sensitive and should be fully redacted
    #[must_use]
    pub fn is_highly_sensitive(input: &str) -> bool {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_redact_aws_keys() -> xtask::sandbox::TestResult<()> {
        let input = "AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        let result = PrivacyFilter::redact_content(input);
        assert!(result.contains("<AWS_ACCESS_KEY>"));
        Ok(())
    }

    #[sinex_test]
    async fn test_redact_url_credentials() -> xtask::sandbox::TestResult<()> {
        let input = "https://user:password123@github.com/repo.git";
        let result = PrivacyFilter::redact_content(input);
        assert!(result.contains("<REDACTED>@"));
        assert!(!result.contains("password123"));
        Ok(())
    }

    #[sinex_test]
    async fn test_redact_github_pat() -> xtask::sandbox::TestResult<()> {
        let input = "GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        let result = PrivacyFilter::redact_content(input);
        assert!(result.contains("<GITHUB_TOKEN>"));
        Ok(())
    }

    #[sinex_test]
    async fn test_redact_generic_secret() -> xtask::sandbox::TestResult<()> {
        let input = "api_key=sk_live_abcdefghijklmnopqrstuvwxyz";
        let result = PrivacyFilter::redact_content(input);
        assert!(result.contains("<REDACTED>") || result.contains("<API_KEY>"));
        Ok(())
    }

    #[sinex_test]
    async fn test_redact_password_manager_title() -> xtask::sandbox::TestResult<()> {
        let input = "KeePassXC - Password for bank.example.com";
        let result = PrivacyFilter::redact_title(input);
        assert!(result.contains("<PASSWORD_MANAGER>") || result.contains("<PASSWORD_ENTRY>"));
        Ok(())
    }

    #[sinex_test]
    async fn test_redact_login_window() -> xtask::sandbox::TestResult<()> {
        let input = "Sign in - Google Accounts";
        let result = PrivacyFilter::redact_title(input);
        assert!(result.contains("<LOGIN_WINDOW>"));
        Ok(())
    }

    #[sinex_test]
    async fn test_passthrough_normal_content() -> xtask::sandbox::TestResult<()> {
        let input = "Hello world, this is normal text";
        let result = PrivacyFilter::redact_content(input);
        assert_eq!(result, input);
        Ok(())
    }

    #[sinex_test]
    async fn test_is_highly_sensitive() -> xtask::sandbox::TestResult<()> {
        assert!(PrivacyFilter::is_highly_sensitive(
            "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA..."
        ));
        assert!(!PrivacyFilter::is_highly_sensitive("normal text"));
        Ok(())
    }

    #[sinex_test]
    async fn test_redact_credit_card() -> xtask::sandbox::TestResult<()> {
        let input = "Card: 4111-1111-1111-1111";
        let result = PrivacyFilter::redact_content(input);
        assert!(result.contains("<CARD_NUMBER>"));
        Ok(())
    }
}
