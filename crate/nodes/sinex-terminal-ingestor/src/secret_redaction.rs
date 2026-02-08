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

/// Redactor for sensitive information in terminal commands
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_aws_access_keys() {
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        let expected = "export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY>";
        assert_eq!(SecretRedactor::redact(input), expected);
    }

    #[test]
    fn test_redact_aws_secret_key_with_context() {
        // Should redact AWS secret keys when preceded by known variable names
        let input = "export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<AWS_SECRET_KEY>"));
        assert!(!result.contains("wJalrXUtnFEMI"));
    }

    #[test]
    fn test_no_false_positive_on_git_hash() {
        // Git hashes are 40 hex chars but should NOT be redacted
        let input = "git show a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
        let result = SecretRedactor::redact(input);
        // Should remain unchanged - no false positive redaction
        assert_eq!(result, input);
    }

    #[test]
    fn test_no_false_positive_on_uuid() {
        // UUIDs should not be redacted
        let input = "resource-id: 123e4567-e89b-12d3-a456-426614174000";
        let result = SecretRedactor::redact(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_redact_url_credentials() {
        let input = "git clone https://user:password123@github.com/repo.git";
        let expected = "git clone https://user:<REDACTED>@github.com/repo.git";
        assert_eq!(SecretRedactor::redact(input), expected);
    }

    #[test]
    fn test_redact_cli_flags() {
        let input = "./deploy --token abcdef123456 --verbose";
        let expected = "./deploy --token <REDACTED> --verbose";
        assert_eq!(SecretRedactor::redact(input), expected);
    }

    #[test]
    fn test_redact_github_pat() {
        let input = "GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<GITHUB_TOKEN>"));
    }

    #[test]
    fn test_redact_stripe_key() {
        let input = "stripe_key=sk_live_abcdefghijklmnopqrstuvwxyz";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<API_KEY>"));
    }

    #[test]
    fn test_redact_generic_password_assignment() {
        // Should redact password assignments
        let input = "export PASSWORD=mysecretpassword";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
        assert!(!result.contains("mysecretpassword"));
    }

    #[test]
    fn test_redact_generic_token_assignment() {
        // Should redact token assignments
        let input = "TOKEN=abc123token";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
    }

    #[test]
    fn test_generic_pattern_word_boundary() {
        // Should NOT match 'password' as substring of 'database_password'
        // Word boundary prevents this false positive
        let input = "export database_password=myvalue";
        let result = SecretRedactor::redact(input);
        // The whole 'database_password' should NOT trigger the generic pattern
        // because 'password' is not at a word boundary
        assert_eq!(
            result, input,
            "word boundary should prevent substring match"
        );
    }

    #[test]
    fn test_generic_pattern_matches_standalone_password() {
        // Should match when password IS at a word boundary
        let input = "PASSWORD=supersecret123";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
        assert!(!result.contains("supersecret123"));
    }

    #[test]
    fn test_url_credentials_with_percent_encoding() {
        // URL-encoded password: p%40ssw0rd = p@ssw0rd (@ encoded as %40)
        let input = "curl https://user:p%40ssw0rd@api.example.com/endpoint";
        let result = SecretRedactor::redact(input);
        // Should redact the password portion
        assert!(result.contains("<REDACTED>"));
        // The encoded password should NOT appear in the output
        assert!(
            !result.contains("p%40ssw0rd"),
            "URL-encoded password should be redacted"
        );
    }

    #[test]
    fn test_url_credentials_with_special_chars() {
        // Password with special characters that might be percent-encoded
        let input = "git clone https://deploy:s3cr3t%21%40%23@github.com/repo.git";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
        assert!(!result.contains("s3cr3t%21%40%23"));
    }

    #[test]
    fn test_url_credentials_ftp_protocol() {
        // Ensure URL credential redaction works across protocols
        let input = "curl ftp://admin:hunter2%26abc@files.example.com/data.csv";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
        assert!(!result.contains("hunter2%26abc"));
    }

    #[test]
    fn test_redact_with_stats_tracks_patterns() {
        // Stats should report which patterns matched
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        let (result, stats) = SecretRedactor::redact_with_stats(input);
        assert!(result.contains("<AWS_ACCESS_KEY>"));
        assert!(stats.any_redacted());
        assert!(stats.matched_patterns.contains(&"aws_access_key"));
    }

    #[test]
    fn test_redact_with_stats_empty_on_clean_input() {
        let input = "ls -la /home/user";
        let (result, stats) = SecretRedactor::redact_with_stats(input);
        assert_eq!(result, input);
        assert!(!stats.any_redacted());
        assert!(stats.matched_patterns.is_empty());
    }

    #[test]
    fn test_redact_with_stats_multiple_patterns() {
        // Input triggering multiple patterns
        let input = "curl --password hunter2 https://user:pass@example.com";
        let (_result, stats) = SecretRedactor::redact_with_stats(input);
        assert!(stats.matched_patterns.len() >= 2);
        assert!(stats.matched_patterns.contains(&"url_credentials"));
        assert!(stats.matched_patterns.contains(&"cli_flag_secret"));
    }
}
