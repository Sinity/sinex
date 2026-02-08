//! Re-export of secret redaction from sinex-primitives.
//!
//! The implementation was moved to `sinex_primitives::secret_redaction` so both
//! terminal-ingestor and system-ingestor can share it.

pub use sinex_primitives::secret_redaction::{RedactionStats, SecretRedactor};

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
        let input = "export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<AWS_SECRET_KEY>"));
        assert!(!result.contains("wJalrXUtnFEMI"));
    }

    #[test]
    fn test_no_false_positive_on_git_hash() {
        let input = "git show a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
        let result = SecretRedactor::redact(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_no_false_positive_on_uuid() {
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
        let input = "export PASSWORD=mysecretpassword";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
        assert!(!result.contains("mysecretpassword"));
    }

    #[test]
    fn test_redact_generic_token_assignment() {
        let input = "TOKEN=abc123token";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
    }

    #[test]
    fn test_generic_pattern_word_boundary() {
        let input = "export database_password=myvalue";
        let result = SecretRedactor::redact(input);
        assert_eq!(
            result, input,
            "word boundary should prevent substring match"
        );
    }

    #[test]
    fn test_generic_pattern_matches_standalone_password() {
        let input = "PASSWORD=supersecret123";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
        assert!(!result.contains("supersecret123"));
    }

    #[test]
    fn test_url_credentials_with_percent_encoding() {
        let input = "curl https://user:p%40ssw0rd@api.example.com/endpoint";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
        assert!(
            !result.contains("p%40ssw0rd"),
            "URL-encoded password should be redacted"
        );
    }

    #[test]
    fn test_url_credentials_with_special_chars() {
        let input = "git clone https://deploy:s3cr3t%21%40%23@github.com/repo.git";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
        assert!(!result.contains("s3cr3t%21%40%23"));
    }

    #[test]
    fn test_url_credentials_ftp_protocol() {
        let input = "curl ftp://admin:hunter2%26abc@files.example.com/data.csv";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<REDACTED>"));
        assert!(!result.contains("hunter2%26abc"));
    }

    #[test]
    fn test_redact_with_stats_tracks_patterns() {
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
        let input = "curl --password hunter2 https://user:pass@example.com";
        let (_result, stats) = SecretRedactor::redact_with_stats(input);
        assert!(stats.matched_patterns.len() >= 2);
        assert!(stats.matched_patterns.contains(&"url_credentials"));
        assert!(stats.matched_patterns.contains(&"cli_flag_secret"));
    }

    #[test]
    fn test_redact_slack_token() {
        let input = "SLACK_TOKEN=xoxb-123456789012-1234567890123-abcdefghijklmnopqrstuvwx";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<SLACK_TOKEN>"));
    }

    #[test]
    fn test_redact_jwt_token() {
        let input = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<JWT_TOKEN>"));
        assert!(!result.contains("eyJhbGci"));
    }

    #[test]
    fn test_redact_google_api_key() {
        let input = "GOOGLE_KEY=AIzaSyA1234567890abcdefghijklmnopqrstuv";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("<GOOGLE_API_KEY>"));
    }

    #[test]
    fn test_redact_azure_connection_string() {
        let input = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=abc123def456ghi789jkl012mno345pqr678stu901vwxyz+A==";
        let result = SecretRedactor::redact(input);
        assert!(result.contains("AccountKey=<REDACTED>"));
        assert!(!result.contains("abc123def456"));
    }
}
