//! Re-export of secret redaction primitives.
//!
//! Consumers should import [`GLOBAL_REDACTOR`] or [`ConfigurableRedactor`]
//! directly from `sinex_primitives::secret_redaction`.

pub use sinex_primitives::secret_redaction::{
    ConfigurableRedactor, OwnedRedactionStats, GLOBAL_REDACTOR,
};

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::redaction_config::RedactionConfig;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    fn redacts_aws_access_key() -> TestResult<()> {
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<AWS_ACCESS_KEY>"), "got: {result}");
        Ok(())
    }

    #[sinex_test]
    fn redacts_aws_secret_key_with_context() -> TestResult<()> {
        let input = "export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<AWS_SECRET_KEY>"), "got: {result}");
        assert!(!result.contains("wJalrXUtnFEMI"));
        Ok(())
    }

    #[sinex_test]
    fn no_false_positive_on_git_hash() -> TestResult<()> {
        let input = "git show a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
        assert_eq!(GLOBAL_REDACTOR.redact_content(input), input);
        Ok(())
    }

    #[sinex_test]
    fn no_false_positive_on_uuid() -> TestResult<()> {
        let input = "resource-id: 123e4567-e89b-12d3-a456-426614174000";
        assert_eq!(GLOBAL_REDACTOR.redact_content(input), input);
        Ok(())
    }

    #[sinex_test]
    fn redacts_url_credentials() -> TestResult<()> {
        let input = "git clone https://user:password123@github.com/repo.git";
        let expected = "git clone https://user:<REDACTED>@github.com/repo.git";
        assert_eq!(GLOBAL_REDACTOR.redact_content(input), expected);
        Ok(())
    }

    #[sinex_test]
    fn redacts_cli_flag() -> TestResult<()> {
        let input = "./deploy --token abcdef123456 --verbose";
        let expected = "./deploy --token <REDACTED> --verbose";
        assert_eq!(GLOBAL_REDACTOR.redact_content(input), expected);
        Ok(())
    }

    #[sinex_test]
    fn redacts_github_pat() -> TestResult<()> {
        let input = "GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<GITHUB_TOKEN>"), "got: {result}");
        Ok(())
    }

    #[sinex_test]
    fn redacts_stripe_key() -> TestResult<()> {
        let input = "stripe_key=sk_live_abcdefghijklmnopqrstuvwxyz";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<API_KEY>"), "got: {result}");
        Ok(())
    }

    #[sinex_test]
    fn redacts_generic_password_assignment() -> TestResult<()> {
        let input = "export PASSWORD=mysecretpassword";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<REDACTED>"), "got: {result}");
        assert!(!result.contains("mysecretpassword"));
        Ok(())
    }

    #[sinex_test]
    fn redacts_standalone_password() -> TestResult<()> {
        let input = "PASSWORD=supersecret123";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<REDACTED>"), "got: {result}");
        assert!(!result.contains("supersecret123"));
        Ok(())
    }

    #[sinex_test]
    fn word_boundary_prevents_database_password_match() -> TestResult<()> {
        let input = "export database_password=myvalue";
        assert_eq!(
            GLOBAL_REDACTOR.redact_content(input),
            input,
            "word boundary should prevent substring match"
        );
        Ok(())
    }

    #[sinex_test]
    fn url_credentials_with_percent_encoding() -> TestResult<()> {
        let input = "curl https://user:p%40ssw0rd@api.example.com/endpoint";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<REDACTED>"), "got: {result}");
        assert!(
            !result.contains("p%40ssw0rd"),
            "URL-encoded password should be redacted"
        );
        Ok(())
    }

    #[sinex_test]
    fn url_credentials_with_special_chars() -> TestResult<()> {
        let input = "git clone https://deploy:s3cr3t%21%40%23@github.com/repo.git";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<REDACTED>"), "got: {result}");
        assert!(!result.contains("s3cr3t%21%40%23"));
        Ok(())
    }

    #[sinex_test]
    fn url_credentials_ftp_protocol() -> TestResult<()> {
        let input = "curl ftp://admin:hunter2%26abc@files.example.com/data.csv";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<REDACTED>"), "got: {result}");
        assert!(!result.contains("hunter2%26abc"));
        Ok(())
    }

    #[sinex_test]
    fn with_stats_tracks_patterns() -> TestResult<()> {
        let config = RedactionConfig {
            track_stats: true,
            ..RedactionConfig::with_defaults()
        };
        let cr = ConfigurableRedactor::new(&config).unwrap();
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        let (result, stats) = cr.redact_content_with_stats(input);
        assert!(result.contains("<AWS_ACCESS_KEY>"), "got: {result}");
        assert!(stats.any_redacted());
        assert!(stats.matched_patterns.iter().any(|p| p == "aws_access_key"));
        Ok(())
    }

    #[sinex_test]
    fn with_stats_empty_on_clean_input() -> TestResult<()> {
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
    fn with_stats_multiple_patterns() -> TestResult<()> {
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

    #[sinex_test]
    fn redacts_slack_token() -> TestResult<()> {
        let input = "SLACK_TOKEN=xoxb-123456789012-1234567890123-abcdefghijklmnopqrstuvwx";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<SLACK_TOKEN>"), "got: {result}");
        Ok(())
    }

    #[sinex_test]
    fn redacts_jwt_token() -> TestResult<()> {
        let input = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<JWT_TOKEN>"), "got: {result}");
        assert!(!result.contains("eyJhbGci"));
        Ok(())
    }

    #[sinex_test]
    fn redacts_google_api_key() -> TestResult<()> {
        let input = "GOOGLE_KEY=AIzaSyA1234567890abcdefghijklmnopqrstuv";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("<GOOGLE_API_KEY>"), "got: {result}");
        Ok(())
    }

    #[sinex_test]
    fn redacts_azure_connection_string() -> TestResult<()> {
        let input = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=abc123def456ghi789jkl012mno345pqr678stu901vwxyz+A==";
        let result = GLOBAL_REDACTOR.redact_content(input);
        assert!(result.contains("AccountKey=<REDACTED>"), "got: {result}");
        assert!(!result.contains("abc123def456"));
        Ok(())
    }
}
