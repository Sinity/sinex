//! Privacy filter for clipboard and window content
//!
//! Redacts sensitive information from clipboard content and window titles
//! before storage. This protects against accidental capture of credentials,
//! API keys, and other sensitive data.
//!
//! Internally delegates to [`ConfigurableRedactor`] from `sinex-primitives`,
//! lazily initialised on first use.  The configuration is loaded from
//! environment variables (see [`RedactionConfig::from_env`]) or falls back to
//! the reference pattern set.

use sinex_primitives::redaction_config::RedactionConfig;
use sinex_primitives::secret_redaction::ConfigurableRedactor;
use std::borrow::Cow;
use std::sync::OnceLock;

/// Lazily-initialised global redactor instance.
///
/// Tries [`RedactionConfig::from_env`] first.  If the environment variables
/// contain invalid JSON the error is logged and the reference defaults are
/// used instead.
fn global_redactor() -> &'static ConfigurableRedactor {
    static INSTANCE: OnceLock<ConfigurableRedactor> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let config = RedactionConfig::from_env().unwrap_or_else(|e| {
            tracing::warn!(
                error = %e,
                "failed to load redaction config from environment, using defaults"
            );
            RedactionConfig::with_defaults()
        });
        ConfigurableRedactor::new(&config).unwrap_or_else(|e| {
            tracing::warn!(
                error = %e,
                "failed to compile redaction patterns from config, using defaults"
            );
            ConfigurableRedactor::with_defaults()
        })
    })
}

/// Privacy filter for desktop ingestor content.
///
/// All methods are **static** (associated functions) and delegate to a
/// lazily-initialised [`ConfigurableRedactor`].
pub struct PrivacyFilter;

impl PrivacyFilter {
    /// Redact sensitive information from clipboard content
    #[must_use]
    pub fn redact_content(input: &str) -> Cow<'_, str> {
        global_redactor().redact_content(input)
    }

    /// Redact sensitive information from window titles
    #[must_use]
    pub fn redact_title(input: &str) -> Cow<'_, str> {
        global_redactor().redact_title(input)
    }

    /// Check if content appears to be sensitive and should be fully redacted
    #[must_use]
    pub fn is_highly_sensitive(input: &str) -> bool {
        global_redactor().is_highly_sensitive(input)
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
