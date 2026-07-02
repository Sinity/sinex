//! URL redaction helpers for operator diagnostics.

/// Invalid URL handling policy for URL redaction helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidUrlPolicy {
    /// Preserve invalid input exactly.
    PreserveInput,
    /// Replace invalid input with `[INVALID_URL]`.
    InvalidUrlMarker,
    /// Replace invalid input with `[REDACTED]`.
    RedactedMarker,
}

impl InvalidUrlPolicy {
    fn render(self, input: &str) -> String {
        match self {
            Self::PreserveInput => input.to_string(),
            Self::InvalidUrlMarker => "[INVALID_URL]".to_string(),
            Self::RedactedMarker => "[REDACTED]".to_string(),
        }
    }
}

/// Strip username and password from a URL for broad operator display.
#[must_use]
pub fn redact_url_credentials_for_display(url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(url) else {
        return InvalidUrlPolicy::PreserveInput.render(url);
    };
    let _ = parsed.set_username("");
    let _ = parsed.set_password(None);
    parsed.to_string()
}

/// Redact only the password component, preserving username when present.
#[must_use]
pub fn redact_url_password_for_diagnostics(url: &str, invalid_policy: InvalidUrlPolicy) -> String {
    let Ok(mut parsed) = url::Url::parse(url) else {
        return invalid_policy.render(url);
    };
    if parsed.password().is_some() {
        let _ = parsed.set_password(Some("***"));
    }
    parsed.to_string()
}

#[cfg(test)]
#[path = "url_redaction_test.rs"]
mod tests;
