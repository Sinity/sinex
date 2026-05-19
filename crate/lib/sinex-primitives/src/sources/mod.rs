//! Operator-facing source-domain types.
//!
//! This module hosts cross-cutting types for source diagnostics that are
//! shared across the gateway, repositories, and CLI (issue #1085 — source
//! continuity diagnostics, issue #1099 — source readiness surface).
//!
//! Types in this module sit alongside (not inside) `crate::rpc::sources`
//! because they describe *operator-facing diagnostics* rather than the
//! material-staging RPC contract. Both surfaces share `SourceFamily` and
//! the `SourceUnitId` already defined under `crate::parser` so the two
//! issues can land independently without colliding.

pub mod continuity;

use schemars::JsonSchema;
use serde::Serialize;
use std::borrow::Cow;

/// Coarse grouping of sources (e.g. "filesystem", "terminal", "browser").
///
/// `SourceFamily` is an operator-facing rollup over the finer `EventSource` /
/// `SourceUnitId` axis. It is loosely validated (lowercase, dotted) so the set
/// can grow without code changes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct SourceFamily(Cow<'static, str>);

impl<'de> serde::Deserialize<'de> for SourceFamily {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::validate_str(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(Cow::Owned(s)))
    }
}

impl SourceFamily {
    /// Construct a validated `SourceFamily` from any string-like value.
    ///
    /// # Errors
    /// Returns an error if the input is empty or contains characters outside
    /// the permitted set (`[a-z0-9._-]`).
    pub fn new(s: impl Into<String>) -> Result<Self, crate::SinexError> {
        let s = s.into();
        Self::validate_str(&s)?;
        Ok(Self(Cow::Owned(s)))
    }

    /// Const constructor for static literals.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        assert!(
            Self::const_validate(s),
            "SourceFamily must match [a-z0-9._-]+"
        );
        Self(Cow::Borrowed(s))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate_str(s: &str) -> Result<(), crate::SinexError> {
        if s.is_empty() {
            return Err(crate::SinexError::validation(
                "SourceFamily must not be empty",
            ));
        }
        if !s.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-'
        }) {
            return Err(crate::SinexError::validation(
                "SourceFamily must contain only [a-z0-9._-]",
            ));
        }
        Ok(())
    }

    const fn const_validate(s: &str) -> bool {
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if !(b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || b == b'.'
                || b == b'_'
                || b == b'-')
            {
                return false;
            }
            i += 1;
        }
        !s.is_empty()
    }
}

impl std::fmt::Display for SourceFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn source_family_validates_charset() -> xtask::sandbox::TestResult<()> {
        SourceFamily::new("filesystem").unwrap();
        SourceFamily::new("browser.history").unwrap();
        SourceFamily::new("integration_polylogue").unwrap();
        assert!(SourceFamily::new("").is_err());
        assert!(SourceFamily::new("Has Caps").is_err());
        assert!(SourceFamily::new("with/slash").is_err());
        Ok(())
    }

    #[sinex_test]
    async fn source_family_const_constructor() -> xtask::sandbox::TestResult<()> {
        const FILESYSTEM: SourceFamily = SourceFamily::from_static("filesystem");
        assert_eq!(FILESYSTEM.as_str(), "filesystem");
        Ok(())
    }

    #[sinex_test]
    async fn source_family_round_trips_serde() -> xtask::sandbox::TestResult<()> {
        let family = SourceFamily::new("terminal").unwrap();
        let json = serde_json::to_string(&family).unwrap();
        assert_eq!(json, "\"terminal\"");
        let back: SourceFamily = serde_json::from_str(&json).unwrap();
        assert_eq!(back, family);
        Ok(())
    }
}
