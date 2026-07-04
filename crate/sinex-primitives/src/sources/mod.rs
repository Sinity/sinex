//! Operator-facing source-domain types.
//!
//! This module hosts cross-cutting types for source diagnostics that are
//! shared across the gateway, repositories, and CLI (issue #1085 — source
//! continuity diagnostics, issue #1099 — source readiness surface).
//!
//! Types in this module sit alongside (not inside) `crate::rpc::sources`
//! because they describe *operator-facing diagnostics* rather than the
//! material-staging RPC contract. Both surfaces share `SourceFamily` and
//! the `SourceId` already defined under `crate::parser` so the two
//! issues can land independently without colliding.

pub mod continuity;

use schemars::JsonSchema;
use serde::Serialize;
use std::borrow::Cow;

/// Coarse grouping of sources (e.g. "filesystem", "terminal", "browser").
///
/// `SourceFamily` is an operator-facing rollup over the finer `EventSource` /
/// `SourceId` axis. It is loosely validated (lowercase, dotted) so the set
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

/// Product lane for a source.
///
/// `Activity` is operator/user-world evidence. `Reflection` is Sinex observing
/// its own runtime, throughput, health, and derived machinery. Keeping this as
/// a typed primitive prevents each query or telemetry surface from carrying a
/// slightly different string-prefix classifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceRole {
    Activity,
    Reflection,
}

impl SourceRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Activity => "activity",
            Self::Reflection => "reflection",
        }
    }

    #[must_use]
    pub const fn throughput_component(self) -> &'static str {
        match self {
            Self::Activity => "ingestion",
            Self::Reflection => "reflection",
        }
    }
}

/// SQL predicate equivalent of [`source_role`] for a column/expression
/// containing the event `source`.
///
/// Use this when a query needs an indexable lane filter. Use
/// [`source_role_sql_case`] when a projection needs the role label as a value.
#[must_use]
pub fn source_role_sql_predicate(source_expr: &str, role: SourceRole) -> String {
    let reflection = format!(
        "{source_expr} = 'sinex' OR {source_expr} LIKE 'sinex.%' OR {source_expr} LIKE 'sinexd.%'"
    );
    match role {
        SourceRole::Reflection => format!("({reflection})"),
        SourceRole::Activity => format!("NOT ({reflection})"),
    }
}

/// Return the namespace before the first `.` in a source identifier.
///
/// Grouping by source family lets query, context, and source-status surfaces
/// share one algebra instead of each carrying per-source special cases.
#[must_use]
pub fn source_family(source: &str) -> &str {
    source.split_once('.').map_or(source, |(head, _)| head)
}

/// Additional source prefixes that belong to a coarse operator-facing family.
///
/// These aliases keep query and source-status projections aligned when a
/// source contract's deployment namespace differs from its emitted event
/// source. For example, browser-adjacent acquisition lives under the `web`
/// source-contract namespace, while emitted events may use `webhistory` or
/// `raindrop`.
#[must_use]
pub fn source_family_aliases(family: &str) -> &'static [&'static str] {
    match family {
        "browser" => &["web", "webhistory", "raindrop"],
        "terminal" => &["shell", "shell.atuin", "shell.history"],
        _ => &[],
    }
}

/// True when a source identifier or source-contract namespace belongs to the
/// requested operator-facing family.
#[must_use]
pub fn source_identity_matches_family(
    source_identifier: &str,
    namespace: &str,
    family: &str,
) -> bool {
    source_family(source_identifier) == family
        || namespace == family
        || source_family_aliases(family).iter().any(|alias| {
            source_identifier == *alias
                || namespace == *alias
                || source_identifier
                    .strip_prefix(alias)
                    .is_some_and(|rest| rest.starts_with('.'))
        })
}

/// Classify a logical source/event source into the product lane it belongs to.
#[must_use]
pub fn source_role(source: &str) -> SourceRole {
    if source == "sinex" || source.starts_with("sinex.") || source.starts_with("sinexd.") {
        SourceRole::Reflection
    } else {
        SourceRole::Activity
    }
}

/// SQL CASE expression equivalent of [`source_role`] for a column/expression
/// containing the event `source`.
///
/// Keep SQL surfaces calling this instead of hand-writing prefix CASE arms.
#[must_use]
pub fn source_role_sql_case(source_expr: &str) -> String {
    format!(
        "CASE WHEN {predicate} THEN 'reflection' ELSE 'activity' END",
        predicate = source_role_sql_predicate(source_expr, SourceRole::Reflection)
    )
}

/// Throughput component SQL CASE expression equivalent to [`source_role`], with
/// the derived and gateway buckets layered above the source role.
#[must_use]
pub fn throughput_component_sql_case(source_expr: &str) -> String {
    format!(
        "CASE WHEN {source_expr} LIKE 'sinexd.api%' THEN 'gateway' WHEN {source_expr} LIKE 'derived.%' THEN 'derived' WHEN ({role_case}) = 'reflection' THEN 'reflection' ELSE 'ingestion' END",
        role_case = source_role_sql_case(source_expr)
    )
}

/// True when a logical source/event source belongs to Sinex's reflection lane.
#[must_use]
pub fn is_self_observation_source(source: &str) -> bool {
    source_role(source) == SourceRole::Reflection
}

/// True when a source-material identifier names reflection/self-observation
/// material.
///
/// Source-material identifiers can carry a material suffix
/// (`#material=<uuid>`), so keep this separate from event-source matching while
/// sharing the same semantic authority.
#[must_use]
pub fn is_self_observation_material_source(source_identifier: &str) -> bool {
    source_identifier.starts_with("sinex.self-observation.")
}

#[cfg(test)]
#[path = "../sources_test.rs"]
mod tests;
