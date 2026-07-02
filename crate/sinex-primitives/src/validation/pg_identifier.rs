//! `PostgreSQL` identifier validation.
//!
//! Provides [`validate_pg_identifier`] for fail-closed validation of identifiers
//! (role names, database names, schema names, table names, column names) before
//! they are interpolated into DDL statements via `format!()`.
//!
//! ## What is allowed
//!
//! Only ASCII letters, digits, and underscores; must start with a letter or
//! underscore; length bounded to 63 bytes (`PostgreSQL`'s `NAMEDATALEN - 1` limit).
//! This rejects anything that could escape an un-quoted identifier context —
//! spaces, semicolons, quotes, dashes — and prevents SQL injection via
//! `format!`-constructed DDL statements.
//!
//! Callers that need Unicode identifiers or reserved-word names should use
//! double-quoting (`"identifier"`) in addition to validation, or switch to
//! parameterized queries when the driver supports it for the relevant DDL.

use crate::error::{Result, SinexError};

/// Validate a `PostgreSQL` identifier against the strict ASCII allowlist.
///
/// Accepts only ASCII letters (`[a-zA-Z]`), digits (`[0-9]`), and underscores
/// (`_`); the first character must be a letter or underscore; length must be
/// 1–63 bytes (`PostgreSQL`'s `NAMEDATALEN - 1`).
///
/// Returns `Err(SinexError::Validation)` for any identifier that fails the
/// check. The `kind` parameter is included in the error message for context
/// (e.g. `"database"`, `"role"`, `"schema"`, `"table"`, `"column"`).
///
/// # Example
///
/// ```rust
/// use sinex_primitives::validation::validate_pg_identifier;
///
/// assert!(validate_pg_identifier("sinex_dev", "database").is_ok());
/// assert!(validate_pg_identifier("sinex_app", "role").is_ok());
/// assert!(validate_pg_identifier("; DROP TABLE events; --", "schema").is_err());
/// assert!(validate_pg_identifier("has space", "table").is_err());
/// assert!(validate_pg_identifier("has'quote", "column").is_err());
/// assert!(validate_pg_identifier("", "database").is_err());
/// ```
pub fn validate_pg_identifier(ident: &str, kind: &str) -> Result<()> {
    let valid = !ident.is_empty()
        && ident.len() <= 63
        && ident
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(SinexError::validation(format!(
            "invalid PostgreSQL {kind} identifier {ident:?}: \
             must match [a-zA-Z_][a-zA-Z0-9_]{{0,62}}"
        ))
        .with_context("kind", kind)
        .with_context("identifier", ident))
    }
}

#[cfg(test)]
#[path = "pg_identifier_test.rs"]
mod tests;
