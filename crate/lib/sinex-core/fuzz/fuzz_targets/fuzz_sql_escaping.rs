//! Fuzz target for SQL identifier escaping.
//!
//! Tests that SQL identifier escaping correctly handles malicious inputs
//! to prevent SQL injection attacks.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

/// Arbitrary input for SQL escaping fuzzing.
#[derive(Debug, Arbitrary)]
struct SqlInput {
    /// Column/table identifier to escape
    identifier: String,
    /// Whether to include SQL keywords
    include_keywords: bool,
    /// Whether to include quotes
    include_quotes: bool,
}

impl SqlInput {
    /// Generate a potentially malicious identifier.
    fn to_identifier(&self) -> String {
        let mut ident = self.identifier.clone();

        if self.include_keywords {
            // Prepend SQL keywords
            ident = format!("SELECT * FROM users; DROP TABLE {}", ident);
        }

        if self.include_quotes {
            // Add various quote characters
            ident = format!(r#"{}"; --"#, ident);
        }

        ident
    }
}

/// Escape a SQL identifier the same way sinex-core does.
/// This duplicates the logic to test it independently.
fn escape_identifier(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

/// Verify escaped identifier doesn't contain unescaped injection vectors.
fn verify_safe_identifier(escaped: &str, original: &str) -> bool {
    // The escaped form should:
    // 1. Start and end with double quotes
    // 2. Not contain unescaped double quotes (all internal " should be "")
    // 3. Not allow breaking out of the quoted context

    if !escaped.starts_with('"') || !escaped.ends_with('"') {
        return false;
    }

    // Remove outer quotes and check internal escaping
    let inner = &escaped[1..escaped.len() - 1];

    // Count consecutive quotes - should always be even (escaped pairs)
    let mut quote_count = 0;
    let mut prev_was_quote = false;

    for ch in inner.chars() {
        if ch == '"' {
            quote_count += 1;
            prev_was_quote = true;
        } else {
            // If we had an odd number of quotes before a non-quote, that's bad
            if prev_was_quote && quote_count % 2 != 0 {
                return false;
            }
            prev_was_quote = false;
            quote_count = 0;
        }
    }

    // If we end with quotes, count should be even
    if prev_was_quote && quote_count % 2 != 0 {
        return false;
    }

    // Verify that the original content (with quotes doubled) is preserved
    let expected_inner = original.replace('"', "\"\"");
    inner == expected_inner
}

fuzz_target!(|input: SqlInput| {
    let identifier = input.to_identifier();

    // Test the escaping function
    let escaped = escape_identifier(&identifier);

    // Verify the escaping is correct
    assert!(
        verify_safe_identifier(&escaped, &identifier),
        "Escaping failed for identifier: {:?} -> {:?}",
        identifier,
        escaped
    );

    // Test that sanitize functions don't panic on SQL-like content
    let _ = sinex_core::db::security::SecurityValidator::sanitize_config_value(&identifier);
    let _ = sinex_core::db::security::SecurityValidator::validate_config_content(&identifier);

    // Test unicode handling on SQL identifiers
    let _ = sinex_core::db::security::SecurityValidator::sanitize_unicode(&identifier);
    let _ = sinex_core::types::validation::normalize_unicode(&identifier);

    // Test shell metacharacter detection (SQL often shares dangerous chars)
    let _ = sinex_core::types::validation::contains_shell_metacharacters(&identifier);
});
