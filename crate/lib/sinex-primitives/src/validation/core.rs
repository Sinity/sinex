use camino::{Utf8Component as Component, Utf8Path as Path, Utf8PathBuf as PathBuf};
use percent_encoding::percent_decode_str;
use serde_json::Value;

use crate::error::{Result, SinexError};

/// Reject NaN and Infinity when deserializing f64 — PostgreSQL JSONB rejects these.
pub fn reject_non_finite_f64<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<f64, D::Error> {
    let value = <f64 as serde::Deserialize>::deserialize(d)?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(serde::de::Error::custom("NaN and Infinity are not supported"))
    }
}

/// Reject NaN and Infinity in optional f64 deserialization.
pub fn reject_non_finite_optional_f64<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<f64>, D::Error> {
    match <Option<f64> as serde::Deserialize>::deserialize(d)? {
        Some(v) if !v.is_finite() => {
            Err(serde::de::Error::custom("NaN and Infinity are not supported"))
        }
        other => Ok(other),
    }
}

const MAX_JSON_SIZE: usize = 10 * 1024 * 1024; // 10MB
const MAX_JSON_DEPTH: usize = 32;
const MAX_JSON_KEYS: usize = 1000;
const MAX_JSON_ARRAY_LEN: usize = 10_000;

/// Validate a file path for security issues
pub fn validate_path(path: &str) -> Result<camino::Utf8PathBuf> {
    // Reject empty paths
    if path.is_empty() {
        return Err(
            SinexError::validation("Path cannot be empty").with_context("validation_type", "path")
        );
    }

    // Check for null bytes
    if path.contains('\0') {
        return Err(SinexError::validation("Path contains null bytes")
            .with_context("validation_type", "path"));
    }

    // On Unix, backslashes are valid filename characters. Rewriting them into `/` changes
    // semantics and violates filename-preservation invariants relied on by tests.
    if path.contains('\\') {
        return Err(SinexError::validation("Path contains backslashes (\\)")
            .with_context("validation_type", "path"));
    }

    // Check length
    if path.len() > 4096 {
        return Err(SinexError::validation("Path too long").with_context("validation_type", "path"));
    }

    // Create PathBuf and clean it to normalize .. and . components
    let path_buf = PathBuf::from(path);
    let cleaned_path = clean_path(&path_buf);
    ensure_path_does_not_traverse(&cleaned_path)?;

    if let Some(decoded) = decode_percent_encoded_path(path)? {
        let cleaned_decoded = clean_path(&decoded);
        ensure_path_does_not_traverse(&cleaned_decoded)?;
    }

    Ok(cleaned_path)
}

/// Simple path cleaning without external dependencies
fn clean_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();

    for component in path.components() {
        match component {
            Component::CurDir => {
                // Skip current directory components
            }
            Component::ParentDir => {
                // Pop the last component if possible
                if let Some(last) = components.last() {
                    if matches!(last, Component::ParentDir | Component::RootDir) {
                        components.push(component);
                    } else {
                        components.pop();
                    }
                } else {
                    components.push(component);
                }
            }
            _ => {
                components.push(component);
            }
        }
    }

    components.iter().collect()
}

fn ensure_path_does_not_traverse(path: &Path) -> Result<()> {
    let mut depth = 0i32;
    for component in path.components() {
        match component {
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return Err(SinexError::validation("Path traversal detected")
                        .with_context("validation_type", "path"));
                }
            }
            Component::Normal(_) => depth += 1,
            Component::RootDir => depth = 0,
            _ => {}
        }
    }
    Ok(())
}

fn decode_percent_encoded_path(path: &str) -> Result<Option<PathBuf>> {
    if !path.as_bytes().contains(&b'%') {
        return Ok(None);
    }

    let mut current = path.to_string();
    let mut decoded_any = false;
    // Decode up to three times to catch nested encodings without risking an
    // unbounded allocation loop.
    for _ in 0..3 {
        if !current.as_bytes().contains(&b'%') {
            break;
        }
        decoded_any = true;
        current = percent_decode_str(&current)
            .decode_utf8()
            .map_err(|_| {
                SinexError::validation("Path contains invalid percent-encoding")
                    .with_context("validation_type", "path")
            })?
            .into_owned();
    }

    if !decoded_any {
        return Ok(None);
    }

    if current.contains('\\') {
        return Err(SinexError::validation("Path contains backslashes (\\)")
            .with_context("validation_type", "path"));
    }

    Ok(Some(PathBuf::from(current)))
}

/// Sanitize a filename component for safe storage and display
pub fn sanitize_filename_component(filename: &str) -> Result<String> {
    if filename.is_empty() {
        return Err(SinexError::validation("Filename cannot be empty")
            .with_context("validation_type", "filename"));
    }

    // Basic sanitization - remove dangerous characters
    let mut sanitized = String::new();
    for ch in filename.chars() {
        match ch {
            // Disallow control characters and dangerous filename chars
            '\0'..='\x1f' | '<' | '>' | ':' | '"' | '|' | '?' | '*' | '\\' | '/' => {
                sanitized.push('_');
            }
            _ => sanitized.push(ch),
        }
    }

    // Remove leading/trailing dots and spaces
    let sanitized = sanitized.trim_matches(|c| c == '.' || c == ' ').to_string();

    if sanitized.is_empty() {
        return Err(
            SinexError::validation("Filename becomes empty after sanitization")
                .with_context("validation_type", "filename"),
        );
    }

    Ok(sanitized)
}

/// Validate a file path stays within a watch root directory
pub fn validate_path_within_root(path: &str, root: &str) -> Result<PathBuf> {
    // First do basic validation
    let path_buf = validate_path(path)?;

    // Convert to absolute paths for comparison
    let abs_path = if path_buf.is_absolute() {
        path_buf
    } else {
        camino::Utf8PathBuf::from_path_buf(std::env::current_dir().map_err(|e| {
            SinexError::io(format!("Failed to get current dir: {e}"))
                .with_context("validation_type", "path")
        })?)
        .map_err(|_| {
            SinexError::io("Path contains invalid UTF-8").with_context("validation_type", "path")
        })?
        .join(&path_buf)
    };

    // Clean the root path as well
    let root_path = clean_path(&PathBuf::from(root));
    let abs_root = if root_path.is_absolute() {
        root_path
    } else {
        camino::Utf8PathBuf::from_path_buf(std::env::current_dir().map_err(|e| {
            SinexError::io(format!("Failed to get current dir: {e}"))
                .with_context("validation_type", "path")
        })?)
        .map_err(|_| {
            SinexError::io("Path contains invalid UTF-8").with_context("validation_type", "path")
        })?
        .join(&root_path)
    };

    // Canonicalize paths to resolve symlinks and normalize
    let canonical_path = abs_path
        .as_std_path()
        .canonicalize()
        .or_else(|_| {
            // If file doesn't exist yet, canonicalize parent and append filename
            abs_path
                .parent()
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid path")
                })
                .and_then(|parent| parent.as_std_path().canonicalize())
                .map(|parent| parent.join(abs_path.file_name().unwrap_or_default()))
        })
        .map_err(|e| {
            SinexError::validation(format!("Path canonicalization failed: {e}"))
                .with_context("validation_type", "path")
        })?;

    let canonical_root = abs_root.as_std_path().canonicalize().map_err(|e| {
        SinexError::validation(format!("Root canonicalization failed: {e}"))
            .with_context("validation_type", "path")
    })?;

    // Check if the canonical path starts with the canonical root
    if !canonical_path.starts_with(&canonical_root) {
        return Err(
            SinexError::validation(format!("Path '{path}' escapes watch root '{root}'"))
                .with_context("validation_type", "path"),
        );
    }

    camino::Utf8PathBuf::from_path_buf(canonical_path).map_err(|_| {
        SinexError::io("Canonical path contains invalid UTF-8")
            .with_context("validation_type", "path")
    })
}

/// Validate JSON with size and depth limits
pub fn validate_json(json_str: &str) -> Result<Value> {
    // Size check
    if json_str.len() > MAX_JSON_SIZE {
        return Err(
            SinexError::validation(format!("JSON too large: {} bytes", json_str.len()))
                .with_context("validation_type", "json"),
        );
    }

    // Parse
    let value: Value = serde_json::from_str(json_str).map_err(|e| {
        SinexError::validation(format!("Invalid JSON: {e}")).with_context("validation_type", "json")
    })?;

    // Validate structure
    validate_json_structure(&value, 0)?;

    Ok(value)
}

/// Validate a JSON Value with depth and structure limits
pub fn validate_json_value(value: &Value) -> Result<()> {
    // Validate structure (depth and key count)
    validate_json_structure(value, 0)?;
    Ok(())
}

fn validate_json_structure(value: &Value, depth: usize) -> Result<()> {
    if depth > MAX_JSON_DEPTH {
        return Err(
            SinexError::validation(format!("JSON too deep: {depth} levels"))
                .with_context("validation_type", "json"),
        );
    }

    match value {
        Value::Object(map) => {
            if map.len() > MAX_JSON_KEYS {
                return Err(
                    SinexError::validation(format!("Too many keys: {}", map.len()))
                        .with_context("validation_type", "json"),
                );
            }

            for (_, v) in map {
                validate_json_structure(v, depth + 1)?;
            }
        }
        Value::Array(arr) => {
            if arr.len() > MAX_JSON_ARRAY_LEN {
                return Err(SinexError::validation(format!(
                    "Array too long: {} elements",
                    arr.len()
                ))
                .with_context("validation_type", "json"));
            }
            for v in arr {
                validate_json_structure(v, depth + 1)?;
            }
        }
        _ => {} // Primitives are fine
    }

    Ok(())
}

/// Normalize and validate Unicode strings
///
/// Uses NFC (Canonical Decomposition followed by Canonical Composition), which is
/// the standard form for string storage and comparison in user-visible strings
/// and database storage.
pub fn normalize_unicode(input: &str) -> Result<String> {
    use unicode_normalization::UnicodeNormalization;
    let normalized = input.nfc().collect::<String>();

    // Check for dangerous characters
    for ch in normalized.chars() {
        match ch {
            // Zero-width characters
            '\u{200B}'..='\u{200D}' | '\u{FEFF}' | '\u{2060}' => {
                return Err(SinexError::validation("Zero-width characters not allowed")
                    .with_context("validation_type", "unicode"));
            }
            // Direction overrides
            '\u{202A}'..='\u{202E}' | '\u{200E}' | '\u{200F}' => {
                return Err(
                    SinexError::validation("Direction control characters not allowed")
                        .with_context("validation_type", "unicode"),
                );
            }
            _ => {}
        }
    }

    Ok(normalized)
}
