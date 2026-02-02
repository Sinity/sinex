use camino::{Utf8Component as Component, Utf8Path as Path, Utf8PathBuf as PathBuf};
use percent_encoding::percent_decode_str;
use serde_json::Value;

use crate::error::{Result, SinexError};

const MAX_JSON_SIZE: usize = 10 * 1024 * 1024; // 10MB
const MAX_JSON_DEPTH: usize = 32;
const MAX_JSON_KEYS: usize = 1000;

/// Validate a file path for security issues
pub fn validate_path(path: &str) -> Result<camino::Utf8PathBuf> {
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
                    if !matches!(last, Component::ParentDir | Component::RootDir) {
                        components.pop();
                    } else {
                        components.push(component);
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

/// Deserialize JSON with validation and enhanced error handling
pub fn deserialize_json_with_validation<T>(json_str: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    // First validate the JSON structure
    let value = validate_json(json_str)?;

    // Then deserialize with path-to-error tracking
    let deserializer = serde_json::from_value::<T>(value);

    deserializer.map_err(|e| {
        SinexError::validation(format!("Deserialization failed: {e}"))
            .with_context("validation_type", "json")
    })
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
            for v in arr {
                validate_json_structure(v, depth + 1)?;
            }
        }
        _ => {} // Primitives are fine
    }

    Ok(())
}

/// Normalize and validate Unicode strings
pub fn normalize_unicode(input: &str) -> Result<String> {
    use unicode_normalization::UnicodeNormalization;
    let normalized = input.nfd().collect::<String>();

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

/// Check if a string contains shell metacharacters
#[must_use]
pub fn contains_shell_metacharacters(s: &str) -> bool {
    const DANGEROUS_CHARS: &[char] = &[
        ';', '|', '&', '$', '`', '(', ')', '{', '}', '<', '>', '\\', '\n', '\r', '\0', '*', '?',
        '[', ']', '!', '~', '"', '\'',
    ];

    s.contains("$(") || s.contains("${") || s.chars().any(|c| DANGEROUS_CHARS.contains(&c))
}

/// Detect potential billion laughs pattern in JSON
pub fn check_json_expansion(value: &Value) -> Result<()> {
    fn estimate_expanded_size(value: &Value, depth: usize) -> Result<usize> {
        if depth > 10 {
            return Err(
                SinexError::validation("Potential billion laughs attack detected")
                    .with_context("validation_type", "json"),
            );
        }

        match value {
            Value::Object(map) => {
                let mut size = 0;
                for (k, v) in map {
                    size += k.len();
                    size += estimate_expanded_size(v, depth + 1)?;
                }
                Ok(size)
            }
            Value::Array(arr) => {
                let mut size = 0;
                for v in arr {
                    size += estimate_expanded_size(v, depth + 1)?;
                }
                // Check for exponential expansion
                if depth > 3 && arr.len() > 100 {
                    return Err(
                        SinexError::validation("Suspicious array expansion detected")
                            .with_context("validation_type", "json"),
                    );
                }
                Ok(size)
            }
            Value::String(s) => Ok(s.len()),
            _ => Ok(8), // Number, bool, null
        }
    }

    let estimated_size = estimate_expanded_size(value, 0)?;

    // If expanded size is more than 100x the original, reject
    if estimated_size > value.to_string().len() * 100 {
        return Err(SinexError::validation("JSON expansion ratio too high")
            .with_context("validation_type", "json"));
    }

    Ok(())
}
