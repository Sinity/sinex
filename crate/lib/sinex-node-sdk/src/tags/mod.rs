//! Tag helpers for dot-scoped tag names and prefix resolution.
//!
//! Tags in sinex use a dot-scoped naming convention: `sys.source.screenshot`,
//! `sys.mime.text-markdown`, `inferred.file-type.rust`. The prefix-based
//! hierarchy enables prefix queries (`sys.source.*` returns all source tags).

/// Known system tag prefixes.
pub mod system {
    /// Applied when a file is registered by the document ingestor.
    pub const MIME_PREFIX: &str = "sys.mime";
    /// Applied when a screenshot is captured.
    pub const SOURCE_SCREENSHOT: &str = "sys.source.screenshot";
    /// Applied when a terminal command is captured.
    pub const SOURCE_TERMINAL: &str = "sys.source.terminal";
    /// Applied when a browser page is captured.
    pub const SOURCE_BROWSER: &str = "sys.source.browser";
    /// Applied when a desktop event is captured.
    pub const SOURCE_DESKTOP: &str = "sys.source.desktop";
    /// Applied when a file is captured.
    pub const SOURCE_FILE: &str = "sys.source.file";
}

/// Known inferred tag prefixes (applied by automata).
pub mod inferred {
    /// File type detection from extension.
    pub const FILE_TYPE_PREFIX: &str = "inferred.file-type";
    /// Quality assessments (e.g. OCR confidence).
    pub const QUALITY_PREFIX: &str = "inferred.quality";
    /// Language detection.
    pub const LANGUAGE_PREFIX: &str = "inferred.language";
}

/// Construct a tag name from a prefix and suffix.
///
/// ```
/// use sinex_node_sdk::tags::tag_name;
/// assert_eq!(tag_name("sys.mime", "text-markdown"), "sys.mime.text-markdown");
/// ```
#[must_use]
pub fn tag_name(prefix: &str, suffix: &str) -> String {
    format!("{prefix}.{suffix}")
}

/// Return all tags that share a given prefix.
///
/// In production this queries `core.tags WHERE name LIKE 'prefix.%'`.
/// This helper provides the canonical prefix format for such queries.
#[must_use]
pub fn prefix_pattern(prefix: &str) -> String {
    format!("{prefix}.%")
}

/// Return the immediate parent prefix of a dot-scoped tag name.
///
/// ```
/// use sinex_node_sdk::tags::parent_prefix;
/// assert_eq!(parent_prefix("sys.mime.text-markdown"), Some("sys.mime".to_string()));
/// assert_eq!(parent_prefix("sys"), None);
/// ```
#[must_use]
pub fn parent_prefix(tag: &str) -> Option<String> {
    tag.rfind('.').map(|pos| tag[..pos].to_string())
}

/// Validate a tag name follows the dot-scoped convention.
#[must_use]
pub fn is_valid_tag_name(name: &str) -> bool {
    if name.is_empty() || name.starts_with('.') || name.ends_with('.') {
        return false;
    }
    name.chars().all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_name_construction() {
        assert_eq!(tag_name("sys.mime", "text-markdown"), "sys.mime.text-markdown");
    }

    #[test]
    fn test_parent_prefix() {
        assert_eq!(parent_prefix("sys.mime.text"), Some("sys.mime".to_string()));
        assert_eq!(parent_prefix("sys"), None);
        assert_eq!(parent_prefix("a.b.c.d"), Some("a.b.c".to_string()));
    }

    #[test]
    fn test_prefix_pattern() {
        assert_eq!(prefix_pattern("sys.mime"), "sys.mime.%");
    }

    #[test]
    fn test_valid_tag_names() {
        assert!(is_valid_tag_name("sys.source.screenshot"));
        assert!(is_valid_tag_name("inferred.file-type.rust"));
        assert!(!is_valid_tag_name(""));
        assert!(!is_valid_tag_name(".bad"));
        assert!(!is_valid_tag_name("bad."));
    }
}
