//! Tag helpers for dot-scoped tag names and prefix resolution.
//!
//! Tags in sinex use a dot-scoped naming convention: `sys.source.screenshot`,
//! `sys.mime.text-markdown`, `inferred.file-type.rust`. The prefix-based
//! hierarchy enables prefix queries (`sys.source.*` returns all source tags).

/// Known system tag prefixes.
pub mod system {
    /// Applied when a file is registered by the document source.
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

/// Map a MIME type to a tag suffix for use with [`system::MIME_PREFIX`].
///
/// Returns `None` when the MIME type has no known suffix mapping.
///
/// ```
/// use crate::runtime::tags::mime_to_tag_suffix;
/// assert_eq!(mime_to_tag_suffix("text/markdown"), Some("text-markdown"));
/// assert_eq!(mime_to_tag_suffix("text/plain"), Some("text-plain"));
/// assert_eq!(mime_to_tag_suffix("application/pdf"), Some("pdf"));
/// assert_eq!(mime_to_tag_suffix("image/png"), None);
/// ```
#[must_use]
pub fn mime_to_tag_suffix(mime_type: &str) -> Option<&'static str> {
    match mime_type {
        "text/markdown" => Some("text-markdown"),
        "text/plain" => Some("text-plain"),
        "text/html" => Some("text-html"),
        "text/css" => Some("text-css"),
        "text/csv" => Some("text-csv"),
        "text/xml" | "application/xml" => Some("xml"),
        "application/json" => Some("json"),
        "application/pdf" => Some("pdf"),
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => Some("docx"),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => Some("xlsx"),
        "application/vnd.openxmlformats-officedocument.presentationml.presentation" => Some("pptx"),
        _ => None,
    }
}

/// Return auto-tags for a detected MIME type.
///
/// Returns a list of `sys.mime.<suffix>` tags from the MIME suffix mapping.
/// Returns an empty vector when the MIME type has no known mapping.
///
/// ```
/// use crate::runtime::tags::auto_tags_for_mime;
/// assert_eq!(auto_tags_for_mime("text/markdown"), vec!["sys.mime.text-markdown"]);
/// assert!(auto_tags_for_mime("image/png").is_empty());
/// ```
#[must_use]
pub fn auto_tags_for_mime(mime_type: &str) -> Vec<String> {
    mime_to_tag_suffix(mime_type)
        .map(|suffix| tag_name(system::MIME_PREFIX, suffix))
        .into_iter()
        .collect()
}

/// Construct a tag name from a prefix and suffix.
///
/// ```
/// use crate::runtime::tags::tag_name;
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
/// use crate::runtime::tags::parent_prefix;
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
    name.chars()
        .all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
}

#[cfg(test)]
#[path = "../tags_test.rs"]
mod tests;
