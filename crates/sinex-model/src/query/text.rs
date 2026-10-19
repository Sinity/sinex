//! Text for full-text search (spec `query.md` §5): words split at Unicode
//! word boundaries, case-folded and NFC-normalized, without stemming. The
//! search read model stores a payload's text in this form and queries are
//! normalized the same way, so the two always agree.
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

/// Case-folded, NFC.
pub fn normalize(text: &str) -> String {
    caseless::default_case_fold_str(text).nfc().collect()
}

/// The normalized words of `text`.
pub fn words(text: &str) -> Vec<String> {
    text.unicode_words().map(normalize).collect()
}

/// Every string in a payload, as normalized words separated by single spaces.
/// Object keys are not text; nested values are walked in order.
pub fn payload_text(payload: &Value) -> String {
    let mut out = String::new();
    collect(payload, &mut out);
    out
}

fn collect(value: &Value, out: &mut String) {
    match value {
        Value::String(s) => {
            for word in words(s) {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(&word);
            }
        }
        Value::Array(items) => items.iter().for_each(|v| collect(v, out)),
        Value::Object(map) => map.values().for_each(|v| collect(v, out)),
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Escape a word for a POSIX regular expression.
pub fn regex_escape(word: &str) -> String {
    let mut out = String::with_capacity(word.len());
    for c in word.chars() {
        if r"\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Escape a literal for `LIKE ... ESCAPE '\'`.
pub fn like_escape(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

#[cfg(test)]
mod tests;
