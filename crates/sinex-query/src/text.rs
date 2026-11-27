//! Text for full-text search: words split at Unicode word boundaries,
//! case-folded and NFC-normalized, without stemming. The search read model
//! stores a payload's text in this form and queries are normalized the same
//! way, so the two always agree.
/// The normalized words of `text`.
pub fn words(text: &str) -> Vec<String> {
    sinex_model::search_text::words(text).collect()
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
