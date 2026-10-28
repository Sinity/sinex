//! PostgreSQL text COPY field encoding. Row and column separators are supplied by the caller.
/// Text-format escaping: backslash, tab, newline and carriage return.
pub fn escape(buf: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '\\' => buf.push_str("\\\\"),
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            c => buf.push(c),
        }
    }
}
