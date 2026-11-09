//! Window titles as programs animate them.

/// A title without the spinner a busy terminal program draws at its start:
/// a leading run of progress glyphs that is followed by a space or ends the
/// title, together with the spaces after it. A glyph touching the text, as in
/// `*draft` (an editor's unsaved mark), is part of the title.
///
/// Progress glyphs are Unicode Braille patterns, the dingbat stars and
/// asterisks, circles with a half or quarter marked, the middle dot and the
/// asterisk.
pub(crate) fn without_progress(title: &str) -> &str {
    let glyph = |c: char| {
        matches!(c,
            '\u{2800}'..='\u{28FF}'   // Braille patterns: ⠋ ⠙ ⠹ ⣾ …
            | '\u{2722}'..='\u{273D}' // dingbat stars and asterisks: ✢ ✳ ✶ ✻ ✽ …
            | '\u{25D0}'..='\u{25D3}' // circles with a half filled: ◐ ◓ ◑ ◒
            | '\u{25F4}'..='\u{25F7}' // circles with a quarter marked: ◴ ◵ ◶ ◷
            | '\u{00B7}'              // middle dot
            | '*')
    };
    let rest = title.trim_start_matches(glyph);
    if rest.len() == title.len() || !(rest.is_empty() || rest.starts_with(char::is_whitespace)) {
        title
    } else {
        rest.trim_start()
    }
}

#[cfg(test)]
mod tests;
