//! Terminal output as plain text.

/// Append `data` to `out` without terminal escape sequences (CSI `ESC [ …
/// final`, OSC `ESC ] … BEL|ST`, two-byte `ESC x`) or other control
/// characters except newline and tab. A carriage return becomes nothing, so
/// redrawn lines read as their final text joined.
pub(crate) fn strip_escapes(data: &str, out: &mut String) {
    let mut chars = data.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\u{1b}' => match chars.next() {
                Some('[') => {
                    for c in chars.by_ref() {
                        if ('@'..='~').contains(&c) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    while let Some(c) = chars.next() {
                        if c == '\u{7}' {
                            break;
                        }
                        if c == '\u{1b}' && chars.peek() == Some(&'\\') {
                            chars.next();
                            break;
                        }
                    }
                }
                _ => {}
            },
            '\n' | '\t' => out.push(c),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
}
