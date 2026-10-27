//! Cue blocks include their identifiers and settings, with exact byte ranges.
pub fn frame(bytes: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
    let mut records = Vec::new();
    let mut start = 0;
    let mut line_start = 0;
    for (i, byte) in bytes.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        if bytes[line_start..i]
            .iter()
            .all(|b| matches!(b, b' ' | b'\t' | b'\r'))
        {
            if bytes[start..line_start]
                .iter()
                .any(|b| !b.is_ascii_whitespace())
            {
                records.push((start, i + 1));
            }
            start = i + 1;
        }
        line_start = i + 1;
    }
    if eof && start < bytes.len() {
        if bytes[start..].iter().any(|b| !b.is_ascii_whitespace()) {
            records.push((start, bytes.len()));
        }
        start = bytes.len();
    }
    (records, start)
}
pub fn whole(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    frame(bytes, true)
}

#[cfg(test)]
mod tests;
