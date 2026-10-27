//! Complete journal blocks. Only true EOF completes an undecided last block.

pub fn frame(bytes: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
    let mut records = Vec::new();
    let mut block = None;
    let mut offset = 0;
    let mut consumed = 0;
    let mut comment = false;
    let mut python = false;
    while offset < bytes.len() {
        let end = match bytes[offset..].iter().position(|b| *b == b'\n') {
            Some(n) => offset + n + 1,
            None if eof => bytes.len(),
            None => break,
        };
        let line = bytes[offset..end].trim_ascii_end();
        if comment {
            if line == b"end comment" || line == b"endcomment" {
                comment = false;
            }
        } else if line.is_empty() && !python {
            if let Some(start) = block.take() {
                records.push((start, end));
            }
            consumed = end;
        } else if !line.is_empty() && !matches!(line.first(), Some(b' ' | b'\t')) {
            if let Some(start) = block.replace(offset) {
                records.push((start, offset));
                consumed = offset;
            }
            let directive = line
                .strip_prefix(b"!")
                .or_else(|| line.strip_prefix(b"@"))
                .unwrap_or(line);
            python =
                directive.split(|b| b.is_ascii_whitespace()).next() == Some(b"python".as_slice());
            if directive == b"comment" {
                comment = true;
            }
        } else if block.is_none() {
            block = Some(offset);
        }
        offset = end;
    }
    if eof {
        if let Some(start) = block {
            records.push((start, bytes.len()));
        }
        consumed = bytes.len();
    }
    (records, consumed)
}

pub fn complete(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    frame(bytes, true)
}

#[cfg(test)]
mod tests;
