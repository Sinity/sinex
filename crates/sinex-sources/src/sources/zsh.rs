//! Plain and extended zsh history (`: <start>:<elapsed>;<command>`), on `AppendFile`.
//!
//! The file has no record IDs, so a record is identified by host, start
//! second, command and its number among equal records ([`Seen`]). Untimed records keep
//! their material locations instead.
//! zsh metafies bytes 0x83..0x9f (0x83 then the byte xor 0x20) and continues a
//! multi-line command with a backslash before the newline. With SHARE_HISTORY
//! the elapsed field is written as 0 before the command ends, so it is kept as
//! written and never read as a duration.
use crate::drivers::append_file::AppendFile;
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::shell_history::Seen;
use crate::{Parser, RecordError};
use sinex_model::{Class, Timestamp, event_types::CommandImported};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.zsh-history",
    driver: Driver::AppendFile,
    excisable: false,
    purpose: "command fusion and shell recall",
    class: Class::Open,
    parser: "zsh-extended-history",
    parser_version: 3,
    event_types: &["command.imported"],
    framer: crate::Framer::incremental(frame),
};

pub struct ZshParser {
    /// The host whose history file this is; zsh does not record it.
    pub host: String,
}

impl Parser for ZshParser {
    type Record<'a> = &'a [u8];
    type Carry = Seen;
    type Payload = CommandImported;

    fn parse(
        &self,
        record: &&[u8],
        seen: &Seen,
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<CommandImported>, Seen), RecordError> {
        let decoded = unmetafy(record)?;
        let text = std::str::from_utf8(&decoded)
            .map_err(|_| RecordError::new("invalid UTF-8 after zsh unmetafy"))?;
        let text = text
            .strip_suffix('\n')
            .ok_or_else(|| RecordError::new("unfinished zsh record"))?;
        let (start, elapsed, command) = if let Some(header) = text.strip_prefix(": ") {
            let (start, rest) = header
                .split_once(':')
                .ok_or_else(|| RecordError::new("invalid extended zsh header"))?;
            let (elapsed, command) = rest
                .split_once(';')
                .ok_or_else(|| RecordError::new("invalid extended zsh header"))?;
            let start = start
                .parse::<i64>()
                .map_err(|_| RecordError::new("invalid zsh start time"))?;
            let elapsed = elapsed
                .parse::<i64>()
                .map_err(|_| RecordError::new("invalid zsh elapsed field"))?;
            (Some(start), Some(elapsed), command)
        } else {
            // A plain-history command starting with ':' has one extra '\'.
            let command = if text.starts_with("\\:") {
                &text[1..]
            } else {
                text
            };
            if command.is_empty() {
                return Ok((vec![], seen.clone()));
            }
            (None, None, command)
        };
        let started_at = start
            .map(Timestamp::from_unix_secs)
            .transpose()
            .map_err(|_| RecordError::new("zsh start time out of range"))?;
        // zsh appends one space when the command's last non-space byte is a
        // backslash. Remove only that space, then decode escaped newlines.
        let command = if command.trim_end_matches(' ').ends_with('\\') {
            command.strip_suffix(' ').unwrap_or(command)
        } else {
            command
        };
        let command = command.replace("\\\n", "\n");
        let (n, seen) = match start {
            Some(start) => seen.next(start, &command),
            None => (0, seen.clone()),
        };
        Ok((
            vec![CommandImported {
                command,
                hostname: self.host.clone(),
                started_at,
                header_elapsed: elapsed,
                paths: vec![],
                n,
            }],
            seen,
        ))
    }
}

pub fn unmetafy(bytes: &[u8]) -> Result<Vec<u8>, RecordError> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x83 {
            i += 1;
            let b = *bytes
                .get(i)
                .ok_or_else(|| RecordError::new("incomplete zsh meta escape"))?;
            out.push(b ^ 0x20);
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    Ok(out)
}

/// Records end at a newline not preceded by a backslash. A trailing partial
/// record is left for the next poll.
pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' && (i == 0 || bytes[i - 1] != b'\\') {
            out.push((start, i + 1));
            start = i + 1;
        }
    }
    (out, start)
}

pub fn source(host: String) -> DeclaredSource<ZshParser> {
    DeclaredSource {
        spec: SPEC,
        parser: ZshParser { host },
    }
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: crate::Framer::incremental(frame),
    }
}

/// `--read zsh=PATH`: a zsh history file.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "zsh",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| Ok(crate::binding::single(&SPEC, input, settings))),
    source: |b| Ok(Box::new(source(b.settings.host.clone()))),
    reader: Some(|b| {
        Ok(crate::binding::appended(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
