//! fish history files (`~/.local/share/fish/fish_history`), on `AppendFile`.
//!
//! fish writes each command as a small YAML-like block:
//!
//! ```text
//! - cmd: cp notes.txt /tmp
//!   when: 1700000000
//!   paths:
//!     - notes.txt
//! ```
//!
//! `cmd` and the paths are escaped: a backslash is written `\\` and a newline
//! `\n`. `when` is the start in epoch seconds. fish appends blocks and now and
//! then rewrites the file without duplicates; records are numbered like zsh's
//! ([`Seen`]), so a block read again is a repeat.
use crate::drivers::append_file::AppendFile;
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::shell_history::Seen;
use crate::{Parser, RecordError};
use sinex_model::{Class, Timestamp, event_types::CommandImported};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.fish-history",
    driver: Driver::AppendFile,
    excisable: false,
    purpose: "command fusion and shell recall",
    class: Class::Open,
    parser: "fish-history",
    parser_version: 1,
    event_types: &["command.imported"],
    framer: crate::Framer::incremental(frame),
};

pub struct FishParser {
    /// The host whose history file this is; fish does not record it.
    pub host: String,
}

/// fish's escaping undone: `\\` is a backslash and `\n` a newline; any other
/// backslash stays as written.
pub fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

impl Parser for FishParser {
    type Record<'a> = &'a [u8];
    type Carry = Seen;
    type Payload = CommandImported;

    fn parse(
        &self,
        record: &&[u8],
        seen: &Seen,
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<CommandImported>, Seen), RecordError> {
        let text = std::str::from_utf8(record)
            .map_err(|_| RecordError::new("fish history record is not UTF-8"))?;
        let mut lines = text.lines();
        let command = lines
            .next()
            .and_then(|l| l.strip_prefix("- cmd:"))
            .ok_or_else(|| RecordError::new("a fish history record starts with `- cmd:`"))?;
        let command = unescape(command.strip_prefix(' ').unwrap_or(command));
        let mut when = None;
        let mut paths = Vec::new();
        let mut in_paths = false;
        for line in lines {
            if let Some(value) = line.strip_prefix("  when:") {
                let value = value.trim();
                when = Some(
                    value
                        .parse::<i64>()
                        .map_err(|_| RecordError::new(format!("invalid fish `when` {value:?}")))?,
                );
                in_paths = false;
            } else if line.trim_end() == "  paths:" {
                in_paths = true;
            } else if let Some(path) = line.strip_prefix("    - ").filter(|_| in_paths) {
                paths.push(unescape(path));
            } else if !line.trim().is_empty() {
                return Err(RecordError::new(format!(
                    "unknown line in a fish history record: {line:?}"
                )));
            }
        }
        let started_at = when
            .map(Timestamp::from_unix_secs)
            .transpose()
            .map_err(|_| RecordError::new("fish start time out of range"))?;
        let (n, seen) = match when {
            Some(when) => seen.next(when, &command),
            None => (0, seen.clone()),
        };
        Ok((
            vec![CommandImported {
                command,
                hostname: self.host.clone(),
                started_at,
                header_elapsed: None,
                paths,
                n,
            }],
            seen,
        ))
    }
}

/// A record is a `- cmd:` line and the indented lines after it; any other
/// line is a record alone, which the parser refuses. Only complete lines are
/// framed.
pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut open = false;
    let mut start = 0;
    while let Some(newline) = bytes[start..].iter().position(|b| *b == b'\n') {
        let end = start + newline + 1;
        let line = &bytes[start..end];
        if line.starts_with(b"- cmd:") {
            out.push((start, end));
            open = true;
        } else if let Some(last) = out.last_mut().filter(|_| open && line.starts_with(b" ")) {
            last.1 = end;
        } else {
            out.push((start, end));
            open = false;
        }
        start = end;
    }
    (out, start)
}

pub fn source(host: String) -> DeclaredSource<FishParser> {
    DeclaredSource {
        spec: SPEC,
        parser: FishParser { host },
    }
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: SPEC.framer,
    }
}

/// `--read fish=PATH`: a fish history file.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "fish",
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
