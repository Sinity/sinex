//! bash history files (`HISTFILE`), on `AppendFile`.
//!
//! bash writes one command per line. With `HISTTIMEFORMAT` set it writes a
//! timestamp line before each command, `#` and the start in epoch seconds;
//! with `lithist` a multi-line command keeps its newlines, so a timed record
//! is its timestamp line and every line up to the next timestamp line. bash
//! reads any line of `#` and a digit as a timestamp, and so does this parser.
//! Timed records are numbered like zsh's ([`Seen`]); untimed ones keep their
//! material locations and an unknown start.
use crate::sources::shell_history::Seen;
use sinex_model::{Class, Timestamp, event_types::CommandImported};
use sinex_source_core::drivers::append_file::AppendFile;
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.bash-history",
    driver: Driver::AppendFile,
    excisable: false,
    purpose: "command fusion and shell recall",
    class: Class::Open,
    parser: "bash-history",
    parser_version: 1,
    event_types: &["command.imported"],
    framer: sinex_source_core::Framer::incremental(frame),
};

pub struct BashParser {
    /// The host whose history file this is; bash does not record it.
    pub host: String,
}

/// The start a timestamp line gives, when `line` is one.
fn timestamp(line: &[u8]) -> Option<&[u8]> {
    let digits = line.strip_prefix(b"#")?;
    digits
        .first()
        .is_some_and(u8::is_ascii_digit)
        .then_some(digits)
}

impl Parser for BashParser {
    type Record<'a> = &'a [u8];
    type Carry = Seen;
    type Payload = CommandImported;

    fn parse(
        &self,
        record: &&[u8],
        seen: &Seen,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<CommandImported>, Seen), RecordError> {
        let text = std::str::from_utf8(record)
            .map_err(|_| RecordError::new("bash history record is not UTF-8"))?;
        let text = text
            .strip_suffix('\n')
            .ok_or_else(|| RecordError::new("unfinished bash history record"))?;
        let (start, command) = match timestamp(text.as_bytes()) {
            Some(_) => {
                let (line, command) = text.split_once('\n').unwrap_or((text, ""));
                let digits = line[1..].trim_end();
                let start = digits
                    .parse::<i64>()
                    .map_err(|_| RecordError::new(format!("invalid bash timestamp {line:?}")))?;
                (Some(start), command)
            }
            None => (None, text),
        };
        if command.is_empty() {
            return Ok((vec![], seen.clone()));
        }
        let started_at = start
            .map(Timestamp::from_unix_secs)
            .transpose()
            .map_err(|_| RecordError::new("bash start time out of range"))?;
        let (n, seen) = match start {
            Some(start) => seen.next(start, command),
            None => (0, seen.clone()),
        };
        Ok((
            vec![CommandImported {
                command: command.to_owned(),
                hostname: self.host.clone(),
                started_at,
                header_elapsed: None,
                paths: vec![],
                n,
            }],
            seen,
        ))
    }
}

/// A timestamp line and the lines after it up to the next timestamp line are
/// one record; a line before any timestamp line is a record alone. Only
/// complete lines are framed, and a final timestamp line waits for its
/// command: both are left for the next poll.
pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut timed = false;
    let mut start = 0;
    let mut waiting = false;
    while let Some(newline) = bytes[start..].iter().position(|b| *b == b'\n') {
        let end = start + newline + 1;
        let line = &bytes[start..end - 1];
        if timestamp(line).is_some() {
            out.push((start, end));
            timed = true;
            waiting = true;
        } else if let Some(last) = out.last_mut().filter(|_| timed) {
            last.1 = end;
            waiting = false;
        } else {
            out.push((start, end));
        }
        start = end;
    }
    if waiting && let Some((last_start, _)) = out.pop() {
        return (out, last_start);
    }
    (out, start)
}

pub fn source(host: String) -> DeclaredSource<BashParser> {
    DeclaredSource {
        spec: SPEC,
        parser: BashParser { host },
    }
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: SPEC.framer,
    }
}

/// `--read bash=PATH`: a bash history file.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "bash",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "environment",
                path: "BASH_HISTFILE",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: Some(|input, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, input, settings))
        }),
        source: |b| Ok(Box::new(source(b.settings.host.clone()))),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::appended(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
