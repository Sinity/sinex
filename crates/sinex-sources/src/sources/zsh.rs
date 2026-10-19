//! zsh extended history (`: <start>:<elapsed>;<command>`), on `AppendFile`.
//!
//! The file has no record IDs, so a record is identified by host, start
//! second, command and its occurrence number among identical (start, command)
//! pairs in file order. The parser carries the counts; a re-read from the
//! start (a lost position, a replay) begins with no carry and numbers the same
//! records the same way.
//! zsh metafies bytes 0x83..0x9f (0x83 then the byte xor 0x20) and continues a
//! multi-line command with a backslash before the newline. With SHARE_HISTORY
//! the elapsed field is written as 0 before the command ends, so it is kept as
//! written and never read as a duration.
use crate::drivers::append_file::AppendFile;
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use sinex_model::{Class, Timestamp, event_types::CommandImported};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.zsh-history",
    driver: Driver::AppendFile,
    consumer: "command fusion and shell recall",
    class: Class::Open,
    parser: "zsh-extended-history",
    parser_version: 2,
    event_types: &["command.imported"],
    framer: frame,
};

pub struct ZshParser {
    /// The host whose history file this is; zsh does not record it.
    pub host: String,
}

/// Occurrences seen per (start second, command). Identical pairs share a start
/// second, so only starts within a day of the latest are kept: a pair needs a
/// second record with that exact start, and history lines are written within
/// minutes of their start. The window decides numbering, never what is kept.
/// The map is persistent: the drain hands each record the carry and takes a
/// new one back, so a clone must not copy the map.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Seen {
    latest: i64,
    /// Stored as a list: JSON object keys are strings only.
    #[serde(with = "pairs")]
    counts: imbl::OrdMap<(i64, String), u32>,
}

mod pairs {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        m: &imbl::OrdMap<(i64, String), u32>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        m.iter()
            .map(|((start, command), n)| (start, command, n))
            .collect::<Vec<_>>()
            .serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<imbl::OrdMap<(i64, String), u32>, D::Error> {
        let list = Vec::<(i64, String, u32)>::deserialize(d)?;
        Ok(list.into_iter().map(|(s, c, n)| ((s, c), n)).collect())
    }
}

const SEEN_WINDOW_SECS: i64 = 86_400;

impl Seen {
    fn next(&self, start: i64, command: &str) -> (u32, Self) {
        let mut seen = self.clone();
        let key = (start, command.to_owned());
        let n = seen.counts.get(&key).copied().unwrap_or(0);
        seen.counts.insert(key, n + 1);
        if start > seen.latest {
            seen.latest = start;
            // Keys sort by start, so the expired ones are at the front.
            let floor = start - SEEN_WINDOW_SECS;
            while let Some(((oldest, _), _)) = seen.counts.get_min() {
                if *oldest >= floor {
                    break;
                }
                let first = seen
                    .counts
                    .get_min()
                    .map(|(k, _)| k.clone())
                    .expect("checked");
                seen.counts.remove(&first);
            }
        }
        (n, seen)
    }
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
        let text = text
            .strip_prefix(": ")
            .ok_or_else(|| RecordError::new("not an extended history record"))?;
        let (start, rest) = text
            .split_once(':')
            .ok_or_else(|| RecordError::new("invalid extended zsh header"))?;
        let (elapsed, command) = rest
            .split_once(';')
            .ok_or_else(|| RecordError::new("invalid extended zsh header"))?;
        let start: i64 = start
            .parse()
            .map_err(|_| RecordError::new("invalid zsh start time"))?;
        let elapsed: i64 = elapsed
            .parse()
            .map_err(|_| RecordError::new("invalid zsh elapsed field"))?;
        let started_at = Timestamp::from_unix_secs(start)
            .map_err(|_| RecordError::new("zsh start time out of range"))?;
        // zsh appends one space when the command's last non-space byte is a
        // backslash. Remove only that space, then decode escaped newlines.
        let command = if command.trim_end_matches(' ').ends_with('\\') {
            command.strip_suffix(' ').unwrap_or(command)
        } else {
            command
        };
        let command = command.replace("\\\n", "\n");
        let (n, seen) = seen.next(start, &command);
        Ok((
            vec![CommandImported {
                command,
                hostname: self.host.clone(),
                started_at,
                header_elapsed: elapsed,
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

pub fn source(host: String) -> Declared<ZshParser> {
    Declared {
        spec: SPEC,
        parser: ZshParser { host },
    }
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: frame,
    }
}

/// `--source zsh=PATH`: a zsh history file.
pub const KIND: crate::binding::Kind = crate::binding::Kind {
    name: "zsh",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| Ok(crate::binding::single(&SPEC, input, settings))),
    source: |b| Ok(Box::new(source(b.settings.host.clone()))),
    reader: Some(|b| {
        Ok(crate::binding::appended(
            driver(b.input.clone(), b.settings.host.clone()),
            frame,
        ))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
