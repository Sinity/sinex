//! zsh extended history (`: <start>:<elapsed>;<command>`), on `AppendFile`.
//!
//! The file has no record IDs, so each record location is its own identity.
//! zsh metafies bytes 0x83..0x9f (0x83 then the byte xor 0x20) and continues a
//! multi-line command with a backslash before the newline. With SHARE_HISTORY
//! the elapsed field is written as 0 before the command ends, so it is kept as
//! written and never read as a duration.
use crate::drivers::append_file::AppendFile;
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use sinex_model::{Class, Timestamp, kinds::CommandImported};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.zsh-history",
    driver: Driver::AppendFile,
    consumer: "command fusion and shell recall",
    class: Class::Open,
    parser: "zsh-extended-history",
    parser_version: 1,
    event_types: &["command.imported"],
    framer: frame,
};

pub struct ZshParser {
    /// The host whose history file this is; zsh does not record it.
    pub host: String,
}

impl Parser for ZshParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = CommandImported;

    fn parse(&self, record: &&[u8], _: &()) -> Result<(Vec<CommandImported>, ()), RecordError> {
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
        Ok((
            vec![CommandImported {
                command: command.replace("\\\n", "\n"),
                hostname: self.host.clone(),
                started_at,
                header_elapsed: elapsed,
            }],
            (),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DrainInput, Limits, drain};
    use proptest::prelude::*;
    use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

    fn parse(bytes: &[u8]) -> Result<String, RecordError> {
        ZshParser { host: "h".into() }
            .parse(&bytes, &())
            .map(|(p, ())| p[0].command.clone())
    }

    #[test]
    fn keeps_original_backslashes_spaces_and_tabs() {
        let records: &[(&[u8], &str)] = &[
            (b": 1:0;echo slash\\ \n", "echo slash\\"),
            (b": 1:0;echo slashspace\\  \n", "echo slashspace\\ "),
            (b": 1:0;echo slashtab\\\t\n", "echo slashtab\\\t"),
            (
                b": 1:0;echo slashnewline\\\\\nnext\n",
                "echo slashnewline\\\nnext",
            ),
            (b": 1:0;echo plainspace  \n", "echo plainspace  "),
        ];
        for (record, expected) in records {
            assert_eq!(frame(record), (vec![(0, record.len())], record.len()));
            assert_eq!(parse(record).unwrap(), *expected);
        }
    }

    #[test]
    fn metafied_bytes_and_continuations() {
        let bytes = b": 3600:0;echo \xc4\x83\x85\n: 3601:0;printf a\\\nb\n: 3602:0;partial";
        let (r, consumed) = frame(bytes);
        assert_eq!(r.len(), 2);
        assert_eq!(consumed, bytes.len() - b": 3602:0;partial".len());
        assert_eq!(parse(&bytes[r[0].0..r[0].1]).unwrap(), "echo ĥ");
        assert_eq!(parse(&bytes[r[1].0..r[1].1]).unwrap(), "printf a\nb");
        assert!(parse(b"plain\n").is_err());
        assert!(unmetafy(b"\x83").is_err());
    }

    fn commands(bytes: &[u8], base: usize) -> Vec<(usize, String)> {
        let (records, _) = frame(bytes);
        let out = drain(
            &ZshParser { host: "h".into() },
            DrainInput {
                source: SPEC.name,
                class: Class::Open,
                material: MaterialId(uuid::Uuid::nil()),
                bytes,
                records: &records,
                carry: serde_json::Value::Null,
                reading_order: &|a| ReadingOrder(a as i64),
                limits: Limits::default(),
            },
        )
        .unwrap();
        out.iter()
            .flat_map(|c| &c.outcomes)
            .map(|o| match o {
                RecordOutcome::Events { location, events } => (
                    base + location.unwrap().anchor as usize,
                    events[0].payload["command"].as_str().unwrap().to_owned(),
                ),
                RecordOutcome::Failure { location, reason } => {
                    (base + location.anchor as usize, format!("FAIL {reason}"))
                }
            })
            .collect()
    }

    proptest! {
        /// Source law: reading the file in pieces gives the same records, at the
        /// same file offsets, as reading it at once.
        #[test]
        fn pieces_equal_whole(cmds in prop::collection::vec("[a-z \\\\]{0,6}", 1..12), cut in 0usize..400) {
            let mut file = Vec::new();
            for (i, c) in cmds.iter().enumerate() {
                file.extend_from_slice(format!(": {}:0;{}\n", 100 + i, c.replace('\\', "\\\\")).as_bytes());
            }
            let whole = commands(&file, 0);
            let cut = cut.min(file.len());
            let (_, consumed) = frame(&file[..cut]);
            let mut pieces = commands(&file[..consumed], 0);
            pieces.extend(commands(&file[consumed..], consumed));
            prop_assert_eq!(whole, pieces);
        }

        /// Source law: an anchor reads back exactly its record.
        #[test]
        fn anchors_read_back_records(cmds in prop::collection::vec("[a-z]{1,5}", 1..8)) {
            let mut file = Vec::new();
            for (i, c) in cmds.iter().enumerate() {
                file.extend_from_slice(format!(": {}:0;{}\n", i, c).as_bytes());
            }
            for (anchor, command) in commands(&file, 0) {
                let (r, _) = frame(&file[anchor..]);
                prop_assert_eq!(parse(&file[anchor..anchor + r[0].1]).unwrap(), command);
            }
        }
    }
}
