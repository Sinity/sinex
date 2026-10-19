//! The systemd journal, on `LiveStream`. Capture runs `journalctl -o json
//! --all` (following, or once to the end) and spools every entry as it
//! arrives; journald rotates and vacuums its files, so the spool and then the
//! content store are what keep an entry.
//!
//! A capture restart resumes after the last journal cursor already in the
//! spool (`--after-cursor`), so the spool holds each entry once even across
//! restarts; an overlap that still happens (a cursor from a vacuumed journal)
//! is harmless, because the cursor is the entry's equivalence key and a second
//! reading is a repeat.
//!
//! Every entry is kept, including the daemon's own log lines and PostgreSQL's
//! connection noise: those are journal entries like any other, and filtering
//! them belongs in views, not in capture.
use crate::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::kinds::machine::JournalEntry;
use sinex_model::{Class, InputRef, Timestamp};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

pub const SPEC: SourceSpec = SourceSpec {
    name: "machine.journald",
    driver: Driver::LiveStream,
    consumer: "machine timeline, service health and recall",
    // Journal messages carry command lines, paths and sometimes secrets.
    class: Class::Guarded,
    parser: "journald-json",
    parser_version: 1,
    event_types: &["journal.entry"],
    framer: crate::drivers::append_file::lines,
};

pub struct JournaldParser;

/// A journal field as text. `journalctl -o json` writes a field that is not
/// printable UTF-8 as an array of byte values, and a field that occurs more
/// than once as an array of its values; the first value is used.
fn text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Array(items) if items.iter().all(Value::is_u64) => {
            let bytes: Option<Vec<u8>> = items
                .iter()
                .map(|b| b.as_u64().and_then(|b| u8::try_from(b).ok()))
                .collect();
            bytes.and_then(|b| String::from_utf8(b).ok())
        }
        Value::Array(items) => items.first().and_then(text),
        _ => None,
    }
}

impl Parser for JournaldParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = JournalEntry;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<JournalEntry>, ()), RecordError> {
        let (_, line) = live_stream::received(record).map_err(RecordError::new)?;
        if line.iter().all(u8::is_ascii_whitespace) {
            return Ok((vec![], ()));
        }
        let mut fields: Map<String, Value> = serde_json::from_slice(&line)
            .map_err(|e| RecordError::new(format!("journal entry is not a JSON object: {e}")))?;
        let cursor = match fields.remove("__CURSOR") {
            Some(Value::String(c)) if !c.is_empty() => c,
            _ => return Err(RecordError::new("journal entry has no __CURSOR")),
        };
        let mut take = |name: &str| -> Option<String> {
            let value = fields.get(name)?;
            let decoded = text(value)?;
            fields.remove(name);
            Some(decoded)
        };
        let logged_at = take("__REALTIME_TIMESTAMP")
            .and_then(|us| us.parse::<i128>().ok())
            .and_then(|us| Timestamp::from_unix_ns(us * 1_000).ok());
        let entry = JournalEntry {
            cursor,
            logged_at,
            boot_id: take("_BOOT_ID"),
            hostname: take("_HOSTNAME"),
            message: take("MESSAGE"),
            priority: take("PRIORITY").and_then(|p| p.parse().ok()),
            unit: take("_SYSTEMD_UNIT"),
            user_unit: take("_SYSTEMD_USER_UNIT"),
            identifier: take("SYSLOG_IDENTIFIER"),
            pid: take("_PID").and_then(|p| p.parse().ok()),
            uid: take("_UID").and_then(|u| u.parse().ok()),
            command: take("_COMM"),
            transport: take("_TRANSPORT"),
            fields,
        };
        Ok((vec![entry], ()))
    }
}

pub fn source() -> Declared<JournaldParser> {
    Declared {
        spec: SPEC,
        parser: JournaldParser,
    }
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    LiveStream {
        spool,
        input: InputRef {
            kind: "journal".into(),
            locator: "journald:system".into(),
            host,
        },
    }
}

/// The cursor of the last complete entry in the spool, to resume after.
pub fn last_spooled_cursor(spool: &Path) -> std::io::Result<Option<String>> {
    if !spool.exists() {
        return Ok(None);
    }
    for index in live_stream::segments(spool)?.into_iter().rev() {
        let path = spool.join(format!("{index:012}.ndjson"));
        let mut file = std::fs::File::open(&path)?;
        let len = file.metadata()?.len();
        // Read backwards in growing windows until a complete line is found.
        let mut window = 64 * 1024u64;
        loop {
            let from = len.saturating_sub(window);
            file.seek(SeekFrom::Start(from))?;
            let mut buf = Vec::new();
            (&mut file).take(len - from).read_to_end(&mut buf)?;
            let complete = match buf.iter().rposition(|b| *b == b'\n') {
                Some(end) => &buf[..end],
                None if from == 0 => break,
                None => {
                    window *= 4;
                    continue;
                }
            };
            let start = complete
                .iter()
                .rposition(|b| *b == b'\n')
                .map_or(0, |p| p + 1);
            if start == 0 && from > 0 {
                window *= 4;
                continue;
            }
            let record: Map<String, Value> = match serde_json::from_slice(&complete[start..]) {
                Ok(r) => r,
                Err(_) => break,
            };
            if let Ok((_, line)) = live_stream::received(&record)
                && let Ok(entry) = serde_json::from_slice::<Map<String, Value>>(&line)
                && let Some(Value::String(cursor)) = entry.get("__CURSOR")
            {
                return Ok(Some(cursor.clone()));
            }
            break;
        }
    }
    Ok(None)
}

/// How far back a first capture (with no spooled cursor) starts.
pub enum Start<'a> {
    /// The whole journal.
    Beginning,
    /// A `journalctl --since` expression, such as `-1d`.
    Since(&'a str),
}

/// Run `journalctl` into the spool. With `follow`, it keeps running until the
/// child exits; without, it stops at the end of the journal. Returns the number
/// of entries captured.
pub fn capture(spool: &Path, follow: bool, start: Start<'_>) -> std::io::Result<u64> {
    let mut command = Command::new("journalctl");
    command.args(["--output=json", "--all", "--no-pager", "--quiet"]);
    match last_spooled_cursor(spool)? {
        Some(cursor) => {
            command.arg(format!("--after-cursor={cursor}"));
        }
        None => match start {
            Start::Beginning => {}
            Start::Since(since) => {
                command.arg(format!("--since={since}"));
            }
        },
    }
    if follow {
        command.args(["--follow", "--lines=all"]);
    }
    let mut child = command.stdout(Stdio::piped()).spawn()?;
    let stdout = child.stdout.take().expect("piped stdout");
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    let count = live_stream::capture_lines(
        BufReader::with_capacity(1 << 20, stdout),
        &mut writer,
        Duration::from_secs(1),
    )?;
    let status = child.wait()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "journalctl exited with {status}"
        )));
    }
    Ok(count)
}

/// `--source journald=SPOOL`: the journal, captured into SPOOL. Options:
/// `since` (a `journalctl --since` expression; the whole journal otherwise)
/// and `follow`.
pub const KIND: crate::binding::Kind = crate::binding::Kind {
    name: "journald",
    specs: || vec![&SPEC],
    plan: Some(|spool, settings| Ok(crate::binding::single(&SPEC, spool, settings))),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::plain(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: Some(|b, _stop| {
        let start = match b.settings.option("since") {
            Some(s) => Start::Since(s),
            None => Start::Beginning,
        };
        let follow = b
            .settings
            .options
            .get("follow")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        capture(&b.input, follow, start).map_err(|e| e.to_string())
    }),
};

#[cfg(test)]
mod tests;
