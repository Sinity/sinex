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
    ) -> Result<(Vec<JournalEntry>, ()), RecordError> {
        let (_, line) = live_stream::received(record).map_err(RecordError::new)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(entry: Value) -> Map<String, Value> {
        serde_json::from_slice(&live_stream::frame(
            Timestamp::now(),
            entry.to_string().as_bytes(),
        ))
        .unwrap()
    }

    #[test]
    fn known_fields_are_typed_and_the_rest_kept() {
        let (out, ()) = JournaldParser
            .parse(
                &rec(json!({
                    "__CURSOR": "s=1;i=2",
                    "__REALTIME_TIMESTAMP": "1700000000123456",
                    "MESSAGE": "Started foo.",
                    "PRIORITY": "6",
                    "_SYSTEMD_UNIT": "init.scope",
                    "_PID": "1",
                    "_UID": "0",
                    "_COMM": "systemd",
                    "_TRANSPORT": "journal",
                    "CODE_FILE": "src/core/job.c",
                })),
                &(),
            )
            .unwrap();
        let e = &out[0];
        assert_eq!(e.cursor, "s=1;i=2");
        assert_eq!(e.logged_at.unwrap().unix_ns(), 1_700_000_000_123_456_000);
        assert_eq!(e.message.as_deref(), Some("Started foo."));
        assert_eq!(e.priority, Some(6));
        assert_eq!((e.pid, e.uid), (Some(1), Some(0)));
        assert_eq!(e.fields.get("CODE_FILE"), Some(&json!("src/core/job.c")));
        assert!(!e.fields.contains_key("MESSAGE"));
    }

    #[test]
    fn byte_array_messages_decode_when_utf8_and_stay_raw_otherwise() {
        // "\x1b[1mhi" (colour codes make journalctl print bytes).
        let (out, ()) = JournaldParser
            .parse(
                &rec(json!({"__CURSOR": "c", "MESSAGE": [27, 91, 49, 109, 104, 105]})),
                &(),
            )
            .unwrap();
        assert_eq!(out[0].message.as_deref(), Some("\u{1b}[1mhi"));
        let (out, ()) = JournaldParser
            .parse(&rec(json!({"__CURSOR": "c", "MESSAGE": [255, 0]})), &())
            .unwrap();
        assert_eq!(out[0].message, None);
        assert_eq!(out[0].fields.get("MESSAGE"), Some(&json!([255, 0])));
    }

    #[test]
    fn a_missing_or_malformed_time_stays_unknown() {
        let (out, ()) = JournaldParser
            .parse(
                &rec(json!({"__CURSOR": "c", "__REALTIME_TIMESTAMP": "soon"})),
                &(),
            )
            .unwrap();
        assert_eq!(out[0].logged_at, None);
        assert_eq!(
            sinex_model::EventPayload::ts_orig(&out[0]).quality,
            sinex_model::TimeQuality::Unknown
        );
    }

    #[test]
    fn an_entry_without_a_cursor_is_a_failure_not_a_guess() {
        assert!(
            JournaldParser
                .parse(&rec(json!({"MESSAGE": "x"})), &())
                .is_err()
        );
    }

    #[test]
    fn capture_resumes_after_the_last_spooled_cursor() {
        let spool = std::env::temp_dir().join(format!("sinex-jd-{}", uuid::Uuid::now_v7()));
        assert_eq!(last_spooled_cursor(&std::env::temp_dir()).unwrap(), None);
        let mut w = SpoolWriter::open(&spool, Rotation::default()).unwrap();
        for i in 0..3 {
            let line = json!({"__CURSOR": format!("c{i}"), "MESSAGE": "x".repeat(70_000)});
            w.append(Timestamp::now(), line.to_string().as_bytes())
                .unwrap();
        }
        w.sync().unwrap();
        assert_eq!(last_spooled_cursor(&spool).unwrap().as_deref(), Some("c2"));
    }
}
