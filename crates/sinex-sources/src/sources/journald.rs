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
use serde_json::{Map, Value};
use sinex_model::event_types::machine::JournalEntry;
use sinex_model::{Class, InputRef, Timestamp};
use sinex_source_core::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

pub const SPEC: SourceSpec = SourceSpec {
    name: "machine.journald",
    driver: Driver::LiveStream,
    excisable: false,
    purpose: "machine timeline, service health and recall",
    // Journal messages carry command lines, paths and sometimes secrets.
    class: Class::Guarded,
    parser: "journald-json",
    parser_version: 1,
    event_types: &["journal.entry"],
    framer: sinex_source_core::Framer::incremental(sinex_source_core::drivers::append_file::lines),
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
        _context: &sinex_source_core::RecordContext<'_>,
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

pub fn source() -> DeclaredSource<JournaldParser> {
    DeclaredSource {
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

/// The last cursor or a private interval's later boundary.
fn resume_argument(spool: &Path) -> std::io::Result<Option<String>> {
    let pending = spool.join(".private-gap.json");
    match std::fs::read(&pending) {
        Ok(bytes) => {
            let _: sinex_source_core::private_mode::Gap =
                serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
            return Ok(Some("--since=now".into()));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    if !spool.exists() {
        return Ok(None);
    }
    for index in live_stream::segments(spool)?.into_iter().rev() {
        let mut file = std::fs::File::open(spool.join(format!("{index:012}.ndjson")))?;
        let len = file.metadata()?.len();
        let mut window = 64 * 1024u64;
        loop {
            let from = len.saturating_sub(window);
            file.seek(SeekFrom::Start(from))?;
            let mut bytes = Vec::new();
            (&mut file).take(len - from).read_to_end(&mut bytes)?;
            if let Some(end) = bytes.iter().rposition(|b| *b == b'\n') {
                let begin = if from == 0 {
                    0
                } else {
                    bytes
                        .iter()
                        .position(|b| *b == b'\n')
                        .map_or(end, |i| i + 1)
                        .min(end)
                };
                for raw in bytes[begin..end].rsplit(|b| *b == b'\n') {
                    let Ok(record) = serde_json::from_slice::<Map<String, Value>>(raw) else {
                        continue;
                    };
                    if let Some(gap) = record.get("capture_gap") {
                        let gap: sinex_source_core::private_mode::Gap =
                            serde_json::from_value(gap.clone()).map_err(std::io::Error::other)?;
                        return Ok(Some(match gap.end {
                            None => "--since=now".into(),
                            Some(end) => {
                                let micros = end.unix_ns().div_euclid(1000) + 1;
                                format!(
                                    "--since=@{}.{:06}",
                                    micros.div_euclid(1_000_000),
                                    micros.rem_euclid(1_000_000)
                                )
                            }
                        }));
                    }
                    if let Ok((_, line)) = live_stream::received(&record)
                        && let Ok(entry) = serde_json::from_slice::<Map<String, Value>>(&line)
                        && let Some(Value::String(cursor)) = entry.get("__CURSOR")
                    {
                        return Ok(Some(format!("--after-cursor={cursor}")));
                    }
                }
            }
            if from == 0 {
                break;
            }
            window = window.saturating_mul(4);
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
fn capture_until(
    spool: &Path,
    follow: bool,
    start: Start<'_>,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    writer.capture(stop, |writer, stop| {
        let mut command = Command::new("journalctl");
        command.args(["--output=json", "--all", "--no-pager", "--quiet"]);
        match resume_argument(spool)? {
            Some(argument) => {
                command.arg(argument);
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
        command.stdout(Stdio::piped());
        let Some(mut child) = stop.spawn(&mut command)? else {
            return Ok(0);
        };
        let stdout = child.stdout().expect("piped stdout");
        let count = live_stream::capture_lines(
            BufReader::with_capacity(1 << 20, stdout),
            writer,
            Duration::from_secs(1),
        )?;
        let status = child.wait()?;
        if !status.success() && !stop.stopped() {
            return Err(std::io::Error::other(format!(
                "journalctl exited with {status}"
            )));
        }
        Ok(count)
    })
}

/// `--read journald=SPOOL`: the journal, captured into SPOOL. Options:
/// `since` (a `journalctl --since` expression; the whole journal otherwise)
/// and `follow`.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "journald",
        specs: || vec![&SPEC],
        plan: Some(|spool, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, spool, settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::polled(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: Some(|b, stop| {
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
            capture_until(&b.input, follow, start, stop).map_err(|e| e.to_string())
        }),
    };

#[cfg(test)]
mod tests;
