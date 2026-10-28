//! The systemd journal, on `LiveStream`. Capture runs `journalctl -o json
//! --all` (following, or once to the end) and spools every entry as it
//! arrives; journald rotates and vacuums its files, so the spool and then the
//! content store are what keep an entry. Archived journal files (a copied
//! `/var/log/journal`, a recovered machine's) are captured the same way from
//! their directory, each archive into its own spool.
//!
//! A capture restart resumes after the last journal cursor already in the
//! spool (`--after-cursor`), so the spool holds each entry once even across
//! restarts. When the reader has already removed every segment holding an
//! entry, the cursor comes from the spool's resume file, which capture
//! rewrites after each sync with the last entry then made durable. An overlap
//! that still happens (a cursor from a vacuumed journal) is harmless, because
//! the cursor is the entry's equivalence key and a second reading is a
//! repeat.
//!
//! Every entry is kept, including Sinex's own log lines and PostgreSQL's
//! connection noise: those are journal entries like any other. Entries Sinex
//! itself wrote are marked (`from_sinex`), so figures about the machine leave
//! Sinex's own activity out.
//!
//! systemd and logind describe state changes in entries with a known
//! `MESSAGE_ID`; such an entry is also read as the change it records (a unit
//! started or stopped, a boot, sleep, a shutdown), keyed by the same cursor.
//! Only entries the expected program wrote count: `_COMM` is set by journald,
//! so another program cannot fake one.
use serde_json::{Map, Value};
use sinex_model::event_types::machine::JournalEntry;
use sinex_model::event_types::system::{
    BootStarted, ShutdownStarted, SleepEnded, SleepStarted, UnitReloaded, UnitStarted, UnitStopped,
};
use sinex_model::{Class, InputRef, Timestamp};
use sinex_source_core::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const SPEC: SourceSpec = SourceSpec {
    name: "machine.journald",
    driver: Driver::LiveStream,
    excisable: false,
    purpose: "machine timeline, service health, unit and boot intervals, and recall",
    // Journal messages carry command lines, paths and sometimes secrets.
    class: Class::Guarded,
    parser: "journald-json",
    parser_version: 2,
    event_types: &[
        "journal.entry",
        "unit.started",
        "unit.stopped",
        "unit.reloaded",
        "boot.started",
        "sleep.started",
        "sleep.ended",
        "shutdown.started",
    ],
    framer: sinex_source_core::Framer::incremental(sinex_source_core::drivers::append_file::lines),
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum JournalReading {
        Entry(JournalEntry),
        UnitStarted(UnitStarted),
        UnitStopped(UnitStopped),
        UnitReloaded(UnitReloaded),
        BootStarted(BootStarted),
        SleepStarted(SleepStarted),
        SleepEnded(SleepEnded),
        ShutdownStarted(ShutdownStarted),
    }
}

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

/// Units Sinex runs, as the NixOS module names them: the daemon, its
/// `sinex-*` services (captures, NATS, database setup, dumps, backfills) and
/// the session service.
fn sinex_unit(unit: &str) -> bool {
    unit == "sinexd.service" || (unit.starts_with("sinex-") && unit.ends_with(".service"))
}

/// systemd's catalog IDs for the entries read as state changes.
mod message {
    pub(super) const START_DONE: &str = "39f53479d3a045ac8e11786248231fbf";
    pub(super) const START_FAILED: &str = "be02cf6855d2428ba40df7e9d022f03d";
    pub(super) const STOP_DONE: &str = "9d1aaa27d60140bd96365438aad20286";
    pub(super) const RELOAD_DONE: &str = "7b05ebc668384222baa8881179cfda54";
    pub(super) const UNIT_SUCCEEDED: &str = "7ad2d189f7e94e70a38c781354912448";
    pub(super) const UNIT_FAILED: &str = "d9b373ed55a64feb8242e02dbe79a49c";
    pub(super) const STARTUP_FINISHED: &str = "b07a249cd024414a82dd00cd181378ff";
    pub(super) const SLEEP_START: &str = "6bbd95ee977941e497c48be27c254128";
    pub(super) const SLEEP_STOP: &str = "8811e6df2a8e40f58a94cea26f8ebf14";
    pub(super) const SHUTDOWN: &str = "98268866d1d54a499c4e98921d93bc40";
}

/// The state change an entry records, if it is one systemd or logind wrote.
fn state_change(entry: &JournalEntry) -> Option<JournalReading> {
    let field = |name: &str| entry.fields.get(name).and_then(text);
    let id = field("MESSAGE_ID")?;
    let program = entry.command.as_deref();
    let cursor = entry.cursor.clone();
    let logged_at = entry.logged_at;
    let unit_change = || {
        if program != Some("systemd") {
            return None;
        }
        let (unit, manager, invocation_id) = match (field("UNIT"), field("USER_UNIT")) {
            (Some(unit), _) => (unit, "system", field("INVOCATION_ID")),
            (None, Some(unit)) => (unit, "user", field("USER_INVOCATION_ID")),
            (None, None) => return None,
        };
        Some((unit, manager.to_owned(), invocation_id))
    };
    let stopped = |outcome: &str, result: Option<String>| {
        let (unit, manager, invocation_id) = unit_change()?;
        Some(JournalReading::UnitStopped(UnitStopped {
            cursor: cursor.clone(),
            logged_at,
            unit,
            manager,
            uid: entry.uid,
            invocation_id,
            outcome: outcome.to_owned(),
            result,
        }))
    };
    match id.as_str() {
        message::START_DONE => {
            let (unit, manager, invocation_id) = unit_change()?;
            Some(JournalReading::UnitStarted(UnitStarted {
                cursor,
                logged_at,
                unit,
                manager,
                uid: entry.uid,
                invocation_id,
                job: field("JOB_TYPE"),
            }))
        }
        message::START_FAILED => stopped("start_failed", field("JOB_RESULT")),
        message::STOP_DONE => stopped("stopped", field("JOB_RESULT")),
        message::UNIT_SUCCEEDED => stopped("succeeded", None),
        message::UNIT_FAILED => stopped("failed", field("UNIT_RESULT")),
        message::RELOAD_DONE => {
            let (unit, manager, invocation_id) = unit_change()?;
            Some(JournalReading::UnitReloaded(UnitReloaded {
                cursor,
                logged_at,
                unit,
                manager,
                uid: entry.uid,
                invocation_id,
                result: field("JOB_RESULT"),
            }))
        }
        // The user manager writes its own "startup finished" with another
        // ID; this one is the system manager's (PID 1).
        message::STARTUP_FINISHED if program == Some("systemd") && entry.pid == Some(1) => {
            let micros = |name: &str| field(name).and_then(|v| v.parse::<i64>().ok());
            let (kernel_us, initrd_us, userspace_us) = (
                micros("KERNEL_USEC"),
                micros("INITRD_USEC"),
                micros("USERSPACE_USEC"),
            );
            // Without an initrd systemd leaves INITRD_USEC out; a missing
            // kernel or userspace phase leaves the start unknown.
            let started_at = match (logged_at, kernel_us, userspace_us) {
                (Some(at), Some(kernel), Some(userspace)) => {
                    let total = kernel + initrd_us.unwrap_or(0) + userspace;
                    Timestamp::from_unix_ns(at.unix_ns() - i128::from(total) * 1_000).ok()
                }
                _ => None,
            };
            Some(JournalReading::BootStarted(BootStarted {
                boot_id: entry.boot_id.clone()?,
                started_at,
                finished_at: logged_at,
                kernel_us,
                initrd_us,
                userspace_us,
                cursor,
            }))
        }
        message::SLEEP_START if program == Some("systemd-sleep") => {
            Some(JournalReading::SleepStarted(SleepStarted {
                cursor,
                logged_at,
                state: field("SLEEP"),
            }))
        }
        message::SLEEP_STOP if program == Some("systemd-sleep") => {
            Some(JournalReading::SleepEnded(SleepEnded {
                cursor,
                logged_at,
                state: field("SLEEP"),
            }))
        }
        message::SHUTDOWN if program == Some("systemd-logind") => {
            Some(JournalReading::ShutdownStarted(ShutdownStarted {
                cursor,
                logged_at,
                kind: field("SHUTDOWN"),
            }))
        }
        _ => None,
    }
}

impl Parser for JournaldParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = JournalReading;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<JournalReading>, ()), RecordError> {
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
        let unit = take("_SYSTEMD_UNIT");
        let user_unit = take("_SYSTEMD_USER_UNIT");
        let from_sinex =
            unit.as_deref().is_some_and(sinex_unit) || user_unit.as_deref().is_some_and(sinex_unit);
        let entry = JournalEntry {
            cursor,
            logged_at,
            boot_id: take("_BOOT_ID"),
            hostname: take("_HOSTNAME"),
            message: take("MESSAGE"),
            priority: take("PRIORITY").and_then(|p| p.parse().ok()),
            unit,
            user_unit,
            identifier: take("SYSLOG_IDENTIFIER"),
            pid: take("_PID").and_then(|p| p.parse().ok()),
            uid: take("_UID").and_then(|u| u.parse().ok()),
            command: take("_COMM"),
            transport: take("_TRANSPORT"),
            from_sinex,
            fields,
        };
        let change = state_change(&entry);
        let mut out = vec![JournalReading::Entry(entry)];
        out.extend(change);
        Ok((out, ()))
    }
}

pub fn source() -> DeclaredSource<JournaldParser> {
    DeclaredSource {
        spec: SPEC,
        parser: JournaldParser,
    }
}

/// The spool reader of the live journal, or of the archive in `directory`.
pub fn driver(spool: PathBuf, host: String, directory: Option<&str>) -> LiveStream {
    LiveStream {
        spool,
        input: InputRef {
            kind: "journal".into(),
            locator: match directory {
                Some(directory) => format!("journald:{directory}"),
                None => "journald:system".into(),
            },
            host,
        },
    }
}

/// The spool's resume file: the cursor of the last entry made durable in the
/// spool, rewritten after each sync.
const RESUME_FILE: &str = "resume-cursor";

/// The cursor to resume after: the last complete entry in the spool, or, when
/// no segment holds one any more, the resume file's.
fn resume_cursor(spool: &Path) -> std::io::Result<Option<String>> {
    if !spool.exists() {
        return Ok(None);
    }
    if let Some(cursor) = last_spooled_cursor(spool)? {
        return Ok(Some(cursor));
    }
    match std::fs::read_to_string(spool.join(RESUME_FILE)) {
        Ok(text) => Ok(Some(text.trim_end().to_owned()).filter(|c| !c.is_empty())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Record `cursor` as the last durable entry: a whole new file renamed over
/// the old, so a crash leaves one or the other.
fn save_resume_cursor(spool: &Path, cursor: &str) -> std::io::Result<()> {
    let temporary = spool.join(format!("{RESUME_FILE}.tmp"));
    std::fs::write(&temporary, cursor.as_bytes())?;
    std::fs::rename(temporary, spool.join(RESUME_FILE))
}

/// The cursor of the last complete entry in the spool's segments.
fn last_spooled_cursor(spool: &Path) -> std::io::Result<Option<String>> {
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
                && let Some(cursor) = entry_cursor(&line)
            {
                return Ok(Some(cursor));
            }
            break;
        }
    }
    Ok(None)
}

/// The `__CURSOR` of one `journalctl -o json` line.
fn entry_cursor(line: &[u8]) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Cursor {
        #[serde(rename = "__CURSOR")]
        cursor: String,
    }
    serde_json::from_slice::<Cursor>(line)
        .ok()
        .map(|c| c.cursor)
        .filter(|c| !c.is_empty())
}

/// What one capture reads.
#[derive(Clone, Copy, Debug, Default)]
pub struct CaptureSettings<'a> {
    /// Archived journal files in this directory, instead of the live journal.
    pub directory: Option<&'a str>,
    /// Where a first capture (with no cursor to resume after) starts: a
    /// `journalctl --since` expression such as `-1d`; the whole journal when
    /// `None`.
    pub since: Option<&'a str>,
    /// Keep following new entries; otherwise stop at the end of the journal.
    pub follow: bool,
}

impl<'a> CaptureSettings<'a> {
    fn of(settings: &'a sinex_source_core::binding::Settings) -> Self {
        Self {
            directory: settings.option("directory"),
            since: settings.option("since"),
            follow: settings
                .options
                .get("follow")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }
}

/// Run `journalctl` into the spool until it ends or capture stops. Returns
/// the number of entries captured.
pub fn capture(
    spool: &Path,
    settings: CaptureSettings<'_>,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    let mut command = Command::new("journalctl");
    command.args(["--output=json", "--all", "--no-pager", "--quiet"]);
    if let Some(directory) = settings.directory {
        command.arg(format!("--directory={directory}"));
    }
    match resume_cursor(spool)? {
        Some(cursor) => {
            command.arg(format!("--after-cursor={cursor}"));
        }
        None => {
            if let Some(since) = settings.since {
                command.arg(format!("--since={since}"));
            }
        }
    }
    if settings.follow {
        command.args(["--follow", "--lines=all"]);
    }
    command.stdout(Stdio::piped());
    let Some(mut child) = stop.spawn(&mut command)? else {
        return Ok(0);
    };
    let stdout = child.stdout().expect("piped stdout");
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    let count = spool_entries(
        BufReader::with_capacity(1 << 20, stdout),
        &mut writer,
        spool,
        Duration::from_secs(1),
    )?;
    let status = child.wait()?;
    if !status.success() && !stop.stopped() {
        return Err(std::io::Error::other(format!(
            "journalctl exited with {status}"
        )));
    }
    Ok(count)
}

/// Copy entries into the spool, syncing at least every `sync_every` and then
/// recording the last synced entry's cursor in the resume file.
fn spool_entries(
    mut reader: impl BufRead,
    writer: &mut SpoolWriter,
    spool: &Path,
    sync_every: Duration,
) -> std::io::Result<u64> {
    let mut line = Vec::new();
    let mut count = 0;
    let mut last_sync = Instant::now();
    let mut last_cursor: Option<String> = None;
    let mut saved_cursor: Option<String> = None;
    let mut sync = |writer: &mut SpoolWriter, last: &Option<String>| -> std::io::Result<()> {
        writer.sync()?;
        if let Some(cursor) = last
            && saved_cursor.as_ref() != Some(cursor)
        {
            save_resume_cursor(spool, cursor)?;
            saved_cursor = Some(cursor.clone());
        }
        Ok(())
    };
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let body = line.strip_suffix(b"\n").unwrap_or(&line);
        writer.append(Timestamp::now(), body)?;
        count += 1;
        if let Some(cursor) = entry_cursor(body) {
            last_cursor = Some(cursor);
        }
        if last_sync.elapsed() >= sync_every {
            sync(writer, &last_cursor)?;
            last_sync = Instant::now();
        }
    }
    sync(writer, &last_cursor)?;
    Ok(count)
}

/// `--read journald=SPOOL`: the journal, captured into SPOOL. Options:
/// `directory` (archived journal files to read instead of the live journal),
/// `since` (where a first capture starts, a `journalctl --since` expression;
/// the whole journal otherwise) and `follow`.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "journald",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "spool",
                path: "journald",
            }],
            capabilities: &["journalctl"],
            credentials: &[],
            streams: &[sinex_source_core::discovery::CaptureStream {
                spool: "journald",
                options: &[sinex_source_core::discovery::CaptureOption {
                    name: "follow",
                    value: sinex_source_core::discovery::CaptureValue::Boolean(true),
                }],
                capabilities: &[],
            }],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: Some(|spool, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, spool, settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::polled(driver(
                b.input.clone(),
                b.settings.host.clone(),
                b.settings.option("directory"),
            )))
        }),
        capture: Some(|b, stop| {
            capture(&b.input, CaptureSettings::of(&b.settings), stop).map_err(|e| e.to_string())
        }),
    };

#[cfg(test)]
mod tests;
