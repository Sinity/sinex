//! asciinema terminal recordings (`<day>/<session>/session.cast`) and the
//! shell events Sinnix's capture logs beside them (`events.jsonl`), both on
//! `AppendFile`: a recording grows while its terminal is open.
//!
//! A cast's first line is its header: version 2 or 3, the start as epoch
//! seconds (`timestamp`), the size (`width`/`height` in v2, `term.cols`/`rows`
//! in v3), the command and environment. Each later line is `[time, code,
//! data]`: in v3 the time is the interval since the previous event, in v2 the
//! offset from the start. The cast stays whole as the material; its events
//! are framed into spans (`cast_spans`): consecutive event lines up to
//! `SPAN_BYTES`, ending early at a resize or the exit, and at a read
//! boundary. Each span is one `terminal.output` with its plain text and its
//! start and end; the span's record is its exact byte range, so the stream is
//! replayable from the material. The parser carries the start and the running
//! time across spans. The session ID is the capture's
//! `SINNIX_CAPTURE_SESSION_ID`, else the directory name.
//!
//! `events.jsonl` lines become `terminal.event`, the same type the live kitty
//! watcher writes: `type` is the event, `ts_ms` its time, `command` its command
//! line, `exit_code` its status, and the rest (with the recording's session)
//! is kept. `session.json` restates the header and the capture's own fields;
//! it is not read.
use crate::drivers::append_file::AppendFile;
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordContext, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::Timestamp;
use sinex_model::event_types::machine::{TerminalOutput, TerminalSession};
use sinex_model::event_types::session::TerminalEvent;
use std::path::PathBuf;

pub const CAST: SourceSpec = SourceSpec {
    name: "terminal.asciinema",
    driver: Driver::AppendFile,
    consumer: "what terminals showed and what was typed, replayable",
    class: sinex_model::Class::Open,
    parser: "asciinema-cast",
    parser_version: 2,
    event_types: &["terminal.session", "terminal.output"],
    framer: cast_spans,
};

pub const EVENTS: SourceSpec = SourceSpec {
    name: "terminal.capture-events",
    driver: Driver::AppendFile,
    consumer: "shell commands around a terminal recording",
    class: sinex_model::Class::Open,
    parser: "terminal-capture-events",
    parser_version: 1,
    event_types: &["terminal.event"],
    framer: complete_lines,
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum CastEvent {
        Session(TerminalSession),
        Output(TerminalOutput),
    }
}

/// The recording's session, start, format and the time of the last event.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Carry {
    pub session: String,
    pub start_ns: Option<i128>,
    pub version: u8,
    pub elapsed_ns: i128,
}

pub struct CastParser;

impl Parser for CastParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = CastEvent;

    fn parse(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<CastEvent>, Carry), RecordError> {
        let text = std::str::from_utf8(record)
            .map_err(|e| RecordError::new(format!("cast span is not UTF-8: {e}")))?;
        let first = text.trim_start();
        if first.is_empty() {
            return Ok((vec![], carry.clone()));
        }
        if first.starts_with('{') {
            return header(first.trim_end(), context);
        }
        span(text, carry)
    }
}

/// A cast's header line: the recording's `terminal.session`.
fn header(line: &str, context: &RecordContext<'_>) -> Result<(Vec<CastEvent>, Carry), RecordError> {
    let value: Value = serde_json::from_str(line)
        .map_err(|e| RecordError::new(format!("cast header is not JSON: {e}")))?;
    let Value::Object(header) = value else {
        return Err(RecordError::new("a cast header is not an object"));
    };
    let version = header.get("version").and_then(Value::as_u64).unwrap_or(0);
    if !matches!(version, 2 | 3) {
        return Err(RecordError::new(format!("cast version {version}")));
    }
    let session = header
        .get("env")
        .and_then(|e| e.get("SINNIX_CAPTURE_SESSION_ID"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| directory(context));
    let start_ns = header
        .get("timestamp")
        .and_then(Value::as_f64)
        .map(|s| (s * 1e9) as i128);
    let size = |v2: &str, v3: &str| {
        header
            .get(v2)
            .or_else(|| header.get("term").and_then(|t| t.get(v3)))
            .and_then(Value::as_i64)
    };
    let started_at = start_ns
        .map(Timestamp::from_unix_ns)
        .transpose()
        .map_err(|_| RecordError::new("cast start out of range"))?;
    Ok((
        vec![CastEvent::Session(TerminalSession {
            session: session.clone(),
            started_at,
            cols: size("width", "cols"),
            rows: size("height", "rows"),
            command: header
                .get("command")
                .and_then(Value::as_str)
                .map(str::to_owned),
            header: Value::Object(header),
        })],
        Carry {
            session,
            start_ns,
            version: version as u8,
            elapsed_ns: 0,
        },
    ))
}

/// A span of cast event lines: one `terminal.output`.
fn span(text: &str, carry: &Carry) -> Result<(Vec<CastEvent>, Carry), RecordError> {
    let start = carry
        .start_ns
        .ok_or_else(|| RecordError::new("a cast event before its header"))?;
    let mut elapsed_ns = carry.elapsed_ns;
    let mut first_ns = None;
    let mut events = 0u32;
    let mut output = String::new();
    let mut input = String::new();
    let mut resize = None;
    let mut exit = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line)
            .map_err(|e| RecordError::new(format!("cast line is not JSON: {e}")))?;
        let Value::Array(parts) = value else {
            return Err(RecordError::new("a cast event is not an array"));
        };
        let [time, code, data] = parts.as_slice() else {
            return Err(RecordError::new("a cast event is not [time, code, data]"));
        };
        let t = time
            .as_f64()
            .ok_or_else(|| RecordError::new("a cast event's time is not a number"))?;
        let t_ns = (t * 1e9).round() as i128;
        elapsed_ns = if carry.version == 3 {
            elapsed_ns + t_ns
        } else {
            t_ns
        };
        first_ns.get_or_insert(elapsed_ns);
        events += 1;
        let data = data.as_str().unwrap_or_default();
        match code.as_str().unwrap_or_default() {
            "o" => strip_escapes(data, &mut output),
            "i" => strip_escapes(data, &mut input),
            "r" => resize = Some(data.to_owned()),
            "x" => exit = Some(data.to_owned()),
            _ => {}
        }
    }
    let Some(first_ns) = first_ns else {
        return Ok((vec![], carry.clone()));
    };
    let at = |ns: i128| {
        Timestamp::from_unix_ns(start + ns)
            .map_err(|_| RecordError::new("cast event time out of range"))
    };
    Ok((
        vec![CastEvent::Output(TerminalOutput {
            session: carry.session.clone(),
            started_at: at(first_ns)?,
            ended_at: at(elapsed_ns)?,
            events,
            output,
            input,
            resize,
            exit,
        })],
        Carry {
            elapsed_ns,
            ..carry.clone()
        },
    ))
}

/// Append `data` to `out` without terminal escape sequences (CSI `ESC [ …
/// final`, OSC `ESC ] … BEL|ST`, two-byte `ESC x`) or other control
/// characters except newline and tab. A carriage return becomes nothing, so
/// redrawn lines read as their final text joined.
fn strip_escapes(data: &str, out: &mut String) {
    let mut chars = data.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\u{1b}' => match chars.next() {
                Some('[') => {
                    for c in chars.by_ref() {
                        if ('@'..='~').contains(&c) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    while let Some(c) = chars.next() {
                        if c == '\u{7}' {
                            break;
                        }
                        if c == '\u{1b}' && chars.peek() == Some(&'\\') {
                            chars.next();
                            break;
                        }
                    }
                }
                _ => {}
            },
            '\n' | '\t' => out.push(c),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
}

/// The most cast bytes in one span.
pub const SPAN_BYTES: usize = 64 << 10;

/// Frame a cast into records: the header line alone, then spans of complete
/// event lines of at most `SPAN_BYTES` (a longer single line is a span
/// alone), each ending early after a resize (`"r"`) or the exit (`"x"`).
/// Only complete lines are framed: a recording still being written ends
/// mid-line.
pub fn cast_spans(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let (lines, consumed) = complete_lines(bytes);
    let mut out = Vec::new();
    let mut open: Option<(usize, usize)> = None;
    for (s, e) in lines {
        let line = &bytes[s..e];
        let trimmed = line.trim_ascii_start();
        if trimmed.first() == Some(&b'{') {
            if let Some(span) = open.take() {
                out.push(span);
            }
            out.push((s, e));
            continue;
        }
        let ends_span = memchr_code(trimmed);
        open = match open {
            Some((start, _)) if e - start > SPAN_BYTES => {
                out.push((start, s));
                Some((s, e))
            }
            Some((start, _)) => Some((start, e)),
            None => Some((s, e)),
        };
        if ends_span && let Some(span) = open.take() {
            out.push(span);
        }
    }
    if let Some(span) = open {
        out.push(span);
    }
    (out, consumed)
}

/// Whether a cast event line is a resize or the exit: `[t, "r", …]` or
/// `[t, "x", …]`, found without parsing the line.
fn memchr_code(line: &[u8]) -> bool {
    let Some(comma) = line.iter().position(|b| *b == b',') else {
        return false;
    };
    let rest = line[comma + 1..].trim_ascii_start();
    rest.starts_with(b"\"r\"") || rest.starts_with(b"\"x\"")
}

/// The recording's directory name, the session when the header names none.
fn directory(context: &RecordContext<'_>) -> String {
    context
        .path()
        .and_then(|p| p.rsplit('/').nth(1))
        .unwrap_or_default()
        .to_owned()
}

pub struct EventsParser;

impl Parser for EventsParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = TerminalEvent;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<TerminalEvent>, ()), RecordError> {
        let event = fields::text(row, "type")?;
        let received_at = fields::from_unix_ms(fields::int(row, "ts_ms")?)?;
        let text = |name: &str| row.get(name).and_then(Value::as_str).map(str::to_owned);
        let mut extra = row.clone();
        for name in ["type", "ts_ms", "cwd", "command", "exit_code", "terminal"] {
            extra.remove(name);
        }
        extra.insert("session".into(), Value::String(directory(context)));
        Ok((
            vec![TerminalEvent {
                received_at,
                terminal: text("terminal").unwrap_or_else(|| "capture".into()),
                event,
                window_id: None,
                cwd: text("cwd"),
                cmdline: text("command"),
                exit_status: row.get("exit_code").and_then(Value::as_i64),
                title: None,
                extra,
            }],
            (),
        ))
    }
}

/// Complete lines only: a recording still being written ends mid-line.
pub fn complete_lines(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    crate::drivers::export_archive::Framing::Lines.frame(bytes, false)
}

pub fn cast_source() -> Declared<CastParser> {
    Declared {
        spec: CAST,
        parser: CastParser,
    }
}

pub fn events_source() -> Declared<EventsParser> {
    Declared {
        spec: EVENTS,
        parser: EventsParser,
    }
}

pub fn driver(path: PathBuf, host: String, spec: &SourceSpec) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: spec.framer,
    }
}

/// `--source asciinema=PATH`: each recording directory under PATH, its cast
/// and the capture's events beside it.
pub const KIND: crate::binding::Kind = crate::binding::Kind {
    name: "asciinema",
    specs: || vec![&CAST, &EVENTS],
    plan: Some(|input, settings| {
        Ok(crate::binding::files(input, true, |p| {
            p.file_name()
                .is_some_and(|n| n == "session.cast" || n == "events.jsonl")
        })
        .into_iter()
        .map(|f| {
            let spec = if f.file_name().is_some_and(|n| n == "session.cast") {
                &CAST
            } else {
                &EVENTS
            };
            crate::binding::Binding::of(spec, &f, settings.clone())
        })
        .collect())
    }),
    source: |b| {
        if b.source == CAST.name {
            Ok(Box::new(cast_source()))
        } else {
            Ok(Box::new(events_source()))
        }
    },
    reader: Some(|b| {
        let spec = if b.source == CAST.name { CAST } else { EVENTS };
        Ok(crate::binding::appended(
            driver(b.input.clone(), b.settings.host.clone(), &spec),
            spec.framer,
        ))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
