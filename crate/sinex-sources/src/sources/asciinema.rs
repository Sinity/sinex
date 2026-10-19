//! asciinema terminal recordings (`<day>/<session>/session.cast`) and the
//! shell events Sinnix's capture logs beside them (`events.jsonl`), both on
//! `AppendFile`: a recording grows while its terminal is open.
//!
//! A cast's first line is its header: version 2 or 3, the start as epoch
//! seconds (`timestamp`), the size (`width`/`height` in v2, `term.cols`/`rows`
//! in v3), the command and environment. Each later line is `[time, code,
//! data]`: in v3 the time is the interval since the previous event, in v2 the
//! offset from the start. The parser carries the start and the running time,
//! so every event gets its own instant. The session ID is the capture's
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
use sinex_model::kinds::machine::{TerminalOutput, TerminalSession};
use sinex_model::kinds::session::TerminalEvent;
use std::path::PathBuf;

pub const CAST: SourceSpec = SourceSpec {
    name: "terminal.asciinema",
    driver: Driver::AppendFile,
    consumer: "what terminals showed and what was typed, replayable",
    class: sinex_model::Class::Open,
    parser: "asciinema-cast",
    parser_version: 1,
    event_types: &["terminal.session", "terminal.output"],
    framer: complete_lines,
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

    fn parse(&self, _: &&[u8], _: &Carry) -> Result<(Vec<CastEvent>, Carry), RecordError> {
        Err(RecordError::new("a cast line needs its file's path"))
    }

    fn parse_record(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<CastEvent>, Carry), RecordError> {
        let line = std::str::from_utf8(record)
            .map_err(|e| RecordError::new(format!("cast line is not UTF-8: {e}")))?
            .trim_end();
        if line.is_empty() {
            return Ok((vec![], carry.clone()));
        }
        let value: Value = serde_json::from_str(line)
            .map_err(|e| RecordError::new(format!("cast line is not JSON: {e}")))?;
        match value {
            Value::Object(header) => {
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
            Value::Array(parts) => {
                let [time, code, data] = parts.as_slice() else {
                    return Err(RecordError::new("a cast event is not [time, code, data]"));
                };
                let start = carry
                    .start_ns
                    .ok_or_else(|| RecordError::new("a cast event before its header"))?;
                let t = time
                    .as_f64()
                    .ok_or_else(|| RecordError::new("a cast event's time is not a number"))?;
                let t_ns = (t * 1e9).round() as i128;
                let elapsed_ns = if carry.version == 3 {
                    carry.elapsed_ns + t_ns
                } else {
                    t_ns
                };
                let at = Timestamp::from_unix_ns(start + elapsed_ns)
                    .map_err(|_| RecordError::new("cast event time out of range"))?;
                Ok((
                    vec![CastEvent::Output(TerminalOutput {
                        session: carry.session.clone(),
                        at,
                        code: code.as_str().unwrap_or_default().to_owned(),
                        data: data.as_str().unwrap_or_default().to_owned(),
                    })],
                    Carry {
                        elapsed_ns,
                        ..carry.clone()
                    },
                ))
            }
            _ => Err(RecordError::new(
                "a cast line is neither a header nor an event",
            )),
        }
    }
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
        _: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<TerminalEvent>, ()), RecordError> {
        Err(RecordError::new("a capture event needs its file's path"))
    }

    fn parse_record(
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(lines: &[&str]) -> Vec<CastEvent> {
        let manifest = json!({"path": "/a/2026/10/04/sess-1/session.cast"});
        let context = RecordContext {
            manifest: &manifest,
            member: None,
        };
        let mut carry = Carry::default();
        let mut out = Vec::new();
        for line in lines {
            let (events, next) = CastParser
                .parse_record(&line.as_bytes(), &carry, &context)
                .unwrap();
            out.extend(events);
            carry = next;
        }
        out
    }

    #[test]
    fn v3_intervals_accumulate_and_v2_offsets_do_not() {
        let out = run(&[
            r#"{"version":3,"term":{"cols":80,"rows":24},"timestamp":1791129020,"command":"zsh"}"#,
            r#"[0.5, "i", "z"]"#,
            r#"[0.25, "o", "z"]"#,
        ]);
        let CastEvent::Session(s) = &out[0] else {
            panic!()
        };
        assert_eq!(
            (s.session.as_str(), s.cols, s.rows),
            ("sess-1", Some(80), Some(24))
        );
        let CastEvent::Output(o) = &out[2] else {
            panic!()
        };
        assert_eq!(o.at.unix_ns(), 1_791_129_020_750_000_000);
        assert_eq!((o.code.as_str(), o.data.as_str()), ("o", "z"));
        let out = run(&[
            r#"{"version":2,"width":80,"height":24,"timestamp":100,"env":{"SINNIX_CAPTURE_SESSION_ID":"x"}}"#,
            r#"[1.0, "o", "a"]"#,
            r#"[1.5, "o", "b"]"#,
        ]);
        let CastEvent::Output(o) = &out[2] else {
            panic!()
        };
        assert_eq!(o.at.unix_ns(), 101_500_000_000);
        assert_eq!(o.session, "x");
    }
}
