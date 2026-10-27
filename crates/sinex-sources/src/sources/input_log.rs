//! The keyboard and pointer log scribe-tap writes, one JSON line per input
//! record into a file per day (`<YYYY-MM-DD>.jsonl`), on `AppendFile`. One
//! process logs each input device under its own `session`.
//!
//! Every record is one event: key presses and releases with the kernel's
//! time of the keystroke, pointer buttons, motion and wheel steps, and the
//! logger's own records. Key records carry `keycode` (the key's name),
//! `code` and `value` (the kernel's), `input_sec`/`input_usec` (the kernel's
//! time) and `changed`; a paste also carries the `clipboard` it read. Older
//! logs have presses only, without code, value or kernel time, and a
//! `window`. Pointer records carry the axis or button in `code` and the step
//! in `value`, and no kernel time. Records have no IDs; [`Numbering`]
//! numbers equal ones.
use crate::drivers::append_file::{AppendFile, lines};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::event_types::input::{
    InputLogger, KeyPressed, KeyReleased, PointerButton, PointerMoved, PointerScrolled,
};
use sinex_model::{Class, Timestamp};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.input-log",
    driver: Driver::AppendFile,
    excisable: false,
    purpose: "keystroke timing, typing and pointer activity",
    // Keystrokes spell out what was typed, passwords included.
    class: Class::Sealed,
    parser: "scribe-tap",
    parser_version: 1,
    event_types: &[
        "key.pressed",
        "key.released",
        "pointer.button",
        "pointer.moved",
        "pointer.scrolled",
        "input.logger",
    ],
    framer: crate::Framer::incremental(lines),
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Input {
        Pressed(KeyPressed),
        Released(KeyReleased),
        Button(PointerButton),
        Moved(PointerMoved),
        Scrolled(PointerScrolled),
        Logger(InputLogger),
    }
}

/// How long a session is remembered after its last record.
const SESSION_NS: i128 = 86_400 * 1_000_000_000;

/// The records of each session at its latest logger time, by what they
/// report: a record's `n` counts the equal records before it. A session's
/// records are written in order by one process, so equal records share a
/// logger time and follow one another in that session; a logger clock set
/// back can number a record like an earlier one.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Numbering {
    sessions: BTreeMap<String, Latest>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Latest {
    logged_ns: i128,
    /// A digest of what each record reports, and how many were seen.
    seen: Vec<(String, u32)>,
}

impl Numbering {
    fn next(&self, session: &str, logged_ns: i128, identity: &str) -> (u32, Self) {
        let digest = blake3::hash(identity.as_bytes()).to_hex()[..32].to_owned();
        let mut next = self.clone();
        next.sessions
            .retain(|_, latest| latest.logged_ns >= logged_ns - SESSION_NS);
        let latest = next.sessions.entry(session.to_owned()).or_default();
        if latest.logged_ns != logged_ns {
            latest.logged_ns = logged_ns;
            latest.seen.clear();
        }
        let n = match latest.seen.iter_mut().find(|(d, _)| *d == digest) {
            Some((_, count)) => {
                *count += 1;
                *count - 1
            }
            None => {
                latest.seen.push((digest, 1));
                0
            }
        };
        (n, next)
    }
}

pub struct InputLogParser;

fn take_text(row: &mut Map<String, Value>, key: &str) -> Option<String> {
    match row.remove(key) {
        Some(Value::String(s)) => Some(s),
        Some(other) => {
            row.insert(key.to_owned(), other);
            None
        }
        None => None,
    }
}

fn take_int(row: &mut Map<String, Value>, key: &str) -> Option<i64> {
    match row.remove(key) {
        Some(Value::Number(n)) if n.as_i64().is_some() => n.as_i64(),
        Some(other) => {
            row.insert(key.to_owned(), other);
            None
        }
        None => None,
    }
}

fn take_bool(row: &mut Map<String, Value>, key: &str) -> Option<bool> {
    match row.remove(key) {
        Some(Value::Bool(b)) => Some(b),
        Some(other) => {
            row.insert(key.to_owned(), other);
            None
        }
        None => None,
    }
}

/// The kernel's time from `input_sec` and `input_usec`, when both are there.
fn kernel_time(row: &mut Map<String, Value>) -> Result<Option<Timestamp>, RecordError> {
    let (Some(Value::Number(sec)), Some(Value::Number(usec))) =
        (row.get("input_sec"), row.get("input_usec"))
    else {
        return Ok(None);
    };
    let (Some(sec), Some(usec)) = (sec.as_i64(), usec.as_i64()) else {
        return Err(RecordError::new("kernel time is not two integers"));
    };
    row.remove("input_sec");
    row.remove("input_usec");
    let us = sec
        .checked_mul(1_000_000)
        .and_then(|s| s.checked_add(usec))
        .ok_or_else(|| RecordError::new("kernel time out of range"))?;
    fields::from_unix_us(us).map(Some)
}

impl Parser for InputLogParser {
    type Record<'a> = Map<String, Value>;
    type Carry = Numbering;
    type Payload = Input;

    fn parse(
        &self,
        record: &Map<String, Value>,
        numbering: &Numbering,
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<Input>, Numbering), RecordError> {
        let mut row = record.clone();
        let logged_at = fields::rfc3339(
            &take_text(&mut row, "ts").ok_or_else(|| RecordError::new("no `ts`"))?,
        )?;
        let event = take_text(&mut row, "event").ok_or_else(|| RecordError::new("no `event`"))?;
        let session =
            take_text(&mut row, "session").ok_or_else(|| RecordError::new("no `session`"))?;
        let logged_ns = logged_at.unix_ns();
        let number = |identity: String| numbering.next(&session, logged_ns, &identity);
        let (input, next) = match event.as_str() {
            "press" | "release" => {
                let kernel_at = kernel_time(&mut row)?;
                let key = take_text(&mut row, "keycode")
                    .ok_or_else(|| RecordError::new("a key record without `keycode`"))?;
                let code = take_int(&mut row, "code").and_then(|c| u16::try_from(c).ok());
                let value = take_int(&mut row, "value").and_then(|v| i32::try_from(v).ok());
                let changed = take_bool(&mut row, "changed");
                let identity = format!(
                    "{event}\0{}\0{key}\0{value:?}",
                    kernel_at.map(|t| t.unix_ns()).unwrap_or_default()
                );
                let (n, next) = number(identity);
                let input = if event == "press" {
                    Input::Pressed(KeyPressed {
                        session: session.clone(),
                        logged_at,
                        kernel_at,
                        key,
                        code,
                        value,
                        changed,
                        clipboard: take_text(&mut row, "clipboard"),
                        window: take_text(&mut row, "window"),
                        n,
                        extra: row,
                    })
                } else {
                    Input::Released(KeyReleased {
                        session: session.clone(),
                        logged_at,
                        kernel_at,
                        key,
                        code,
                        value,
                        changed,
                        n,
                        extra: row,
                    })
                };
                (input, next)
            }
            "pointer_button_press" | "pointer_button_release" => {
                let button = take_text(&mut row, "code")
                    .ok_or_else(|| RecordError::new("a button record without `code`"))?;
                let value = take_int(&mut row, "value")
                    .ok_or_else(|| RecordError::new("a button record without `value`"))?;
                let pressed = event == "pointer_button_press";
                let (n, next) = number(format!("{event}\0{button}\0{value}"));
                (
                    Input::Button(PointerButton {
                        session: session.clone(),
                        logged_at,
                        button,
                        pressed,
                        value,
                        n,
                        extra: row,
                    }),
                    next,
                )
            }
            "pointer_rel" | "pointer_abs" => {
                let axis = take_text(&mut row, "code")
                    .ok_or_else(|| RecordError::new("a pointer record without `code`"))?;
                let value = take_int(&mut row, "value")
                    .ok_or_else(|| RecordError::new("a pointer record without `value`"))?;
                let (n, next) = number(format!("{event}\0{axis}\0{value}"));
                let input = if axis.contains("WHEEL") {
                    Input::Scrolled(PointerScrolled {
                        session: session.clone(),
                        logged_at,
                        axis,
                        value,
                        n,
                        extra: row,
                    })
                } else {
                    Input::Moved(PointerMoved {
                        session: session.clone(),
                        logged_at,
                        axis,
                        value,
                        absolute: event == "pointer_abs",
                        n,
                        extra: row,
                    })
                };
                (input, next)
            }
            _ => {
                let (n, next) = number(format!("{event}\0{}", Value::Object(row.clone())));
                (
                    Input::Logger(InputLogger {
                        session: session.clone(),
                        logged_at,
                        event,
                        n,
                        fields: row,
                    }),
                    next,
                )
            }
        };
        Ok((vec![input], next))
    }
}

pub fn source() -> DeclaredSource<InputLogParser> {
    DeclaredSource {
        spec: SPEC,
        parser: InputLogParser,
    }
}

/// A day's log, `<YYYY-MM-DD>.jsonl`. Other files beside them (a repair's
/// `.torn-original.jsonl` copy, temporary files) are not the log.
pub fn is_day_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let Some(day) = name.strip_suffix(".jsonl") else {
        return false;
    };
    let b = day.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter()
            .enumerate()
            .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit())
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: SPEC.framer,
    }
}

/// `--read input-log=DIR`: each day's log in DIR, or one log file.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "input-log",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        Ok(crate::binding::per_file(
            &SPEC,
            input,
            settings,
            false,
            is_day_file,
        ))
    }),
    source: |_| Ok(Box::new(source())),
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
