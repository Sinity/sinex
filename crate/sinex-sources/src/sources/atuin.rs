//! Atuin shell history (`~/.local/share/atuin/history.db`), on `SqliteRows`.
//!
//! Atuin writes a row when a command starts and updates it in place when the
//! command ends (duration, exit) or is deleted (`deleted_at`). Its own row ID
//! is the equivalence key, so an update is a newer reading of the same record.
//! Running commands store -1 for duration and exit; those are unknown, not -1.
use crate::drivers::sqlite_rows::SqliteRows;
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::{Class, Timestamp, kinds::CommandExecuted};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.atuin-history",
    driver: Driver::SqliteRows,
    consumer: "command fusion and shell recall",
    class: Class::Open,
    parser: "atuin-history",
    parser_version: 1,
    event_types: &["command.executed"],
    framer: crate::drivers::append_file::lines,
};

const KNOWN: &[&str] = &[
    "_rowid",
    "id",
    "timestamp",
    "duration",
    "exit",
    "command",
    "cwd",
    "session",
    "hostname",
    "deleted_at",
    "author",
    "intent",
    "shell",
];

pub struct AtuinParser;

impl Parser for AtuinParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = CommandExecuted;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<CommandExecuted>, ()), RecordError> {
        let text = |name: &str| -> Result<String, RecordError> {
            match row.get(name) {
                Some(Value::String(s)) => Ok(s.clone()),
                other => Err(RecordError::new(format!("{name} is not text: {other:?}"))),
            }
        };
        let optional_text = |name: &str| row.get(name).and_then(Value::as_str).map(str::to_owned);
        let int = |name: &str| -> Result<i64, RecordError> {
            row.get(name)
                .and_then(Value::as_i64)
                .ok_or_else(|| RecordError::new(format!("{name} is not an integer")))
        };
        let time = |ns: i64| {
            Timestamp::from_unix_ns(i128::from(ns))
                .map_err(|_| RecordError::new(format!("time {ns} out of range")))
        };
        let duration = int("duration")?;
        let running = duration < 0;
        let deleted_at = match row.get("deleted_at") {
            None | Some(Value::Null) => None,
            Some(v) => {
                Some(time(v.as_i64().ok_or_else(|| {
                    RecordError::new("deleted_at is not an integer")
                })?)?)
            }
        };
        let extra: Map<String, Value> = row
            .iter()
            .filter(|(k, _)| !KNOWN.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let payload = CommandExecuted {
            native_id: text("id")?,
            command: text("command")?,
            cwd: text("cwd")?,
            session: text("session")?,
            hostname: text("hostname")?,
            started_at: time(int("timestamp")?)?,
            duration_ns: (!running).then_some(duration),
            exit_code: if running { None } else { Some(int("exit")?) },
            deleted_at,
            author: optional_text("author"),
            intent: optional_text("intent"),
            shell: optional_text("shell"),
            extra,
        };
        Ok((vec![payload], ()))
    }
}

pub fn source() -> Declared<AtuinParser> {
    Declared {
        spec: SPEC,
        parser: AtuinParser,
    }
}

pub fn driver(path: PathBuf, host: String) -> SqliteRows {
    SqliteRows {
        path,
        host,
        table: "history",
        required: &[
            "id",
            "timestamp",
            "duration",
            "exit",
            "command",
            "cwd",
            "session",
            "hostname",
        ],
        // Rows still running, or started in the last day, can still change.
        mutable: Some("duration = -1 OR timestamp > ?1"),
        full_pass_secs: 3600,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(duration: i64, exit: i64) -> Map<String, Value> {
        json!({"_rowid": 1, "id": "019", "timestamp": 1_743_722_259_001_000_000i64, "duration": duration,
               "exit": exit, "command": "ls", "cwd": "/", "session": "s", "hostname": "h:u",
               "deleted_at": null, "author": "sinity", "intent": null, "shell": "zsh", "future_column": 3})
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn running_commands_have_unknown_duration_and_exit() {
        let (p, ()) = AtuinParser.parse(&row(-1, -1), &()).unwrap();
        assert_eq!((p[0].duration_ns, p[0].exit_code), (None, None));
        let (p, ()) = AtuinParser.parse(&row(5, -9), &()).unwrap();
        assert_eq!(
            (p[0].duration_ns, p[0].exit_code),
            (Some(5), Some(-9)),
            "exit codes are signed"
        );
    }

    #[test]
    fn unknown_columns_are_kept_and_deletion_is_removal() {
        let mut r = row(5, 0);
        r.insert("deleted_at".into(), json!(1_743_722_300_000_000_000i64));
        let (p, ()) = AtuinParser.parse(&r, &()).unwrap();
        assert_eq!(p[0].extra.get("future_column"), Some(&json!(3)));
        assert!(p[0].deleted_at.is_some());
        assert_eq!(
            sinex_model::EventPayload::record_state(&p[0]),
            sinex_model::RecordState::Removed
        );
    }

    #[test]
    fn wrong_types_fail_the_record() {
        let mut r = row(5, 0);
        r.insert("command".into(), json!(7));
        assert!(AtuinParser.parse(&r, &()).is_err());
    }
}
