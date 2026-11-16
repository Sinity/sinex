//! Atuin shell history (`~/.local/share/atuin/history.db`), on `SqliteRows`.
//!
//! Atuin writes a row when a command starts and updates it in place when the
//! command ends (duration, exit) or is deleted (`deleted_at`). Its own row ID
//! is the equivalence key, so an update is a newer reading of the same record.
//! Running commands store -1 for duration and exit; those are unknown, not -1.
use serde_json::{Map, Value};
use sinex_model::json::JsonExt;
use sinex_model::{Timestamp, event_types::CommandExecuted};
use sinex_source_core::RecordError;
use sinex_source_core::RecordResult;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::sqlite_rows::{Changes, SqliteRows};
use sinex_source_core::registry::{DeclaredSource, Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Stateless, StatelessParser};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.atuin-history",
    origin: Origin::Input(Shape::SqliteRows),
    expect: Expectation::continuous_days(2),
    purpose: "command fusion and shell recall",
    parser: "atuin-history",
    event_types: &["command.executed"],
    framer: sinex_source_core::Framer::LINES,
    ..SourceSpec::DEFAULT
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

impl StatelessParser for AtuinParser {
    type Record<'a> = Map<String, Value>;
    type Payload = CommandExecuted;

    fn parse(
        &self,
        row: &Map<String, Value>,

        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<Vec<CommandExecuted>, RecordError> {
        let text = |name: &str| -> Result<String, RecordError> {
            match row.get(name) {
                Some(Value::String(s)) => Ok(s.clone()),
                other => Err(RecordError::new(format!("{name} is not text: {other:?}"))),
            }
        };
        let optional_text = |name: &str| row.str(name).map(str::to_owned);
        let int = |name: &str| -> Result<i64, RecordError> {
            row.i64(name)
                .record_with(|| format!("{name} is not an integer"))
        };
        let time = |ns: i64| {
            Timestamp::from_unix_ns(i128::from(ns))
                .record_with(|| format!("time {ns} out of range"))
        };
        let duration = int("duration")?;
        let running = duration < 0;
        let deleted_at = match row.get("deleted_at") {
            None | Some(Value::Null) => None,
            Some(v) => Some(time(v.as_i64().record("deleted_at is not an integer")?)?),
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
        Ok(vec![payload])
    }
}

pub fn source() -> DeclaredSource<Stateless<AtuinParser>> {
    DeclaredSource {
        spec: SPEC,
        parser: Stateless(AtuinParser),
    }
}

pub fn driver(path: PathBuf, host: String) -> SqliteRows {
    SqliteRows {
        path,
        host,
        table: "history",
        from: None,
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
        changes: Changes::InPlace {
            mutable: "duration = -1 OR timestamp > ?1",
            full_pass_secs: 3600,
        },
    }
}

super::input_format! {
    /// `stage --parser atuin PATH`: Atuin's history database.
    pub FORMAT = "atuin" {
    specs: [SPEC],
    source: |_b| Ok(Box::new(source())),
    discovery: DiscoverySpec::system(&[Location::new("data", "atuin/history.db")]),
    accepts: [sinex_source_core::stage::Structure::SqliteTable {
        table: "history",
        columns: &["id", "timestamp", "duration", "exit", "command", "cwd", "session", "hostname"],
    }],
    plan: single,
    reader: rows(driver),
} }

#[cfg(test)]
mod tests;
