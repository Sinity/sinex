//! Machine telemetry: the SQLite databases a host's telemetry collector
//! writes (`/realm/state/machine-telemetry` on a Sinnix host), on
//! `SqliteRows`, one binding per database file and table.
//!
//! The collector appends rows and never changes them, so each table is read
//! as appended: only rows after the cursor, a page at a time. Every row is
//! one event: a sample table's row is `resource.sampled`, a `kill_event` row
//! `process.killed` and a `hardware_state` row `hardware.state`.
//!
//! The collector writes `telemetry.sqlite` and from time to time renames it
//! to a sealed file (`telemetry.<time>.sqlite`) and starts a new one, carrying
//! each table's row numbering over, so IDs continue across the files and no
//! two files hold different rows under one ID. A sealed file holds rows the
//! binding of `telemetry.sqlite` already read before the rename; its own
//! binding reads them again, and the key (table, ID, host) makes them
//! repeats. The plan lists the files again every minute, so a new sealed file
//! gets its binding.
//!
//! `source_status` is not read: it is the collector's own bookkeeping (when
//! each of its inputs was last checked, its journal cursor), overwritten in
//! place.
use crate::binding::{Binding, InputFormat, Settings};
use crate::drivers::sqlite_rows::{Changes, SqliteRows};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::telemetry::{HardwareState, ProcessKilled, ResourceSampled};
use std::path::{Path, PathBuf};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Telemetry {
        Sampled(ResourceSampled),
        Killed(ProcessKilled),
        Hardware(HardwareState),
    }
}

const SAMPLED: &[&str] = &["resource.sampled"];

/// The tables read, each with its own parser name, so a replay knows which
/// table a stored row came from.
const SPECS: [SourceSpec; 13] = [
    spec("machine-telemetry:metric_sample", SAMPLED),
    spec("machine-telemetry:service_state", SAMPLED),
    spec("machine-telemetry:gpu_sample", SAMPLED),
    spec("machine-telemetry:block_device_sample", SAMPLED),
    spec("machine-telemetry:service_cgroup_io_sample", SAMPLED),
    spec(
        "machine-telemetry:service_cgroup_pressure_sample",
        SAMPLED,
    ),
    spec("machine-telemetry:cgroup_memory_sample", SAMPLED),
    spec("machine-telemetry:process_io_delta_sample", SAMPLED),
    spec("machine-telemetry:process_memory_sample", SAMPLED),
    spec("machine-telemetry:network_sample", SAMPLED),
    spec("machine-telemetry:filesystem_capacity_sample", SAMPLED),
    spec("machine-telemetry:kill_event", &["process.killed"]),
    spec("machine-telemetry:hardware_state", &["hardware.state"]),
];

const fn spec(parser: &'static str, event_types: &'static [&'static str]) -> SourceSpec {
    SourceSpec {
        name: "machine.telemetry",
        driver: Driver::SqliteRows,
        // A sealed file repeats rows the active file's binding read.
        excisable: false,
        purpose: "resource figures and what the machine was doing: power, temperatures, \
                  memory, IO and pressure per service and process, and OOM kills",
        class: Class::Open,
        parser,
        parser_version: 1,
        event_types,
        framer: crate::Framer::incremental(crate::drivers::append_file::lines),
    }
}

/// The table a parser name reads.
fn table_of(parser: &str) -> Result<&'static str, String> {
    SPECS
        .iter()
        .find(|s| s.parser == parser)
        .and_then(|s| s.parser.strip_prefix("machine-telemetry:"))
        .ok_or_else(|| format!("{parser}: not a machine telemetry table"))
}

pub struct TelemetryParser {
    pub table: &'static str,
}

impl Parser for TelemetryParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = Telemetry;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<Telemetry>, ()), RecordError> {
        let mut columns = row.clone();
        // The SQLite-row driver's own field, not a column of the table.
        columns.remove("_rowid");
        let id = take_int(&mut columns, "id")?
            .ok_or_else(|| RecordError::new("the row has no id"))?;
        let payload = match self.table {
            "kill_event" => Telemetry::Killed(ProcessKilled {
                id,
                observed_at: take_time(&mut columns, "observed_at")?,
                host: take_text(&mut columns, "host")?,
                boot_id: take_text(&mut columns, "boot_id")?,
                killer: take_text(&mut columns, "killer")?
                    .ok_or_else(|| RecordError::new("killer is missing"))?,
                victim_comm: take_text(&mut columns, "victim_comm")?,
                victim_pid: take_int(&mut columns, "victim_pid")?,
                victim_rss_mib: take_int(&mut columns, "victim_rss_mib")?,
                cgroup_path: take_text(&mut columns, "cgroup_path")?,
                oom_score: take_int(&mut columns, "oom_score")?,
                raw_line: take_text(&mut columns, "raw_line")?
                    .ok_or_else(|| RecordError::new("raw_line is missing"))?,
                journal_cursor: take_text(&mut columns, "journal_cursor")?,
                values: values(columns),
            }),
            "hardware_state" => {
                let state = take_text(&mut columns, "payload_json")?
                    .ok_or_else(|| RecordError::new("payload_json is missing"))?;
                Telemetry::Hardware(HardwareState {
                    id,
                    captured_at: take_time(&mut columns, "captured_at")?,
                    host: take_text(&mut columns, "host")?,
                    boot_id: take_text(&mut columns, "boot_id")?,
                    state: serde_json::from_str(&state)
                        .map_err(|e| RecordError::new(format!("payload_json is not JSON: {e}")))?,
                    values: values(columns),
                })
            }
            table => Telemetry::Sampled(ResourceSampled {
                table: table.to_owned(),
                id,
                observed_at: take_time(&mut columns, "observed_at")?,
                host: take_text(&mut columns, "host")?,
                boot_id: take_text(&mut columns, "boot_id")?,
                values: values(columns),
            }),
        };
        Ok((vec![payload], ()))
    }
}

/// A text column; `None` when it is missing or null.
fn take_text(columns: &mut Map<String, Value>, name: &str) -> Result<Option<String>, RecordError> {
    match columns.remove(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(other) => Err(RecordError::new(format!("{name} is not text: {other}"))),
    }
}

/// An integer column; `None` when it is missing or null.
fn take_int(columns: &mut Map<String, Value>, name: &str) -> Result<Option<i64>, RecordError> {
    match columns.remove(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_i64()
            .map(Some)
            .ok_or_else(|| RecordError::new(format!("{name} is not an integer: {value}"))),
    }
}

/// A time column, written with its offset. A time that does not read as one
/// is unknown, not the read time.
fn take_time(
    columns: &mut Map<String, Value>,
    name: &str,
) -> Result<Option<sinex_model::Timestamp>, RecordError> {
    Ok(take_text(columns, name)?.and_then(|text| super::fields::rfc3339(&text).ok()))
}

/// The remaining columns, without nulls; JSON text columns as their JSON.
fn values(columns: Map<String, Value>) -> Map<String, Value> {
    columns
        .into_iter()
        .filter(|(_, value)| !value.is_null())
        .map(|(name, value)| {
            let value = match value {
                Value::String(text) if name.ends_with("_json") => {
                    serde_json::from_str(&text).unwrap_or(Value::String(text))
                }
                value => value,
            };
            (name, value)
        })
        .collect()
}

pub fn source(table: &'static str) -> Result<DeclaredSource<TelemetryParser>, String> {
    let spec = SPECS
        .iter()
        .find(|s| s.parser.strip_prefix("machine-telemetry:") == Some(table))
        .ok_or_else(|| format!("{table}: not a machine telemetry table"))?
        .clone();
    Ok(DeclaredSource {
        spec,
        parser: TelemetryParser { table },
    })
}

pub fn driver(path: PathBuf, host: String, table: &'static str) -> SqliteRows {
    SqliteRows {
        path,
        host,
        table,
        from: None,
        required: match table {
            "kill_event" => &["id", "killer", "raw_line"],
            "hardware_state" => &["id", "payload_json"],
            _ => &["id"],
        },
        changes: Changes::Appended,
    }
}

/// The bindings for `input`: a database file, or a directory of them
/// (`*.sqlite`), one per table the file has. A file whose tables cannot be
/// listed gets a binding for every table, and each says why it cannot read.
fn plan(input: &Path, settings: &Settings) -> Result<Vec<Binding>, String> {
    let is_database = |path: &Path| path.extension().is_some_and(|e| e == "sqlite");
    let mut bindings = Vec::new();
    for file in crate::binding::files(input, false, is_database) {
        let present = tables(&file);
        for spec in &SPECS {
            let table = table_of(spec.parser)?;
            if present.as_ref().is_none_or(|tables| tables.iter().any(|t| t == table)) {
                bindings.push(Binding::of(spec, &file, settings.clone()).named(table));
            }
        }
    }
    Ok(bindings)
}

/// The tables in the database at `path`, or `None` when it cannot be read.
fn tables(path: &Path) -> Option<Vec<String>> {
    let connection = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .ok()?;
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .ok()?
        .collect::<Result<_, _>>()
        .ok()
}

/// `--read machine-telemetry=PATH`: the collector's databases.
pub const FORMAT: InputFormat = InputFormat {
    name: "machine-telemetry",
    specs: || SPECS.iter().collect(),
    plan: Some(plan),
    source: |b| Ok(Box::new(source(table_of(&b.parser)?)?)),
    reader: Some(|b| {
        Ok(crate::binding::rows(driver(
            b.input.clone(),
            b.settings.host.clone(),
            table_of(&b.parser)?,
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
