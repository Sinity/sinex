//! `SqliteRows`: another program's SQLite database (Atuin, ActivityWatch,
//! Chromium, the machine telemetry collector). Each poll reads one consistent
//! snapshot. How much it reads depends on how the provider changes its rows
//! ([`Changes`]):
//!
//! - Rows updated in place (Atuin fills in duration and exit when a command
//!   ends, and sets `deleted_at`): rows after the cursor, plus rows that can
//!   still change. A full pass runs periodically, and on the first poll after
//!   a start, so an update outside the window is still seen.
//! - Rows only appended: rows after the cursor, a page of at most
//!   [`ROWS_PER_PART`] at a time; the next poll reads the next page. Nothing
//!   is remembered per row, so a table of hundreds of millions of rows costs
//!   no memory and a restart reads on from the cursor.
//!
//! The provider's schema is checked on open and a missing column is named.
//!
//! Rows are stored as a JSON-lines material ("rows part"); each line is one
//! row's columns, so the record location is the line's offset and a replay
//! reads exactly the bytes the parser saw. For rows updated in place, rows
//! identical to what this driver already stored are left out of the material.
//! The held material and its row hashes are kept until its final admitted
//! cursor confirms them.
//!
//! For rows updated in place, what the driver already stored survives a
//! restart: the rows still open travel in the cursor, and the row hashes come
//! from the reader memory saved at the end of a recent part
//! ([`RowState::remember_saved`], on the cadence of [`super::memory_due`])
//! plus the stored parts after it ([`RowState::remember_rows`]), so a restart
//! neither reads the whole part sequence nor stores every row again.
use super::{Acquired, DriverError, NewMaterial, StoredBytes, file_facts};
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::{InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

pub struct SqliteRows {
    pub path: PathBuf,
    pub host: String,
    pub table: &'static str,
    /// What to read rows from when the parser needs more than `table` (a join
    /// that names a bucket, say), as a parenthesized subquery exposing a column
    /// named `rowid` (the table's row ID). `required` is still checked against
    /// `table`.
    pub from: Option<&'static str>,
    /// Columns the parser needs; a missing one makes the input unavailable,
    /// named, rather than silently empty.
    pub required: &'static [&'static str],
    pub changes: Changes,
}

/// How the provider changes the rows of its table.
#[derive(Clone, Copy, Debug)]
pub enum Changes {
    /// Rows can be updated after they are written.
    InPlace {
        /// Rows that can still change, as a SQL predicate over the table.
        /// `?1` is the start of the mutable window, one day ago, in
        /// nanoseconds since the epoch.
        mutable: &'static str,
        /// How often every row is read and compared, in seconds.
        full_pass_secs: i64,
    },
    /// Rows are only appended, in rowid order, and never change once
    /// committed (a collector's sample tables).
    Appended,
}

/// The most rows one part of an appended table holds.
pub const ROWS_PER_PART: usize = 50_000;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    /// Observed file identity within the input host. A replacement database
    /// starts a new row namespace even when its path and row IDs repeat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_generation: Option<String>,
    pub max_rowid: i64,
    pub last_full_pass_ns: i64,
    /// The highest reading order given out, so every later reading is newer:
    /// a row read again in a later poll, or later in the same poll, replaces
    /// an earlier reading of it.
    #[serde(default)]
    pub last_order: i64,
    pub material: Option<MaterialId>,
    pub part_index: u32,
    /// Rows that were still open (matched the mutable predicate) when last
    /// read; re-read by rowid until they settle.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open: Vec<i64>,
    /// How many bytes of `material` the stored readings cover; a restart
    /// rebuilds the row state from those bytes only.
    #[serde(default)]
    pub through: usize,
    /// The part at whose end the reader last saved its memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_part: Option<u32>,
    /// Rows stored since that part.
    #[serde(default)]
    pub changed_since_memory: u64,
}

type RowHashes = Vec<(i64, [u8; 32])>;

/// What this driver already stored: the hash of each row's stored line, by
/// rowid. Kept for the life of the source task and rebuilt on a restart from
/// the saved memory and the parts after it. An empty state (no stored parts
/// yet) triggers a full pass, whose repeats admission suppresses.
#[derive(Debug, Default)]
pub struct RowState {
    hashes: HashMap<i64, [u8; 32]>,
    /// Whether this process has polled since the state was made: the first
    /// poll after a start is a full pass even when the hashes were rebuilt,
    /// so an update made while Sinex was down is seen.
    polled: bool,
    /// The newest material and the row hashes it adds once admitted.
    held: Option<(super::held::HeldMaterial, RowHashes)>,
}

impl RowState {
    /// Remember the rows of one stored part. Parts are offered newest
    /// first, so a row already remembered keeps its newer line.
    pub fn remember_rows(&mut self, bytes: &[u8]) {
        for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let Ok(row) = serde_json::from_slice::<Map<String, Value>>(line) else {
                continue;
            };
            if let Some(rowid) = row.get("_rowid").and_then(Value::as_i64) {
                self.hashes
                    .entry(rowid)
                    .or_insert(*blake3::hash(line).as_bytes());
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Remember the rows of a saved memory that no newer part decided.
    pub fn remember_saved(&mut self, memory: &[u8]) -> Result<(), DriverError> {
        let (entries, rest) = memory.as_chunks::<MEMORY_ENTRY>();
        if !rest.is_empty() {
            return Err(DriverError::Cursor("saved row memory is damaged".into()));
        }
        for entry in entries {
            let (rowid, hash) = entry.split_at(8);
            let rowid = i64::from_le_bytes(rowid.try_into().expect("eight bytes"));
            self.hashes
                .entry(rowid)
                .or_insert(hash.try_into().expect("32 bytes"));
        }
        Ok(())
    }

    /// The memory to save once `newer` (row hashes of a part not yet
    /// admitted) is admitted: each row ID, eight bytes little-endian, and its
    /// line hash, in row order.
    fn memory_with(&self, newer: &[(i64, [u8; 32])]) -> Arc<[u8]> {
        let mut rows: std::collections::BTreeMap<i64, &[u8; 32]> = self
            .hashes
            .iter()
            .map(|(rowid, hash)| (*rowid, hash))
            .collect();
        rows.extend(newer.iter().map(|(rowid, hash)| (*rowid, hash)));
        let mut bytes = Vec::with_capacity(rows.len() * MEMORY_ENTRY);
        for (rowid, hash) in rows {
            bytes.extend_from_slice(&rowid.to_le_bytes());
            bytes.extend_from_slice(hash);
        }
        bytes.into()
    }
}

/// One remembered row in a saved memory: its ID and line hash.
const MEMORY_ENTRY: usize = 8 + 32;

impl SqliteRows {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "sqlite".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }

    pub fn poll(&self, cursor: &Value, state: &mut RowState) -> Result<Acquired, DriverError> {
        let mut previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        if let Some((held, hashes)) = state.held.take() {
            if held.admitted(cursor) {
                state.hashes.extend(hashes);
                state.polled = true;
            } else {
                let acquired = held.acquired(cursor)?;
                state.held = Some((held, hashes));
                return Ok(acquired);
            }
        }
        let unavailable = |reason: String| Acquired {
            input: self.input(),
            observation: ObservationFacts {
                unavailable: Some(reason),
                observed_at: Some(Timestamp::now()),
                ..Default::default()
            },
            revision: RevisionChange::Same,
            material: None,
            cursor_without_material: cursor.clone(),
        };
        let facts = match file_facts(&self.path) {
            Ok(f) => f,
            Err(e) => return Ok(unavailable(format!("{}: {e}", self.path.display()))),
        };
        let generation = format!(
            "{}:{}:{}",
            self.host,
            facts.device.expect("file device"),
            facts.inode.expect("file inode")
        );
        if previous.database_generation.as_deref() != Some(&generation) {
            state.hashes.clear();
            state.polled = false;
            previous = Cursor {
                database_generation: Some(generation.clone()),
                last_order: previous.last_order,
                ..Cursor::default()
            };
        }
        let now = Timestamp::now();
        let now_ns = i64::try_from(now.unix_ns()).unwrap_or(i64::MAX);
        let connection = match open_for_read(&self.path) {
            Ok(c) => c,
            Err(e) => return Ok(unavailable(e)),
        };
        let (mutable, full) = match self.changes {
            Changes::InPlace {
                mutable,
                full_pass_secs,
            } => (
                Some(mutable),
                !state.polled
                    || now_ns.saturating_sub(previous.last_full_pass_ns)
                        >= full_pass_secs.saturating_mul(1_000_000_000),
            ),
            Changes::Appended => (None, false),
        };

        let columns: Vec<String> = {
            let mut stmt = connection
                .prepare(&format!(
                    "SELECT name FROM pragma_table_info('{}')",
                    self.table
                ))
                .map_err(|e| DriverError::Io(e.to_string()))?;
            stmt.query_map([], |r| r.get::<_, String>(0))
                .and_then(|rows| rows.collect::<Result<_, _>>())
                .map_err(|e| DriverError::Io(e.to_string()))?
        };
        if let Some(missing) = self
            .required
            .iter()
            .find(|c| !columns.iter().any(|have| have == *c))
        {
            return Ok(unavailable(format!(
                "table {} has no column {missing}",
                self.table
            )));
        }
        let mut open: std::collections::BTreeSet<i64> = previous.open.iter().copied().collect();
        let reopen = if open.is_empty() {
            String::new()
        } else {
            let ids: Vec<String> = open.iter().map(i64::to_string).collect();
            format!(" OR rowid IN ({})", ids.join(","))
        };
        let filter = match (full, mutable) {
            (true, _) => "1".to_string(),
            (false, Some(m)) => format!("rowid > ?2 OR ({m}){reopen}"),
            (false, None) => "rowid > ?2".to_string(),
        };
        let open_column = mutable.map_or("0".to_string(), |m| format!("({m})"));
        let page = match self.changes {
            Changes::InPlace { .. } => String::new(),
            Changes::Appended => format!(" LIMIT {ROWS_PER_PART}"),
        };
        // Both parameters are always bound; this keeps every variant of the
        // statement at the same parameter count.
        let sql = format!(
            "SELECT {open_column} AS _open, rowid AS _rowid, * FROM {} WHERE ({filter}) AND ?1 IS NOT NULL AND ?2 IS NOT NULL ORDER BY rowid{page}",
            self.from.unwrap_or(self.table)
        );
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|e| DriverError::Io(e.to_string()))?;
        let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut staged = tempfile::tempfile().map_err(|e| DriverError::Io(e.to_string()))?;
        let mut length = 0usize;
        let mut hashes = Vec::new();
        let mut records = Vec::new();
        let mut ends: Vec<(usize, i64)> = Vec::new();
        let mut max_rowid = previous.max_rowid;
        {
            let mut rows = stmt
                .query(rusqlite::params![
                    now_ns - 86_400_000_000_000,
                    previous.max_rowid
                ])
                .map_err(|e| DriverError::Io(e.to_string()))?;
            while let Some(row) = rows.next().map_err(|e| DriverError::Io(e.to_string()))? {
                let mut object = Map::new();
                let is_open: i64 = row.get(0).unwrap_or(0);
                let rowid: i64 = row.get(1).map_err(|e| DriverError::Io(e.to_string()))?;
                for (i, name) in names.iter().enumerate().skip(1) {
                    let value = match row.get_ref(i).map_err(|e| DriverError::Io(e.to_string()))? {
                        ValueRef::Null => Value::Null,
                        ValueRef::Integer(n) => json!(n),
                        ValueRef::Real(f) => json!(f),
                        ValueRef::Text(t) => match std::str::from_utf8(t) {
                            Ok(s) => json!(s),
                            Err(_) => json!({"invalid_utf8_hex": hex(t)}),
                        },
                        ValueRef::Blob(b) => json!({"blob_hex": hex(b)}),
                    };
                    object.insert(name.clone(), value);
                }
                max_rowid = max_rowid.max(rowid);
                if is_open != 0 {
                    open.insert(rowid);
                } else {
                    open.remove(&rowid);
                }
                let line = Value::Object(object).to_string();
                if mutable.is_some() {
                    let hash = *blake3::hash(line.as_bytes()).as_bytes();
                    if state.hashes.get(&rowid) == Some(&hash) {
                        continue;
                    }
                    hashes.push((rowid, hash));
                }
                let start = length;
                staged
                    .write_all(line.as_bytes())
                    .and_then(|_| staged.write_all(b"\n"))
                    .map_err(|e| DriverError::Io(e.to_string()))?;
                length = length.checked_add(line.len() + 1).ok_or_else(|| {
                    DriverError::Io("row material exceeds addressable size".into())
                })?;
                records.push((start, length));
                ends.push((length, rowid));
            }
        }
        drop(stmt);
        connection.execute_batch("COMMIT").ok();
        match file_facts(&self.path) {
            Ok(after) if after.device == facts.device && after.inode == facts.inode => {}
            _ => {
                return Ok(unavailable(
                    "database generation changed during the read".into(),
                ));
            }
        }

        let last_full = if full {
            now_ns
        } else {
            previous.last_full_pass_ns
        };
        let open: Vec<i64> = open.into_iter().collect();
        if records.is_empty() {
            state.polled = true;
            let unchanged = Cursor {
                max_rowid,
                last_full_pass_ns: last_full,
                open,
                ..previous
            };
            return Ok(Acquired {
                input: self.input(),
                observation: ObservationFacts {
                    changed: false,
                    ..facts
                },
                revision: RevisionChange::Same,
                material: None,
                cursor_without_material: serde_json::to_value(unchanged)
                    .expect("cursor serializes"),
            });
        }
        let id = MaterialId::new();
        // Orders are the snapshot's time plus the row's position in it, and
        // never below what earlier polls gave out, so a row read again later
        // (in this poll or a later one) is always newer.
        let base = now_ns.max(previous.last_order.saturating_add(1));
        let manifest = sinex_model::Manifest::from_json(
            json!({"role": "records", "path": self.path.display().to_string(), "host": self.host, "table": self.table, "from": self.from, "media_type": "application/x-ndjson", "full_pass": full, "read_at_ns": now_ns, "order_base": base, "read_from": "database", "database_generation": generation}),
        );
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        let changed = previous
            .changed_since_memory
            .saturating_add(hashes.len() as u64);
        let added = hashes
            .iter()
            .filter(|(rowid, _)| !state.hashes.contains_key(rowid))
            .count();
        // An appended table's reader remembers nothing but its cursor.
        let due = mutable.is_some()
            && super::memory_due(
                part_index,
                previous.memory_part,
                changed,
                (state.hashes.len() + added) as u64,
            );
        let memory = due.then(|| state.memory_with(&hashes));
        let scanned = Cursor {
            database_generation: Some(generation),
            max_rowid,
            last_full_pass_ns: last_full,
            last_order: base.saturating_add(length as i64),
            material: Some(id),
            part_index,
            open,
            through: length,
            memory_part: if due {
                Some(part_index)
            } else {
                previous.memory_part
            },
            changed_since_memory: if due { 0 } else { changed },
        };
        let previous_part = previous.material;
        let cursor_at = move |end: usize| {
            let upto = ends
                .iter()
                .take_while(|(e, _)| *e <= end)
                .map(|(_, r)| *r)
                .max()
                .unwrap_or(previous.max_rowid);
            let last = end == length;
            serde_json::to_value(Cursor {
                max_rowid: upto.max(previous.max_rowid),
                last_full_pass_ns: if last {
                    scanned.last_full_pass_ns
                } else {
                    previous.last_full_pass_ns
                },
                last_order: base.saturating_add(end as i64),
                open: if last {
                    scanned.open.clone()
                } else {
                    previous.open.clone()
                },
                through: end,
                memory_part: if last {
                    scanned.memory_part
                } else {
                    previous.memory_part
                },
                changed_since_memory: if last {
                    scanned.changed_since_memory
                } else {
                    previous.changed_since_memory
                },
                ..scanned.clone()
            })
            .expect("cursor serializes")
        };
        let acquired = Acquired {
            input: self.input(),
            observation: facts,
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes: Vec::new(),
                stored: Some(StoredBytes::FramedFile(Arc::new(staged))),
                records,
                part_index,
                previous_part,
                reading_order: super::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(move |end| Ok(cursor_at(end))),
                aligned: false,
                removed: Vec::new(),
                memory,
                owned_files: Vec::new(),
            }),
            cursor_without_material: Value::Null,
        };
        let held = super::held::HeldMaterial::hold(acquired, cursor)?;
        let acquired = held.acquired(cursor)?;
        state.held = Some((held, hashes));
        Ok(acquired)
    }
}

/// SQLite owns the committed view, including WAL and rollback-journal rules.
/// A locked database is unavailable: a stable main file may contain spilled
/// uncommitted pages, so copying it cannot replace a read transaction.
fn open_for_read(path: &std::path::Path) -> Result<Connection, String> {
    let c = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("cannot open read-only: {e}"))?;
    c.busy_timeout(std::time::Duration::from_secs(2))
        .map_err(|e| e.to_string())?;
    c.execute_batch("BEGIN")
        .map_err(|e| format!("cannot begin committed read: {e}"))?;
    // Establish the snapshot before inspecting its schema and rows.
    c.query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))
        .map_err(|e| format!("cannot read committed snapshot: {e}"))?;
    Ok(c)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests;
