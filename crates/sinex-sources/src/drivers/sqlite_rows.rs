//! `SqliteRows`: another program's SQLite database (Atuin, ActivityWatch,
//! Chromium). Each poll reads one consistent snapshot: rows after the cursor,
//! plus rows that can still change, because providers update rows in place
//! (Atuin fills in duration and exit when a command ends, and sets
//! `deleted_at`). A full pass runs periodically, and on the first poll after a
//! start, so an update outside the window is still seen. The provider's
//! schema is checked on open and a missing column is named.
//!
//! Rows are stored as a JSON-lines material ("rows part"); each line is one
//! row's columns, so the record location is the line's offset and a replay
//! reads exactly the bytes the parser saw. Rows identical to what this driver
//! already stored are left out of the material. Pending material and row hashes
//! are retained until its final admitted cursor confirms them.
//!
//! What the driver already stored survives a restart: the rows still open
//! travel in the cursor, and the row hashes are rebuilt from the stored parts
//! ([`RowState::rebuild`]), so a restart does not read and store every row
//! again.
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
    /// Rows that can still change, as a SQL predicate over the table. `?1` is
    /// the start of the mutable window, one day ago, in nanoseconds since the
    /// epoch.
    pub mutable: Option<&'static str>,
    /// How often every row is read and compared, in seconds.
    pub full_pass_secs: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
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
}

/// What this driver already stored: the hash of each row's stored line, by
/// rowid. Kept for the life of the source task and rebuilt from the stored
/// parts after a restart. An empty state (no stored parts yet) triggers a
/// full pass, whose repeats admission suppresses.
#[derive(Debug, Default)]
pub struct RowState {
    hashes: HashMap<i64, [u8; 32]>,
    /// Whether this process has polled since the state was made: the first
    /// poll after a start is a full pass even when the hashes were rebuilt,
    /// so an update made while Sinex was down is seen (law L20).
    polled: bool,
    pending: Option<Pending>,
}

#[derive(Debug)]
struct Pending {
    stored: StoredBytes,
    length: usize,
    records: Vec<(usize, usize)>,
    ends: Vec<(usize, i64)>,
    hashes: Vec<(i64, [u8; 32])>,
    id: MaterialId,
    previous: Cursor,
    scanned: Cursor,
    base: i64,
    manifest: Value,
    observation: ObservationFacts,
}

impl Pending {
    fn acquired(&self, input: InputRef, cursor: &Cursor) -> Acquired {
        let through = if cursor.material == Some(self.id) {
            cursor.through
        } else {
            0
        };
        let records = self
            .records
            .iter()
            .copied()
            .filter(|&(_, end)| end > through)
            .collect();
        let previous = self.previous.clone();
        let scanned = self.scanned.clone();
        let ends = self.ends.clone();
        let base = self.base;
        let length = self.length;
        Acquired {
            input,
            observation: self.observation.clone(),
            revision: RevisionChange::Snapshot,
            material: (!self.records.is_empty()).then(|| NewMaterial {
                id: self.id,
                bytes: Vec::new(),
                stored: Some(self.stored.clone()),
                records,
                part_index: scanned.part_index,
                previous_part: previous.material,
                reading_order: super::reading_order(&self.manifest),
                manifest: self.manifest.clone(),
                cursor_at: Box::new({
                    let cursor = move |end| {
                        let upto = ends
                            .iter()
                            .take_while(|(e, _)| *e <= end)
                            .map(|(_, r)| *r)
                            .max()
                            .unwrap_or(previous.max_rowid);
                        serde_json::to_value(Cursor {
                            max_rowid: upto.max(previous.max_rowid),
                            last_full_pass_ns: if end == length {
                                scanned.last_full_pass_ns
                            } else {
                                previous.last_full_pass_ns
                            },
                            last_order: base.saturating_add(end as i64),
                            open: if end == length {
                                scanned.open.clone()
                            } else {
                                previous.open.clone()
                            },
                            through: end,
                            ..scanned.clone()
                        })
                        .expect("cursor serializes")
                    };
                    move |end| Ok(cursor(end))
                }),
                aligned: false,
                vanished: Vec::new(),
            }),
            cursor_unchanged: serde_json::to_value(&self.scanned).expect("cursor serializes"),
        }
    }
}

impl RowState {
    /// The state after the stored rows parts `materials`, in order: the
    /// last stored line of each row wins.
    pub fn rebuild<'a>(materials: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut state = Self::default();
        for bytes in materials {
            for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
                let Ok(row) = serde_json::from_slice::<Map<String, Value>>(line) else {
                    continue;
                };
                if let Some(rowid) = row.get("_rowid").and_then(Value::as_i64) {
                    state.hashes.insert(rowid, *blake3::hash(line).as_bytes());
                }
            }
        }
        state
    }

    /// Restore the exact registered material, including unadmitted suffix rows.
    pub fn restore_pending(
        &mut self,
        reference: sinex_model::MaterialRef,
        stored: super::RetainedRecords,
        cursor: &Value,
    ) -> Result<(), DriverError> {
        let current: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        if current.material == Some(reference.id) && current.through as u64 >= reference.length {
            return Ok(());
        }
        let resume = &reference.manifest["row_resume"];
        let previous: Cursor = serde_json::from_value(resume["previous"].clone())
            .map_err(|e| DriverError::Cursor(e.to_string()))?;
        if current.material != Some(reference.id) && current != previous {
            return Err(DriverError::Cursor(
                "stored SQLite predecessor cursor differs from reservation".into(),
            ));
        }
        let scanned: Cursor = serde_json::from_value(resume["scanned"].clone())
            .map_err(|e| DriverError::Cursor(e.to_string()))?;
        let ends: Vec<(usize, i64)> = serde_json::from_value(resume["ends"].clone())
            .map_err(|e| DriverError::Cursor(e.to_string()))?;
        let length = usize::try_from(reference.length)
            .map_err(|_| DriverError::Io("row material exceeds address space".into()))?;
        if scanned.material != Some(reference.id)
            || scanned.through != length
            || ends.last().map(|p| p.0) != Some(length)
        {
            return Err(DriverError::Cursor(
                "stored SQLite resume plan differs from material".into(),
            ));
        }
        let mut start = 0;
        let mut hashes = Vec::new();
        let mut records = Vec::new();
        for &(end, rowid) in &ends {
            if end <= start || end > length {
                return Err(DriverError::Cursor(
                    "stored SQLite row ranges are invalid".into(),
                ));
            }
            let mut line = vec![0; end - start];
            stored
                .bytes
                .read_exact_at(start as u64, &mut line)
                .map_err(|e| DriverError::Io(e.to_string()))?;
            if line.pop() != Some(b'\n') {
                return Err(DriverError::Cursor(
                    "stored SQLite row has no delimiter".into(),
                ));
            }
            if line.iter().all(|b| *b == b' ') {
                records.push((start, end));
                start = end;
                continue;
            }
            hashes.push((rowid, *blake3::hash(&line).as_bytes()));
            records.push((start, end));
            start = end;
        }
        self.pending = Some(Pending {
            stored: StoredBytes::FramedReader(stored),
            length,
            records,
            ends,
            hashes,
            id: reference.id,
            previous,
            scanned,
            base: resume["base"]
                .as_i64()
                .ok_or_else(|| DriverError::Cursor("missing SQLite order base".into()))?,
            manifest: reference.manifest,
            observation: ObservationFacts::default(),
        });
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }
}

impl SqliteRows {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "sqlite".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }

    pub fn poll(&self, cursor: &Value, state: &mut RowState) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        if let Some(pending) = state.pending.take() {
            if previous == pending.scanned {
                state.hashes.extend(pending.hashes);
                state.polled = true;
            } else {
                let acquired = pending.acquired(self.input(), &previous);
                state.pending = Some(pending);
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
            cursor_unchanged: cursor.clone(),
        };
        let facts = match file_facts(&self.path) {
            Ok(f) => f,
            Err(e) => return Ok(unavailable(format!("{}: {e}", self.path.display()))),
        };
        let now = Timestamp::now();
        let now_ns = i64::try_from(now.unix_ns()).unwrap_or(i64::MAX);
        let connection = match open_for_read(&self.path) {
            Ok(c) => c,
            Err(e) => return Ok(unavailable(e)),
        };
        let full = !state.polled
            || now_ns.saturating_sub(previous.last_full_pass_ns)
                >= self.full_pass_secs.saturating_mul(1_000_000_000);

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
        let filter = match (full, self.mutable) {
            (true, _) => "1".to_string(),
            (false, Some(m)) => format!("rowid > ?2 OR ({m}){reopen}"),
            (false, None) => "rowid > ?2".to_string(),
        };
        let open_column = self.mutable.map_or("0".to_string(), |m| format!("({m})"));
        // Both parameters are always bound; this keeps every variant of the
        // statement at the same parameter count.
        let sql = format!(
            "SELECT {open_column} AS _open, rowid AS _rowid, * FROM {} WHERE ({filter}) AND ?1 IS NOT NULL AND ?2 IS NOT NULL ORDER BY rowid",
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
                let hash = *blake3::hash(line.as_bytes()).as_bytes();
                if state.hashes.get(&rowid) == Some(&hash) {
                    continue;
                }
                hashes.push((rowid, hash));
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
                cursor_unchanged: serde_json::to_value(unchanged).expect("cursor serializes"),
            });
        }
        let id = MaterialId::new();
        // Orders are the snapshot's time plus the row's position in it, and
        // never below what earlier polls gave out, so a row read again later
        // (in this poll or a later one) is always newer.
        let base = now_ns.max(previous.last_order.saturating_add(1));
        let mut manifest = json!({"path": self.path.display().to_string(), "host": self.host, "table": self.table, "from": self.from, "media_type": "application/x-ndjson", "full_pass": full, "read_at_ns": now_ns, "order_base": base, "read_from": "database"});
        manifest["role"] = json!("records");
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        let pending = Pending {
            stored: StoredBytes::FramedFile(Arc::new(staged)),
            length,
            records,
            ends,
            hashes,
            id,
            previous: previous.clone(),
            scanned: Cursor {
                max_rowid,
                last_full_pass_ns: last_full,
                last_order: base.saturating_add(length as i64),
                material: Some(id),
                part_index,
                open,
                through: length,
            },
            base,
            manifest,
            observation: facts,
        };
        let mut pending = pending;
        pending.manifest["row_resume"] = json!({"previous":pending.previous,"scanned":pending.scanned,"base":pending.base,"ends":pending.ends});
        let acquired = pending.acquired(self.input(), &Cursor::default());
        state.pending = Some(pending);
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
