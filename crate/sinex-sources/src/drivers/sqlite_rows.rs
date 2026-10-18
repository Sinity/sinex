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
//! already stored are left out of the material.
use super::{Acquired, DriverError, NewMaterial, file_facts};
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::{
    InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};
use std::collections::HashMap;
use std::path::PathBuf;

pub struct SqliteRows {
    pub path: PathBuf,
    pub host: String,
    pub table: &'static str,
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
    pub material: Option<MaterialId>,
    pub part_index: u32,
}

/// What this driver already stored, kept for the life of the source task: row
/// hashes by rowid, and the rows that were still open (matched the mutable
/// predicate) when last read. Open rows are re-read by rowid, because a row
/// that changes stops matching the predicate that selected it. An empty
/// memory (after a restart) triggers a full pass, whose repeats admission
/// suppresses.
#[derive(Default)]
pub struct RowMemory {
    hashes: HashMap<i64, [u8; 32]>,
    open: std::collections::BTreeSet<i64>,
}

impl SqliteRows {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "sqlite".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }

    pub fn poll(&self, cursor: &Value, memory: &mut RowMemory) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
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
        let full = memory.hashes.is_empty()
            || now_ns.saturating_sub(previous.last_full_pass_ns)
                >= self.full_pass_secs.saturating_mul(1_000_000_000);

        let connection = match Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) {
            Ok(c) => c,
            Err(e) => return Ok(unavailable(format!("cannot open read-only: {e}"))),
        };
        let _ = connection.busy_timeout(std::time::Duration::from_secs(2));
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
        let reopen = if memory.open.is_empty() {
            String::new()
        } else {
            let ids: Vec<String> = memory.open.iter().map(i64::to_string).collect();
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
            self.table
        );
        // One read transaction: a consistent snapshot even while the provider writes.
        connection
            .execute_batch("BEGIN")
            .map_err(|e| DriverError::Io(e.to_string()))?;
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|e| DriverError::Io(e.to_string()))?;
        let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut bytes = Vec::new();
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
                let open: i64 = row.get(0).unwrap_or(0);
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
                let rowid = object
                    .get("_rowid")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                max_rowid = max_rowid.max(rowid);
                if open != 0 {
                    memory.open.insert(rowid);
                } else {
                    memory.open.remove(&rowid);
                }
                let line = Value::Object(object).to_string();
                let hash = *blake3::hash(line.as_bytes()).as_bytes();
                if memory.hashes.get(&rowid) == Some(&hash) {
                    continue;
                }
                memory.hashes.insert(rowid, hash);
                let start = bytes.len();
                bytes.extend_from_slice(line.as_bytes());
                bytes.push(b'\n');
                records.push((start, bytes.len()));
                ends.push((bytes.len(), rowid));
            }
        }
        drop(stmt);
        connection.execute_batch("COMMIT").ok();

        let last_full = if full {
            now_ns
        } else {
            previous.last_full_pass_ns
        };
        if records.is_empty() {
            let unchanged = Cursor {
                max_rowid,
                last_full_pass_ns: last_full,
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
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        let cursor_at = move |end: usize| -> Value {
            // Rows are in rowid order, so the cursor is the last row at or before `end`.
            let upto = ends
                .iter()
                .take_while(|(e, _)| *e <= end)
                .map(|(_, r)| *r)
                .max()
                .unwrap_or(previous.max_rowid);
            serde_json::to_value(Cursor {
                max_rowid: upto.max(previous.max_rowid),
                last_full_pass_ns: last_full,
                material: Some(id),
                part_index,
            })
            .expect("cursor serializes")
        };
        Ok(Acquired {
            input: self.input(),
            observation: facts,
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes,
                records,
                part_index,
                previous_part: previous.material,
                manifest: json!({"path": self.path.display().to_string(), "host": self.host, "table": self.table, "media_type": "application/x-ndjson", "full_pass": full}),
                // Every row in this snapshot was read now; a later snapshot is newer.
                reading_order: Box::new(move |_| ReadingOrder(now_ns)),
                cursor_at: Box::new(cursor_at),
            }),
            cursor_unchanged: Value::Null,
        })
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sinex-sqlite-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("h.db");
        let c = Connection::open(&path).unwrap();
        c.execute_batch("CREATE TABLE t (id TEXT, n INTEGER, b BLOB); INSERT INTO t VALUES ('a', 1, x'00ff'), ('b', -1, NULL);").unwrap();
        path
    }

    fn driver(path: PathBuf) -> SqliteRows {
        SqliteRows {
            path,
            host: "h".into(),
            table: "t",
            required: &["id", "n"],
            mutable: Some("n = -1"),
            full_pass_secs: 3600,
        }
    }

    fn lines(a: &Acquired) -> Vec<Value> {
        let m = a.material.as_ref().unwrap();
        m.records
            .iter()
            .map(|(s, e)| serde_json::from_slice(&m.bytes[*s..*e]).unwrap())
            .collect()
    }

    #[test]
    fn reads_rows_then_only_new_and_changed_ones() {
        let path = db();
        let d = driver(path.clone());
        let mut memory = RowMemory::default();
        let first = d.poll(&Value::Null, &mut memory).unwrap();
        let rows = lines(&first);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["b"], json!({"blob_hex": "00ff"}));
        let m = first.material.as_ref().unwrap();
        let cursor = (m.cursor_at)(m.bytes.len());
        assert_eq!(cursor["max_rowid"], json!(2));

        let c = Connection::open(&path).unwrap();
        c.execute_batch("UPDATE t SET n = 0 WHERE id = 'b'; INSERT INTO t VALUES ('c', 5, NULL);")
            .unwrap();
        let next = d.poll(&cursor, &mut memory).unwrap();
        let rows = lines(&next);
        assert_eq!(
            rows.iter().map(|r| r["id"].clone()).collect::<Vec<_>>(),
            vec![json!("b"), json!("c")],
            "the changed row and the new row; the unchanged row is left out"
        );
    }

    #[test]
    fn missing_column_is_named() {
        let mut d = driver(db());
        d.required = &["id", "nope"];
        let a = d.poll(&Value::Null, &mut RowMemory::default()).unwrap();
        assert_eq!(
            a.observation.unavailable.as_deref(),
            Some("table t has no column nope")
        );
    }
}
