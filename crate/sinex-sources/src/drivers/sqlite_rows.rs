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
//!
//! What the driver already stored survives a restart: the rows still open
//! travel in the cursor, and the row hashes are rebuilt from the stored parts
//! ([`RowMemory::rebuild`]), so a restart does not read and store every row
//! again.
use super::{Acquired, DriverError, NewMaterial, file_facts};
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::{InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};
use std::collections::HashMap;
use std::path::PathBuf;

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
    /// rebuilds the row memory from those bytes only.
    #[serde(default)]
    pub through: usize,
}

/// What this driver already stored: the hash of each row's stored line, by
/// rowid. Kept for the life of the source task and rebuilt from the stored
/// parts after a restart. An empty memory (no stored parts yet) triggers a
/// full pass, whose repeats admission suppresses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowMemory {
    hashes: HashMap<i64, [u8; 32]>,
}

impl RowMemory {
    /// The memory after the stored rows parts `materials`, in order: the
    /// last stored line of each row wins.
    pub fn rebuild<'a>(materials: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut memory = Self::default();
        for bytes in materials {
            for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
                let Ok(row) = serde_json::from_slice::<Map<String, Value>>(line) else {
                    continue;
                };
                if let Some(rowid) = row.get("_rowid").and_then(Value::as_i64) {
                    memory.hashes.insert(rowid, *blake3::hash(line).as_bytes());
                }
            }
        }
        memory
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

        let (connection, copy) = match open_for_read(&self.path) {
            Ok(opened) => opened,
            Err(e) => return Ok(unavailable(e)),
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
                let is_open: i64 = row.get(0).unwrap_or(0);
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
                if is_open != 0 {
                    open.insert(rowid);
                } else {
                    open.remove(&rowid);
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
        let open: Vec<i64> = open.into_iter().collect();
        if records.is_empty() {
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
        let manifest = json!({"path": self.path.display().to_string(), "host": self.host, "table": self.table, "from": self.from, "media_type": "application/x-ndjson", "full_pass": full, "read_at_ns": now_ns, "order_base": base, "read_from": if copy.is_some() { "verified copy" } else { "database" }});
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
                last_order: base.saturating_add(end as i64),
                material: Some(id),
                part_index,
                open: open.clone(),
                through: end,
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
                stored_file: None,
                records,
                part_index,
                previous_part: previous.material,
                reading_order: super::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(cursor_at),
                aligned: false,
            }),
            cursor_unchanged: Value::Null,
        })
    }
}

/// Open `path` read-only. When its program holds it locked (Chromium keeps
/// `History` locked while it runs), copy it and read the copy, accepted only
/// if the file's size and times did not change while copying (`y0o3.4`). The
/// returned guard removes the copy.
fn open_for_read(path: &std::path::Path) -> Result<(Connection, Option<Copy>), String> {
    let open = |p: &std::path::Path, immutable: bool| -> rusqlite::Result<Connection> {
        let mut flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let target = if immutable {
            flags |= OpenFlags::SQLITE_OPEN_URI;
            format!("file:{}?immutable=1", p.display())
        } else {
            p.display().to_string()
        };
        let c = Connection::open_with_flags(target, flags)?;
        c.busy_timeout(std::time::Duration::from_secs(2))?;
        // Touch the schema so a lock shows now rather than mid-read.
        c.query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))?;
        Ok(c)
    };
    match open(path, false) {
        Ok(c) => return Ok((c, None)),
        Err(e) if !is_locked(&e) => return Err(format!("cannot open read-only: {e}")),
        Err(_) => {}
    }
    let facts = |p: &std::path::Path| file_facts(p).map(|f| (f.size, f.mtime_ns, f.ctime_ns));
    for _ in 0..3 {
        let before = facts(path).map_err(|e| e.to_string())?;
        let copy = Copy::new()?;
        std::fs::copy(path, &copy.file).map_err(|e| format!("copy {}: {e}", path.display()))?;
        if facts(path).map_err(|e| e.to_string())? == before {
            let c = open(&copy.file, true).map_err(|e| format!("cannot read the copy: {e}"))?;
            return Ok((c, Some(copy)));
        }
    }
    Err(format!(
        "{} is locked and kept changing while being copied",
        path.display()
    ))
}

fn is_locked(e: &rusqlite::Error) -> bool {
    matches!(
        e.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

/// A temporary copy of a locked database, removed when dropped.
struct Copy {
    dir: PathBuf,
    file: PathBuf,
}

impl Copy {
    fn new() -> Result<Self, String> {
        let dir = std::env::temp_dir().join(format!("sinex-sqlite-copy-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let file = dir.join("db");
        Ok(Self { dir, file })
    }
}

impl Drop for Copy {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
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
            from: None,
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
    fn a_restart_with_rebuilt_memory_stores_nothing_new() {
        let path = db();
        let d = driver(path.clone());
        let mut memory = RowMemory::default();
        let first = d.poll(&Value::Null, &mut memory).unwrap();
        let m = first.material.as_ref().unwrap();
        let cursor = (m.cursor_at)(m.bytes.len());
        assert_eq!(
            cursor["open"],
            json!([2]),
            "the running row travels in the cursor"
        );
        assert!(m.manifest["read_at_ns"].as_i64().is_some());

        // A new process: the memory comes back from the stored part.
        let mut rebuilt = RowMemory::rebuild([m.bytes.as_slice()]);
        assert_eq!(rebuilt, memory);
        let again = d.poll(&cursor, &mut rebuilt).unwrap();
        assert!(again.material.is_none(), "no full pass, no new part");

        // The open row settles: read by rowid, though it no longer matches.
        Connection::open(&path)
            .unwrap()
            .execute_batch("UPDATE t SET n = 7 WHERE id = 'b'")
            .unwrap();
        let settled = d.poll(&again.cursor_unchanged, &mut rebuilt).unwrap();
        assert_eq!(lines(&settled)[0]["n"], json!(7));
        let s = settled.material.as_ref().unwrap();
        assert_eq!((s.cursor_at)(s.bytes.len())["open"], Value::Null);
    }

    #[test]
    fn reading_orders_only_grow() {
        let path = db();
        let d = driver(path.clone());
        let mut memory = RowMemory::default();
        let first = d.poll(&Value::Null, &mut memory).unwrap();
        let m = first.material.as_ref().unwrap();
        let (a, b) = (
            (m.reading_order)(m.records[0].0),
            (m.reading_order)(m.records[1].0),
        );
        assert!(b > a, "later in one snapshot is newer");
        let cursor = (m.cursor_at)(m.bytes.len());
        let c = Connection::open(&path).unwrap();
        c.execute_batch("UPDATE t SET n = 7 WHERE id = 'a';")
            .unwrap();
        // A full pass reads every row again; force one by forgetting.
        let next = d.poll(&cursor, &mut RowMemory::default()).unwrap();
        let n = next.material.as_ref().unwrap();
        assert!(
            (n.reading_order)(0) > b,
            "a later poll is newer than anything before"
        );
    }

    #[test]
    fn a_locked_database_is_read_from_a_verified_copy() {
        let path = db();
        let holder = Connection::open(&path).unwrap();
        holder
            .execute_batch("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE; UPDATE t SET n = n;")
            .unwrap();
        let a = driver(path)
            .poll(&Value::Null, &mut RowMemory::default())
            .unwrap();
        assert!(
            a.observation.unavailable.is_none(),
            "{:?}",
            a.observation.unavailable
        );
        let m = a.material.as_ref().unwrap();
        assert_eq!(m.records.len(), 2);
        assert_eq!(m.manifest["read_from"], "verified copy");
        drop(holder);
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
