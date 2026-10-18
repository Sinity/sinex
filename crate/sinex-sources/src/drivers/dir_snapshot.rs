//! `DirSnapshot`: a directory of files read as a whole (a notes vault, a
//! Maildir), architecture §5.3.
//!
//! Each poll walks the tree comparing file facts with what was already read.
//! New and changed files are read in full and stored together as one material:
//! their exact bytes one after another, with a manifest member per file
//! (path, range in the material, size, modification time). One file is one
//! record. A file whose facts changed but whose bytes did not is skipped.
//!
//! When a walk completes without reaching the part size, files read before but
//! not seen now are recorded as vanished: a member holding a short marker line,
//! so each has its own record location and the parser reads the removal.
//!
//! The memory of what was read lives for the source task. After a restart the
//! first walk reads everything again and admission suppresses the repeats.
use super::{Acquired, DriverError, NewMaterial, file_facts};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::{InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct DirSnapshot {
    pub root: PathBuf,
    pub host: String,
    /// Which files to read, by path relative to the root.
    pub select: fn(&str) -> bool,
    /// Bytes per material; a walk that reaches it continues on the next poll.
    pub part_bytes: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub material: Option<MaterialId>,
    pub part_index: u32,
    /// When the last walk that saw every file finished.
    pub last_complete_scan_ns: Option<i128>,
}

/// What was already read, by relative path: size, modification time, hash.
#[derive(Default)]
pub struct DirMemory {
    files: HashMap<String, (u64, i128, [u8; 32])>,
}

/// The marker a vanished file's member holds.
pub fn vanished_marker(path: &str) -> Vec<u8> {
    let mut line = json!({"vanished": path}).to_string().into_bytes();
    line.push(b'\n');
    line
}

impl DirSnapshot {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "directory".into(),
            locator: self.root.display().to_string(),
            host: self.host.clone(),
        }
    }

    pub fn poll(&self, cursor: &Value, memory: &mut DirMemory) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let mut facts = match file_facts(&self.root) {
            Ok(f) => f,
            Err(e) => {
                return Ok(Acquired {
                    input: self.input(),
                    observation: ObservationFacts {
                        unavailable: Some(format!("{}: {e}", self.root.display())),
                        changed: false,
                        observed_at: Some(Timestamp::now()),
                        ..Default::default()
                    },
                    revision: RevisionChange::Same,
                    material: None,
                    cursor_unchanged: cursor.clone(),
                });
            }
        };
        let now = Timestamp::now();
        // Sorted walk: a stable order makes materials and part cuts repeatable.
        let mut found = BTreeMap::new();
        walk(&self.root, &self.root, self.select, &mut found)?;

        let mut bytes = Vec::new();
        let mut records = Vec::new();
        let mut members = Vec::new();
        let mut complete = true;
        let mut seen = HashSet::new();
        for (relative, (size, mtime)) in &found {
            seen.insert(relative.clone());
            if let Some((s, m, _)) = memory.files.get(relative)
                && *s == *size
                && *m == *mtime
            {
                continue;
            }
            if !bytes.is_empty() && bytes.len() + *size as usize > self.part_bytes {
                complete = false;
                break;
            }
            let data = match std::fs::read(self.root.join(relative)) {
                Ok(d) => d,
                // Gone or unreadable between the walk and the read: the next
                // walk sees it again.
                Err(_) => continue,
            };
            let hash = *blake3::hash(&data).as_bytes();
            let same = memory
                .files
                .get(relative)
                .is_some_and(|(_, _, h)| *h == hash);
            memory.files.insert(relative.clone(), (*size, *mtime, hash));
            if same {
                continue;
            }
            let offset = bytes.len();
            members.push(json!({
                "path": relative,
                "stream_offset": offset,
                "length": data.len(),
                "mtime_ns": mtime.to_string(),
            }));
            records.push((offset, offset + data.len()));
            bytes.extend_from_slice(&data);
        }
        if complete {
            let mut gone: Vec<String> = memory
                .files
                .keys()
                .filter(|p| !seen.contains(*p))
                .cloned()
                .collect();
            gone.sort();
            for relative in gone {
                memory.files.remove(&relative);
                let marker = vanished_marker(&relative);
                let offset = bytes.len();
                members.push(json!({
                    "path": relative,
                    "stream_offset": offset,
                    "length": marker.len(),
                    "vanished_at_ns": now.unix_ns().to_string(),
                }));
                records.push((offset, offset + marker.len()));
                bytes.extend_from_slice(&marker);
            }
        }
        let scanned = Cursor {
            last_complete_scan_ns: if complete {
                Some(now.unix_ns())
            } else {
                previous.last_complete_scan_ns
            },
            ..previous.clone()
        };
        if records.is_empty() {
            facts.changed = false;
            return Ok(Acquired {
                input: self.input(),
                observation: facts,
                revision: RevisionChange::Same,
                material: None,
                cursor_unchanged: serde_json::to_value(scanned).expect("cursor serializes"),
            });
        }
        // Every file read by one walk is as new as the walk; the records are
        // listed so a replay or an excision finds them in the bundle.
        let manifest = json!({
            "path": self.root.display().to_string(),
            "host": self.host,
            "media_type": "application/x-sinex-file-bundle",
            "scan_complete": complete,
            "members": members,
            "records": records,
            "read_at_ns": i64::try_from(now.unix_ns()).unwrap_or(i64::MAX),
        });
        let id = MaterialId::new();
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        let cursor_at = move |_end: usize| -> Value {
            serde_json::to_value(Cursor {
                material: Some(id),
                part_index,
                ..scanned.clone()
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

fn walk(
    root: &Path,
    dir: &Path,
    select: fn(&str) -> bool,
    found: &mut BTreeMap<String, (u64, i128)>,
) -> Result<(), DriverError> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| DriverError::Io(format!("{}: {e}", dir.display())))?;
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if kind.is_dir() {
            walk(root, &path, select, found)?;
        } else if kind.is_file() {
            let Ok(relative) = path.strip_prefix(root) else {
                continue;
            };
            let relative = relative.to_string_lossy().into_owned();
            if !select(&relative) {
                continue;
            }
            if let Ok(f) = file_facts(&path) {
                found.insert(relative, (f.size.unwrap_or(0), f.mtime_ns.unwrap_or(0)));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vault() -> PathBuf {
        let d = std::env::temp_dir().join(format!("sinex-dir-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(d.join("sub")).unwrap();
        std::fs::write(d.join("a.md"), "alpha").unwrap();
        std::fs::write(d.join("sub/b.md"), "beta").unwrap();
        std::fs::write(d.join("skip.png"), "x").unwrap();
        d
    }

    fn driver(root: PathBuf, part_bytes: usize) -> DirSnapshot {
        DirSnapshot {
            root,
            host: "h".into(),
            select: |p| p.ends_with(".md"),
            part_bytes,
        }
    }

    fn paths(a: &Acquired) -> Vec<String> {
        a.material.as_ref().map_or(vec![], |m| {
            m.manifest["members"]
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x["path"].as_str().unwrap().to_owned())
                .collect()
        })
    }

    #[test]
    fn reads_changed_files_and_records_vanished_ones() {
        let root = vault();
        let d = driver(root.clone(), 1 << 20);
        let mut memory = DirMemory::default();
        let first = d.poll(&Value::Null, &mut memory).unwrap();
        assert_eq!(paths(&first), vec!["a.md", "sub/b.md"]);
        let m = first.material.as_ref().unwrap();
        assert_eq!(&m.bytes[m.records[1].0..m.records[1].1], b"beta");
        let cursor = (m.cursor_at)(m.bytes.len());

        assert!(d.poll(&cursor, &mut memory).unwrap().material.is_none());

        std::fs::write(root.join("a.md"), "alpha 2").unwrap();
        std::fs::remove_file(root.join("sub/b.md")).unwrap();
        let next = d.poll(&cursor, &mut memory).unwrap();
        assert_eq!(paths(&next), vec!["a.md", "sub/b.md"]);
        let m = next.material.as_ref().unwrap();
        assert_eq!(
            &m.bytes[m.records[1].0..m.records[1].1],
            &vanished_marker("sub/b.md")[..]
        );
        assert!(m.manifest["members"][1].get("vanished_at_ns").is_some());
        assert_eq!(m.manifest["records"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn a_walk_cut_by_the_part_size_does_not_record_vanished_files() {
        let root = vault();
        let mut memory = DirMemory::default();
        let small = driver(root.clone(), 6);
        let first = small.poll(&Value::Null, &mut memory).unwrap();
        assert_eq!(paths(&first), vec!["a.md"]);
        assert_eq!(
            first.material.as_ref().unwrap().manifest["scan_complete"],
            json!(false)
        );
        let m = first.material.as_ref().unwrap();
        let second = small.poll(&(m.cursor_at)(0), &mut memory).unwrap();
        assert_eq!(paths(&second), vec!["sub/b.md"]);
    }

    #[test]
    fn a_touched_but_unchanged_file_is_skipped() {
        let root = vault();
        let d = driver(root.clone(), 1 << 20);
        let mut memory = DirMemory::default();
        d.poll(&Value::Null, &mut memory).unwrap();
        let f = std::fs::File::options()
            .append(true)
            .open(root.join("a.md"))
            .unwrap();
        f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(5))
            .unwrap();
        assert!(
            d.poll(&Value::Null, &mut memory)
                .unwrap()
                .material
                .is_none()
        );
    }
}
