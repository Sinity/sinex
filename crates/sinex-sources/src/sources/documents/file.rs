//! A selected file is read alone; its neighbours are never acquired or removed.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::{InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};
use sinex_source_core::binding::Reader;
use sinex_source_core::drivers::{Acquired, DriverError, NewMaterial, StoredBytes, file_facts};
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Default, Serialize, Deserialize)]
struct Cursor {
    material: Option<MaterialId>,
    through: Option<u64>,
    part_index: u32,
    content: Option<String>,
    missing: bool,
}
pub(super) struct FileReader {
    path: PathBuf,
    host: String,
    held: Option<sinex_source_core::drivers::held::HeldMaterial>,
}
impl FileReader {
    pub(super) fn new(path: PathBuf, host: String) -> Self {
        Self {
            path,
            host,
            held: None,
        }
    }
    fn input(&self) -> InputRef {
        InputRef {
            kind: "file".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }
    fn unavailable(&self, cursor: &Value, reason: String) -> Acquired {
        Acquired {
            input: self.input(),
            observation: ObservationFacts {
                unavailable: Some(reason),
                observed_at: Some(Timestamp::now()),
                ..Default::default()
            },
            revision: RevisionChange::Same,
            material: None,
            cursor_without_material: cursor.clone(),
        }
    }
}
impl Reader for FileReader {
    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        if let Some(held) = &self.held {
            if !held.admitted(cursor) {
                return held.acquired(cursor);
            }
            self.held = None;
        }
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let name = self
            .path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| DriverError::Io("document file needs a UTF-8 name".into()))?;
        let root = self
            .path
            .parent()
            .ok_or_else(|| DriverError::Io("document file needs a parent directory".into()))?;
        let mut bytes = tempfile::tempfile().map_err(|e| DriverError::Io(e.to_string()))?;
        bytes
            .write_all(&[0])
            .map_err(|e| DriverError::Io(e.to_string()))?;
        let now = Timestamp::now();
        let mut facts = match file_facts(&self.path) {
            Ok(facts) => facts,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound && previous.material.is_some() =>
            {
                match std::fs::read_dir(root) {
                    Ok(_) => ObservationFacts {
                        changed: true,
                        observed_at: Some(now),
                        ..Default::default()
                    },
                    Err(error) => return Ok(self.unavailable(cursor, error.to_string())),
                }
            }
            Err(error) => return Ok(self.unavailable(cursor, error.to_string())),
        };
        let missing = facts.size.is_none();
        let (length, content) = if missing {
            let marker = sinex_source_core::drivers::dir_snapshot::removed_marker(name);
            bytes
                .write_all(&marker)
                .map_err(|e| DriverError::Io(e.to_string()))?;
            (marker.len() as u64, None)
        } else {
            let metadata = std::fs::symlink_metadata(&self.path)
                .map_err(|e| DriverError::Io(e.to_string()))?;
            if !metadata.file_type().is_file() {
                return Ok(
                    self.unavailable(cursor, "selected document is not a regular file".into())
                );
            }
            let mut input =
                std::fs::File::open(&self.path).map_err(|e| DriverError::Io(e.to_string()))?;
            let opened = input
                .metadata()
                .map_err(|e| DriverError::Io(e.to_string()))?;
            if !opened.is_file()
                || Some(opened.ino()) != facts.inode
                || Some(opened.dev()) != facts.device
            {
                return Err(DriverError::Io("document changed before reading".into()));
            }
            let mut hash = blake3::Hasher::new();
            let mut length = 0u64;
            let mut buffer = [0; 64 * 1024];
            loop {
                let n = match input.read(&mut buffer) {
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    result => result.map_err(|e| DriverError::Io(e.to_string()))?,
                };
                if n == 0 {
                    break;
                }
                bytes
                    .write_all(&buffer[..n])
                    .map_err(|e| DriverError::Io(e.to_string()))?;
                hash.update(&buffer[..n]);
                length += n as u64;
            }
            let after = file_facts(&self.path).map_err(|e| DriverError::Io(e.to_string()))?;
            if facts.size != after.size
                || facts.mtime_ns != after.mtime_ns
                || facts.ctime_ns != after.ctime_ns
                || facts.inode != after.inode
                || facts.device != after.device
                || facts.size != Some(length)
            {
                return Err(DriverError::Io("document changed while reading".into()));
            }
            (length, Some(hash.finalize().to_hex().to_string()))
        };
        if previous.content == content && previous.missing == missing {
            facts.changed = false;
            return Ok(Acquired {
                input: self.input(),
                observation: facts,
                revision: RevisionChange::Same,
                material: None,
                cursor_without_material: cursor.clone(),
            });
        }
        let id = MaterialId::new();
        let part_index = if previous.material.is_some() {
            previous
                .part_index
                .checked_add(1)
                .ok_or_else(|| DriverError::Cursor("document part index exceeds u32".into()))?
        } else {
            0
        };
        let mut member = json!({"path":name,"stream_offset":1,"length":length});
        if missing {
            member["removed_at_ns"] = json!(now.unix_ns().to_string());
        } else {
            member["hash"] = json!(content);
            if let Some(mtime) = facts.mtime_ns {
                member["mtime_ns"] = json!(mtime.to_string());
            }
        }
        let end = length
            .checked_add(1)
            .ok_or_else(|| DriverError::Io("document exceeds address space".into()))?;
        let records = vec![(
            1,
            usize::try_from(end)
                .map_err(|_| DriverError::Io("document exceeds address space".into()))?,
        )];
        let manifest = sinex_model::Manifest::from_json(
            json!({"path":root,"host":self.host,"media_type":"application/x-sinex-file-bundle","records":records,"members":[member],"read_at_ns":i64::try_from(now.unix_ns()).unwrap_or(i64::MAX)}),
        );
        let next = Cursor {
            material: Some(id),
            through: Some(end),
            part_index,
            content,
            missing,
        };
        let acquired = Acquired {
            input: self.input(),
            observation: facts,
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes: vec![],
                stored: Some(StoredBytes::FramedFile(Arc::new(bytes))),
                records,
                manifest: manifest.clone(),
                part_index,
                previous_part: previous.material,
                reading_order: sinex_source_core::drivers::reading_order(&manifest),
                cursor_at: Box::new(move |_| {
                    serde_json::to_value(&next).map_err(|e| DriverError::Cursor(e.to_string()))
                }),
                aligned: false,
                removed: vec![],
                memory: None,
                owned_files: vec![],
            }),
            cursor_without_material: Value::Null,
        };
        let held = sinex_source_core::drivers::held::HeldMaterial::hold(acquired, cursor)?;
        let result = held.acquired(cursor)?;
        self.held = Some(held);
        Ok(result)
    }
}
#[cfg(test)]
mod tests;
