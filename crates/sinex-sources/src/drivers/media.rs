//! `Media`: screen and audio capture, which produce whole files rather than
//! lines. Capture writes each finished segment into the spool directory as
//! `<started_ns>-<ended_ns>.<kind>.<ext>` with a JSON sidecar of the same stem
//! (`<stem>.json`: kind, times, MIME type, target, dimensions), the data file
//! renamed into place last, so a segment is complete exactly when both exist.
//!
//! Each poll stores the oldest complete segment after the cursor as one
//! material holding one record (the whole file), its sidecar as the manifest.
//! Segments are linked as parts of one input, like a growing file's parts.
use super::{Acquired, DriverError, NewMaterial};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::{InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};
use std::path::{Path, PathBuf};

/// One record, the whole stored file.
pub fn whole(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    (vec![(0, bytes.len())], bytes.len())
}

pub struct Media {
    pub spool: PathBuf,
    pub input: InputRef,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Cursor {
    /// The stem of the last segment stored.
    pub last: Option<String>,
    pub material: Option<MaterialId>,
    pub part_index: u32,
}

/// Complete segments in `dir`, oldest first: (stem, data file).
pub fn complete(dir: &Path) -> std::io::Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.ends_with(".json") || name.ends_with(".partial") || name.starts_with('.') {
            continue;
        }
        // `<started>-<ended>.<kind>.<ext>`: the stem is everything before the extension.
        let Some((stem, _ext)) = name.rsplit_once('.') else {
            continue;
        };
        if dir.join(format!("{stem}.json")).exists() {
            out.push((stem.to_owned(), path));
        }
    }
    out.sort();
    Ok(out)
}

impl Media {
    pub fn poll(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let io = |e: std::io::Error| DriverError::Io(format!("{}: {e}", self.spool.display()));
        let next = complete(&self.spool)
            .map_err(io)?
            .into_iter()
            .find(|(stem, _)| previous.last.as_ref().is_none_or(|last| stem > last));
        let now = Timestamp::now();
        let Some((stem, path)) = next else {
            return Ok(Acquired {
                image: None,
                input: self.input.clone(),
                observation: ObservationFacts {
                    observed_at: Some(now),
                    changed: false,
                    ..Default::default()
                },
                revision: RevisionChange::Same,
                material: None,
                cursor_unchanged: cursor.clone(),
            });
        };
        let bytes = std::fs::read(&path).map_err(io)?;
        let sidecar: Value = serde_json::from_slice(
            &std::fs::read(self.spool.join(format!("{stem}.json"))).map_err(io)?,
        )
        .map_err(|e| DriverError::Cursor(format!("segment sidecar {stem}.json: {e}")))?;
        let mut manifest = sidecar;
        manifest["file"] = json!(path.file_name().and_then(|n| n.to_str()));
        manifest["read_at_ns"] = json!(i64::try_from(now.unix_ns()).unwrap_or(i64::MAX));
        let id = MaterialId::new();
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        let size = bytes.len() as u64;
        let records = vec![(0, bytes.len())];
        let next_cursor = Cursor {
            last: Some(stem),
            material: Some(id),
            part_index,
        };
        Ok(Acquired {
            image: None,
            input: self.input.clone(),
            observation: ObservationFacts {
                observed_at: Some(now),
                size: Some(size),
                changed: true,
                ..Default::default()
            },
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes,
                stored: None,
                records,
                part_index,
                previous_part: previous.material,
                reading_order: super::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(move |_| {
                    serde_json::to_value(&next_cursor).expect("cursor serializes")
                }),
                aligned: false,
                vanished: Vec::new(),
            }),
            cursor_unchanged: Value::Null,
        })
    }

    /// Delete segments up to the committed cursor: their bytes are in the
    /// content store.
    pub fn remove_passed(&self, committed: &Value) -> std::io::Result<usize> {
        let Ok(Cursor {
            last: Some(last), ..
        }) = serde_json::from_value::<Cursor>(committed.clone())
        else {
            return Ok(0);
        };
        let mut n = 0;
        for (stem, path) in complete(&self.spool)? {
            if stem <= last {
                std::fs::remove_file(&path)?;
                let _ = std::fs::remove_file(self.spool.join(format!("{stem}.json")));
                n += 1;
            }
        }
        Ok(n)
    }
}

/// Write a finished segment: the sidecar first, the data renamed into place
/// last. `data` is a file capture already wrote in `dir` (with `.partial`).
pub fn publish_segment(
    dir: &Path,
    partial: &Path,
    stem: &str,
    ext: &str,
    sidecar: &Value,
) -> std::io::Result<PathBuf> {
    let side = dir.join(format!("{stem}.json"));
    let tmp = dir.join(format!(".{stem}.json.tmp"));
    std::fs::write(&tmp, serde_json::to_vec(sidecar).expect("JSON serializes"))?;
    std::fs::rename(&tmp, &side)?;
    let data = dir.join(format!("{stem}.{ext}"));
    std::fs::rename(partial, &data)?;
    Ok(data)
}

#[cfg(test)]
mod tests;
