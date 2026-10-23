//! Rebuild only acknowledged membership. Current files are reread after a
//! restart; fingerprints belong to captures completed by this reader.
use super::super::RetainedRecords;
use super::*;
use sinex_model::MaterialRef;

impl DirState {
    /// Each anchor is backed by durable progress or an actual admitted outcome.
    /// Do not turn an isolated later outcome into proof of earlier records.
    pub fn remember_members(
        &mut self,
        manifest: &Value,
        admitted: &[usize],
    ) -> Result<(), DriverError> {
        let records = records(manifest)?;
        let admitted: HashSet<_> = admitted.iter().copied().collect();
        for (start, _) in records {
            if !admitted.contains(&start) {
                continue;
            }
            let member = crate::drain::member_at(manifest, start)
                .ok_or_else(|| DriverError::Cursor("directory member is ambiguous".into()))?;
            let path = member["path"]
                .as_str()
                .ok_or_else(|| DriverError::Cursor("directory member has no path".into()))?;
            if vanished(member)? {
                self.files.remove(path);
            } else {
                self.files.insert(path.to_owned(), None);
            }
        }
        Ok(())
    }

    /// Resume the original material before a fresh walk, even when its source
    /// changed or vanished. The retained body was verified by its owning reader.
    pub fn restore_pending(
        &mut self,
        reference: MaterialRef,
        stored: RetainedRecords,
        cursor: &Value,
    ) -> Result<(), DriverError> {
        let current: Cursor = serde_json::from_value(cursor.clone())
            .map_err(|e| DriverError::Cursor(e.to_string()))?;
        let length = usize::try_from(reference.length)
            .map_err(|_| DriverError::Io("directory material exceeds address space".into()))?;
        let through = current.through.ok_or_else(|| {
            DriverError::Cursor("directory cursor has no admitted byte end".into())
        })?;
        if current.material != Some(reference.id) || through >= length {
            return Err(DriverError::Cursor(
                "directory pending material differs from cursor".into(),
            ));
        }
        if stored.bytes.length() != reference.length || stored.blob != reference.blob {
            return Err(DriverError::Cursor(
                "directory retained body differs from material".into(),
            ));
        }
        let records = records(&reference.manifest)?;
        if through != 0 && !records.iter().any(|(_, end)| *end == through) {
            return Err(DriverError::Cursor(
                "directory cursor is not a record boundary".into(),
            ));
        }
        let resume = &reference.manifest["directory_resume"];
        let (previous, scanned): (Cursor, Cursor) = if !resume.is_null() {
            (
                serde_json::from_value(resume["previous"].clone())
                    .map_err(|e| DriverError::Cursor(e.to_string()))?,
                serde_json::from_value(resume["scanned"].clone())
                    .map_err(|e| DriverError::Cursor(e.to_string()))?,
            )
        } else {
            // Earlier bundles retain the original scan time and member ranges.
            // The current prefix supplies its previous completed-scan marker.
            let at = reference.manifest["read_at_ns"].as_i64().ok_or_else(|| {
                DriverError::Cursor("legacy directory material has no scan time".into())
            })?;
            (
                Cursor {
                    material: reference.previous_part,
                    part_index: reference.part_index.saturating_sub(1),
                    last_complete_scan_ns: current.last_complete_scan_ns,
                    through: None,
                },
                Cursor {
                    last_complete_scan_ns: if reference.manifest["scan_complete"] == true {
                        Some(i128::from(at))
                    } else {
                        current.last_complete_scan_ns
                    },
                    ..current.clone()
                },
            )
        };
        if previous.material != reference.previous_part {
            return Err(DriverError::Cursor(
                "directory previous cursor differs from material chain".into(),
            ));
        }
        let mut files = self.files.clone();
        for &(start, end) in &records {
            let member = crate::drain::member_at(&reference.manifest, start)
                .ok_or_else(|| DriverError::Cursor("directory member is ambiguous".into()))?;
            let path = member["path"]
                .as_str()
                .ok_or_else(|| DriverError::Cursor("directory member has no path".into()))?;
            if vanished(member)? {
                files.remove(path);
                continue;
            }
            let size = u64::try_from(end - start)
                .map_err(|_| DriverError::Cursor("directory member size is invalid".into()))?;
            let mtime = member["mtime_ns"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    DriverError::Cursor("directory member has no modification time".into())
                })?;
            let mut hash = blake3::Hasher::new();
            let mut position = start;
            let mut bytes = [0; 64 * 1024];
            while position < end {
                let n = bytes.len().min(end - position);
                stored
                    .bytes
                    .read_exact_at(position as u64, &mut bytes[..n])
                    .map_err(io)?;
                hash.update(&bytes[..n]);
                position += n;
            }
            files.insert(
                path.to_owned(),
                Some((size, mtime, *hash.finalize().as_bytes())),
            );
        }
        let mut observation: ObservationFacts = if let Some(value) = resume.get("observation") {
            serde_json::from_value(value.clone()).map_err(|e| DriverError::Cursor(e.to_string()))?
        } else {
            ObservationFacts {
                observed_at: reference.manifest["read_at_ns"]
                    .as_i64()
                    .and_then(|n| Timestamp::from_unix_ns(i128::from(n)).ok()),
                ..Default::default()
            }
        };
        observation.changed = false;
        self.pending = Some(Pending {
            stored: StoredBytes::FramedReader(stored),
            length,
            records,
            manifest: reference.manifest,
            id: reference.id,
            part_index: reference.part_index,
            previous,
            scanned,
            files,
            observation,
        });
        Ok(())
    }
}

fn records(manifest: &Value) -> Result<Vec<(usize, usize)>, DriverError> {
    if manifest["media_type"] != format::MEDIA_TYPE {
        return Err(DriverError::Cursor(
            "retained material is not a directory bundle".into(),
        ));
    }
    let records: Vec<(usize, usize)> = serde_json::from_value(manifest["records"].clone())
        .map_err(|e| DriverError::Cursor(format!("directory records: {e}")))?;
    format::validate(manifest, &records).map_err(|e| DriverError::Cursor(e.to_string()))?;
    Ok(records)
}

fn vanished(member: &Value) -> Result<bool, DriverError> {
    match member.get("vanished_at_ns") {
        None => Ok(false),
        Some(value)
            if value
                .as_str()
                .and_then(|s| s.parse::<i128>().ok())
                .and_then(|n| Timestamp::from_unix_ns(n).ok())
                .is_some() =>
        {
            Ok(true)
        }
        Some(_) => Err(DriverError::Cursor(
            "directory removal time is invalid".into(),
        )),
    }
}
