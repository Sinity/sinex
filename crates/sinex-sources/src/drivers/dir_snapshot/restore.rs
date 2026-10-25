//! Rebuild which files the admitted bundles hold. Their fingerprints are not
//! kept, so the next walk reads them again.
use super::*;

impl DirState {
    /// Remember the files whose records in the bundle `manifest` end at or
    /// before `admitted`. Bundles are offered newest first; a path already
    /// decided by a newer bundle keeps that decision.
    pub fn remember_members(&mut self, manifest: &Value, admitted: u64) -> Result<(), DriverError> {
        for (start, end) in records(manifest)? {
            if end as u64 > admitted {
                continue;
            }
            let member = crate::drain::member_at(manifest, start)
                .ok_or_else(|| DriverError::Cursor("directory member is ambiguous".into()))?;
            let path = member["path"]
                .as_str()
                .ok_or_else(|| DriverError::Cursor("directory member has no path".into()))?;
            if !self.remembered.insert(path.to_owned()) {
                continue;
            }
            if !vanished(member)? {
                self.files.insert(path.to_owned(), None);
            }
        }
        Ok(())
    }

    /// End of a restart's history.
    pub fn finish_remembering(&mut self) {
        self.remembered = HashSet::new();
    }
}

fn records(manifest: &Value) -> Result<Vec<(usize, usize)>, DriverError> {
    if manifest["media_type"] != format::MEDIA_TYPE {
        return Err(DriverError::Cursor(
            "stored material is not a directory bundle".into(),
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
