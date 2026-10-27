//! Rebuild the file facts of a restarted reader from its saved memory and the
//! admitted bundles after it.
use super::*;
use std::os::unix::ffi::{OsStrExt, OsStringExt};

type Facts = Option<(u64, i128, [u8; 32])>;

impl DirState {
    /// Remember the files whose records in the bundle `manifest` end at or
    /// before `admitted`. Bundles are offered newest first; a path already
    /// decided by a newer bundle keeps that decision.
    pub fn remember_members(
        &mut self,
        manifest: &sinex_model::Manifest,
        admitted: u64,
    ) -> Result<(), DriverError> {
        for (start, end) in records(manifest)? {
            if end as u64 > admitted {
                continue;
            }
            let member = manifest
                .member_at(start)
                .ok_or_else(|| DriverError::Cursor("directory member is ambiguous".into()))?;
            if member.details.contains_key("read_error") {
                continue;
            }
            let path = paths::native(member)?;
            if !self.remembered.insert(path.to_owned()) {
                continue;
            }
            if !removed(member)? {
                self.files.insert(path.to_owned(), facts(member)?);
            }
        }
        Ok(())
    }

    /// Remember the files of a saved memory that no newer bundle decided.
    pub fn remember_saved(&mut self, memory: &[u8]) -> Result<(), DriverError> {
        let mut rest = memory;
        while !rest.is_empty() {
            let length = u32::from_le_bytes(take(&mut rest)?) as usize;
            let path = PathBuf::from(std::ffi::OsString::from_vec(
                take_slice(&mut rest, length)?.to_vec(),
            ));
            let file = match take::<1>(&mut rest)? {
                [0] => None,
                [1] => Some((
                    u64::from_le_bytes(take(&mut rest)?),
                    i128::from_le_bytes(take(&mut rest)?),
                    take(&mut rest)?,
                )),
                _ => return Err(damaged()),
            };
            if self.remembered.insert(path.clone()) {
                self.files.insert(path, file);
            }
        }
        Ok(())
    }

    /// End of a restart's history.
    pub fn finish_remembering(&mut self) {
        self.remembered = HashSet::new();
    }
}

fn damaged() -> DriverError {
    DriverError::Cursor("saved directory memory is damaged".into())
}

fn take_slice<'a>(rest: &mut &'a [u8], count: usize) -> Result<&'a [u8], DriverError> {
    if rest.len() < count {
        return Err(damaged());
    }
    let (taken, after) = rest.split_at(count);
    *rest = after;
    Ok(taken)
}

fn take<const N: usize>(rest: &mut &[u8]) -> Result<[u8; N], DriverError> {
    Ok(take_slice(rest, N)?.try_into().expect("N bytes"))
}

/// The memory to save: for each file, in path order, the path's length
/// (four bytes, little-endian) and native bytes, then 1 and its size (eight
/// bytes), modification time (sixteen) and content hash (32), or 0 when the
/// facts are unknown.
pub(super) fn memory_of(files: &HashMap<PathBuf, Facts>) -> Arc<[u8]> {
    let mut paths: Vec<(&PathBuf, &Facts)> = files.iter().collect();
    paths.sort_by(|a, b| a.0.cmp(b.0));
    let mut bytes = Vec::new();
    for (path, file) in paths {
        let path = path.as_os_str();
        bytes.extend_from_slice(&(path.len() as u32).to_le_bytes());
        bytes.extend_from_slice(path.as_bytes());
        match file {
            None => bytes.push(0),
            Some((size, mtime, hash)) => {
                bytes.push(1);
                bytes.extend_from_slice(&size.to_le_bytes());
                bytes.extend_from_slice(&mtime.to_le_bytes());
                bytes.extend_from_slice(hash);
            }
        }
    }
    bytes.into()
}

/// A member's size, modification time and content hash.
fn facts(member: &sinex_model::Member) -> Result<Facts, DriverError> {
    let invalid = || DriverError::Cursor("directory member facts are invalid".into());
    let Some(hash) = member.details.get("hash") else {
        return Ok(None);
    };
    let hash = hash
        .as_str()
        .and_then(|hex| blake3::Hash::from_hex(hex).ok())
        .ok_or_else(invalid)?;
    let size = member.length;
    let mtime = member.details["mtime_ns"]
        .as_str()
        .and_then(|text| text.parse::<i128>().ok())
        .ok_or_else(invalid)?;
    Ok(Some((size, mtime, *hash.as_bytes())))
}

fn records(manifest: &sinex_model::Manifest) -> Result<Vec<(usize, usize)>, DriverError> {
    if manifest.media_type.as_deref() != Some(format::MEDIA_TYPE) {
        return Err(DriverError::Cursor(
            "stored material is not a directory bundle".into(),
        ));
    }
    let records = manifest
        .records
        .clone()
        .ok_or_else(|| DriverError::Cursor("directory bundle lists no records".into()))?;
    format::validate(manifest, &records).map_err(|e| DriverError::Cursor(e.to_string()))?;
    Ok(records)
}

fn removed(member: &sinex_model::Member) -> Result<bool, DriverError> {
    match member.details.get("removed_at_ns") {
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
