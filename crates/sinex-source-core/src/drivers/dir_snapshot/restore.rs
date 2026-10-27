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
            if !self.remembered.insert(path.clone()) {
                continue;
            }
            if !removed(member)? {
                self.files.insert(path.clone(), facts(member)?);
            }
        }
        Ok(())
    }

    /// Remember the files of a saved memory that no newer bundle decided.
    pub fn remember_saved(&mut self, memory: &[u8]) -> Result<(), DriverError> {
        let mut rest = memory;
        while !rest.is_empty() {
            let length = u32::from_le_bytes(take(&mut rest)?);
            if length == LOCATIONS {
                return self.remember_saved_locations(rest);
            }
            let length = length as usize;
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

    /// Remember where the files whose records in the bundle `material`
    /// end at or before `admitted` were stored, for a driver that records
    /// removals at the source. Bundles are offered newest first.
    pub fn remember_locations(
        &mut self,
        manifest: &sinex_model::Manifest,
        admitted: u64,
        material: MaterialId,
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
            if !self.located.insert(path.clone()) {
                continue;
            }
            if !removed(member)? {
                self.locations.insert(
                    path,
                    RecordLocation {
                        material,
                        anchor: start as u64,
                    },
                );
            }
        }
        Ok(())
    }

    /// Every remembered file needs the location whose reading a removal replaces.
    pub(crate) fn check_locations(&self) -> Result<(), DriverError> {
        if self
            .files
            .keys()
            .any(|path| !self.locations.contains_key(path))
        {
            return Err(DriverError::Cursor(
                "saved directory memory lacks removal locations; rebuild the reader memory from stored material history".into(),
            ));
        }
        Ok(())
    }

    /// End of a restart's history.
    pub fn finish_remembering(&mut self) {
        self.remembered = HashSet::new();
        self.located = HashSet::new();
    }

    fn remember_saved_locations(&mut self, mut rest: &[u8]) -> Result<(), DriverError> {
        while !rest.is_empty() {
            let length = u32::from_le_bytes(take(&mut rest)?) as usize;
            let path = PathBuf::from(std::ffi::OsString::from_vec(
                take_slice(&mut rest, length)?.to_vec(),
            ));
            let material = MaterialId(uuid::Uuid::from_bytes(take(&mut rest)?));
            let anchor = u64::from_le_bytes(take(&mut rest)?);
            if self.located.insert(path.clone()) {
                self.locations
                    .insert(path, RecordLocation { material, anchor });
            }
        }
        Ok(())
    }
}

/// Where the saved file facts end and the saved locations begin: a path
/// length no path has.
const LOCATIONS: u32 = u32::MAX;

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
/// facts are unknown. When locations are kept, [`LOCATIONS`] follows, then
/// for each path in order its length and bytes, the material ID (sixteen
/// bytes) and the anchor (eight).
pub(super) fn memory_of(
    files: &HashMap<PathBuf, Facts>,
    locations: &HashMap<PathBuf, RecordLocation>,
) -> Arc<[u8]> {
    let mut paths: Vec<(&PathBuf, &Facts)> = files.iter().collect();
    paths.sort_by(|a, b| a.0.cmp(b.0));
    let mut bytes = Vec::new();
    for (path, file) in paths {
        bytes.extend_from_slice(&(path.as_os_str().as_bytes().len() as u32).to_le_bytes());
        bytes.extend_from_slice(path.as_os_str().as_bytes());
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
    if !locations.is_empty() {
        bytes.extend_from_slice(&LOCATIONS.to_le_bytes());
        let mut paths: Vec<(&PathBuf, &RecordLocation)> = locations.iter().collect();
        paths.sort_by(|a, b| a.0.cmp(b.0));
        for (path, location) in paths {
            bytes.extend_from_slice(&(path.as_os_str().as_bytes().len() as u32).to_le_bytes());
            bytes.extend_from_slice(path.as_os_str().as_bytes());
            bytes.extend_from_slice(location.material.0.as_bytes());
            bytes.extend_from_slice(&location.anchor.to_le_bytes());
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
