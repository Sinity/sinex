//! Rebuild the file facts of a restarted reader from its saved memory and the
//! admitted bundles after it.
use super::*;
use sinex_model::json::JsonExt;
use std::os::unix::ffi::{OsStrExt, OsStringExt};

type Facts = Option<FileFacts>;

impl DirState {
    pub(crate) fn restore_part(
        &mut self,
        reference: &sinex_model::MaterialRef,
        admitted: u64,
        removed_at_source: bool,
    ) -> Result<bool, DriverError> {
        if !self.saved_memory {
            self.remember_members(&reference.manifest, admitted)?;
        }
        if removed_at_source {
            self.remember_locations(&reference.manifest, admitted, reference.id)?;
        }
        Ok(!self.saved_memory || self.check_locations().is_err())
    }

    pub(crate) fn restore_checkpoint(
        &mut self,
        memory: &[u8],
        memory_part: Option<&sinex_model::MaterialRef>,
        admitted: Option<u64>,
        removed_at_source: bool,
    ) -> Result<bool, DriverError> {
        let saved_through = crate::drivers::dir_snapshot::saved_through(memory)?;
        let admitted = admitted
            .ok_or_else(|| DriverError::Cursor("directory memory has no admitted prefix".into()))?;
        if memory_part.is_some_and(|memory_part| admitted > memory_part.length) {
            return Err(DriverError::Cursor(
                "directory memory has a contradictory admitted prefix".into(),
            ));
        }
        if saved_through > admitted {
            return Err(DriverError::Cursor(
                "directory checkpoint is beyond its admitted prefix".into(),
            ));
        }
        if saved_through < admitted {
            let memory_part = memory_part.ok_or_else(|| {
                DriverError::Cursor("directory checkpoint suffix has no immutable material".into())
            })?;
            self.remember_members_after(&memory_part.manifest, admitted, Some(saved_through))?;
            if removed_at_source {
                self.remember_locations_after(
                    &memory_part.manifest,
                    admitted,
                    memory_part.id,
                    Some(saved_through),
                )?;
            }
        }
        self.remember_saved(memory, saved_through)?;
        self.saved_memory = true;
        Ok(removed_at_source && self.check_locations().is_err())
    }

    pub(crate) fn committed(&mut self, cursor: &Value) {
        if self.checkpoint_cursor.as_ref() == Some(cursor) {
            self.checkpoint_pending = false;
            self.checkpoint_cursor = None;
        }
        self.release_admitted(cursor);
    }

    /// Once `cursor` admits the held bundle, take the file facts it records
    /// and drop its bytes.
    pub(crate) fn release_admitted(&mut self, cursor: &Value) {
        if let Some(held) = &self.held
            && held.material.admitted(cursor)
        {
            let held = self.held.take().expect("checked");
            self.files = held.files;
            self.failures = held.failures;
            self.retries = held.retries;
            self.locations = held.locations;
        }
    }

    /// Remember the files whose records in the bundle `manifest` end at or
    /// before `admitted`. Bundles are offered newest first; a path already
    /// decided by a newer bundle keeps that decision.
    pub fn remember_members(
        &mut self,
        manifest: &sinex_model::Manifest,
        admitted: u64,
    ) -> Result<(), DriverError> {
        self.remember_members_after(manifest, admitted, None)
    }

    pub(crate) fn remember_members_after(
        &mut self,
        manifest: &sinex_model::Manifest,
        admitted: u64,
        after: Option<u64>,
    ) -> Result<(), DriverError> {
        let records = records(manifest)?;
        if format::stored_format(manifest) == Some(format::DirectoryFormat::FramedBundle) {
            self.checkpoint_pending = true;
        }
        let mut remembered = Vec::new();
        for (start, end) in records {
            if end as u64 > admitted || after.is_some_and(|after| end as u64 <= after) {
                continue;
            }
            let member = manifest
                .member_at(start)
                .ok_or_else(|| DriverError::Cursor("directory member is ambiguous".into()))?;
            if member.details.contains_key("read_error") {
                continue;
            }
            let path = paths::member_path(member)?;
            if self.remembered.contains(&path) {
                continue;
            }
            let file = if removed(member)? {
                None
            } else {
                Some(facts(
                    member,
                    format::stored_format(manifest).expect("checked directory"),
                )?)
            };
            remembered.push((path, file));
        }
        // A malformed part must fail on every retry, without marking its
        // paths decided before all the admitted facts have been checked.
        for (path, file) in remembered {
            if self.remembered.insert(path.clone()) {
                match file {
                    None => {
                        self.files.remove(&path);
                    }
                    Some(facts) => {
                        self.files.insert(path, facts);
                    }
                }
            }
        }
        Ok(())
    }

    /// Remember the files of a saved memory that no newer bundle decided.
    pub fn remember_saved(&mut self, memory: &[u8], through: u64) -> Result<(), DriverError> {
        if saved_through(memory)? != through {
            return Err(damaged());
        }
        let mut rest = &memory[MEMORY_TAG.len() + 8..];
        let mut files = HashMap::new();
        let mut locations = HashMap::new();
        let count = u64::from_le_bytes(take(&mut rest)?);
        for _ in 0..count {
            let path = read_path(&mut rest)?;
            let file = match take::<1>(&mut rest)? {
                [0] => None,
                [1] => Some(FileFacts {
                    size: u64::from_le_bytes(take(&mut rest)?),
                    hash: take(&mut rest)?,
                    modified_ns: read_time(&mut rest)?,
                    changed_ns: read_time(&mut rest)?,
                }),
                _ => return Err(damaged()),
            };
            if files.insert(path, file).is_some() {
                return Err(damaged());
            }
        }
        let count = u64::from_le_bytes(take(&mut rest)?);
        for _ in 0..count {
            let path = read_path(&mut rest)?;
            let location = RecordLocation {
                material: MaterialId(uuid::Uuid::from_bytes(take(&mut rest)?)),
                record_offset: u64::from_le_bytes(take(&mut rest)?),
            };
            if locations.insert(path, location).is_some() {
                return Err(damaged());
            }
        }
        if !rest.is_empty() {
            return Err(damaged());
        }
        for (path, file) in files {
            if self.remembered.insert(path.clone()) {
                self.files.insert(path, file);
            }
        }
        for (path, location) in locations {
            if self.located.insert(path.clone()) {
                self.locations.insert(path, location);
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
        self.remember_locations_after(manifest, admitted, material, None)
    }

    pub(crate) fn remember_locations_after(
        &mut self,
        manifest: &sinex_model::Manifest,
        admitted: u64,
        material: MaterialId,
        after: Option<u64>,
    ) -> Result<(), DriverError> {
        for (start, end) in records(manifest)? {
            if end as u64 > admitted || after.is_some_and(|after| end as u64 <= after) {
                continue;
            }
            let member = manifest
                .member_at(start)
                .ok_or_else(|| DriverError::Cursor("directory member is ambiguous".into()))?;
            if member.details.contains_key("read_error") {
                continue;
            }
            let path = paths::member_path(member)?;
            if self.saved_memory && !self.files.contains_key(&path) {
                continue;
            }
            if self.located.contains(&path) {
                continue;
            }
            let removed = removed(member)?;
            self.located.insert(path.clone());
            if !removed {
                self.locations.insert(
                    path,
                    RecordLocation {
                        material,
                        record_offset: start as u64,
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
                "saved directory memory lacks removal locations in admitted material history"
                    .into(),
            ));
        }
        Ok(())
    }

    /// End of a restart's history.
    pub fn finish_remembering(&mut self) {
        self.remembered = HashSet::new();
        self.located = HashSet::new();
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

/// The saved memory's encoding: this tag, the admitted position it was saved
/// through, then the file facts and the removal locations.
const MEMORY_TAG: &[u8] = b"SXDIR\0\x01\0";

pub(crate) fn saved_through(memory: &[u8]) -> Result<u64, DriverError> {
    let mut rest = memory.strip_prefix(MEMORY_TAG).ok_or_else(damaged)?;
    Ok(u64::from_le_bytes(take(&mut rest)?))
}

fn read_path(rest: &mut &[u8]) -> Result<PathBuf, DriverError> {
    let count = u32::from_le_bytes(take(rest)?) as usize;
    Ok(PathBuf::from(std::ffi::OsString::from_vec(
        take_slice(rest, count)?.to_vec(),
    )))
}

fn read_time(rest: &mut &[u8]) -> Result<Option<i128>, DriverError> {
    match take::<1>(rest)? {
        [0] => Ok(None),
        [1] => Ok(Some(i128::from_le_bytes(take(rest)?))),
        _ => Err(damaged()),
    }
}

fn write_path(bytes: &mut Vec<u8>, path: &Path) {
    bytes.extend_from_slice(&(path.as_os_str().as_bytes().len() as u32).to_le_bytes());
    bytes.extend_from_slice(path.as_os_str().as_bytes());
}

fn write_time(bytes: &mut Vec<u8>, time: Option<i128>) {
    bytes.push(u8::from(time.is_some()));
    if let Some(time) = time {
        bytes.extend_from_slice(&time.to_le_bytes());
    }
}

pub(super) fn memory_of(
    files: &FileMemory,
    locations: &HashMap<PathBuf, RecordLocation>,
    through: u64,
) -> Arc<[u8]> {
    let mut bytes = MEMORY_TAG.to_vec();
    bytes.extend_from_slice(&through.to_le_bytes());
    let mut files: Vec<_> = files.iter().collect();
    files.sort_by(|a, b| a.0.cmp(b.0));
    bytes.extend_from_slice(&(files.len() as u64).to_le_bytes());
    for (path, file) in files {
        write_path(&mut bytes, path);
        match file {
            None => bytes.push(0),
            Some(file) => {
                bytes.push(1);
                bytes.extend_from_slice(&file.size.to_le_bytes());
                bytes.extend_from_slice(&file.hash);
                write_time(&mut bytes, file.modified_ns);
                write_time(&mut bytes, file.changed_ns);
            }
        }
    }
    let mut locations: Vec<_> = locations.iter().collect();
    locations.sort_by(|a, b| a.0.cmp(b.0));
    bytes.extend_from_slice(&(locations.len() as u64).to_le_bytes());
    for (path, location) in locations {
        write_path(&mut bytes, path);
        bytes.extend_from_slice(location.material.0.as_bytes());
        bytes.extend_from_slice(&location.record_offset.to_le_bytes());
    }
    bytes.into()
}

fn facts(
    member: &sinex_model::Member,
    format: format::DirectoryFormat,
) -> Result<Facts, DriverError> {
    let invalid = || DriverError::Cursor("directory member facts are invalid".into());
    match format {
        format::DirectoryFormat::Tar => {
            let node = member.node.as_ref().ok_or_else(invalid)?;
            Ok(Some(FileFacts {
                size: node.size,
                hash: node.content_hash,
                modified_ns: Some(node.mtime_ns.parse().ok().ok_or_else(invalid)?),
                changed_ns: Some(node.ctime_ns.parse().ok().ok_or_else(invalid)?),
            }))
        }
        format::DirectoryFormat::FramedBundle => {
            let Some(hash) = member.details.get("hash") else {
                return Ok(None);
            };
            let hash = hash
                .as_str()
                .and_then(|hex| blake3::Hash::from_hex(hex).ok())
                .ok_or_else(invalid)?;
            let modified_ns = member
                .details
                .str("mtime_ns")
                .and_then(|text| text.parse().ok())
                .ok_or_else(invalid)?;
            Ok(Some(FileFacts {
                size: member.length,
                hash: *hash.as_bytes(),
                modified_ns: Some(modified_ns),
                changed_ns: None,
            }))
        }
    }
}

fn records(manifest: &sinex_model::Manifest) -> Result<Vec<(usize, usize)>, DriverError> {
    if !format::is_directory(manifest) {
        return Err(DriverError::Cursor(
            "stored material has no recognized directory capture format".into(),
        ));
    }
    let records = manifest
        .records
        .clone()
        .ok_or_else(|| DriverError::Cursor("directory tar lists no records".into()))?;
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
