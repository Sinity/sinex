//! Encoded custody for one authenticated producer frame. Logical names and
//! lengths describe decoded bytes; custody files use the shared frame codec.
mod permissions;

use hashed_blobs::{EncodedBlob, Hash, Kind, Preparation, Prepared, ReadLimits};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Mismatch(String),
    #[error("{0}")]
    Storage(String),
}
impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Self::Invalid(value.into())
    }
}
fn error(value: impl std::fmt::Display) -> Error {
    Error::Storage(value.to_string())
}
fn encoding_error(value: hashed_blobs::Error) -> Error {
    match value {
        hashed_blobs::Error::Io(error) => Error::Storage(error.to_string()),
        other => Error::Mismatch(other.to_string()),
    }
}

/// Producer text never becomes a path component.
pub fn directory(root: &Path, producer: &str, frame: &str) -> PathBuf {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinex frame bytes\0");
    hash.update(producer.as_bytes());
    hash.update(&[0]);
    hash.update(frame.as_bytes());
    root.join("incoming")
        .join("producers")
        .join(hash.finalize().to_hex().as_str())
}
fn file_name(blob: &str) -> Result<String, Error> {
    blob.strip_prefix("blake3:")
        .filter(|hex| blake3::Hash::from_hex(hex).is_ok())
        .map(str::to_owned)
        .ok_or_else(|| Error::Invalid(format!("invalid blob name {blob}")))
}
fn validate(descriptor: &EncodedBlob, blob: &str, length: Option<u64>) -> Result<(), Error> {
    file_name(blob)?;
    if descriptor.hash.to_string() != blob
        || length.is_some_and(|length| length != descriptor.decoded_len)
    {
        return Err(Error::Mismatch(
            "encoded custody differs from the logical name or length".into(),
        ));
    }
    if descriptor.kind != Kind::Plain {
        return Err(Error::Invalid(
            "outside producer encodings must be plain; the owner supplies sealing policy".into(),
        ));
    }
    Ok(())
}
fn prepare_directory(
    root: &Path,
    producer: &str,
    frame: &str,
) -> Result<(PathBuf, Option<u32>), Error> {
    let dir = directory(root, producer, frame);
    let group = permissions::prepare_directory(root, &dir)?;
    sync_directories(root, &dir)?;
    Ok((dir, group))
}
fn descriptor_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}.descriptor"))
}
fn store_descriptor(dir: &Path, descriptor: &EncodedBlob, group: Option<u32>) -> Result<(), Error> {
    let name = file_name(&descriptor.hash.to_string())?;
    let path = descriptor_path(dir, &name);
    permissions::check_existing_file(&path, group)?;
    let mut temporary = tempfile::NamedTempFile::new_in(dir).map_err(error)?;
    temporary.write_all(&descriptor.encode()).map_err(error)?;
    permissions::share_completed_file(temporary.as_file(), group)?;
    temporary.as_file().sync_all().map_err(error)?;
    temporary.persist(path).map_err(error)?;
    sync_directory(dir)
}
fn load_descriptor(dir: &Path, blob: &str) -> Result<EncodedBlob, Error> {
    let file = std::fs::File::open(descriptor_path(dir, &file_name(blob)?)).map_err(error)?;
    let plain = EncodedBlob {
        hash: Hash([0; 32]),
        decoded_len: 0,
        stored_len: 0,
        kind: Kind::Plain,
        index_hash: Hash([0; 32]),
        encoded_hash: Hash([0; 32]),
    };
    let mut bytes = Vec::new();
    file.take(plain.encode().len() as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(error)?;
    let descriptor =
        EncodedBlob::decode(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    validate(&descriptor, blob, None)?;
    Ok(descriptor)
}
fn finish(
    root: &Path,
    producer: &str,
    frame: &str,
    temporary: tempfile::NamedTempFile,
    descriptor: &EncodedBlob,
) -> Result<(), Error> {
    let (dir, group) = prepare_directory(root, producer, frame)?;
    let path = dir.join(file_name(&descriptor.hash.to_string())?);
    permissions::check_existing_file(&path, group)?;
    permissions::share_completed_file(temporary.as_file(), group)?;
    temporary.as_file().sync_all().map_err(error)?;
    store_descriptor(&dir, descriptor, group)?;
    temporary.persist(path).map_err(error)?;
    sync_directory(&dir)
}

/// A raw-reader convenience: encoding is written directly into its durable custody inode.
pub fn write(
    root: &Path,
    producer: &str,
    frame: &str,
    blob: &str,
    length: u64,
    input: impl Read,
) -> Result<(), Error> {
    file_name(blob)?;
    let (dir, _) = prepare_directory(root, producer, frame)?;
    let temporary = tempfile::NamedTempFile::new_in(&dir).map_err(error)?;
    let prepared = Preparation::new(&dir)
        .map_err(encoding_error)?
        .plain_into(temporary.reopen().map_err(error)?, input, 3)
        .map_err(encoding_error)?;
    let descriptor = prepared.descriptor();
    validate(&descriptor, blob, Some(length))?;
    drop(prepared);
    finish(root, producer, frame, temporary, &descriptor)
}
/// Copy only already encoded bytes, including from a pinned file whose source path retired.
pub fn write_prepared(
    root: &Path,
    producer: &str,
    frame: &str,
    prepared: Prepared,
) -> Result<(), Error> {
    let descriptor = prepared.descriptor();
    write_encoded(root, producer, frame, &descriptor, prepared.into_file())
}
/// Copy an encoding from a file or network reader and verify every identity before persistence.
pub fn write_encoded(
    root: &Path,
    producer: &str,
    frame: &str,
    descriptor: &EncodedBlob,
    mut input: impl Read,
) -> Result<(), Error> {
    validate(descriptor, &descriptor.hash.to_string(), None)?;
    let (dir, _) = prepare_directory(root, producer, frame)?;
    let mut temporary = tempfile::NamedTempFile::new_in(&dir).map_err(error)?;
    copy_encoded(&mut input, &mut temporary, descriptor)?;
    let checked = Prepared::from_encoded(
        temporary.reopen().map_err(error)?,
        descriptor.clone(),
        None,
        ReadLimits::default(),
    )
    .map_err(encoding_error)?;
    drop(checked);
    finish(root, producer, frame, temporary, descriptor)
}
/// A local, owned-spool optimization. This path is never part of the wire protocol.
/// Cross-filesystem or incompatible group custody copies encoded bytes instead.
pub fn link_encoded(
    root: &Path,
    producer: &str,
    frame: &str,
    descriptor: &EncodedBlob,
    source: &Path,
) -> Result<(), Error> {
    validate(descriptor, &descriptor.hash.to_string(), None)?;
    let source_file = std::fs::File::open(source).map_err(error)?;
    let checked = Prepared::from_encoded(
        source_file.try_clone().map_err(error)?,
        descriptor.clone(),
        None,
        ReadLimits::default(),
    )
    .map_err(encoding_error)?;
    let (dir, group) = prepare_directory(root, producer, frame)?;
    if group.is_some_and(|group| {
        source_file
            .metadata()
            .map(|metadata| metadata.gid() != group)
            .unwrap_or(true)
    }) {
        return write_prepared(root, producer, frame, checked);
    }
    let target = dir.join(file_name(&descriptor.hash.to_string())?);
    permissions::check_existing_file(&target, group)?;
    permissions::share_completed_file(&source_file, group)?;
    source_file.sync_all().map_err(error)?;
    match std::fs::hard_link(source, &target) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::CrossesDevices => {
            return write_prepared(root, producer, frame, checked);
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = std::fs::File::open(&target).map_err(self::error)?;
            Prepared::from_encoded(existing, descriptor.clone(), None, ReadLimits::default())
                .map_err(encoding_error)?;
        }
        Err(error) => return Err(self::error(error)),
    }
    store_descriptor(&dir, descriptor, group)?;
    sync_directory(&dir)
}
/// Open the encoded stream for transfer; the fixed frame directory owns its location.
pub fn encoded(
    root: &Path,
    producer: &str,
    frame: &str,
    blob: &str,
) -> Result<(EncodedBlob, std::fs::File), Error> {
    let dir = directory(root, producer, frame);
    let descriptor = load_descriptor(&dir, blob)?;
    let file = std::fs::File::open(dir.join(file_name(blob)?)).map_err(error)?;
    if file.metadata().map_err(error)?.len() != descriptor.stored_len {
        return Err(Error::Mismatch("encoded custody length differs".into()));
    }
    Ok((descriptor, file))
}
/// Verify the owned encoding before admission. Append checks it again against later mutation.
pub fn prepared(
    root: &Path,
    producer: &str,
    frame: &str,
    blob: &str,
    length: u64,
) -> Result<Prepared, Error> {
    let (descriptor, file) = encoded(root, producer, frame, blob)?;
    validate(&descriptor, blob, Some(length))?;
    Prepared::from_encoded(file, descriptor, None, ReadLimits::default()).map_err(encoding_error)
}
/// A receiver-owned encoded snapshot gives parsers a stable bounded decoded reader.
pub fn reader(
    root: &Path,
    producer: &str,
    frame: &str,
    blob: &str,
    length: u64,
) -> Result<hashed_blobs::BlobReader, Error> {
    let (descriptor, mut input) = encoded(root, producer, frame, blob)?;
    validate(&descriptor, blob, Some(length))?;
    let mut file = tempfile::tempfile_in(directory(root, producer, frame)).map_err(error)?;
    copy_encoded(&mut input, &mut file, &descriptor)?;
    file.rewind().map_err(error)?;
    let prepared = Prepared::from_encoded(file, descriptor, None, ReadLimits::default())
        .map_err(encoding_error)?;
    prepared
        .reader(None, ReadLimits::default())
        .map_err(encoding_error)
}
fn copy_encoded(
    mut input: impl Read,
    mut output: impl Write,
    descriptor: &EncodedBlob,
) -> Result<(), Error> {
    let mut hash = blake3::Hasher::new();
    let mut copied = 0u64;
    let mut buffer = [0; 65536];
    loop {
        let n = input.read(&mut buffer).map_err(error)?;
        if n == 0 {
            break;
        }
        copied = copied
            .checked_add(n as u64)
            .ok_or("encoded length overflow")?;
        if copied > descriptor.stored_len {
            return Err(Error::Mismatch("encoded custody exceeds its length".into()));
        }
        hash.update(&buffer[..n]);
        output.write_all(&buffer[..n]).map_err(error)?;
    }
    if copied != descriptor.stored_len
        || Hash(*hash.finalize().as_bytes()) != descriptor.encoded_hash
    {
        return Err(Error::Mismatch(
            "encoded custody differs from its digest or length".into(),
        ));
    }
    Ok(())
}

/// Receive one ordered encoded unit. The final unit validates the encoded digest;
/// authoritative admission also verifies the decoded identity before appending.
pub fn receive(
    root: &Path,
    producer: &str,
    frame: &str,
    descriptor: &EncodedBlob,
    offset: u64,
    data: &[u8],
) -> Result<bool, Error> {
    let blob = descriptor.hash.to_string();
    let blob = blob.as_str();
    let total = descriptor.stored_len;
    validate(descriptor, blob, None)?;
    let end = offset
        .checked_add(data.len() as u64)
        .ok_or("producer chunk offset overflow")?;
    if end > total {
        return Err("producer chunk extends past its total".into());
    }
    let (dir, group) = prepare_directory(root, producer, frame)?;
    let name = file_name(blob)?;
    if descriptor_path(&dir, &name).exists() {
        if load_descriptor(&dir, blob)? != *descriptor {
            return Err(Error::Mismatch(
                "producer resend changed its encoded descriptor".into(),
            ));
        }
    } else {
        store_descriptor(&dir, descriptor, group)?;
    }
    let completed = match std::fs::File::open(dir.join(&name)) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(self::error(error)),
    };
    if let Some(mut file) = completed {
        if file.metadata().map_err(error)?.len() != total {
            return Err(Error::Mismatch(
                "received encoding has a different length".into(),
            ));
        }
        file.seek(std::io::SeekFrom::Start(offset)).map_err(error)?;
        if !same_bytes(&mut file, data)? {
            return Err(Error::Mismatch(
                "producer resend differs from received encoding".into(),
            ));
        }
        return Ok(true);
    }
    let path = dir.join(format!("{name}.partial"));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .map_err(error)?;
    file.lock().map_err(error)?;
    let have = file.metadata().map_err(error)?.len();
    if have > total || offset > have {
        return Err("producer chunks are not an ordered prefix".into());
    }
    let overlap = (have.min(end) - offset) as usize;
    file.seek(std::io::SeekFrom::Start(offset)).map_err(error)?;
    if !same_bytes(&mut file, &data[..overlap])? {
        return Err(Error::Mismatch(
            "producer resend differs from received prefix".into(),
        ));
    }
    file.seek(std::io::SeekFrom::End(0)).map_err(error)?;
    file.write_all(&data[overlap..]).map_err(error)?;
    file.sync_all().map_err(error)?;
    sync_directory(&dir)?;
    if have.max(end) < total {
        return Ok(false);
    }
    file.rewind().map_err(error)?;
    if let Err(error) = copy_encoded(&mut file, std::io::sink(), descriptor) {
        drop(file);
        std::fs::remove_file(&path).map_err(self::error)?;
        sync_directory(&dir)?;
        return Err(error);
    }
    permissions::share_completed_file(&file, group)?;
    file.sync_all().map_err(error)?;
    std::fs::rename(&path, dir.join(name)).map_err(error)?;
    sync_directory(&dir)?;
    Ok(true)
}
fn same_bytes(file: &mut std::fs::File, data: &[u8]) -> Result<bool, Error> {
    let mut buffer = [0; 65536];
    let mut at = 0;
    while at < data.len() {
        let n = (data.len() - at).min(buffer.len());
        file.read_exact(&mut buffer[..n]).map_err(error)?;
        if buffer[..n] != data[at..at + n] {
            return Ok(false);
        }
        at += n;
    }
    Ok(true)
}

/// Remove only custody files this frame owns, preserving unrelated entries.
pub fn discard(root: &Path, producer: &str, frame: &str) -> Result<(), Error> {
    let dir = directory(root, producer, frame);
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(self::error(error)),
    };
    for entry in entries {
        let entry = entry.map_err(error)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let hash = name
            .strip_suffix(".partial")
            .or_else(|| name.strip_suffix(".descriptor"))
            .unwrap_or(&name);
        if blake3::Hash::from_hex(hash).is_ok() {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(self::error(error)),
            }
        }
    }
    sync_directory(&dir)?;
    match std::fs::remove_dir(&dir) {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
            ) => {}
        Err(error) => return Err(self::error(error)),
    };
    sync_directory(dir.parent().expect("frame has parent"))
}
fn sync_directory(dir: &Path) -> Result<(), Error> {
    std::fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(error)
}
fn sync_directories(root: &Path, leaf: &Path) -> Result<(), Error> {
    let mut at = leaf;
    loop {
        sync_directory(at)?;
        if at == root {
            return Ok(());
        }
        at = at.parent().ok_or("producer frame left store root")?;
    }
}

#[cfg(test)]
mod tests;
