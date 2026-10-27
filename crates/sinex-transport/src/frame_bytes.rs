//! Frame bytes: the bytes an outside producer gives Sinex for one frame, in
//! that frame's directory under the store root, each file named by the BLAKE3
//! hash of its bytes. A producer on the same host writes them with [`write`];
//! one on another host sends chunks that the receiver collects with
//! [`receive`]. The receiver reads a verified copy with [`snapshot`], stores
//! it, and removes the directory with [`discard`] once the frame is recorded.
mod permissions;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Storage(String),
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Self::Invalid(message.into())
    }
}

use std::io::{Read, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// The directory holding the bytes for `producer`'s frame `frame`. The name is
/// a hash, so no producer-supplied text can leave the frame root.
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

/// The file name of the bytes named `blob` (`blake3:<hex>`).
fn file_name(blob: &str) -> Result<String, Error> {
    let hex = blob
        .strip_prefix("blake3:")
        .filter(|hex| blake3::Hash::from_hex(hex).is_ok())
        .ok_or_else(|| Error::Invalid(format!("invalid blob name {blob}")))?;
    Ok(hex.to_owned())
}

fn error(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

/// Copy `input` to `output`, checking it is exactly the bytes named `blob`
/// of `length`.
fn copy_checked(
    mut input: impl Read,
    mut output: impl Write,
    blob: &str,
    length: u64,
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
            .ok_or("producer byte length overflow")?;
        if copied > length {
            return Err("producer bytes are longer than their declared length".into());
        }
        hash.update(&buffer[..n]);
        output.write_all(&buffer[..n]).map_err(error)?;
    }
    if copied != length || format!("blake3:{}", hash.finalize().to_hex()) != blob {
        return Err("producer bytes differ from their name or length".into());
    }
    Ok(())
}

/// Write the bytes named `blob` for `frame`, as the producer.
pub fn write(
    root: &Path,
    producer: &str,
    frame: &str,
    blob: &str,
    length: u64,
    input: impl Read,
) -> Result<(), Error> {
    let name = file_name(blob)?;
    let dir = directory(root, producer, frame);
    let group = permissions::prepare_directory(root, &dir)?;
    permissions::check_existing_file(&dir.join(&name), group)?;
    sync_directories(root, &dir)?;
    let mut staged = tempfile::NamedTempFile::new_in(&dir).map_err(error)?;
    copy_checked(input, &mut staged, blob, length)?;
    permissions::share_completed_file(staged.as_file(), group)?;
    staged.as_file().sync_all().map_err(error)?;
    staged.persist(dir.join(name)).map_err(error)?;
    sync_directory(&dir)
}

/// A verified copy of the bytes named `blob` for `frame`, owned by the
/// receiver, so a producer rewriting its file afterwards changes nothing.
pub fn snapshot(
    root: &Path,
    producer: &str,
    frame: &str,
    blob: &str,
    length: u64,
) -> Result<std::fs::File, Error> {
    let path = directory(root, producer, frame).join(file_name(blob)?);
    let input = std::fs::File::open(path).map_err(error)?;
    let mut file = tempfile::tempfile().map_err(error)?;
    copy_checked(input, &mut file, blob, length)?;
    file.rewind().map_err(error)?;
    Ok(file)
}

/// Add one chunk of the bytes named `blob` (`total` long) for `frame`.
/// Chunks arrive in order; one sent again must match what was received.
/// True once the bytes are complete and checked against their name.
pub fn receive(
    root: &Path,
    producer: &str,
    frame: &str,
    blob: &str,
    total: u64,
    offset: u64,
    data: &[u8],
) -> Result<bool, Error> {
    let name = file_name(blob)?;
    let end = offset
        .checked_add(data.len() as u64)
        .ok_or("producer chunk offset overflow")?;
    if end > total {
        return Err("producer chunk extends past its total".into());
    }
    let dir = directory(root, producer, frame);
    let _ = permissions::prepare_directory(root, &dir)?;
    sync_directories(root, &dir)?;
    let completed = match std::fs::File::open(dir.join(&name)) {
        Ok(file) => Some(file),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(error(e)),
    };
    if let Some(mut complete) = completed {
        if complete.metadata().map_err(error)?.len() != total {
            return Err("received producer bytes have a different length".into());
        }
        complete
            .seek(std::io::SeekFrom::Start(offset))
            .map_err(error)?;
        if !same_bytes(&mut complete, data)? {
            return Err("producer resend differs from the received bytes".into());
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
        return Err("producer resend differs from the received prefix".into());
    }
    file.seek(std::io::SeekFrom::End(0)).map_err(error)?;
    file.write_all(&data[overlap..]).map_err(error)?;
    file.sync_all().map_err(error)?;
    sync_directory(&dir)?;
    if have.max(end) < total {
        return Ok(false);
    }
    file.rewind().map_err(error)?;
    let mut hash = blake3::Hasher::new();
    let length = std::io::copy(&mut (&mut file).take(total + 1), &mut hash).map_err(error)?;
    if length != total || format!("blake3:{}", hash.finalize().to_hex()) != blob {
        // This attempt's bytes are wrong; drop them so a new attempt can
        // send the right ones.
        drop(file);
        std::fs::remove_file(&path).map_err(error)?;
        sync_directory(&dir)?;
        return Err("producer bytes differ from their name or length".into());
    }
    std::fs::rename(&path, dir.join(name)).map_err(error)?;
    sync_directory(&dir)?;
    Ok(true)
}

/// Whether the next `data.len()` bytes of `file` equal `data`, compared
/// without allocating in proportion to the chunk.
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

/// Remove `frame`'s directory and the bytes in it. A file that is not named
/// by a hash is left in place, and the directory with it.
pub fn discard(root: &Path, producer: &str, frame: &str) -> Result<(), Error> {
    let dir = directory(root, producer, frame);
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(error(e)),
    };
    for entry in entries {
        let entry = entry.map_err(error)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let hash = name.strip_suffix(".partial").unwrap_or(&name);
        if blake3::Hash::from_hex(hash).is_ok() {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(error(e)),
            }
        }
    }
    sync_directory(&dir)?;
    match std::fs::remove_dir(&dir) {
        Ok(()) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
            ) => {}
        Err(e) => return Err(error(e)),
    }
    sync_directory(dir.parent().expect("frame directory has a parent"))
}

fn sync_directory(dir: &Path) -> Result<(), Error> {
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(error)
}

fn sync_directories(root: &Path, leaf: &Path) -> Result<(), Error> {
    let mut at = leaf;
    loop {
        sync_directory(at)?;
        if at == root {
            return Ok(());
        }
        at = at
            .parent()
            .ok_or("producer frame directory left the store root")?;
    }
}

#[cfg(test)]
mod tests;
