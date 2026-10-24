//! Frozen producer queue bytes, separate from public CAS. A receiver always
//! copies and verifies a producer-writable file before it can install it.
use sinex_protocol::Ingress;
use sinex_protocol::producer_bytes::{Bytes, Material, Publication, Role, encode_frame, spool_key};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub fn directory(root: &Path, producer: &str, publication: &Publication) -> PathBuf {
    root.join("incoming")
        .join("producers")
        .join(publication.spool_key(producer))
}
fn expected(publication: &Publication, role: Role) -> Result<&Bytes, String> {
    publication.validate()?;
    publication
        .roles()
        .find_map(|(r, b)| (r == role).then_some(b))
        .ok_or_else(|| "producer publication lacks this byte role".into())
}
fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn copy_checked(mut input: impl Read, mut output: impl Write, bytes: &Bytes) -> Result<(), String> {
    let mut hash = blake3::Hasher::new();
    let mut length = 0u64;
    let mut buffer = [0; 65536];
    loop {
        let n = input.read(&mut buffer).map_err(error)?;
        if n == 0 {
            break;
        }
        length = length
            .checked_add(n as u64)
            .ok_or("producer byte length overflow")?;
        if length > bytes.length {
            return Err("producer bytes exceed their frozen length".into());
        }
        hash.update(&buffer[..n]);
        output.write_all(&buffer[..n]).map_err(error)?;
    }
    if length != bytes.length || format!("blake3:{}", hash.finalize().to_hex()) != bytes.blob {
        return Err("producer bytes differ from their frozen name or length".into());
    }
    Ok(())
}

pub fn stage(
    root: &Path,
    producer: &str,
    publication: &Publication,
    role: Role,
    input: impl Read,
) -> Result<(), String> {
    let bytes = expected(publication, role)?;
    let dir = directory(root, producer, publication);
    std::fs::create_dir_all(&dir).map_err(error)?;
    sync_directories(root, &dir)?;
    let mut staged = tempfile::NamedTempFile::new_in(&dir).map_err(error)?;
    copy_checked(input, &mut staged, bytes)?;
    staged.as_file().sync_all().map_err(error)?;
    staged.persist(dir.join(role.name())).map_err(error)?;
    std::fs::File::open(&dir)
        .and_then(|f| f.sync_all())
        .map_err(error)?;
    Ok(())
}

/// No producer-writable inode leaves this function. The returned anonymous
/// receiver-owned snapshot is the version whose name/length were checked.
pub fn snapshot(
    root: &Path,
    producer: &str,
    publication: &Publication,
    role: Role,
) -> Result<std::fs::File, String> {
    let bytes = expected(publication, role)?;
    let path = directory(root, producer, publication).join(role.name());
    let input =
        std::fs::File::open(path).map_err(|e| format!("frozen producer spool unavailable: {e}"))?;
    let mut file = tempfile::tempfile().map_err(error)?;
    copy_checked(input, &mut file, bytes)?;
    file.rewind().map_err(error)?;
    Ok(file)
}

/// Acknowledged prefixes are synced. A resend must match every overlapping byte;
/// a new attempt cannot silently replace the frozen publication's prefix.
pub fn receive(
    root: &Path,
    producer: &str,
    publication: &Publication,
    role: Role,
    offset: u64,
    data: &[u8],
) -> Result<bool, String> {
    let bytes = expected(publication, role)?;
    let end = offset
        .checked_add(data.len() as u64)
        .ok_or("producer chunk offset overflow")?;
    if end > bytes.length {
        return Err("producer chunk exceeds frozen length".into());
    }
    let dir = directory(root, producer, publication);
    std::fs::create_dir_all(&dir).map_err(error)?;
    sync_directories(root, &dir)?;
    if let Ok(mut ready) = std::fs::File::open(dir.join(role.name())) {
        if ready.metadata().map_err(error)?.len() != bytes.length {
            return Err("frozen producer spool length differs".into());
        }
        ready
            .seek(std::io::SeekFrom::Start(offset))
            .map_err(error)?;
        let mut buffer = [0; 65536];
        let mut at = 0;
        while at < data.len() {
            let n = (data.len() - at).min(buffer.len());
            ready.read_exact(&mut buffer[..n]).map_err(error)?;
            if buffer[..n] != data[at..at + n] {
                return Err("producer resend differs from frozen bytes".into());
            }
            at += n;
        }
        // The role was synced and whole-name checked when completed. Final
        // consumption verifies a receiver-owned snapshot again; a chunk retry
        // need not copy/rehash the whole material for every small prefix.
        return Ok(true);
    }
    let path = dir.join(format!("{}.partial", role.name()));
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
    if have > bytes.length || offset > have {
        return Err("producer chunks are not an exact ordered prefix".into());
    }
    let overlap = (have.min(end) - offset) as usize;
    file.seek(std::io::SeekFrom::Start(offset)).map_err(error)?;
    // Compare without allocating proportional to an incoming offset or total.
    let mut buffer = [0; 65536];
    let mut at = 0;
    while at < overlap {
        let n = (overlap - at).min(buffer.len());
        file.read_exact(&mut buffer[..n]).map_err(error)?;
        if buffer[..n] != data[at..at + n] {
            return Err("producer resend differs from the frozen prefix".into());
        }
        at += n;
    }
    file.seek(std::io::SeekFrom::End(0)).map_err(error)?;
    file.write_all(&data[overlap..]).map_err(error)?;
    file.sync_all().map_err(error)?;
    std::fs::File::open(&dir)
        .and_then(|f| f.sync_all())
        .map_err(error)?;
    if have.max(end) < bytes.length {
        return Ok(false);
    }
    file.rewind().map_err(error)?;
    let mut hash = blake3::Hasher::new();
    let length =
        std::io::copy(&mut (&mut file).take(bytes.length + 1), &mut hash).map_err(error)?;
    if length != bytes.length || format!("blake3:{}", hash.finalize().to_hex()) != bytes.blob {
        // A completed attempt proved not to be the reserved bytes. Retire only
        // its own partial file; a new attempt may resend the frozen original.
        drop(file);
        std::fs::remove_file(&path).map_err(error)?;
        std::fs::File::open(&dir)
            .and_then(|f| f.sync_all())
            .map_err(error)?;
        return Err("producer bytes differ from their frozen name or length".into());
    }
    std::fs::rename(&path, dir.join(role.name())).map_err(error)?;
    std::fs::File::open(&dir)
        .and_then(|f| f.sync_all())
        .map_err(error)?;
    Ok(true)
}

pub fn discard_bytes(root: &Path, producer: &str, publication: &Publication) -> Result<(), String> {
    let dir = directory(root, producer, publication);
    if !dir.exists() {
        return Ok(());
    }
    for (role, _) in publication.roles() {
        for name in [role.name().to_owned(), format!("{}.partial", role.name())] {
            match std::fs::remove_file(dir.join(name)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(error(e)),
            }
        }
    }
    std::fs::File::open(&dir)
        .and_then(|f| f.sync_all())
        .map_err(error)?;
    // Unknown files retain the attributable owner rather than being guessed away.
    std::fs::remove_dir(&dir).map_err(error)?;
    std::fs::File::open(dir.parent().expect("spool parent"))
        .and_then(|f| f.sync_all())
        .map_err(error)?;
    Ok(())
}

/// Sender cleanup follows final acknowledgment. Its frozen descriptor outlives
/// receiver byte cleanup, including when that acknowledgment was lost in transit.
pub fn discard(root: &Path, producer: &str, publication: &Publication) -> Result<(), String> {
    discard_bytes(root, producer, publication)?;
    match std::fs::remove_file(artifact_path(root, producer, &publication.frame)) {
        Ok(()) => std::fs::File::open(
            artifact_path(root, producer, &publication.frame)
                .parent()
                .expect("artifact parent"),
        )
        .and_then(|f| f.sync_all())
        .map_err(error),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(error(e)),
    }
}

fn artifact_path(root: &Path, producer: &str, frame: &str) -> PathBuf {
    artifact_directory(root, producer).join(format!("{}.json", spool_key(producer, frame)))
}

fn artifact_directory(root: &Path, producer: &str) -> PathBuf {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinex producer artifact directory 1\0");
    hash.update(producer.as_bytes());
    root.join("incoming")
        .join("producer-requests")
        .join(hash.finalize().to_hex().as_str())
}

/// Stream this producer's committed frozen artifacts, one whole typed frame at
/// a time. Uncommitted private temporary files follow the existing tmp policy.
pub struct Pending {
    entries: Option<std::fs::ReadDir>,
    root: PathBuf,
    producer: String,
}
pub fn pending(root: &Path, producer: &str) -> Result<Pending, String> {
    let entries = match std::fs::read_dir(artifact_directory(root, producer)) {
        Ok(entries) => Some(entries),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(error(e)),
    };
    Ok(Pending {
        entries,
        root: root.to_owned(),
        producer: producer.into(),
    })
}
impl Iterator for Pending {
    type Item = Result<Frozen, String>;
    fn next(&mut self) -> Option<Self::Item> {
        let entries = self.entries.as_mut()?;
        loop {
            let entry = match entries.next()? {
                Ok(entry) => entry,
                Err(e) => return Some(Err(error(e))),
            };
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(".producer-request-")
            {
                continue;
            }
            let result = (|| {
                if !entry.file_type().map_err(error)?.is_file() {
                    return Err("producer artifact is not a regular file".into());
                }
                let frozen: Frozen =
                    serde_json::from_reader(std::fs::File::open(entry.path()).map_err(error)?)
                        .map_err(error)?;
                frozen.validate()?;
                if entry.path()
                    != artifact_path(&self.root, &self.producer, &frozen.publication.frame)
                {
                    return Err(
                        "producer artifact filename differs from its frozen identity".into(),
                    );
                }
                Ok(frozen)
            })();
            return Some(result);
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frozen {
    pub publication: Publication,
    /// Exact typed inner frame. wire_frame adds the declared indirect wrapper.
    pub frame: Ingress,
}
impl Frozen {
    pub fn validate(&self) -> Result<(), String> {
        self.publication.validate()?;
        match &self.frame {
            Ingress::Material { material, .. }
                if self.publication.material.as_ref() == Some(&Material::from_frame(material)) => {}
            Ingress::Events { .. }
                if self.publication.material.is_none() && self.publication.envelope.is_some() => {}
            _ => {
                return Err(
                    "frozen producer artifact differs from its typed material descriptor".into(),
                );
            }
        }
        if let Some(envelope) = &self.publication.envelope {
            let bytes = encode_frame(&self.frame);
            if envelope.length != bytes.len() as u64
                || envelope.blob != format!("blake3:{}", blake3::hash(&bytes).to_hex())
            {
                return Err(
                    "frozen producer envelope differs from its exact typed frame encoding".into(),
                );
            }
        } else if self.frame.frame() != self.publication.frame {
            return Err("frozen producer frame identity differs".into());
        }
        Ok(())
    }
    pub fn wire_frame(&self) -> Ingress {
        if let Some(envelope) = &self.publication.envelope {
            Ingress::Indirect {
                frame: self.publication.frame.clone(),
                blob: envelope.blob.clone(),
            }
        } else {
            self.frame.clone()
        }
    }
}

pub fn load(root: &Path, producer: &str, frame: &str) -> Result<Option<Frozen>, String> {
    let file = match std::fs::File::open(artifact_path(root, producer, frame)) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(error(e)),
    };
    let frozen: Frozen = serde_json::from_reader(file).map_err(error)?;
    frozen.validate()?;
    if frozen.publication.frame != frame {
        return Err("frozen producer artifact has a different final frame".into());
    }
    Ok(Some(frozen))
}

/// The caller loads before minting a new MaterialId. No differing artifact can
/// overwrite a frozen request; persist_noclobber also handles concurrent creators.
pub fn freeze(root: &Path, producer: &str, frozen: &Frozen) -> Result<Frozen, String> {
    frozen.validate()?;
    let path = artifact_path(root, producer, &frozen.publication.frame);
    let dir = path.parent().expect("artifact has a parent");
    std::fs::create_dir_all(dir).map_err(error)?;
    sync_directories(root, dir)?;
    let mut staged = tempfile::Builder::new()
        .prefix(".producer-request-")
        .tempfile_in(dir)
        .map_err(error)?;
    serde_json::to_writer(&mut staged, frozen).map_err(error)?;
    staged.as_file().sync_all().map_err(error)?;
    match staged.persist_noclobber(&path) {
        Ok(_) => {}
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(error(e)),
    }
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(error)?;
    let actual = load(root, producer, &frozen.publication.frame)?
        .ok_or("frozen producer artifact disappeared")?;
    if actual != *frozen {
        return Err("producer publication already froze a different typed request".into());
    }
    Ok(actual)
}

#[cfg(test)]
mod tests;

fn sync_directories(root: &Path, leaf: &Path) -> Result<(), String> {
    let mut at = leaf;
    loop {
        std::fs::File::open(at)
            .and_then(|f| f.sync_all())
            .map_err(error)?;
        if at == root {
            return Ok(());
        }
        at = at.parent().ok_or("producer spool escaped managed root")?;
    }
}
