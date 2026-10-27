//! The content store adapter over `hashed-blobs`: bytes named by BLAKE3, stored
//! as seekable zstd. Bytes are stored before the admission that refers to them.
use crate::Result;
pub use hashed_blobs::{DEFAULT_FRAME as BLOB_FRAME_BYTES, SealedReadLimits};
use std::io::{Read, Seek};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
mod plain_seek;

#[derive(Clone)]
pub struct Blobs(Arc<hashed_blobs::Store>, PathBuf);

impl Blobs {
    pub fn root(&self) -> &Path {
        self.1.parent().expect("incoming has a store root")
    }

    pub fn open(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)
            .map_err(|e| crate::Error::Invalid(format!("{}: {e}", root.display())))?;
        // The configured protected store shares new containers with its group.
        // The writer's umask still applies; dedup never changes old file modes.
        let metadata = std::fs::metadata(root)
            .map_err(|e| crate::Error::Invalid(format!("{}: {e}", root.display())))?;
        let mode = if metadata.permissions().mode() & 0o7777 == 0o2770 {
            0o640
        } else {
            0o600
        };
        Ok(Self(
            Arc::new(hashed_blobs::Store::open(root, vec![], mode)?),
            root.join("incoming"),
        ))
    }

    /// Store `bytes` and return their name, `blake3:<hex>`.
    pub fn put(&self, bytes: &[u8]) -> Result<String> {
        let hash = self.0.put(bytes, 3)?;
        Ok(format!("blake3:{}", hash.hex()))
    }

    /// Store bytes sealed under `key` (the sealed class): encrypted frames
    /// named by a keyed hash, so the store never confirms a known plaintext.
    pub fn put_sealed(&self, bytes: &[u8], key: &sinex_sealing::DataKey) -> Result<String> {
        let hash = self.0.put_sealed(std::io::Cursor::new(bytes), 3, key)?;
        Ok(format!("blake3:{}", hash.hex()))
    }

    /// Seal a seekable input in bounded frames without staging plaintext on disk.
    pub fn put_sealed_reader(
        &self,
        reader: impl Read + Seek,
        key: &sinex_sealing::DataKey,
    ) -> Result<String> {
        let hash = self.0.put_sealed(reader, 3, key)?;
        Ok(format!("blake3:{}", hash.hex()))
    }

    /// Sequential sealed reads under the operation's explicit key and memory budgets.
    pub fn read_sealed(
        &self,
        name: &str,
        expected_len: Option<u64>,
        key: &sinex_sealing::DataKey,
        limits: SealedReadLimits,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::SealedReader> {
        Ok(self
            .0
            .read_sealed(&parse(name)?, expected_len, key, limits, check)?)
    }

    /// Whether the blob named `name` is sealed.
    pub fn is_sealed(&self, name: &str) -> Result<bool> {
        Ok(matches!(
            self.0.kind(&parse(name)?)?,
            hashed_blobs::Kind::Sealed { .. }
        ))
    }

    /// Whether the store holds the bytes named `name` (an outside producer
    /// places a material's bytes before registering it).
    pub fn contains(&self, name: &str) -> Result<bool> {
        Ok(self.0.contains(&parse(name)?))
    }

    /// Store bytes read from `reader` (an export archive of any size) without
    /// holding them in memory, and return their name.
    pub fn put_reader(&self, reader: impl std::io::Read) -> Result<String> {
        let hash = self.0.put(reader, 3)?;
        Ok(format!("blake3:{}", hash.hex()))
    }

    pub fn read_range(&self, name: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0.read_range(&parse(name)?, offset, len)?)
    }

    /// Verify and open a plain blob for bounded sequential reads. The same representation
    /// stays open; the callback can cancel verification and subsequent reads. Delta and sealed
    /// encodings keep their separate read/key APIs and are refused by this operation.
    pub fn read_plain(
        &self,
        name: &str,
        expected_len: Option<u64>,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<hashed_blobs::PlainReader> {
        Ok(self.0.read_plain(&parse(name)?, expected_len, check)?)
    }

    /// A seekable reader over one stored blob's plain bytes, checked as read.
    pub fn read_plain_seek(
        &self,
        name: &str,
        length: u64,
        check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<impl Read + Seek + Send + 'static> {
        let hash = parse(name)?;
        if self.0.kind(&hash)? != hashed_blobs::Kind::Plain {
            return Err(crate::Error::Invalid(
                "scoped plain seek requires a plain representation".into(),
            ));
        }
        Ok(plain_seek::PlainSeek::new(
            self.root(),
            hash,
            length,
            Box::new(check),
        )?)
    }

    /// The whole blob, verified against its name as it is read.
    pub fn read(&self, name: &str) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut self.0.read(&parse(name)?)?, &mut out)
            .map_err(|e| crate::Error::Invalid(format!("read {name}: {e}")))?;
        Ok(out)
    }

    /// Copy verified content into `out`. Plain blobs use bounded sequential
    /// reads; a delta-encoded blob is decoded whole first. Sealed blobs require
    /// a key and are refused here. Returns the number of bytes written.
    pub fn copy_to(&self, name: &str, out: &mut impl std::io::Write) -> Result<u64> {
        self.copy_to_checked(name, out, || Ok(()))
    }

    /// Copy one verified representation with cancellation during plain reads.
    /// Delta-encoded blobs decode their body whole in the shared reader;
    /// cancellation is checked before and after that decode. Sealed bytes
    /// require the keyed API.
    pub fn copy_to_checked(
        &self,
        name: &str,
        out: &mut impl std::io::Write,
        mut check: impl FnMut() -> hashed_blobs::Result<()> + Send + 'static,
    ) -> Result<u64> {
        let hash = parse(name)?;
        let copied = match self.0.kind(&hash)? {
            hashed_blobs::Kind::Plain => {
                std::io::copy(&mut self.0.read_plain(&hash, None, check)?, out)
            }
            _ => {
                check()?;
                let copied = std::io::copy(&mut self.0.read(&hash)?, out);
                check()?;
                copied
            }
        };
        copied.map_err(|e| crate::Error::Invalid(format!("read {name}: {e}")))
    }

    /// Delete a blob's file once no row refers to it: rows first, the file
    /// after commit. A file already gone is fine.
    pub fn delete(&self, name: &str) -> Result<()> {
        match self.0.delete(&parse(name)?) {
            Ok(()) | Err(hashed_blobs::Error::Missing(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn stored_size(&self, name: &str) -> Result<u64> {
        Ok(self.0.stored_size(&parse(name)?)?)
    }

    /// Read a blob back and check it against its name.
    pub fn verify(&self, name: &str) -> Result<hashed_blobs::Verified> {
        Ok(self.0.verify(&parse(name)?)?)
    }

    /// The names of the stored files, read from disk one directory at a time.
    pub fn names(&self) -> Result<impl Iterator<Item = hashed_blobs::Result<String>>> {
        Ok(self
            .0
            .names()?
            .map(|name| name.map(|hash| format!("blake3:{}", hash.hex()))))
    }
}

/// Files [`delete_files_without_rows`] found in the content store.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FilesWithoutRows {
    /// Blob files deleted because no row names them.
    pub deleted: Vec<String>,
    /// Entries under `blobs/` that are not blob files, left alone.
    pub not_blobs: Vec<String>,
}

/// How many listed names one query checks against `core.blobs`.
const NAMES_PER_QUERY: usize = 1000;

/// Delete every blob file that no `core.blobs` row names. A driver writes a
/// material's bytes before the admission that adds their row, so a process
/// that stops before that admission commits leaves a file with no row.
///
/// The caller holds the writer, and every process that writes to the store
/// holds it while it does, so no admission adds a row for a listed file while
/// this runs. The daemon runs it at start, before any source reads.
pub async fn delete_files_without_rows(
    conn: &mut sqlx::PgConnection,
    blobs: &Blobs,
) -> Result<FilesWithoutRows> {
    let mut found = FilesWithoutRows::default();
    let mut names = blobs.names()?;
    loop {
        let mut listed = Vec::with_capacity(NAMES_PER_QUERY);
        for name in names.by_ref() {
            match name {
                Ok(name) => listed.push(name),
                Err(hashed_blobs::Error::Io(error))
                    if error.kind() == std::io::ErrorKind::InvalidData =>
                {
                    found.not_blobs.push(error.to_string());
                }
                Err(error) => return Err(error.into()),
            }
            if listed.len() == NAMES_PER_QUERY {
                break;
            }
        }
        if listed.is_empty() {
            return Ok(found);
        }
        let with_rows: std::collections::HashSet<String> =
            sqlx::query_scalar!("SELECT hash FROM core.blobs WHERE hash = ANY($1)", &listed)
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .collect();
        for name in listed {
            if !with_rows.contains(&name) {
                blobs.delete(&name)?;
                found.deleted.push(name);
            }
        }
    }
}

pub(crate) fn parse(name: &str) -> Result<hashed_blobs::Hash> {
    let invalid = || crate::Error::Invalid(format!("invalid blob name {name}"));
    let hex = name.strip_prefix("blake3:").ok_or_else(invalid)?;
    if hex.len() != 64 || !hex.is_ascii() {
        return Err(invalid());
    }
    let mut raw = [0u8; 32];
    for (i, b) in raw.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| invalid())?;
    }
    Ok(hashed_blobs::Hash(raw))
}

#[cfg(test)]
mod tests;
