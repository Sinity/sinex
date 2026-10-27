//! Names recorded durably before installation, checked on the next start.
use super::*;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
type Result<T> = hashed_blobs::Result<T>;
type Names<'a> = Box<dyn Iterator<Item = hashed_blobs::Result<String>> + 'a>;

impl Blobs {
    fn unadmitted_dir(&self) -> PathBuf {
        self.root().join("unadmitted-blobs")
    }

    pub(super) fn note_unadmitted(&self, hash: hashed_blobs::Hash) -> Result<()> {
        let dir = self.unadmitted_dir();
        std::fs::create_dir_all(&dir)?;
        // Concurrent puts can see the new directory before its creator synced it.
        File::open(self.root())?.sync_all()?;
        let marker = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(dir.join(hash.hex()))?;
        marker.sync_all()?;
        File::open(dir)?.sync_all()?;
        Ok(())
    }

    pub(super) fn forget_unadmitted(&self, name: &str) -> Result<()> {
        let path = self.unadmitted_dir().join(
            parse(name)
                .map_err(|e| hashed_blobs::Error::Io(std::io::Error::other(e)))?
                .hex(),
        );
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub(super) fn unadmitted_names(&self) -> Result<Names<'_>> {
        if !self.root().join("unadmitted-blobs-ready").try_exists()? {
            // Existing stores need one complete sweep before the recorded
            // names cover every file that could lack a row.
            return Ok(Box::new(
                self.names()
                    .map_err(|e| hashed_blobs::Error::Io(std::io::Error::other(e)))?,
            ));
        }
        self.recorded_names()
    }

    pub(super) fn recorded_names(&self) -> Result<Names<'_>> {
        let dir = self.unadmitted_dir();
        std::fs::create_dir_all(&dir)?;
        Ok(Box::new(std::fs::read_dir(dir)?.map(|entry| {
            let entry = entry?;
            let name = format!("blake3:{}", entry.file_name().to_string_lossy());
            parse(&name).map_err(|e| hashed_blobs::Error::Io(std::io::Error::other(e)))?;
            Ok(name)
        })))
    }

    pub(super) fn sync_recorded_names(&self) -> Result<()> {
        File::open(self.unadmitted_dir())?.sync_all()?;
        Ok(())
    }

    pub(super) fn sweep_finished(&self) -> Result<()> {
        std::fs::create_dir_all(self.unadmitted_dir())?;
        File::open(self.unadmitted_dir())?.sync_all()?;
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(self.root().join("unadmitted-blobs-ready"))?
            .sync_all()?;
        File::open(self.root())?.sync_all()?;
        Ok(())
    }
}
