//! Names recorded durably before installation, checked on the next start.
use super::*;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
type Result<T> = hashed_blobs::Result<T>;
type Names<'a> = Box<dyn Iterator<Item = hashed_blobs::Result<String>> + Send + 'a>;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(super) struct DatabasePosition {
    system_identifier: String,
    database: String,
    commit_seq: i64,
    blob_changes: i64,
}

impl DatabasePosition {
    pub(super) async fn read(conn: &mut sqlx::PgConnection) -> crate::Result<Self> {
        let (system_identifier, database): (String, String) = sqlx::query_as(
            "SELECT system_identifier::text, current_database()::text FROM pg_control_system()",
        )
        .fetch_one(&mut *conn)
        .await?;
        Ok(Self {
            system_identifier,
            database,
            commit_seq: crate::commit_log::latest(conn).await?,
            blob_changes: sqlx::query_scalar(
                "SELECT value FROM core.counters WHERE name = 'blob_changes'",
            )
            .fetch_one(&mut *conn)
            .await?,
        })
    }
}

impl Blobs {
    fn unadmitted_dir(&self) -> PathBuf {
        self.root().join("unadmitted-blobs")
    }

    pub(super) fn prepare_unadmitted_dir(&self) -> Result<()> {
        std::fs::create_dir_all(self.unadmitted_dir())?;
        File::open(self.unadmitted_dir())?.sync_all()?;
        // Every opener syncs the parent, including one racing the directory's creator.
        File::open(self.root())?.sync_all()?;
        Ok(())
    }

    pub(super) fn note_unadmitted(&self, hash: hashed_blobs::Hash) -> Result<()> {
        let dir = self.unadmitted_dir();
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

    fn saved_position(&self) -> Result<Option<DatabasePosition>> {
        match std::fs::read(self.root().join("unadmitted-blobs-ready")) {
            Ok(bytes) => Ok(serde_json::from_slice::<DatabasePosition>(&bytes).ok()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn can_advance_position(&self, position: &DatabasePosition) -> Result<bool> {
        Ok(self.saved_position()?.is_some_and(|saved| {
            saved.system_identifier == position.system_identifier
                && saved.database == position.database
                && saved.commit_seq <= position.commit_seq
                && saved.blob_changes <= position.blob_changes
        }))
    }

    pub(super) fn unadmitted_names(&self, position: &DatabasePosition) -> Result<Names<'_>> {
        let saved = self.saved_position()?;
        if saved.as_ref() != Some(position) {
            // A different database or commit position can leave historical files without rows.
            return Ok(Box::new(
                self.names()
                    .map_err(|e| hashed_blobs::Error::Io(std::io::Error::other(e)))?
                    .chain(self.recorded_names()?),
            ));
        }
        self.recorded_names()
    }

    pub(super) fn recorded_names(&self) -> Result<Names<'_>> {
        Ok(Box::new(std::fs::read_dir(self.unadmitted_dir())?.map(
            |entry| {
                let entry = entry?;
                let name = format!("blake3:{}", entry.file_name().to_string_lossy());
                parse(&name).map_err(|e| hashed_blobs::Error::Io(std::io::Error::other(e)))?;
                Ok(name)
            },
        )))
    }

    pub(super) fn sync_recorded_names(&self) -> Result<()> {
        File::open(self.unadmitted_dir())?.sync_all()?;
        Ok(())
    }

    pub(super) fn sweep_finished(&self, position: &DatabasePosition) -> Result<()> {
        self.sync_recorded_names()?;
        let path = self
            .root()
            .join(format!(".unadmitted-blobs-ready-{}", uuid::Uuid::now_v7()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        let bytes = serde_json::to_vec(position).map_err(std::io::Error::other)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(path, self.root().join("unadmitted-blobs-ready"))?;
        File::open(self.root())?.sync_all()?;
        Ok(())
    }
}
