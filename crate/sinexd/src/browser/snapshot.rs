//! Consistent reads of a live History file, and periodic SQLite backups of it.
//! When Chromium holds an exclusive lock, the reader copies the database and
//! its rollback journal instead. That copy assumes a local filesystem where
//! unchanged size and nanosecond times mean unchanged bytes; a passing
//! integrity check alone does not show the copy is consistent.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::browser::FileIdentity;
use sqlx::{
    Connection,
    sqlite::{SqliteConnectOptions, SqliteConnection},
};
use std::{
    ffi::{c_char, c_int, c_void},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use uuid::Uuid;

// The variant, not the message, decides whether the generation is fenced.
#[derive(Debug)]
pub(super) enum Failure {
    Unavailable(String),
    ReconciliationRequired(&'static str),
}
impl From<String> for Failure {
    fn from(reason: String) -> Self {
        Self::Unavailable(reason)
    }
}
impl From<&str> for Failure {
    fn from(reason: &str) -> Self {
        Self::Unavailable(reason.to_owned())
    }
}
impl Failure {
    pub(super) fn into_outcome(self) -> (String, bool) {
        match self {
            Self::Unavailable(reason) => (reason, false),
            Self::ReconciliationRequired(reason) => (reason.to_owned(), true),
        }
    }
}

unsafe extern "C" {
    fn sqlite3_backup_init(
        dest: *mut c_void,
        dest_name: *const c_char,
        source: *mut c_void,
        source_name: *const c_char,
    ) -> *mut c_void;
    fn sqlite3_backup_step(backup: *mut c_void, pages: c_int) -> c_int;
    fn sqlite3_backup_finish(backup: *mut c_void) -> c_int;
    fn ioctl(fd: c_int, request: std::ffi::c_ulong, ...) -> c_int;
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Stamp {
    pub identity: FileIdentity,
    pub size: u64,
    pub mtime_ns: i128,
    pub ctime_ns: i128,
}
fn stamp(path: &Path) -> Result<Stamp, String> {
    let m = std::fs::metadata(path).map_err(|_| "source file metadata unavailable")?;
    if !m.is_file() {
        return Err("source is not a regular file".into());
    }
    Ok(Stamp {
        identity: FileIdentity {
            device: m.dev(),
            inode: m.ino(),
            resolved: std::fs::canonicalize(path)
                .map_err(|_| "source resolution unavailable")?
                .to_string_lossy()
                .into_owned(),
        },
        size: m.len(),
        mtime_ns: i128::from(m.mtime()) * 1_000_000_000 + i128::from(m.mtime_nsec()),
        ctime_ns: i128::from(m.ctime()) * 1_000_000_000 + i128::from(m.ctime_nsec()),
    })
}
fn quiet(stamps: &FileStamps) -> bool {
    let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return false;
    };
    let Ok(now) = i128::try_from(now.as_nanos()) else {
        return false;
    };
    std::iter::once(&stamps.main)
        .chain(stamps.journal.iter())
        .chain(stamps.wal.iter())
        .all(|s| s.mtime_ns <= now - 50_000_000 && s.ctime_ns <= now - 50_000_000)
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}
fn optional_stamp(path: &Path) -> Result<Option<Stamp>, String> {
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.file_type().is_symlink() => {
            Err("SQLite sidecar symlinks are unsupported".into())
        }
        Ok(_) => stamp(path).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err("SQLite sidecar availability unknown".into()),
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct FileStamps {
    pub main: Stamp,
    pub journal: Option<Stamp>,
    pub wal: Option<Stamp>,
}
fn file_stamps(path: &Path) -> Result<FileStamps, String> {
    Ok(FileStamps {
        main: stamp(path)?,
        journal: optional_stamp(&sidecar(path, "-journal"))?,
        wal: optional_stamp(&sidecar(path, "-wal"))?,
    })
}
fn check_handle(file: &File, configured: &Path, identity: &FileIdentity) -> Result<(), Failure> {
    let m = file
        .metadata()
        .map_err(|_| "open source file unavailable")?;
    let current = stamp(configured)?;
    if m.dev() != identity.device || m.ino() != identity.inode || &current.identity != identity {
        return Err(Failure::ReconciliationRequired(
            "configured History path no longer names the opened file; generation reconciliation required",
        ));
    }
    Ok(())
}
fn stopped(stop: &tokio::sync::watch::Receiver<bool>) -> Result<(), String> {
    if *stop.borrow() {
        Err("browser read cancelled".into())
    } else {
        Ok(())
    }
}
fn verify_stamp(file: &File, expected: &Stamp) -> Result<(), String> {
    let m = file
        .metadata()
        .map_err(|_| "copied source handle metadata unavailable")?;
    if m.dev() != expected.identity.device
        || m.ino() != expected.identity.inode
        || m.len() != expected.size
        || i128::from(m.mtime()) * 1_000_000_000 + i128::from(m.mtime_nsec()) != expected.mtime_ns
        || i128::from(m.ctime()) * 1_000_000_000 + i128::from(m.ctime_nsec()) != expected.ctime_ns
    {
        return Err("History changed while it was copied".into());
    }
    Ok(())
}
fn copy_file(
    source: &Path,
    dest: &Path,
    expected: &Stamp,
    stop: &tokio::sync::watch::Receiver<bool>,
) -> Result<(u64, bool), String> {
    let mut input = File::open(source).map_err(|_| "source copy open unavailable")?;
    verify_stamp(&input, expected)?;
    let mut output = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(dest)
        .map_err(|_| "owned copy creation failed")?;
    stopped(stop)?;
    // SAFETY: both descriptors are live and owned here; FICLONE changes only the
    // destination. Filesystems without reflinks fall back to streaming.
    let cloned = unsafe { ioctl(output.as_raw_fd(), 0x4004_9409u64 as _, input.as_raw_fd()) } == 0;
    let mut bytes = 0u64;
    if !cloned {
        output.set_len(0).map_err(|_| "owned copy reset failed")?;
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            stopped(stop)?;
            let n = input
                .read(&mut buffer)
                .map_err(|_| "source copy read failed")?;
            if n == 0 {
                break;
            }
            output
                .write_all(&buffer[..n])
                .map_err(|_| "owned copy write failed")?;
            bytes = bytes
                .checked_add(n as u64)
                .ok_or("copy byte count overflow")?;
        }
    }
    verify_stamp(&input, expected)?;
    if output
        .metadata()
        .map_err(|_| "copy length unavailable")?
        .len()
        != expected.size
    {
        return Err("copied length differs from the source".into());
    }
    output.sync_all().map_err(|_| "owned copy fsync failed")?;
    Ok((bytes, cloned))
}
fn reject_super_journal(path: &Path) -> Result<(), String> {
    let mut file = File::open(path).map_err(|_| "copied journal unavailable")?;
    if file
        .metadata()
        .map_err(|_| "copied journal length unavailable")?
        .len()
        >= 8
    {
        file.seek(SeekFrom::End(-8))
            .map_err(|_| "copied journal trailer unavailable")?;
        let mut magic = [0; 8];
        file.read_exact(&mut magic)
            .map_err(|_| "copied journal trailer unavailable")?;
        if magic == [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7] {
            return Err(
                "super-journal recovery is unsupported; no original external file may be touched"
                    .into(),
            );
        }
    }
    Ok(())
}
async fn open(path: &Path, readonly: bool) -> Result<SqliteConnection, sqlx::Error> {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .read_only(readonly)
            .create_if_missing(false)
            .busy_timeout(Duration::from_millis(500)),
    )
    .await
}
fn busy(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .is_some_and(|code| matches!(code.as_ref(), "5" | "6"))
}

/// Back up the caller's open read transaction into a new file at `dest`.
/// Both SQLite handles stay locked for the duration of the C API calls.
pub(super) async fn backup(
    source: &mut SqliteConnection,
    dest: &Path,
    stop: &tokio::sync::watch::Receiver<bool>,
) -> Result<u64, String> {
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(dest)
        .map_err(|_| "owned snapshot destination exists or unavailable")?;
    let mut target = open(dest, false)
        .await
        .map_err(|_| "owned snapshot connection unavailable")?;
    {
        let mut source = source
            .lock_handle()
            .await
            .map_err(|_| "source SQLite handle unavailable")?;
        let mut target = target
            .lock_handle()
            .await
            .map_err(|_| "owned SQLite handle unavailable")?;
        // SAFETY: SQLx locks both live sqlite3 handles; the backup object does not
        // escape this block and finish runs on every path.
        let result = tokio::task::block_in_place(|| unsafe {
            let b = sqlite3_backup_init(
                target.as_raw_handle().as_ptr().cast(),
                c"main".as_ptr(),
                source.as_raw_handle().as_ptr().cast(),
                c"main".as_ptr(),
            );
            if b.is_null() {
                return Err("SQLite backup initialization refused".to_string());
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            let result = loop {
                if let Err(e) = stopped(stop) {
                    break Err(e);
                }
                if Instant::now() >= deadline {
                    break Err("SQLite backup busy or execution budget exhausted".into());
                }
                match sqlite3_backup_step(b, 64) {
                    101 => break Ok(()),
                    0 => {}
                    5 | 6 => std::thread::sleep(Duration::from_millis(5)),
                    _ => break Err("SQLite backup failed".into()),
                }
            };
            let finish = sqlite3_backup_finish(b);
            result.and_then(|_| {
                if finish == 0 {
                    Ok(())
                } else {
                    Err("SQLite backup finalization failed".into())
                }
            })
        });
        result?;
    }
    let integrity: String = tokio::time::timeout(
        Duration::from_secs(10),
        sqlx::query_scalar("PRAGMA quick_check").fetch_one(&mut target),
    )
    .await
    .map_err(|_| "owned snapshot integrity budget exhausted")?
    .map_err(|_| "owned snapshot integrity unavailable")?;
    if integrity != "ok" {
        return Err("owned snapshot integrity failed".into());
    }
    target
        .close()
        .await
        .map_err(|_| "owned snapshot close failed")?;
    File::open(dest)
        .and_then(|f| f.sync_all())
        .map_err(|_| "owned snapshot fsync failed")?;
    std::fs::metadata(dest)
        .map(|m| m.len())
        .map_err(|_| "owned snapshot length unavailable".into())
}

// A copy is deleted with its sidecars on every error, cancellation and retry,
// unless it passed verification and became the cached view.
struct CopyScratch {
    path: PathBuf,
    keep: bool,
}
impl Drop for CopyScratch {
    fn drop(&mut self) {
        if !self.keep {
            for suffix in ["", "-journal", "-wal", "-shm"] {
                let _ = std::fs::remove_file(sidecar(&self.path, suffix));
            }
        }
    }
}

pub(super) struct HistorySource {
    pub identity: FileIdentity,
    pub epoch: Uuid,
    file: File,
    configured: PathBuf,
    pub direct: Option<SqliteConnection>,
    root: PathBuf,
    cached: Option<(FileStamps, PathBuf)>,
    sequence: u64,
}
impl HistorySource {
    pub async fn new(configured: &Path, root: PathBuf) -> Result<Self, Failure> {
        let identity = stamp(configured)?.identity;
        let file = File::open(configured).map_err(|_| "History file unavailable")?;
        check_handle(&file, configured, &identity)?;
        std::fs::create_dir_all(&root).map_err(|_| "browser copy directory unavailable")?;
        if std::fs::symlink_metadata(&root)
            .map_err(|_| "browser copy directory metadata unavailable")?
            .file_type()
            .is_symlink()
        {
            return Err("browser copy directory cannot be a symlink".into());
        }
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| "browser copy directory permissions unavailable")?;
        let direct = match open(configured, true).await {
            Ok(mut connection) => {
                crate::sqlite_file::verify(&mut connection, configured).await?;
                // Opening does not take SQLite's read lock; a query does, and
                // reports whether Chromium holds an exclusive lock.
                match sqlx::query_scalar::<_, i64>("PRAGMA schema_version")
                    .fetch_one(&mut connection)
                    .await
                {
                    Ok(_) => Some(connection),
                    Err(e) if busy(&e) => None,
                    Err(_) => return Err("History schema version unavailable".into()),
                }
            }
            Err(e) if busy(&e) => None,
            Err(_) => return Err("History read-only open unavailable".into()),
        };
        Ok(Self {
            identity,
            epoch: Uuid::now_v7(),
            file,
            configured: configured.to_owned(),
            direct,
            root,
            cached: None,
            sequence: 0,
        })
    }
    pub fn verify(&self) -> Result<(), Failure> {
        check_handle(&self.file, &self.configured, &self.identity)
    }
    pub fn next_path(&mut self, prefix: &str) -> PathBuf {
        self.sequence += 1;
        self.root
            .join(format!("{prefix}-{}-{}.sqlite", self.epoch, self.sequence))
    }
    pub async fn copied_view(
        &mut self,
        stop: &tokio::sync::watch::Receiver<bool>,
        pause: &mut Option<sinex_store::TestPause>,
    ) -> Result<(PathBuf, Value), Failure> {
        let mut last = "locked History copy unavailable".to_string();
        for attempt in 0..5 {
            stopped(stop)?;
            self.verify()?;
            let source = PathBuf::from(&self.identity.resolved);
            let before = file_stamps(&source)?;
            if before.wal.is_some() {
                return Err("locked WAL History cannot use rollback-journal copying".into());
            }
            let mut header = [0u8; 20];
            File::open(&source)
                .and_then(|mut f| f.read_exact(&mut header))
                .map_err(|_| "SQLite header unavailable")?;
            if &header[..16] != b"SQLite format 3\0" || header[18] != 1 || header[19] != 1 {
                return Err("locked source is not a supported rollback-journal database".into());
            }
            if let Some((prior, path)) = &self.cached
                && &before == prior
            {
                tokio::select! {_ = tokio::time::sleep(Duration::from_millis(50))=>{},_ = async {let mut stop=stop.clone();let _=stop.changed().await;}=>return Err("browser acquisition cancelled".into())}
                let after = file_stamps(&source)?;
                self.verify()?;
                if after == before && quiet(&before) {
                    return Ok((
                        path.clone(),
                        json!({"kind":"verified_locked_copy_reused","source_pair":before,"copied_bytes":0,"premise":"unchanged coherent local-FS identities/size/nanosecond metadata imply unchanged bytes; unobserved ABA and arbitrary byte overwrites are not proved"}),
                    ));
                }
            }
            let destination = self.next_path("view");
            let mut scratch = CopyScratch {
                path: destination.clone(),
                keep: false,
            };
            let source_copy = source.clone();
            let dest_copy = destination.clone();
            let stop_copy = stop.clone();
            let copied_stamp = before.clone();
            let result = tokio::task::spawn_blocking(move || -> Result<(u64, bool), String> {
                let (mut bytes, cloned) =
                    copy_file(&source_copy, &dest_copy, &copied_stamp.main, &stop_copy)?;
                if let Some(journal) = &copied_stamp.journal {
                    let (n, _) = copy_file(
                        &sidecar(&source_copy, "-journal"),
                        &sidecar(&dest_copy, "-journal"),
                        journal,
                        &stop_copy,
                    )?;
                    bytes = bytes.checked_add(n).ok_or("journal copy byte overflow")?;
                    reject_super_journal(&sidecar(&dest_copy, "-journal"))?;
                }
                Ok((bytes, cloned))
            })
            .await
            .map_err(|_| "owned copy worker failed")?;
            sinex_store::TestPause::browser_copy_before_check(
                pause,
                json!({"source_pair":before,"attempt":attempt,"owned_copy":destination}),
            )
            .await
            .map_err(|_| "test pause refused")?;
            let after = file_stamps(&source);
            if let (Ok((bytes, cloned)), Ok(after)) = (result, after) {
                self.verify()?;
                if after == before {
                    tokio::select! {_ = tokio::time::sleep(Duration::from_millis(50))=>{},_ = async {let mut stop=stop.clone();let _=stop.changed().await;}=>return Err("browser acquisition cancelled".into())}
                    let rechecked = file_stamps(&source)?;
                    self.verify()?;
                    if rechecked == before && quiet(&before) {
                        // Opening read-write replays the copied journal into the copy
                        // only. Super-journals were refused above; SHM is never copied.
                        let mut connection = open(&destination, false)
                            .await
                            .map_err(|_| "owned journal recovery failed")?;
                        let integrity: String = sqlx::query_scalar("PRAGMA quick_check")
                            .fetch_one(&mut connection)
                            .await
                            .map_err(|_| "owned copy integrity unavailable")?;
                        connection
                            .close()
                            .await
                            .map_err(|_| "owned copy close failed")?;
                        if integrity == "ok" {
                            if let Some((_, old)) = self.cached.take() {
                                let _ = std::fs::remove_file(&old);
                                let _ = std::fs::remove_file(sidecar(&old, "-journal"));
                            }
                            self.cached = Some((before.clone(), destination.clone()));
                            scratch.keep = true;
                            return Ok((
                                destination,
                                json!({"kind":"verified_locked_rollback_copy","attempts":attempt+1,"source_pair":before,"copied_bytes":bytes,"main_reflink":cloned,"integrity":"quick_check_ok","premise":"paired files stable before/after copying and quiet recheck under coherent local-FS metadata; integrity alone is not concurrency proof; no unconditional atomic snapshot claim"}),
                            ));
                        }
                        last = "owned copy failed SQLite integrity".into();
                    } else {
                        last = "source changed at quiet recheck".into();
                    }
                } else {
                    last = "source or journal changed during copy".into();
                }
            } else {
                last = "source or journal appearance/disappearance/copy unavailable".into();
            }
            drop(scratch);
            if attempt < 4 {
                tokio::select! {_ = tokio::time::sleep(Duration::from_secs(1<<attempt))=>{},_ = async {let mut stop=stop.clone();let _=stop.changed().await;}=>return Err("browser acquisition cancelled".into())}
            }
        }
        Err(format!("busy_skipped after five verified-copy attempts: {last}").into())
    }
}
impl Drop for HistorySource {
    fn drop(&mut self) {
        if let Some((_, path)) = &self.cached {
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_file(sidecar(path, "-journal"));
        }
    }
}
#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
