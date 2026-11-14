//! A producer pauses publication for a backup and copies its own state files.
use futures::StreamExt;
use sinex_model::backup::{File, Hold};
use sinex_protocol::{
    Subjects,
    backup::{Reply, Request},
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

const IDLE: Duration = Duration::from_secs(60);
const READ_BYTES: u64 = 64 * 1024;

/// Every send, checkpoint advance and retirement holds this gate. Acquisition does not.
#[derive(Clone, Default)]
pub struct Publication(Arc<RwLock<()>>);
impl Publication {
    pub async fn enter(&self) -> OwnedRwLockReadGuard<()> {
        self.0.clone().read_owned().await
    }
    pub async fn pause(&self) -> OwnedRwLockWriteGuard<()> {
        self.0.clone().write_owned().await
    }
}

/// Native producer-owned files, closed and synced before returning from capture.
pub struct Captured {
    pub directory: tempfile::TempDir,
    pub files: Vec<File>,
}

/// Answers the daemon's backup requests for one producer until dropped.
pub struct BackupResponder(tokio::task::JoinHandle<()>);
impl Drop for BackupResponder {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A backup in progress: publication stays paused while this exists.
struct PausedBackup {
    token: String,
    _guard: OwnedRwLockWriteGuard<()>,
    captured: Option<(Hold, Captured)>,
}
impl BackupResponder {
    pub async fn start(
        client: async_nats::Client,
        subjects: Subjects,
        producer: String,
        publication: Publication,
        capture: Arc<dyn Fn(Arc<AtomicBool>) -> Result<Captured, String> + Send + Sync>,
    ) -> Result<Self, String> {
        if !sinex_protocol::nats::valid_producer(&producer) {
            return Err("invalid backup producer".into());
        }
        let mut requests = client
            .subscribe(subjects.backup(&producer))
            .await
            .map_err(|e| e.to_string())?;
        client.flush().await.map_err(|e| e.to_string())?;
        let run_id = uuid::Uuid::now_v7().to_string();
        Ok(Self(tokio::spawn(async move {
            let mut paused: Option<PausedBackup> = None;
            loop {
                // An idle backup expires and publication resumes; its token stops
                // working, so a late copy can never describe the resumed state.
                let message = match tokio::time::timeout(IDLE, requests.next()).await {
                    Ok(Some(message)) => message,
                    Ok(None) => break,
                    Err(_) => {
                        if paused.is_some() {
                            eprintln!("backup producer {producer}: pause expired in run {run_id}");
                        }
                        paused = None;
                        continue;
                    }
                };
                let Some(inbox) = message.reply else {
                    continue;
                };
                if !inbox
                    .as_str()
                    .starts_with(&format!("{}.", subjects.backup_inbox(&producer)))
                {
                    continue;
                }
                let build = async {
                    let result: Result<Reply, String> = async {
                        let request: Request =
                            serde_json::from_slice(&message.payload).map_err(|e| e.to_string())?;
                        match request {
                            Request::Pause { token } => {
                                if let Some(current) = &paused {
                                    if current.token != token {
                                        return Err("another backup is in progress".into());
                                    }
                                } else {
                                    let guard = publication.0.clone().write_owned().await;
                                    paused = Some(PausedBackup {
                                        token: token.clone(),
                                        _guard: guard,
                                        captured: None,
                                    });
                                }
                                Ok(Reply::Paused {
                                    token,
                                    run_id: run_id.clone(),
                                })
                            }
                            Request::Capture { token, boundary } => {
                                let current = paused
                                    .as_mut()
                                    .filter(|c| c.token == token)
                                    .ok_or_else(|| {
                                        format!(
                                            "backup pause is not active in producer run {run_id}"
                                        )
                                    })?;
                                if let Some((previous, _)) = &current.captured {
                                    if previous != &boundary {
                                        return Err(
                                            "backup already copied for a different hold".into()
                                        );
                                    }
                                } else {
                                    let callback = capture.clone();
                                    let cancelled = Arc::new(AtomicBool::new(false));
                                    struct Cancel(Arc<AtomicBool>);
                                    impl Drop for Cancel {
                                        fn drop(&mut self) {
                                            self.0.store(true, Ordering::Release);
                                        }
                                    }
                                    let _cancel = Cancel(cancelled.clone());
                                    let captured = tokio::task::spawn_blocking(move || {
                                        let captured = callback(cancelled.clone())?;
                                        validate(&captured, &cancelled)?;
                                        Ok::<_, String>(captured)
                                    })
                                    .await
                                    .map_err(|e| e.to_string())??;
                                    current.captured = Some((boundary.clone(), captured));
                                }
                                let files = current
                                    .captured
                                    .as_ref()
                                    .expect("captured above")
                                    .1
                                    .files
                                    .clone();
                                Ok(Reply::Captured {
                                    token,
                                    run_id: run_id.clone(),
                                    boundary,
                                    files,
                                })
                            }
                            Request::Read {
                                token,
                                file,
                                offset,
                            } => {
                                let current = paused
                                    .as_ref()
                                    .filter(|c| c.token == token)
                                    .ok_or("backup expired")?;
                                let captured = &current
                                    .captured
                                    .as_ref()
                                    .ok_or("backup files are not copied yet")?
                                    .1;
                                let descriptor =
                                    captured.files.get(file).ok_or("unknown captured file")?;
                                if offset > descriptor.bytes {
                                    return Err("offset beyond captured prefix".into());
                                }
                                let mut input = std::fs::File::open(
                                    captured.directory.path().join(&descriptor.path),
                                )
                                .map_err(|e| e.to_string())?;
                                input
                                    .seek(SeekFrom::Start(offset))
                                    .map_err(|e| e.to_string())?;
                                let mut data = Vec::new();
                                input
                                    .take(READ_BYTES.min(descriptor.bytes - offset))
                                    .read_to_end(&mut data)
                                    .map_err(|e| e.to_string())?;
                                if data.len() as u64 != READ_BYTES.min(descriptor.bytes - offset) {
                                    return Err("captured prefix shortened".into());
                                }
                                Ok(Reply::Bytes { offset, data })
                            }
                            Request::Release { token } => {
                                if paused.as_ref().is_some_and(|c| c.token != token) {
                                    return Err("another backup is in progress".into());
                                }
                                paused = None;
                                Ok(Reply::Released)
                            }
                        }
                    }
                    .await;
                    Some(result.unwrap_or_else(|reason| Reply::Unavailable { reason }))
                };
                if crate::replies::respond_json_wait(&client, inbox.to_string(), build, IDLE)
                    .await
                    .is_err()
                {
                    paused = None;
                }
            }
        })))
    }
}

fn relative(path: &str) -> Result<(), String> {
    if path.is_empty()
        || Path::new(path)
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err("captured file name must be relative".into());
    }
    Ok(())
}
fn validate(captured: &Captured, cancelled: &AtomicBool) -> Result<(), String> {
    let mut names = std::collections::HashSet::new();
    for file in &captured.files {
        relative(&file.path)?;
        if !names.insert(&file.path) {
            return Err("duplicate captured file".into());
        }
        let path = captured.directory.path().join(&file.path);
        if std::fs::symlink_metadata(&path)
            .map_err(|e| e.to_string())?
            .file_type()
            .is_symlink()
        {
            return Err("captured file is a symlink".into());
        }
        let mut input = std::fs::File::open(path).map_err(|e| e.to_string())?;
        if input.metadata().map_err(|e| e.to_string())?.len() != file.bytes {
            return Err("captured file length differs".into());
        }
        let hash = copy_prefix(&mut input, &mut std::io::sink(), file.bytes, cancelled)?;
        if hash != file.blake3 {
            return Err("captured file identity differs".into());
        }
    }
    Ok(())
}

fn cancelled(flag: &AtomicBool) -> Result<(), String> {
    if flag.load(Ordering::Acquire) {
        Err("producer capture cancelled".into())
    } else {
        Ok(())
    }
}

fn copy_prefix(
    input: &mut impl Read,
    output: &mut impl Write,
    length: u64,
    flag: &AtomicBool,
) -> Result<String, String> {
    let mut remaining = length;
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0; 64 * 1024];
    cancelled(flag)?;
    while remaining != 0 {
        cancelled(flag)?;
        let bytes =
            usize::try_from(remaining.min(buffer.len() as u64)).map_err(|e| e.to_string())?;
        input
            .read_exact(&mut buffer[..bytes])
            .map_err(|e| format!("short or unreadable captured prefix: {e}"))?;
        output
            .write_all(&buffer[..bytes])
            .map_err(|e| e.to_string())?;
        hash.update(&buffer[..bytes]);
        remaining -= bytes as u64;
    }
    cancelled(flag)?;
    Ok(hash.finalize().to_hex().to_string())
}

/// Copy each open file's bounded prefix; appending acquisition may continue.
/// The owner prevents replacement, deletion and checkpoint advance until this returns.
/// Run on a blocking worker so cancellation and protocol flow keep progressing.
pub fn capture_tree(
    source: &Path,
    destination: &Path,
    flag: &AtomicBool,
) -> Result<Vec<File>, String> {
    fn copy(
        source: &Path,
        destination: &Path,
        base: &Path,
        files: &mut Vec<File>,
        flag: &AtomicBool,
    ) -> Result<(), String> {
        cancelled(flag)?;
        std::fs::create_dir_all(destination).map_err(|e| e.to_string())?;
        for entry in std::fs::read_dir(source).map_err(|e| e.to_string())? {
            cancelled(flag)?;
            let entry = entry.map_err(|e| e.to_string())?;
            let kind = entry.file_type().map_err(|e| e.to_string())?;
            let output = destination.join(entry.file_name());
            if kind.is_dir() {
                copy(&entry.path(), &output, base, files, flag)?;
            } else if kind.is_file() {
                let mut input = std::fs::File::open(entry.path()).map_err(|e| e.to_string())?;
                let bytes = input.metadata().map_err(|e| e.to_string())?.len();
                let mut output_file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&output)
                    .map_err(|e| e.to_string())?;
                let hash = match copy_prefix(&mut input, &mut output_file, bytes, flag) {
                    Ok(hash) => hash,
                    Err(error) => {
                        drop(output_file);
                        if let Err(cleanup) = std::fs::remove_file(&output) {
                            return Err(format!(
                                "{error}; incomplete captured file cleanup: {cleanup}"
                            ));
                        }
                        return Err(error);
                    }
                };
                output_file.sync_all().map_err(|e| e.to_string())?;
                let path = output
                    .strip_prefix(base)
                    .map_err(|e| e.to_string())?
                    .to_str()
                    .ok_or("backup file path is not UTF-8")?
                    .to_owned();
                files.push(File {
                    path,
                    bytes,
                    blake3: hash,
                });
            } else {
                return Err("producer state contains a non-regular file".into());
            }
        }
        cancelled(flag)?;
        std::fs::File::open(destination)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())
    }
    let mut files = Vec::new();
    copy(source, destination, destination, &mut files, flag)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

pub async fn request(
    client: &async_nats::Client,
    subjects: &Subjects,
    producer: &str,
    request: &Request,
) -> Result<Reply, String> {
    let bytes = serde_json::to_vec(request).map_err(|e| e.to_string())?;
    crate::replies::request_inbox(
        client,
        subjects.backup(producer),
        subjects.backup_inbox(producer),
        bytes,
        IDLE,
    )
    .await
    .map_err(|e| e.to_string())
}

/// A producer paused for one backup; its files are copied for that backup's hold.
pub struct PausedProducer {
    client: async_nats::Client,
    subjects: Subjects,
    pub producer: String,
    token: String,
    pub run_id: String,
}
impl PausedProducer {
    pub async fn pause(
        client: async_nats::Client,
        subjects: Subjects,
        producer: String,
    ) -> Result<Self, String> {
        let token = uuid::Uuid::now_v7().to_string();
        match request(
            &client,
            &subjects,
            &producer,
            &Request::Pause {
                token: token.clone(),
            },
        )
        .await?
        {
            Reply::Paused {
                token: found,
                run_id,
            } if found == token => Ok(Self {
                client,
                subjects,
                producer,
                token,
                run_id,
            }),
            other => Err(format!("producer did not pause: {other:?}")),
        }
    }
    pub async fn acquire(&self, boundary: &Hold, output: &Path) -> Result<Vec<File>, String> {
        let files = match request(
            &self.client,
            &self.subjects,
            &self.producer,
            &Request::Capture {
                token: self.token.clone(),
                boundary: boundary.clone(),
            },
        )
        .await?
        {
            Reply::Captured {
                token,
                run_id,
                boundary: found,
                files,
            } if token == self.token && run_id == self.run_id && &found == boundary => files,
            other => {
                return Err(format!(
                    "producer did not copy its files for this hold: {other:?}"
                ));
            }
        };
        for (index, file) in files.iter().enumerate() {
            relative(&file.path)?;
            let path = output.join(&file.path);
            std::fs::create_dir_all(path.parent().ok_or("captured file has no parent")?)
                .map_err(|e| e.to_string())?;
            let mut out = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .map_err(|e| e.to_string())?;
            let mut offset = 0;
            let mut hash = blake3::Hasher::new();
            while offset < file.bytes {
                match request(
                    &self.client,
                    &self.subjects,
                    &self.producer,
                    &Request::Read {
                        token: self.token.clone(),
                        file: index,
                        offset,
                    },
                )
                .await?
                {
                    Reply::Bytes {
                        offset: found,
                        data,
                    } if found == offset
                        && !data.is_empty()
                        && data.len() as u64 <= file.bytes - offset =>
                    {
                        out.write_all(&data).map_err(|e| e.to_string())?;
                        hash.update(&data);
                        offset += data.len() as u64;
                    }
                    other => return Err(format!("producer transfer incomplete: {other:?}")),
                }
            }
            out.sync_all().map_err(|e| e.to_string())?;
            if hash.finalize().to_hex().as_str() != file.blake3 {
                return Err("producer artifact digest differs".into());
            }
        }
        let mut directories = std::collections::BTreeSet::new();
        for file in &files {
            let mut parent = output.join(&file.path).parent().map(Path::to_owned);
            while let Some(path) = parent {
                if !path.starts_with(output) {
                    break;
                }
                directories.insert(path.clone());
                if path == output {
                    break;
                }
                parent = path.parent().map(Path::to_owned);
            }
        }
        for directory in directories.into_iter().rev() {
            std::fs::File::open(directory)
                .and_then(|f| f.sync_all())
                .map_err(|e| e.to_string())?;
        }
        Ok(files)
    }
    pub async fn release(&self) -> Result<(), String> {
        match request(
            &self.client,
            &self.subjects,
            &self.producer,
            &Request::Release {
                token: self.token.clone(),
            },
        )
        .await?
        {
            Reply::Released => Ok(()),
            other => Err(format!("producer release failed: {other:?}")),
        }
    }
}

#[cfg(test)]
mod tests;
