//! A producer owns its frozen publication frontier and immutable captured bytes.
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
    pub async fn freeze(&self) -> OwnedRwLockWriteGuard<()> {
        self.0.clone().write_owned().await
    }
}

/// Native producer-owned files, closed and synced before returning from capture.
pub struct Captured {
    pub directory: tempfile::TempDir,
    pub files: Vec<File>,
}

pub struct Participant(tokio::task::JoinHandle<()>);
impl Drop for Participant {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Cut {
    token: String,
    _guard: OwnedRwLockWriteGuard<()>,
    captured: Option<(Hold, Captured)>,
}
impl Participant {
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
        let incarnation = uuid::Uuid::now_v7().to_string();
        Ok(Self(tokio::spawn(async move {
            let mut cut: Option<Cut> = None;
            loop {
                // Expiry invalidates the token before publication resumes. A delayed
                // capture can never certify bytes from an expired frontier.
                let message = match tokio::time::timeout(IDLE, requests.next()).await {
                    Ok(Some(message)) => message,
                    Ok(None) => break,
                    Err(_) => {
                        cut = None;
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
                    let request: Request = serde_json::from_slice(&message.payload).map_err(|e|e.to_string())?;
                    match request {
                        Request::Freeze { token } => {
                            if let Some(current) = &cut {
                                if current.token != token { return Err("another backup cut is active".into()); }
                            } else {
                                let guard = tokio::time::timeout(IDLE, publication.0.clone().write_owned()).await.map_err(|_|"publication has not settled")?;
                                cut = Some(Cut { token: token.clone(), _guard: guard, captured: None });
                            }
                            Ok(Reply::Frozen { token, incarnation: incarnation.clone() })
                        }
                        Request::Capture { token, boundary } => {
                            let current = cut.as_mut().filter(|c|c.token==token).ok_or("backup cut expired or belongs to another incarnation")?;
                            if let Some((previous, _)) = &current.captured {
                                if previous != &boundary { return Err("backup cut already captured for a different core boundary".into()); }
                            } else {
                                let callback = capture.clone();
                                let cancelled = Arc::new(AtomicBool::new(false));
                                struct Cancel(Arc<AtomicBool>);
                                impl Drop for Cancel {fn drop(&mut self){self.0.store(true,Ordering::Release);}}
                                let _cancel = Cancel(cancelled.clone());
                                let captured = tokio::task::spawn_blocking(move || {
                                    let captured = callback(cancelled)?;
                                    validate(&captured)?;
                                    Ok::<_,String>(captured)
                                }).await.map_err(|e|e.to_string())??;
                                current.captured = Some((boundary.clone(), captured));
                            }
                            let files = current.captured.as_ref().expect("captured above").1.files.clone();
                            Ok(Reply::Captured { token, incarnation: incarnation.clone(), boundary, files })
                        }
                        Request::Read { token, file, offset } => {
                            let current = cut.as_ref().filter(|c|c.token==token).ok_or("backup cut expired")?;
                            let captured = &current.captured.as_ref().ok_or("backup cut not captured")?.1;
                            let descriptor = captured.files.get(file).ok_or("unknown captured file")?;
                            if offset > descriptor.bytes { return Err("offset beyond captured prefix".into()); }
                            let mut input = std::fs::File::open(captured.directory.path().join(&descriptor.path)).map_err(|e|e.to_string())?;
                            input.seek(SeekFrom::Start(offset)).map_err(|e|e.to_string())?;
                            let mut data = Vec::new();
                            input.take(READ_BYTES.min(descriptor.bytes-offset)).read_to_end(&mut data).map_err(|e|e.to_string())?;
                            if data.len() as u64 != READ_BYTES.min(descriptor.bytes-offset) { return Err("captured prefix shortened".into()); }
                            Ok(Reply::Bytes { offset, data })
                        }
                        Request::Release { token } => {
                            if cut.as_ref().is_some_and(|c|c.token!=token) { return Err("another backup cut is active".into()); }
                            cut = None;
                            Ok(Reply::Released)
                        }
                    }
                }.await;
                    Some(result.unwrap_or_else(|reason| Reply::Unavailable { reason }))
                };
                let _ = crate::replies::respond_json_wait(&client, inbox.to_string(), build, IDLE)
                    .await;
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
fn validate(captured: &Captured) -> Result<(), String> {
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
        let mut hash = blake3::Hasher::new();
        let bytes = std::io::copy(&mut input, &mut hash).map_err(|e| e.to_string())?;
        if bytes != file.bytes || hash.finalize().to_hex().as_str() != file.blake3 {
            return Err("captured file identity differs".into());
        }
    }
    Ok(())
}

/// Copy each open file's bounded prefix; appending acquisition may continue.
/// The owner prevents replacement, deletion and checkpoint advance until this returns.
pub fn capture_tree(source: &Path, destination: &Path) -> Result<Vec<File>, String> {
    fn copy(
        source: &Path,
        destination: &Path,
        base: &Path,
        files: &mut Vec<File>,
    ) -> Result<(), String> {
        std::fs::create_dir_all(destination).map_err(|e| e.to_string())?;
        for entry in std::fs::read_dir(source).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let kind = entry.file_type().map_err(|e| e.to_string())?;
            let output = destination.join(entry.file_name());
            if kind.is_dir() {
                copy(&entry.path(), &output, base, files)?;
            } else if kind.is_file() {
                let input = std::fs::File::open(entry.path()).map_err(|e| e.to_string())?;
                let length = input.metadata().map_err(|e| e.to_string())?.len();
                let mut output_file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&output)
                    .map_err(|e| e.to_string())?;
                let bytes = std::io::copy(&mut input.take(length), &mut output_file)
                    .map_err(|e| e.to_string())?;
                if bytes != length {
                    return Err("producer prefix shortened during capture".into());
                }
                output_file.sync_all().map_err(|e| e.to_string())?;
                let mut input = std::fs::File::open(&output).map_err(|e| e.to_string())?;
                let mut hash = blake3::Hasher::new();
                std::io::copy(&mut input, &mut hash).map_err(|e| e.to_string())?;
                let path = output
                    .strip_prefix(base)
                    .map_err(|e| e.to_string())?
                    .to_str()
                    .ok_or("non UTF-8 custody path")?
                    .to_owned();
                files.push(File {
                    path,
                    bytes,
                    blake3: hash.finalize().to_hex().to_string(),
                });
            } else {
                return Err("producer custody contains a non-regular file".into());
            }
        }
        std::fs::File::open(destination)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())
    }
    let mut files = Vec::new();
    copy(source, destination, destination, &mut files)?;
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

/// A frozen participant must be captured against the exact exported core cut.
pub struct Frozen {
    client: async_nats::Client,
    subjects: Subjects,
    pub producer: String,
    token: String,
    pub incarnation: String,
}
impl Frozen {
    pub async fn freeze(
        client: async_nats::Client,
        subjects: Subjects,
        producer: String,
    ) -> Result<Self, String> {
        let token = uuid::Uuid::now_v7().to_string();
        match request(
            &client,
            &subjects,
            &producer,
            &Request::Freeze {
                token: token.clone(),
            },
        )
        .await?
        {
            Reply::Frozen {
                token: found,
                incarnation,
            } if found == token => Ok(Self {
                client,
                subjects,
                producer,
                token,
                incarnation,
            }),
            other => Err(format!("producer did not freeze: {other:?}")),
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
                incarnation,
                boundary: found,
                files,
            } if token == self.token && incarnation == self.incarnation && &found == boundary => {
                files
            }
            other => {
                return Err(format!(
                    "producer did not capture frozen core cut: {other:?}"
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
