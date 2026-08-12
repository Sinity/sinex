use crate::runtime::{RuntimeResult, SinexError};
use camino::{Utf8Path, Utf8PathBuf};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use sinex_primitives::domain::ContentKey;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command as AsyncCommand;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Directory mode for content-store-owned directories: owner+group only.
pub(super) const CONTENT_STORE_DIR_MODE: u32 = 0o750;
/// File mode for content-store-owned files: owner+group read/write, no
/// other-permission bits.
pub(super) const CONTENT_STORE_FILE_MODE: u32 = 0o640;

/// Explicitly restrict permissions on a content-store-owned path (sinex-vyi3).
/// See the identical helper in `event_engine::material_assembler::io` for the
/// full rationale -- same defense-in-depth pattern, duplicated here rather
/// than shared across modules to avoid a cross-cutting dependency for a
/// single small utility.
pub(super) fn restrict_permissions(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    if let Err(error) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)) {
        warn!(
            path = %path.display(),
            %error,
            "failed to restrict permissions on content-store-owned path"
        );
    }
}

pub mod cas_fsck;
pub mod gc;
pub mod manager;
pub mod path_validator;

pub use cas_fsck::{CasFileStatus, CasFsckReport, CasStatus, check_cas, sweep_orphans_cas};
pub use manager::{BlobMetadata, ContentStoreManager};
pub use path_validator::{VerifiedPath, create_secure_temp_path, validate_and_convert_path};

pub const LOCAL_BLAKE3_CAS_BACKEND: &str = ContentKey::LOCAL_BLAKE3_CAS_BACKEND;
const LOCAL_BLAKE3_CAS_DIR: &str = "sinex-cas";
pub(crate) const CAS_LIFECYCLE_DIR: &str = ".sinex-cas-lifecycle";
const CAS_PENDING_DELETE_DIR: &str = "pending-deletes";
const CAS_QUARANTINE_DIR: &str = "quarantine";
const CONTENT_STORE_PROCESS_COUNTERS_PATH_ENV: &str = "SINEX_CONTENT_STORE_PROCESS_COUNTERS_PATH";

static CONTENT_STORE_PROCESS_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
static CONTENT_STORE_PROCESS_COUNTERS: OnceLock<ContentStoreProcessCounterState> = OnceLock::new();
#[cfg(test)]
static TEST_FAIL_NEXT_PENDING_DELETE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentStoreProcessCounters {
    pub blocking_commands: u64,
    pub async_commands: u64,
    pub git_commands: u64,
    pub git_annex_commands: u64,
}

impl ContentStoreProcessCounters {
    #[must_use]
    pub fn saturating_delta_since(self, baseline: Self) -> Self {
        Self {
            blocking_commands: self
                .blocking_commands
                .saturating_sub(baseline.blocking_commands),
            async_commands: self.async_commands.saturating_sub(baseline.async_commands),
            git_commands: self.git_commands.saturating_sub(baseline.git_commands),
            git_annex_commands: self
                .git_annex_commands
                .saturating_sub(baseline.git_annex_commands),
        }
    }
}

#[derive(Default)]
struct ContentStoreProcessCounterState {
    blocking_commands: AtomicU64,
    async_commands: AtomicU64,
    git_commands: AtomicU64,
    git_annex_commands: AtomicU64,
}

fn content_store_process_lock() -> &'static AsyncMutex<()> {
    CONTENT_STORE_PROCESS_LOCK.get_or_init(|| AsyncMutex::new(()))
}

fn content_store_process_counters() -> &'static ContentStoreProcessCounterState {
    CONTENT_STORE_PROCESS_COUNTERS.get_or_init(ContentStoreProcessCounterState::default)
}

#[must_use]
pub fn content_store_process_counters_snapshot() -> ContentStoreProcessCounters {
    let counters = content_store_process_counters();
    ContentStoreProcessCounters {
        blocking_commands: counters.blocking_commands.load(Ordering::Relaxed),
        async_commands: counters.async_commands.load(Ordering::Relaxed),
        git_commands: counters.git_commands.load(Ordering::Relaxed),
        git_annex_commands: counters.git_annex_commands.load(Ordering::Relaxed),
    }
}

pub fn reset_content_store_process_counters() {
    let counters = content_store_process_counters();
    counters.blocking_commands.store(0, Ordering::Relaxed);
    counters.async_commands.store(0, Ordering::Relaxed);
    counters.git_commands.store(0, Ordering::Relaxed);
    counters.git_annex_commands.store(0, Ordering::Relaxed);
    persist_content_store_process_counters_snapshot(content_store_process_counters_snapshot());
}

fn persist_content_store_process_counters_snapshot(snapshot: ContentStoreProcessCounters) {
    let Some(path) = std::env::var_os(CONTENT_STORE_PROCESS_COUNTERS_PATH_ENV) else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    if let Some(parent) = path.parent()
        && let Err(error) = std::fs::create_dir_all(parent)
    {
        warn!(
            path = %parent.display(),
            error = %error,
            "Failed to create content-store process counter snapshot directory"
        );
        return;
    }

    let temp_path = path.with_extension(format!("json.tmp-{}", std::process::id()));
    let bytes = match serde_json::to_vec_pretty(&snapshot) {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(
                error = %error,
                "Failed to serialize content-store process counter snapshot"
            );
            return;
        }
    };
    if let Err(error) = std::fs::write(&temp_path, bytes) {
        warn!(
            path = %temp_path.display(),
            error = %error,
            "Failed to write content-store process counter snapshot"
        );
        return;
    }
    if let Err(error) = std::fs::rename(&temp_path, &path) {
        warn!(
            source = %temp_path.display(),
            target = %path.display(),
            error = %error,
            "Failed to publish content-store process counter snapshot"
        );
    }
}

fn record_process_invocation(program: &OsStr, blocking: bool) {
    let counters = content_store_process_counters();
    if blocking {
        counters.blocking_commands.fetch_add(1, Ordering::Relaxed);
    } else {
        counters.async_commands.fetch_add(1, Ordering::Relaxed);
    }

    let command_name = std::path::Path::new(program)
        .file_name()
        .unwrap_or(program)
        .to_string_lossy();
    match command_name.as_ref() {
        "git" => {
            counters.git_commands.fetch_add(1, Ordering::Relaxed);
        }
        "git-annex" => {
            counters.git_annex_commands.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
    persist_content_store_process_counters_snapshot(content_store_process_counters_snapshot());
}

fn run_command_blocking(
    mut cmd: Command,
    context: &'static str,
) -> RuntimeResult<std::process::Output> {
    let _guard = loop {
        if let Ok(guard) = content_store_process_lock().try_lock() {
            break guard;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    record_process_invocation(cmd.get_program(), true);
    cmd.output()
        .map_err(|e| SinexError::processing(context).with_source(e))
}

async fn run_command_async(
    mut cmd: AsyncCommand,
    context: &'static str,
) -> RuntimeResult<std::process::Output> {
    let _guard = content_store_process_lock().lock().await;
    record_process_invocation(cmd.as_std().get_program(), false);
    cmd.output()
        .await
        .map_err(|e| SinexError::processing(context).with_source(e))
}

/// Resolve the shared content-store root path.
///
/// Reads `SINEX_CONTENT_STORE_PATH` (the shared key), falling back to
/// `$HOME/.local/share/sinex/content-store` or the environment work directory.
/// Both the gateway (`GatewayConfig::content_store_path`) and source runtimes
/// resolve from this same key so they address one CAS — replay reading source
/// material must hit the exact store that ingestion wrote, or it fails closed.
#[must_use]
pub fn default_content_store_path() -> camino::Utf8PathBuf {
    if let Ok(v) = std::env::var("SINEX_CONTENT_STORE_PATH") {
        match sinex_primitives::validation::validate_path(&v) {
            Ok(path) => return path,
            Err(error) => {
                warn!(
                    value = %v,
                    %error,
                    "Invalid content-store path override; using the shared fallback"
                );
            }
        }
    }
    std::env::var("HOME").map_or_else(
        |_| {
            camino::Utf8PathBuf::from(
                sinex_primitives::environment::environment()
                    .work_directory("content-store")
                    .to_string_lossy()
                    .into_owned(),
            )
        },
        |home| camino::Utf8PathBuf::from(format!("{home}/.local/share/sinex/content-store")),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentStoreConfig {
    pub root_path: Utf8PathBuf,
    pub num_copies: Option<u8>,
    pub large_files: Option<String>,
    /// When true, git-annex is available for legacy large-object storage.
    /// When false (default), only local BLAKE3 CAS is used.
    #[serde(default)]
    pub legacy_annex_enabled: bool,
    /// Maximum blob size in bytes before ingestion is rejected.
    /// Defaults to the material assembler's 512 MiB acceptance ceiling. Set to 0 to disable.
    #[serde(default = "default_max_blob_size")]
    pub max_blob_size: usize,
}

const fn default_max_blob_size() -> usize {
    sinex_primitives::constants::limits::DEFAULT_SOURCE_MATERIAL_MAX_BYTES
}

fn configured_max_blob_size() -> usize {
    sinex_primitives::env::parse_or(
        "SINEX_CONTENT_STORE_MAX_BLOB_SIZE",
        default_max_blob_size(),
        "content-store maximum blob size",
    )
}

impl Default for ContentStoreConfig {
    fn default() -> Self {
        Self {
            root_path: Utf8PathBuf::new(),
            num_copies: None,
            large_files: None,
            legacy_annex_enabled: false,
            max_blob_size: configured_max_blob_size(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentBackend {
    LocalBlake3Cas,
    BackendDigest { backend: String },
}

impl ContentBackend {
    #[must_use]
    pub fn from_storage_backend(backend: impl Into<String>) -> Self {
        let backend = backend.into();
        if backend == LOCAL_BLAKE3_CAS_BACKEND {
            Self::LocalBlake3Cas
        } else {
            Self::BackendDigest { backend }
        }
    }

    #[must_use]
    pub fn storage_backend(&self) -> &str {
        match self {
            Self::LocalBlake3Cas => LOCAL_BLAKE3_CAS_BACKEND,
            Self::BackendDigest { backend } => backend,
        }
    }

    #[must_use]
    pub fn is_local_blake3_cas(&self) -> bool {
        matches!(self, Self::LocalBlake3Cas)
    }
}

impl Serialize for ContentBackend {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.storage_backend())
    }
}

impl<'de> Deserialize<'de> for ContentBackend {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BackendVisitor;

        impl Visitor<'_> for BackendVisitor {
            type Value = ContentBackend;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a content-store backend identifier")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(ContentBackend::from_storage_backend(value))
            }
        }

        deserializer.deserialize_str(BackendVisitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentStoreKey {
    pub key: String,
    pub backend: ContentBackend,
    pub size: u64,
    pub digest: String,
}

/// Durable record for a CAS object that has crossed the reference recheck and
/// is waiting for irreversible removal. The record is written before the
/// source name is moved, so a crash can always be reconciled without guessing
/// whether the object was already quarantined.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingDeletion {
    pub operation_id: String,
    pub key: ContentStoreKey,
    pub source_path: Utf8PathBuf,
    pub quarantine_path: Utf8PathBuf,
    pub created_at_unix_secs: u64,
    #[serde(skip)]
    pub(crate) record_path: Utf8PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnusedContentEntry {
    pub number: u32,
    pub key: ContentStoreKey,
}

#[derive(Debug, Clone)]
pub struct ContentVerificationResult {
    pub output: String,
    pub success: bool,
}

impl ContentStoreKey {
    pub fn parse(key_str: &str) -> RuntimeResult<Self> {
        let content_key = ContentKey::from_str(key_str).map_err(|err| {
            SinexError::processing(format!("Invalid content-store key format: {key_str}"))
                .with_context("reason", err)
        })?;
        let components = content_key.parse_components();
        let backend = ContentBackend::from_storage_backend(components.backend);
        let size = match content_key.parse_size_bytes() {
            Ok(Some(size)) => size,
            Ok(None) => {
                return Err(SinexError::processing(format!(
                    "Invalid size format in content-store key (missing '-s'): {key_str}"
                )));
            }
            Err(err) => {
                return Err(SinexError::processing(format!(
                    "Failed to parse size from content-store key: {key_str}"
                ))
                .with_context("reason", err));
            }
        };
        if backend.is_local_blake3_cas() {
            validate_local_blake3_digest(components.name)?;
        }

        Ok(ContentStoreKey {
            key: key_str.to_string(),
            backend,
            size,
            digest: components.name.to_string(),
        })
    }

    #[must_use]
    pub fn storage_backend(&self) -> &str {
        self.backend.storage_backend()
    }

    #[must_use]
    pub fn is_local_blake3_cas(&self) -> bool {
        self.backend.is_local_blake3_cas()
    }
}

fn validate_local_blake3_digest(digest: &str) -> RuntimeResult<()> {
    ContentKey::validate_local_blake3_digest(digest)
        .map_err(|err| SinexError::validation(err).with_context("digest_len", digest.len()))
}

async fn canonicalize_path_within_root(
    root: &Utf8Path,
    path: &Utf8Path,
) -> RuntimeResult<Utf8PathBuf> {
    let canonical_root = tokio::fs::canonicalize(root)
        .await
        .map_err(SinexError::io)?;
    let canonical_path = tokio::fs::canonicalize(path)
        .await
        .map_err(SinexError::io)?;
    let canonical_root = Utf8PathBuf::from_path_buf(canonical_root).map_err(|path| {
        SinexError::validation("canonical content-store root path is not valid UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    let canonical_path = Utf8PathBuf::from_path_buf(canonical_path).map_err(|path| {
        SinexError::validation("canonical content-store path is not valid UTF-8")
            .with_context("path", path.display().to_string())
    })?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(
            SinexError::validation("canonical content-store path escapes configured root")
                .with_context("path", canonical_path.to_string())
                .with_context("root", canonical_root.to_string()),
        );
    }
    Ok(canonical_path)
}

#[derive(Debug)]
pub struct MaterialContentStore {
    pub config: ContentStoreConfig,
}

/// A restart-safe cursor for the local CAS prefix tree.
///
/// The cursor advances only after an entire `XX/YY` directory has been
/// drained. If a pass stops inside that directory, resuming replays that
/// directory rather than risking a skipped entry from an unspecified
/// filesystem enumeration order.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CasWalkCheckpoint {
    pub prefix_a: Option<String>,
    pub prefix_b: Option<String>,
    pub complete: bool,
}

#[derive(Debug)]
pub struct CasWalkBatch {
    pub entries: Vec<(String, Utf8PathBuf, u64)>,
    pub checkpoint: CasWalkCheckpoint,
    pub complete: bool,
}

#[derive(Debug)]
pub struct CasWalker {
    prefix_dirs: Vec<Utf8PathBuf>,
    prefix_index: usize,
    hash_dirs: Vec<Utf8PathBuf>,
    hash_index: usize,
    hash_entries: Option<tokio::fs::ReadDir>,
    checkpoint: CasWalkCheckpoint,
}

async fn read_sorted_cas_directories(path: &Utf8Path) -> RuntimeResult<Vec<Utf8PathBuf>> {
    let mut read_dir = tokio::fs::read_dir(path).await.map_err(SinexError::io)?;
    let mut directories = Vec::new();
    while let Some(entry) = read_dir.next_entry().await.map_err(SinexError::io)? {
        if !entry.file_type().await.map_err(SinexError::io)?.is_dir() {
            continue;
        }
        let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            SinexError::processing(format!(
                "non-UTF-8 path in CAS directory tree: {}",
                path.display()
            ))
        })?;
        directories.push(path);
    }
    directories.sort_unstable();
    Ok(directories)
}

fn cas_path_name(path: &Utf8Path) -> RuntimeResult<String> {
    path.file_name().map(str::to_owned).ok_or_else(|| {
        SinexError::processing(format!("CAS directory path has no filename: {path}"))
    })
}

impl CasWalker {
    async fn new(cas_root: Utf8PathBuf, checkpoint: CasWalkCheckpoint) -> RuntimeResult<Self> {
        let mut prefix_dirs = if checkpoint.complete {
            Vec::new()
        } else {
            read_sorted_cas_directories(&cas_root).await?
        };
        if let Some(prefix_a) = checkpoint.prefix_a.as_deref() {
            prefix_dirs
                .retain(|path| cas_path_name(path).is_ok_and(|name| name.as_str() >= prefix_a));
        }
        Ok(Self {
            prefix_dirs,
            prefix_index: 0,
            hash_dirs: Vec::new(),
            hash_index: 0,
            hash_entries: None,
            checkpoint,
        })
    }

    /// Read at most `batch_size` CAS files while retaining only one open
    /// directory and the bounded two-level prefix lists.
    pub async fn next_batch(&mut self, batch_size: usize) -> RuntimeResult<CasWalkBatch> {
        let batch_size = batch_size.max(1);
        let mut entries = Vec::with_capacity(batch_size);

        while entries.len() < batch_size {
            if let Some(hash_entries) = &mut self.hash_entries {
                match hash_entries.next_entry().await.map_err(SinexError::io)? {
                    Some(entry) => {
                        if !entry.file_type().await.map_err(SinexError::io)?.is_file() {
                            continue;
                        }
                        let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                            SinexError::processing(format!(
                                "non-UTF-8 path in CAS tree: {}",
                                path.display()
                            ))
                        })?;
                        let hash = cas_path_name(&path)?;
                        let size = tokio::fs::metadata(&path)
                            .await
                            .map_err(SinexError::io)?
                            .len();
                        entries.push((hash, path, size));
                    }
                    None => {
                        self.hash_entries = None;
                        let prefix_a = cas_path_name(&self.prefix_dirs[self.prefix_index])?;
                        let prefix_b = cas_path_name(&self.hash_dirs[self.hash_index])?;
                        self.checkpoint = CasWalkCheckpoint {
                            prefix_a: Some(prefix_a),
                            prefix_b: Some(prefix_b),
                            complete: false,
                        };
                        self.hash_index += 1;
                    }
                }
                continue;
            }

            if self.prefix_index >= self.prefix_dirs.len() {
                self.checkpoint.complete = true;
                return Ok(CasWalkBatch {
                    entries,
                    checkpoint: self.checkpoint.clone(),
                    complete: true,
                });
            }

            if self.hash_index >= self.hash_dirs.len() {
                if !self.hash_dirs.is_empty() {
                    self.prefix_index += 1;
                    self.hash_dirs.clear();
                    self.hash_index = 0;
                    continue;
                }
                let prefix_path = &self.prefix_dirs[self.prefix_index];
                self.hash_dirs = read_sorted_cas_directories(prefix_path).await?;
                let prefix_a = cas_path_name(prefix_path)?;
                if self.checkpoint.prefix_a.as_deref() == Some(prefix_a.as_str()) {
                    if let Some(prefix_b) = self.checkpoint.prefix_b.as_deref() {
                        self.hash_dirs.retain(|path| {
                            cas_path_name(path).is_ok_and(|name| name.as_str() > prefix_b)
                        });
                    }
                }
                self.hash_index = 0;
                if self.hash_dirs.is_empty() {
                    self.prefix_index += 1;
                    continue;
                }
            }

            let hash_path = self.hash_dirs[self.hash_index].clone();
            self.hash_entries = Some(
                tokio::fs::read_dir(hash_path)
                    .await
                    .map_err(SinexError::io)?,
            );
        }

        Ok(CasWalkBatch {
            entries,
            checkpoint: self.checkpoint.clone(),
            complete: false,
        })
    }
}

impl MaterialContentStore {
    pub fn new(config: ContentStoreConfig) -> RuntimeResult<Self> {
        // Ensure the content-store root directory exists.
        std::fs::create_dir_all(&config.root_path).map_err(SinexError::io)?;
        restrict_permissions(config.root_path.as_std_path(), CONTENT_STORE_DIR_MODE);

        // Always ensure the local CAS directory structure exists.
        let cas_dir = config.root_path.join(LOCAL_BLAKE3_CAS_DIR);
        std::fs::create_dir_all(&cas_dir).map_err(SinexError::io)?;
        restrict_permissions(cas_dir.as_std_path(), CONTENT_STORE_DIR_MODE);

        if config.legacy_annex_enabled {
            // Verify git-annex is available
            which::which("git-annex").map_err(|e| {
                SinexError::processing("git-annex not found in PATH").with_source(e)
            })?;

            let git_dir = config.root_path.join(".git");
            if !git_dir.exists() {
                info!(
                    "Initializing git repository for content store at {:?}",
                    config.root_path
                );

                let mut git_cmd = Command::new("git");
                git_cmd.arg("init").current_dir(&config.root_path);
                let git_output =
                    run_command_blocking(git_cmd, "Failed to run git init for content-store root")?;
                if !git_output.status.success() {
                    return Err(SinexError::processing(format!(
                        "git init failed for content-store root: {}",
                        String::from_utf8_lossy(&git_output.stderr)
                    )));
                }
            }

            let annex_dir = git_dir.join("annex");
            if !annex_dir.exists() {
                info!(
                    "Initializing git-annex repository at {:?}",
                    config.root_path
                );

                let mut annex_cmd = Command::new("git-annex");
                annex_cmd
                    .args(["init", "sinex"])
                    .current_dir(&config.root_path);
                let annex_output = run_command_blocking(
                    annex_cmd,
                    "Failed to run git-annex init for content-store root",
                )?;
                if !annex_output.status.success() {
                    return Err(SinexError::processing(format!(
                        "git-annex init failed for content-store root: {}",
                        String::from_utf8_lossy(&annex_output.stderr)
                    )));
                }
            }
        }

        Ok(MaterialContentStore { config })
    }

    /// Get the repository path
    #[must_use]
    pub fn root_path(&self) -> &Utf8Path {
        &self.config.root_path
    }

    fn lifecycle_root(&self) -> Utf8PathBuf {
        self.config.root_path.join(CAS_LIFECYCLE_DIR)
    }

    fn pending_delete_root(&self) -> Utf8PathBuf {
        self.lifecycle_root().join(CAS_PENDING_DELETE_DIR)
    }

    fn quarantine_root(&self) -> Utf8PathBuf {
        self.lifecycle_root().join(CAS_QUARANTINE_DIR)
    }

    async fn sync_directory(path: &Utf8Path) -> RuntimeResult<()> {
        tokio::fs::File::open(path)
            .await
            .map_err(SinexError::io)?
            .sync_all()
            .await
            .map_err(SinexError::io)
    }

    async fn write_pending_deletion(&self, pending: &PendingDeletion) -> RuntimeResult<()> {
        let bytes = serde_json::to_vec(pending).map_err(|error| {
            SinexError::serialization("serialize CAS pending-delete record").with_source(error)
        })?;
        let temp_path = pending
            .record_path
            .with_extension(format!("json.tmp-{}", Uuid::now_v7()));
        let mut file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(SinexError::io)?;
        file.write_all(&bytes).await.map_err(SinexError::io)?;
        file.sync_all().await.map_err(SinexError::io)?;
        drop(file);
        tokio::fs::rename(&temp_path, &pending.record_path)
            .await
            .map_err(SinexError::io)?;
        Self::sync_directory(
            pending
                .record_path
                .parent()
                .ok_or_else(|| SinexError::processing("CAS pending-delete record has no parent"))?,
        )
        .await
    }

    /// List durable CAS deletion records. Malformed records fail closed: the
    /// caller must repair the lifecycle directory before any sweep can mutate
    /// content.
    pub async fn list_pending_deletions(&self) -> RuntimeResult<Vec<PendingDeletion>> {
        let root = self.pending_delete_root();
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut entries = tokio::fs::read_dir(&root).await.map_err(SinexError::io)?;
        let mut pending = Vec::new();
        while let Some(entry) = entries.next_entry().await.map_err(SinexError::io)? {
            if !entry.file_type().await.map_err(SinexError::io)?.is_file()
                || entry.path().extension().and_then(|ext| ext.to_str()) != Some("json")
            {
                continue;
            }
            let record_path = Self::require_utf8_path(entry.path())?;
            let bytes = tokio::fs::read(&record_path)
                .await
                .map_err(SinexError::io)?;
            let mut record: PendingDeletion = serde_json::from_slice(&bytes).map_err(|error| {
                SinexError::serialization("parse CAS pending-delete record")
                    .with_context("path", record_path.to_string())
                    .with_source(error)
            })?;
            record.record_path = record_path;
            pending.push(record);
        }
        pending.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
        Ok(pending)
    }

    /// Atomically move a local CAS object into a durable quarantine. The
    /// pending-delete record is created and fsynced before the rename, and is
    /// intentionally retained until the quarantine bytes are gone.
    pub async fn quarantine_local_cas(
        &self,
        key: &ContentStoreKey,
    ) -> RuntimeResult<Option<PendingDeletion>> {
        if !key.is_local_blake3_cas() {
            return Err(SinexError::validation(
                "CAS quarantine requires a local BLAKE3 content key",
            ));
        }
        let source_path = self
            .path_if_local(&key.key)?
            .ok_or_else(|| SinexError::validation("local CAS key did not resolve to a path"))?;
        if !source_path.exists() {
            return Ok(None);
        }
        self.canonicalize_local_cas_path(&source_path).await?;

        let lifecycle_root = self.lifecycle_root();
        let records_root = self.pending_delete_root();
        let quarantine_root = self.quarantine_root();
        tokio::fs::create_dir_all(&records_root)
            .await
            .map_err(SinexError::io)?;
        tokio::fs::create_dir_all(&quarantine_root)
            .await
            .map_err(SinexError::io)?;
        restrict_permissions(lifecycle_root.as_std_path(), CONTENT_STORE_DIR_MODE);
        restrict_permissions(records_root.as_std_path(), CONTENT_STORE_DIR_MODE);
        restrict_permissions(quarantine_root.as_std_path(), CONTENT_STORE_DIR_MODE);

        let operation_id = Uuid::now_v7().to_string();
        let quarantine_path = quarantine_root.join(format!("{operation_id}-{}", key.digest));
        let record_path = records_root.join(format!("{operation_id}.json"));
        let created_at_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let pending = PendingDeletion {
            operation_id,
            key: key.clone(),
            source_path: source_path.clone(),
            quarantine_path: quarantine_path.clone(),
            created_at_unix_secs,
            record_path,
        };
        self.write_pending_deletion(&pending).await?;
        if let Err(error) = tokio::fs::rename(&source_path, &quarantine_path).await {
            let _ = tokio::fs::remove_file(&pending.record_path).await;
            return Err(SinexError::io(error));
        }
        Self::sync_directory(
            source_path
                .parent()
                .ok_or_else(|| SinexError::processing("local CAS object has no parent"))?,
        )
        .await?;
        Self::sync_directory(&quarantine_root).await?;
        Ok(Some(pending))
    }

    /// Finish a previously quarantined deletion. If the unlink fails, both
    /// the quarantine bytes and its record remain for a later retry.
    pub async fn finalize_pending_deletion(&self, pending: &PendingDeletion) -> RuntimeResult<()> {
        #[cfg(test)]
        if TEST_FAIL_NEXT_PENDING_DELETE.swap(false, Ordering::SeqCst) {
            return Err(SinexError::io("injected CAS quarantine delete failure"));
        }
        match tokio::fs::remove_file(&pending.quarantine_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(SinexError::io(error)),
        }
        Self::sync_directory(&self.quarantine_root()).await?;
        match tokio::fs::remove_file(&pending.record_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(SinexError::io(error)),
        }
        Self::sync_directory(&self.pending_delete_root()).await
    }

    /// Restore a pending deletion when a database reference reappears during
    /// the quarantine grace period.
    pub async fn restore_pending_deletion(&self, pending: &PendingDeletion) -> RuntimeResult<()> {
        if pending.quarantine_path.exists() {
            if pending.source_path.exists() {
                tokio::fs::remove_file(&pending.quarantine_path)
                    .await
                    .map_err(SinexError::io)?;
            } else {
                let parent = pending.source_path.parent().ok_or_else(|| {
                    SinexError::processing("pending CAS source path has no parent")
                })?;
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(SinexError::io)?;
                tokio::fs::rename(&pending.quarantine_path, &pending.source_path)
                    .await
                    .map_err(SinexError::io)?;
                Self::sync_directory(parent).await?;
            }
            Self::sync_directory(&self.quarantine_root()).await?;
        }
        match tokio::fs::remove_file(&pending.record_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(SinexError::io(error)),
        }
        Self::sync_directory(&self.pending_delete_root()).await
    }

    #[cfg(test)]
    pub(crate) fn fail_next_pending_delete_for_tests() {
        TEST_FAIL_NEXT_PENDING_DELETE.store(true, Ordering::SeqCst);
    }

    /// Initialize a content-store root. Uses git-annex when `legacy_annex_enabled` is true;
    /// otherwise creates only the local CAS directory structure.
    pub async fn init_with_config(
        repo_path: &Utf8Path,
        description: Option<&str>,
        legacy_annex_enabled: bool,
    ) -> RuntimeResult<()> {
        info!("Initializing content-store repository at {:?}", repo_path);

        // Ensure directory exists
        tokio::fs::create_dir_all(repo_path)
            .await
            .map_err(SinexError::io)?;
        restrict_permissions(repo_path.as_std_path(), CONTENT_STORE_DIR_MODE);

        // Always create the local CAS directory structure
        let cas_dir = repo_path.join(LOCAL_BLAKE3_CAS_DIR);
        tokio::fs::create_dir_all(&cas_dir)
            .await
            .map_err(SinexError::io)?;
        restrict_permissions(cas_dir.as_std_path(), CONTENT_STORE_DIR_MODE);

        if legacy_annex_enabled {
            // Initialize git repository if needed
            let git_dir = repo_path.join(".git");
            if !git_dir.exists() {
                let mut git_cmd = AsyncCommand::new("git");
                git_cmd.arg("init").current_dir(repo_path);
                let output = run_command_async(git_cmd, "Failed to run git init").await?;

                if !output.status.success() {
                    return Err(SinexError::processing(format!(
                        "git init failed: {}",
                        String::from_utf8_lossy(&output.stderr)
                    )));
                }
            }

            // Initialize git-annex
            let mut cmd = AsyncCommand::new("git-annex");
            cmd.arg("init").current_dir(repo_path);

            if let Some(desc) = description {
                cmd.arg(desc);
            }

            let output = run_command_async(cmd, "Failed to run git-annex init").await?;

            if !output.status.success() {
                return Err(SinexError::processing(format!(
                    "git-annex init failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                )));
            }

            info!("Successfully initialized git-annex repository");
        } else {
            info!("Content-store root initialized with local CAS only");
        }
        Ok(())
    }

    fn require_utf8_path(path: impl AsRef<Path>) -> RuntimeResult<Utf8PathBuf> {
        Utf8PathBuf::from_path_buf(path.as_ref().to_path_buf()).map_err(|path| {
            SinexError::validation(format!(
                "content-store path is not valid UTF-8: {}",
                path.display()
            ))
        })
    }

    async fn resolve_root_contained_file_path(
        &self,
        file_path: Utf8PathBuf,
    ) -> RuntimeResult<Utf8PathBuf> {
        let candidate = if file_path.is_absolute() {
            file_path
        } else {
            self.config.root_path.join(file_path)
        };
        self.canonicalize_root_contained_path(&candidate).await
    }

    /// Store a file and return the backend-neutral content-store key.
    pub async fn store_file(&self, file_path: impl AsRef<Path>) -> RuntimeResult<ContentStoreKey> {
        let file_path = Self::require_utf8_path(file_path)?;
        debug!("Storing file in content store: {:?}", file_path);

        // Resolve and contain the path before metadata, hashing, or copying.
        // This protects both absolute inputs and root-relative traversal from
        // escaping through a symlink or parent component.
        let resolved_path = self.resolve_root_contained_file_path(file_path).await?;

        let file_size = tokio::fs::metadata(&resolved_path)
            .await
            .map_err(SinexError::io)?
            .len();
        self.ensure_file_size_allowed(file_size)?;
        self.store_file_local_cas(&resolved_path, file_size).await
    }

    pub(super) fn ensure_file_size_allowed(&self, file_size: u64) -> RuntimeResult<()> {
        let Some(max_blob_size) = (self.config.max_blob_size > 0)
            .then(|| u64::try_from(self.config.max_blob_size))
            .transpose()
            .map_err(|error| {
                SinexError::validation("configured content-store size limit is unsupported")
                    .with_source(error)
            })?
        else {
            return Ok(());
        };
        if file_size > max_blob_size {
            return Err(SinexError::blob_storage(format!(
                "blob size {file_size} exceeds limit {}",
                self.config.max_blob_size
            )));
        }
        Ok(())
    }

    async fn store_file_local_cas(
        &self,
        resolved_path: &Utf8Path,
        file_size: u64,
    ) -> RuntimeResult<ContentStoreKey> {
        let hash =
            Self::compute_blake3_hash_with_limit(resolved_path, self.config.max_blob_size).await?;
        let target = self.local_blake3_cas_path_for_hash(&hash)?;
        if target.exists() {
            self.canonicalize_local_cas_path(&target).await?;
        } else {
            let parent = target.parent().ok_or_else(|| {
                SinexError::processing(format!("Local CAS target has no parent: {target}"))
            })?;
            let mut existing_parent = parent;
            while !existing_parent.exists() {
                existing_parent = existing_parent.parent().ok_or_else(|| {
                    SinexError::validation("local CAS path has no existing ancestor")
                })?;
            }
            canonicalize_path_within_root(&self.local_blake3_cas_root(), existing_parent).await?;
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(SinexError::io)?;
            restrict_permissions(parent.as_std_path(), CONTENT_STORE_DIR_MODE);

            let tmp = parent.join(format!("{hash}.tmp-{}", Uuid::now_v7()));
            tokio::fs::copy(resolved_path, &tmp)
                .await
                .map_err(SinexError::io)?;
            let copied_size = tokio::fs::metadata(&tmp)
                .await
                .map_err(SinexError::io)?
                .len();
            if let Err(error) = self.ensure_file_size_allowed(copied_size) {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(error);
            }
            if copied_size != file_size {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(SinexError::processing(
                    "source file changed while copying into local CAS",
                )
                .with_context("observed_size", copied_size.to_string())
                .with_context("initial_size", file_size.to_string()));
            }
            // tokio::fs::copy (like std::fs::copy) preserves the SOURCE file's
            // permission bits -- if resolved_path was ever more permissive
            // than CAS content should be, that permissiveness would otherwise
            // carry into the durable CAS store. Force the correct mode
            // regardless of the source's permissions (sinex-vyi3).
            restrict_permissions(tmp.as_std_path(), CONTENT_STORE_FILE_MODE);
            let file = tokio::fs::File::open(&tmp).await.map_err(SinexError::io)?;
            file.sync_all().await.map_err(SinexError::io)?;
            if target.exists() {
                tokio::fs::remove_file(&tmp).await.map_err(SinexError::io)?;
            } else {
                tokio::fs::rename(&tmp, &target)
                    .await
                    .map_err(SinexError::io)?;
                // The file fsync makes the object durable, while the parent
                // directory fsync makes the atomic name publication durable.
                // Without the latter, a power loss can lose the directory
                // entry after callers have acknowledged the material.
                std::fs::File::open(parent.as_std_path())
                    .map_err(SinexError::io)?
                    .sync_all()
                    .map_err(SinexError::io)?;
            }
        }

        Ok(ContentStoreKey {
            key: format!("{LOCAL_BLAKE3_CAS_BACKEND}-s{file_size}--{hash}"),
            backend: ContentBackend::LocalBlake3Cas,
            size: file_size,
            digest: hash,
        })
    }

    fn local_blake3_cas_root(&self) -> Utf8PathBuf {
        self.config.root_path.join(LOCAL_BLAKE3_CAS_DIR)
    }

    fn local_blake3_cas_path_for_hash(&self, hash: &str) -> RuntimeResult<Utf8PathBuf> {
        validate_local_blake3_digest(hash)?;
        let prefix_a = hash.get(0..2).unwrap_or("xx");
        let prefix_b = hash.get(2..4).unwrap_or("xx");
        let path = self
            .local_blake3_cas_root()
            .join(prefix_a)
            .join(prefix_b)
            .join(hash);
        self.ensure_local_cas_path_within_root(&path)?;
        Ok(path)
    }

    fn ensure_local_cas_path_within_root(&self, path: &Utf8Path) -> RuntimeResult<()> {
        let root = self.local_blake3_cas_root();
        if path.starts_with(&root) {
            return Ok(());
        }
        Err(
            SinexError::validation("local CAS path escapes content-store root")
                .with_context("path", path.to_string())
                .with_context("root", root.to_string()),
        )
    }

    pub async fn canonicalize_local_cas_path(&self, path: &Utf8Path) -> RuntimeResult<Utf8PathBuf> {
        self.ensure_local_cas_path_within_root(path)?;
        canonicalize_path_within_root(&self.local_blake3_cas_root(), path).await
    }

    /// Canonicalize an existing path and require that it remains under the
    /// configured content-store root. This is used for paths returned by
    /// external backends, which may contain symlinks or absolute paths.
    pub async fn canonicalize_root_contained_path(
        &self,
        path: &Utf8Path,
    ) -> RuntimeResult<Utf8PathBuf> {
        canonicalize_path_within_root(&self.config.root_path, path).await
    }

    pub(super) async fn resolve_command_path(
        &self,
        stdout: &[u8],
        context: &'static str,
    ) -> RuntimeResult<Utf8PathBuf> {
        let reported = String::from_utf8(stdout.to_vec())
            .map_err(|error| SinexError::processing(context).with_source(error))?;
        let reported = reported.trim();
        if reported.is_empty() {
            return Err(SinexError::processing(context)
                .with_context("reason", "command returned an empty content path"));
        }
        let candidate = if Path::new(reported).is_absolute() {
            Utf8PathBuf::from(reported)
        } else {
            self.config.root_path.join(reported)
        };
        self.canonicalize_root_contained_path(&candidate).await
    }

    pub fn path_if_local(&self, key: &str) -> RuntimeResult<Option<Utf8PathBuf>> {
        let Ok(parsed) = ContentStoreKey::parse(key) else {
            return Ok(None);
        };
        if !parsed.is_local_blake3_cas() {
            return Ok(None);
        }
        Ok(Some(self.local_blake3_cas_path_for_hash(&parsed.digest)?))
    }

    /// Resolve a git-annex content key to a local file path.
    ///
    /// Uses `git-annex contentlocation` to find the file backing a legacy annex key.
    /// Returns an error when `legacy_annex_enabled` is false or the key is local CAS.
    pub async fn resolve_annex_content_path(&self, key: &str) -> RuntimeResult<Utf8PathBuf> {
        if let Some(path) = self.path_if_local(key)? {
            return Err(SinexError::processing(format!(
                "resolve_annex_content_path is for legacy annex keys, but got local CAS key: {key}"
            ))
            .with_context("local_cas_path", path.to_string()));
        }
        if !self.config.legacy_annex_enabled {
            return Err(SinexError::processing(
                "legacy annex is disabled; cannot resolve annex content path",
            ));
        }
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("contentlocation")
            .arg(key)
            .current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to run git-annex contentlocation").await?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex contentlocation failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        self.resolve_command_path(
            &output.stdout,
            "Failed to resolve git-annex contentlocation path",
        )
        .await
    }

    /// Get the content-store key for a file.
    ///
    /// When `legacy_annex_enabled` is false, compute the BLAKE3 hash and
    /// build a local CAS key directly (no git-annex subprocess).
    pub async fn lookup_content_key(
        &self,
        file_path: impl AsRef<Path>,
    ) -> RuntimeResult<ContentStoreKey> {
        let file_path = Self::require_utf8_path(file_path)?;
        if !self.config.legacy_annex_enabled {
            let resolved_path = self.resolve_root_contained_file_path(file_path).await?;
            let file_size = tokio::fs::metadata(&resolved_path)
                .await
                .map_err(SinexError::io)?
                .len();
            self.ensure_file_size_allowed(file_size)?;
            let hash =
                Self::compute_blake3_hash_with_limit(&resolved_path, self.config.max_blob_size)
                    .await?;
            let _path = self.local_blake3_cas_path_for_hash(&hash)?;
            return Ok(ContentStoreKey {
                key: format!("{LOCAL_BLAKE3_CAS_BACKEND}-s{file_size}--{hash}"),
                backend: ContentBackend::LocalBlake3Cas,
                size: file_size,
                digest: hash,
            });
        }
        let (_is_key, argument) = self.resolve_argument(file_path.as_str()).await?;
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("lookupkey")
            .arg(argument)
            .current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to run git-annex lookupkey").await?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex lookupkey failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let key_str = String::from_utf8(output.stdout)
            .map_err(|e| {
                SinexError::processing("Invalid UTF-8 in content-store key").with_source(e)
            })?
            .trim()
            .to_string();

        ContentStoreKey::parse(&key_str)
    }

    async fn resolve_argument(&self, key_or_path: &str) -> RuntimeResult<(bool, String)> {
        let candidate = self.config.root_path.join(key_or_path);
        if candidate.exists() {
            let canonical_root = tokio::fs::canonicalize(&self.config.root_path)
                .await
                .map_err(SinexError::io)?;
            let canonical_candidate = tokio::fs::canonicalize(&candidate)
                .await
                .map_err(SinexError::io)?;
            if !canonical_candidate.starts_with(&canonical_root) {
                return Err(SinexError::validation(
                    "content-store path escapes configured root",
                ));
            }
            let rel = canonical_candidate
                .strip_prefix(&canonical_root)
                .map_err(|_| SinexError::validation("content-store path is not root-relative"))?;
            return Ok((false, rel.to_string_lossy().into_owned()));
        } else {
            let path = Path::new(key_or_path);
            if path.is_absolute()
                || path
                    .components()
                    .any(|part| matches!(part, std::path::Component::ParentDir))
            {
                return Err(SinexError::validation(
                    "content-store argument must be a key or a root-contained relative path",
                ));
            }
            Ok((true, key_or_path.to_string()))
        }
    }

    /// Ensure content is available locally
    pub async fn ensure_content_local(&self, key_or_path: &str) -> RuntimeResult<()> {
        debug!("Getting content for: {key_or_path}");

        if let Some(path) = self.path_if_local(key_or_path)? {
            if path.exists() {
                self.canonicalize_local_cas_path(&path).await?;
                return Ok(());
            }
            return Err(SinexError::processing(format!(
                "local CAS content missing for key {key_or_path}: {path}"
            )));
        }

        if !self.config.legacy_annex_enabled {
            return Err(SinexError::processing(format!(
                "legacy annex is disabled; cannot retrieve non-local-CAS key: {key_or_path}"
            )));
        }

        let (is_key, argument) = self.resolve_argument(key_or_path).await?;

        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("get");
        if is_key {
            cmd.arg("--key").arg(&argument);
        } else {
            cmd.arg(&argument);
        }

        cmd.current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to run git-annex get").await?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex get failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        Ok(())
    }

    /// Drop content if sufficient copies exist elsewhere
    pub async fn drop_content(&self, key_or_path: &str, force: bool) -> RuntimeResult<()> {
        debug!("Dropping content for: {key_or_path}");

        if let Some(path) = self.path_if_local(key_or_path)? {
            if !force {
                return Err(SinexError::processing(format!(
                    "cannot drop local CAS content without force: {key_or_path}"
                )));
            }
            let key = ContentStoreKey::parse(key_or_path)?;
            if !path.exists() {
                if let Some(pending) = self
                    .list_pending_deletions()
                    .await?
                    .into_iter()
                    .find(|pending| pending.key == key)
                {
                    self.finalize_pending_deletion(&pending).await?;
                }
                return Ok(());
            }
            self.canonicalize_local_cas_path(&path).await?;
            if let Some(pending) = self.quarantine_local_cas(&key).await? {
                self.finalize_pending_deletion(&pending).await?;
            }
            return Ok(());
        }

        if !self.config.legacy_annex_enabled {
            return Err(SinexError::processing(format!(
                "legacy annex is disabled; cannot drop non-local-CAS key: {key_or_path}"
            )));
        }

        let (is_key, argument) = self.resolve_argument(key_or_path).await?;
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("drop");
        if is_key {
            cmd.arg("--key").arg(&argument);
        } else {
            cmd.arg(&argument);
        }

        if force {
            cmd.arg("--force");
        }

        cmd.current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to run git-annex drop").await?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex drop failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        Ok(())
    }

    /// Check content integrity for either the local CAS or the git-annex backend.
    pub async fn verify_key(
        &self,
        fast: bool,
        incremental: bool,
        key: Option<&str>,
    ) -> RuntimeResult<ContentVerificationResult> {
        info!("Running git-annex fsck");

        if let Some(key) = key
            && let Some(path) = self.path_if_local(key)?
        {
            let parsed = ContentStoreKey::parse(key)?;
            if !path.exists() {
                return Ok(ContentVerificationResult {
                    output: format!("missing local CAS content for {key}"),
                    success: false,
                });
            }
            let metadata = tokio::fs::metadata(&path).await.map_err(SinexError::io)?;
            self.ensure_file_size_allowed(metadata.len())?;
            let path = self.canonicalize_local_cas_path(&path).await?;
            let hash =
                Self::compute_blake3_hash_with_limit(&path, self.config.max_blob_size).await?;
            return Ok(ContentVerificationResult {
                output: format!("local CAS verification {key}"),
                success: hash == parsed.digest,
            });
        }

        if !self.config.legacy_annex_enabled {
            return Ok(ContentVerificationResult {
                output: format!(
                    "legacy annex disabled; cannot verify non-local-CAS key: {:?}",
                    key.unwrap_or("<none>")
                ),
                success: false,
            });
        }

        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("fsck");

        if fast {
            cmd.arg("--fast");
        }

        if incremental {
            cmd.arg("--incremental");
        }

        if let Some(k) = key {
            cmd.arg("--key").arg(k);
        }

        cmd.current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to run git-annex fsck").await?;

        let success = output.status.success();
        let result = String::from_utf8(output.stdout)
            .map_err(|e| SinexError::processing("Invalid UTF-8 in fsck output").with_source(e))?;

        if !success {
            warn!(
                "git-annex fsck completed with errors: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(ContentVerificationResult {
            output: result,
            success,
        })
    }

    /// Get repository status information.
    ///
    /// When `legacy_annex_enabled` is false, returns a local CAS directory summary.
    pub async fn status(&self) -> RuntimeResult<String> {
        if !self.config.legacy_annex_enabled {
            return self.local_cas_status().await;
        }
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("status").current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to run git-annex status").await?;

        String::from_utf8(output.stdout)
            .map_err(|e| SinexError::processing("Invalid UTF-8 in status output").with_source(e))
    }

    /// List content-store keys reported as unused by the current repository.
    ///
    /// When `legacy_annex_enabled` is false, this operation is not applicable;
    /// use CAS fsck (`cas_fsck::sweep_orphans_cas`) instead.
    pub async fn list_unused(&self) -> RuntimeResult<Vec<UnusedContentEntry>> {
        if !self.config.legacy_annex_enabled {
            return Err(SinexError::processing(
                "git-annex unused is not available when legacy_annex_enabled is false. \
                 Use CAS fsck for orphan detection.",
            ));
        }
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("unused")
            .arg("--json")
            .current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to run git-annex unused").await?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex unused failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        parse_unused_output(&output.stdout).map_err(SinexError::processing)
    }

    /// Drop unused git-annex content by the numbered slots returned from `unused`.
    ///
    /// When `legacy_annex_enabled` is false, returns an error.
    pub async fn drop_unused(&self, numbers: &[u32], force: bool) -> RuntimeResult<()> {
        if numbers.is_empty() {
            return Ok(());
        }
        if !self.config.legacy_annex_enabled {
            return Err(SinexError::processing(
                "git-annex dropunused is not available when legacy_annex_enabled is false",
            ));
        }

        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("dropunused");
        if force {
            cmd.arg("--force");
        }
        for number in numbers {
            cmd.arg(number.to_string());
        }
        cmd.current_dir(&self.config.root_path);

        let output = run_command_async(cmd, "Failed to run git-annex dropunused").await?;
        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex dropunused failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        Ok(())
    }

    /// Compute BLAKE3 hash for deduplication
    pub async fn compute_blake3_hash(file_path: &Utf8Path) -> RuntimeResult<String> {
        Self::compute_blake3_hash_with_limit(file_path, 0).await
    }

    pub(super) async fn compute_blake3_hash_with_limit(
        file_path: &Utf8Path,
        max_blob_size: usize,
    ) -> RuntimeResult<String> {
        let content = Self::read_file_with_limit(file_path, max_blob_size).await?;

        let hash = blake3::hash(&content);
        Ok(hash.to_hex().to_string())
    }

    pub(super) async fn read_file_with_limit(
        file_path: &Utf8Path,
        max_blob_size: usize,
    ) -> RuntimeResult<Vec<u8>> {
        let mut file = tokio::fs::File::open(file_path)
            .await
            .map_err(SinexError::io)?;
        let mut content = Vec::new();
        let mut chunk = [0_u8; 64 * 1024];
        loop {
            let bytes_read = file.read(&mut chunk).await.map_err(SinexError::io)?;
            if bytes_read == 0 {
                break;
            }
            let new_len = content.len().checked_add(bytes_read).ok_or_else(|| {
                SinexError::blob_storage("blob content size overflow while reading")
            })?;
            if max_blob_size > 0 && new_len > max_blob_size {
                return Err(SinexError::blob_storage(format!(
                    "blob size exceeds limit {max_blob_size} while reading {file_path}"
                )));
            }
            content.extend_from_slice(&chunk[..bytes_read]);
        }
        Ok(content)
    }

    /// Walk the local CAS directory structure and yield all discovered hash paths.
    ///
    /// Returns a list of `(hash_hex, full_path, file_size)` tuples.
    /// The `sinex-cas/XX/YY/` prefix layout is traversed recursively. This
    /// compatibility collector is intentionally separate from fsck, which
    /// consumes `cas_walker()` in bounded batches.
    pub async fn walk_cas(&self) -> RuntimeResult<Vec<(String, Utf8PathBuf, u64)>> {
        let cas_root = self.config.root_path.join(LOCAL_BLAKE3_CAS_DIR);
        if !cas_root.exists() {
            return Ok(Vec::new());
        }
        let mut walker = self.cas_walker(None).await?;
        let mut entries = Vec::new();
        loop {
            let batch = walker.next_batch(256).await?;
            entries.extend(batch.entries);
            if batch.complete {
                break;
            }
        }
        Ok(entries)
    }

    /// Create a resumable, bounded-state walker over the local CAS tree.
    pub async fn cas_walker(
        &self,
        checkpoint: Option<CasWalkCheckpoint>,
    ) -> RuntimeResult<CasWalker> {
        CasWalker::new(
            self.config.root_path.join(LOCAL_BLAKE3_CAS_DIR),
            checkpoint.unwrap_or_default(),
        )
        .await
    }

    /// Produce a human-readable summary of the local CAS directory.
    async fn local_cas_status(&self) -> RuntimeResult<String> {
        let cas_root = self.config.root_path.join(LOCAL_BLAKE3_CAS_DIR);
        if !cas_root.exists() {
            return Ok("Local CAS directory does not exist.".to_string());
        }

        let entries = self.walk_cas().await?;
        let total_size: u64 = entries.iter().map(|(_, _, s)| s).sum();
        let mut out = format!(
            "Local CAS status:\n  Path: {}\n  Files: {}\n  Total size: {} bytes\n",
            cas_root,
            entries.len(),
            total_size,
        );
        if !entries.is_empty() {
            out.push_str(&format!(
                "  Largest file: {} bytes ({})\n",
                entries
                    .iter()
                    .map(|(_, _, s)| s)
                    .max()
                    .copied()
                    .unwrap_or(0),
                entries
                    .iter()
                    .max_by_key(|(_, _, s)| s)
                    .map_or("N/A", |(h, _, _)| h.as_str()),
            ));
        }
        Ok(out)
    }

    /// Configure repository settings.
    ///
    /// When `legacy_annex_enabled` is false, this is a no-op (no annex config to set).
    pub async fn configure(&self) -> RuntimeResult<()> {
        if !self.config.legacy_annex_enabled {
            return Ok(());
        }
        if let Some(num_copies) = self.config.num_copies {
            self.set_config("annex.numcopies", &num_copies.to_string())
                .await?;
        }

        if let Some(ref large_files) = self.config.large_files {
            self.set_config("annex.largefiles", large_files).await?;
        }

        Ok(())
    }

    async fn set_config(&self, key: &str, value: &str) -> RuntimeResult<()> {
        let mut cmd = AsyncCommand::new("git");
        cmd.arg("config")
            .arg(key)
            .arg(value)
            .current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to set git config").await?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "Failed to set config {key}: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        Ok(())
    }
}

fn parse_unused_output(stdout: &[u8]) -> Result<Vec<UnusedContentEntry>, String> {
    let output = std::str::from_utf8(stdout)
        .map_err(|error| format!("git-annex unused output was not valid UTF-8: {error}"))?;
    let mut invalid_line: Option<String> = None;
    let mut entries = Vec::new();

    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parsed: JsonValue = match serde_json::from_str(line) {
            Ok(parsed) => parsed,
            Err(error) => {
                invalid_line.get_or_insert_with(|| {
                    format!(
                        "git-annex unused output contained invalid JSON line `{}`: {error}",
                        line.chars().take(120).collect::<String>()
                    )
                });
                continue;
            }
        };

        let Some(unused_list) = parsed
            .get("unused-list")
            .and_then(|value| value.as_object())
        else {
            continue;
        };

        for (number, raw_key) in unused_list {
            let number = number.parse::<u32>().map_err(|error| {
                format!("git-annex unused entry number `{number}` was not a valid u32: {error}")
            })?;
            let raw_key = raw_key.as_str().ok_or_else(|| {
                format!("git-annex unused entry `{number}` did not contain a string key")
            })?;
            let key = ContentStoreKey::parse(raw_key).map_err(|error| {
                format!("git-annex unused entry `{number}` had invalid key: {error}")
            })?;
            entries.push(UnusedContentEntry { number, key });
        }
    }

    if entries.is_empty()
        && let Some(error) = invalid_line
    {
        return Err(error);
    }

    entries.sort_by_key(|entry| entry.number);
    Ok(entries)
}

#[cfg(test)]
#[path = "../content_store_test.rs"]
mod tests;
