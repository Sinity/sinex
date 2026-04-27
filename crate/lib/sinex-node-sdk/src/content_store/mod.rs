#![doc = include_str!("../../docs/content_store.md")]

use crate::{NodeResult, SinexError};
use camino::{Utf8Path, Utf8PathBuf};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::process::Command as AsyncCommand;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

pub mod gc;
pub mod manager;
pub mod path_validator;

pub use manager::{BlobMetadata, ContentStoreManager};
pub use path_validator::{VerifiedPath, create_secure_temp_path, validate_and_convert_path};

pub const LOCAL_BLAKE3_CAS_BACKEND: &str = "SINEXBLAKE3";
const LOCAL_BLAKE3_CAS_DIR: &str = "sinex-cas";
const LOCAL_BLAKE3_CAS_MAX_BYTES: u64 = 16 * 1024 * 1024;
const CONTENT_STORE_PROCESS_COUNTERS_PATH_ENV: &str = "SINEX_CONTENT_STORE_PROCESS_COUNTERS_PATH";

static CONTENT_STORE_PROCESS_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
static CONTENT_STORE_PROCESS_COUNTERS: OnceLock<ContentStoreProcessCounterState> = OnceLock::new();

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
) -> NodeResult<std::process::Output> {
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
) -> NodeResult<std::process::Output> {
    {
        let _guard = content_store_process_lock().lock().await;
        record_process_invocation(cmd.as_std().get_program(), false);
    }
    cmd.output()
        .await
        .map_err(|e| SinexError::processing(context).with_source(e))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentStoreConfig {
    pub root_path: Utf8PathBuf,
    pub num_copies: Option<u8>,
    pub large_files: Option<String>,
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
    pub fn parse(key_str: &str) -> NodeResult<Self> {
        // Parse content-store key format: BACKEND-s<size>--digest
        // Example: SHA256E-s12345--hash.dat
        let (prefix, digest) = key_str.split_once("--").ok_or_else(|| {
            SinexError::processing(format!(
                "Invalid content-store key format (missing '--'): {key_str}"
            ))
        })?;

        let (backend, size_part) = prefix.split_once("-s").ok_or_else(|| {
            SinexError::processing(format!(
                "Invalid size format in content-store key (missing '-s'): {key_str}"
            ))
        })?;

        let size = size_part.parse::<u64>().map_err(|e| {
            SinexError::processing(format!(
                "Failed to parse size from content-store key: {key_str}"
            ))
            .with_source(e)
        })?;

        Ok(ContentStoreKey {
            key: key_str.to_string(),
            backend: ContentBackend::from_storage_backend(backend),
            size,
            digest: digest.to_string(),
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

#[derive(Debug)]
pub struct MaterialContentStore {
    pub config: ContentStoreConfig,
}

impl MaterialContentStore {
    pub fn new(config: ContentStoreConfig) -> NodeResult<Self> {
        // Verify git-annex is available
        which::which("git-annex")
            .map_err(|e| SinexError::processing("git-annex not found in PATH").with_source(e))?;

        // Ensure repository exists; initialize git + git-annex even when the
        // directory already exists (e.g., tempdirs created by tests).
        std::fs::create_dir_all(&config.root_path).map_err(SinexError::io)?;

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

        Ok(MaterialContentStore { config })
    }

    /// Get the repository path
    #[must_use]
    pub fn root_path(&self) -> &Utf8Path {
        &self.config.root_path
    }

    /// Initialize a content-store root with the current git-annex large-object backend.
    pub async fn init(repo_path: &Utf8Path, description: Option<&str>) -> NodeResult<()> {
        info!("Initializing git-annex repository at {:?}", repo_path);

        // Ensure directory exists
        tokio::fs::create_dir_all(repo_path)
            .await
            .map_err(SinexError::io)?;

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
        Ok(())
    }

    fn require_utf8_path(path: impl AsRef<Path>) -> NodeResult<Utf8PathBuf> {
        Utf8PathBuf::from_path_buf(path.as_ref().to_path_buf()).map_err(|path| {
            SinexError::validation(format!(
                "content-store path is not valid UTF-8: {}",
                path.display()
            ))
        })
    }

    /// Store a file and return the backend-neutral content-store key.
    pub async fn store_file(&self, file_path: impl AsRef<Path>) -> NodeResult<ContentStoreKey> {
        let file_path = Self::require_utf8_path(file_path)?;
        debug!("Storing file in content store: {:?}", file_path);

        // Allow callers to pass either absolute paths or paths relative to the
        // content-store root. Resolve to an absolute path for validation so
        // we don't accidentally check the process CWD (which may differ from
        // the repo path for systemd services).
        let resolved_path = if file_path.is_absolute() {
            file_path
        } else {
            self.config.root_path.join(&file_path)
        };

        if !resolved_path.exists() {
            return Err(SinexError::processing(format!(
                "File does not exist: {resolved_path:?}"
            )));
        }

        let file_size = tokio::fs::metadata(&resolved_path)
            .await
            .map_err(SinexError::io)?
            .len();
        if file_size <= LOCAL_BLAKE3_CAS_MAX_BYTES {
            return self.store_file_local_cas(&resolved_path, file_size).await;
        }

        let (ingest_path, needs_cleanup) = if resolved_path.starts_with(&self.config.root_path) {
            (resolved_path.clone(), false)
        } else {
            let temp_name = format!("ingest-{}.tmp", Uuid::now_v7());
            let target = self.config.root_path.join(temp_name);
            tokio::fs::copy(&resolved_path, &target)
                .await
                .map_err(SinexError::io)?;
            (target, true)
        };

        let relative_path = ingest_path
            .strip_prefix(&self.config.root_path)
            .unwrap_or(&ingest_path)
            .to_owned();

        // Keep git-annex bounded to the finalization operation. A resident
        // add --batch process retains Haskell runtime memory inside service
        // cgroups; source streams must reduce material cardinality before
        // this storage boundary instead.
        let key = self.add_file_direct(&relative_path, &resolved_path).await?;

        if needs_cleanup && let Err(e) = tokio::fs::remove_file(&ingest_path).await {
            warn!(
                error = %e,
                path = %ingest_path,
                "Failed to clean up staged ingest file inside content-store root"
            );
        }

        Ok(key)
    }

    async fn store_file_local_cas(
        &self,
        resolved_path: &Utf8Path,
        file_size: u64,
    ) -> NodeResult<ContentStoreKey> {
        let hash = Self::compute_blake3_hash(resolved_path).await?;
        let target = self.local_blake3_cas_path_for_hash(&hash);
        if !target.exists() {
            let parent = target.parent().ok_or_else(|| {
                SinexError::processing(format!("Local CAS target has no parent: {target}"))
            })?;
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(SinexError::io)?;

            let tmp = parent.join(format!("{hash}.tmp-{}", Uuid::now_v7()));
            tokio::fs::copy(resolved_path, &tmp)
                .await
                .map_err(SinexError::io)?;
            let file = tokio::fs::File::open(&tmp).await.map_err(SinexError::io)?;
            file.sync_all().await.map_err(SinexError::io)?;
            if target.exists() {
                tokio::fs::remove_file(&tmp).await.map_err(SinexError::io)?;
            } else {
                tokio::fs::rename(&tmp, &target)
                    .await
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

    fn local_blake3_cas_path_for_hash(&self, hash: &str) -> Utf8PathBuf {
        let prefix_a = hash.get(0..2).unwrap_or("xx");
        let prefix_b = hash.get(2..4).unwrap_or("xx");
        self.config
            .root_path
            .join(LOCAL_BLAKE3_CAS_DIR)
            .join(prefix_a)
            .join(prefix_b)
            .join(hash)
    }

    pub fn path_if_local(&self, key: &str) -> NodeResult<Option<Utf8PathBuf>> {
        let Ok(parsed) = ContentStoreKey::parse(key) else {
            return Ok(None);
        };
        if !parsed.is_local_blake3_cas() {
            return Ok(None);
        }
        Ok(Some(self.local_blake3_cas_path_for_hash(&parsed.digest)))
    }

    async fn add_file_direct(
        &self,
        relative_path: &Utf8Path,
        resolved_path: &Utf8Path,
    ) -> NodeResult<ContentStoreKey> {
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("add")
            .arg("--json")
            .arg(relative_path.as_str())
            .current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to run git-annex add").await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stderr_lower = stderr.to_lowercase();
            if stderr_lower.contains("no space left") {
                return Err(SinexError::processing(format!(
                    "git-annex add failed: disk is full for {resolved_path:?}"
                )));
            }
            if stderr_lower.contains("permission denied") {
                return Err(SinexError::processing(format!(
                    "git-annex add failed: permission denied for {resolved_path:?}"
                )));
            }
            if stderr_lower.contains("annex") && stderr_lower.contains("corrupt") {
                return Err(SinexError::processing(format!(
                    "git-annex add failed due to possible corruption at {resolved_path:?}: {stderr}"
                )));
            }
            return Err(SinexError::processing(format!(
                "git-annex add failed: {stderr}"
            )));
        }

        match parse_store_output_for_key(&output.stdout) {
            Ok(Some(key)) => Ok(key),
            Ok(None) => self.lookup_content_key(relative_path).await,
            Err(error) => {
                warn!(
                    error = %error,
                    output_preview = %String::from_utf8_lossy(&output.stdout[..output.stdout.len().min(160)]),
                    "Failed to parse git-annex add JSON output; falling back to lookupkey"
                );
                self.lookup_content_key(relative_path).await
            }
        }
    }

    /// Get the content-store key for a file.
    pub async fn lookup_content_key(
        &self,
        file_path: impl AsRef<Path>,
    ) -> NodeResult<ContentStoreKey> {
        let file_path = Self::require_utf8_path(file_path)?;
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("lookupkey")
            .arg(file_path.as_str())
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

    fn resolve_argument(&self, key_or_path: &str) -> (bool, String) {
        let candidate = self.config.root_path.join(key_or_path);
        if candidate.exists() {
            let rel = candidate
                .strip_prefix(&self.config.root_path)
                .unwrap_or(&candidate);
            (false, rel.to_string())
        } else {
            (true, key_or_path.to_string())
        }
    }

    /// Ensure content is available locally
    pub async fn ensure_content_local(&self, key_or_path: &str) -> NodeResult<()> {
        debug!("Getting content for: {key_or_path}");

        if let Some(path) = self.path_if_local(key_or_path)? {
            if path.exists() {
                return Ok(());
            }
            return Err(SinexError::processing(format!(
                "local CAS content missing for key {key_or_path}: {path}"
            )));
        }

        let (is_key, argument) = self.resolve_argument(key_or_path);

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
    pub async fn drop_content(&self, key_or_path: &str, force: bool) -> NodeResult<()> {
        debug!("Dropping content for: {key_or_path}");

        if let Some(path) = self.path_if_local(key_or_path)? {
            if !force {
                return Err(SinexError::processing(format!(
                    "cannot drop local CAS content without force: {key_or_path}"
                )));
            }
            match tokio::fs::remove_file(&path).await {
                Ok(()) => return Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(SinexError::io(error)),
            }
        }

        let (is_key, argument) = self.resolve_argument(key_or_path);
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
    ) -> NodeResult<ContentVerificationResult> {
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
            let hash = Self::compute_blake3_hash(&path).await?;
            return Ok(ContentVerificationResult {
                output: format!("local CAS verification {key}"),
                success: hash == parsed.digest,
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

    /// Get repository status information
    pub async fn status(&self) -> NodeResult<String> {
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("status").current_dir(&self.config.root_path);
        let output = run_command_async(cmd, "Failed to run git-annex status").await?;

        String::from_utf8(output.stdout)
            .map_err(|e| SinexError::processing("Invalid UTF-8 in status output").with_source(e))
    }

    /// List content-store keys reported as unused by the current repository.
    pub async fn list_unused(&self) -> NodeResult<Vec<UnusedContentEntry>> {
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
    pub async fn drop_unused(&self, numbers: &[u32], force: bool) -> NodeResult<()> {
        if numbers.is_empty() {
            return Ok(());
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
    pub async fn compute_blake3_hash(file_path: &Utf8Path) -> NodeResult<String> {
        let content = tokio::fs::read(file_path).await.map_err(SinexError::io)?;

        let hash = blake3::hash(&content);
        Ok(hash.to_hex().to_string())
    }

    /// Configure repository settings
    pub async fn configure(&self) -> NodeResult<()> {
        if let Some(num_copies) = self.config.num_copies {
            self.set_config("annex.numcopies", &num_copies.to_string())
                .await?;
        }

        if let Some(ref large_files) = self.config.large_files {
            self.set_config("annex.largefiles", large_files).await?;
        }

        Ok(())
    }

    async fn set_config(&self, key: &str, value: &str) -> NodeResult<()> {
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

fn parse_store_output_for_key(stdout: &[u8]) -> Result<Option<ContentStoreKey>, String> {
    let output = std::str::from_utf8(stdout)
        .map_err(|error| format!("git-annex add output was not valid UTF-8: {error}"))?;
    let mut invalid_line: Option<String> = None;
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
                        "git-annex add output contained invalid JSON line `{}`: {error}",
                        line.chars().take(120).collect::<String>()
                    )
                });
                continue;
            }
        };
        let key = parsed.get("key").and_then(|value| value.as_str());
        if let Some(key) = key {
            match ContentStoreKey::parse(key) {
                Ok(parsed_key) => return Ok(Some(parsed_key)),
                Err(err) => {
                    warn!(
                        error = %err,
                        raw_key = %key,
                        "Failed to parse content-store key from add output"
                    );
                }
            }
        }
    }
    match invalid_line {
        Some(error) => Err(error),
        None => Ok(None),
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
mod tests {
    // Small inline tests are used here because the parser helper is private
    // and tightly coupled to git-annex output semantics.
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn parse_store_output_for_key_reports_invalid_utf8() -> ::xtask::sandbox::TestResult<()> {
        let error =
            parse_store_output_for_key(&[0xff]).expect_err("invalid utf-8 must be reported");
        assert!(error.contains("not valid UTF-8"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_unused_output_extracts_numbered_unused_entries()
    -> ::xtask::sandbox::TestResult<()> {
        let entries = parse_unused_output(
            br#"{"unused-list":{"2":"SHA256E-s4--beef.txt","1":"SHA256E-s5--deadbeef.dat"}}"#,
        )
        .expect("valid unused output should parse");

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].number, 1);
        assert_eq!(entries[0].key.key, "SHA256E-s5--deadbeef.dat");
        assert_eq!(entries[1].number, 2);
        assert_eq!(entries[1].key.digest, "beef.txt");
        Ok(())
    }

    #[sinex_test]
    async fn parse_unused_output_rejects_non_numeric_entry_numbers()
    -> ::xtask::sandbox::TestResult<()> {
        let error = parse_unused_output(br#"{"unused-list":{"oops":"SHA256E-s5--deadbeef.dat"}}"#)
            .expect_err("non-numeric unused entry number must fail honestly");

        assert!(error.contains("valid u32"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_store_output_for_key_reports_invalid_json_without_key()
    -> ::xtask::sandbox::TestResult<()> {
        let error =
            parse_store_output_for_key(b"not-json\n").expect_err("invalid json must be reported");
        assert!(error.contains("invalid JSON line"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_store_output_for_key_prefers_valid_key_when_present()
    -> ::xtask::sandbox::TestResult<()> {
        let key = parse_store_output_for_key(
            br#"{"note":"noise"}
{"key":"SHA256E-s42--deadbeef.txt"}"#,
        )
        .expect("valid json output should parse")
        .expect("valid content-store key should be returned");
        assert_eq!(key.storage_backend(), "SHA256E");
        assert_eq!(key.size, 42);
        assert_eq!(key.digest, "deadbeef.txt");
        Ok(())
    }

    #[sinex_test]
    async fn small_files_use_local_cas_without_content_store_process()
    -> ::xtask::sandbox::TestResult<()> {
        let repo_dir = tempfile::tempdir()?;
        let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
            .expect("temp path should be valid utf-8");
        let content_store = MaterialContentStore::new(ContentStoreConfig {
            root_path: repo_path.clone(),
            num_copies: None,
            large_files: None,
        })?;
        reset_content_store_process_counters();

        let source_path = repo_path.join("small-material.jsonl");
        tokio::fs::write(&source_path, br#"{"event":"small"}"#).await?;

        let key = content_store.store_file(&source_path).await?;
        assert_eq!(key.storage_backend(), LOCAL_BLAKE3_CAS_BACKEND);
        assert_eq!(key.size, 17);
        let counters = content_store_process_counters_snapshot();
        assert_eq!(
            counters.git_annex_commands, 0,
            "small-file storage should stay on local CAS and avoid git-annex subprocesses"
        );

        let content_path = content_store
            .path_if_local(&key.key)?
            .expect("local CAS key should resolve to a local path");
        assert!(content_path.exists());
        content_store.ensure_content_local(&key.key).await?;

        let verification = content_store
            .verify_key(false, false, Some(&key.key))
            .await?;
        assert!(verification.success);

        content_store.drop_content(&key.key, true).await?;
        assert!(!content_path.exists());
        Ok(())
    }
}
