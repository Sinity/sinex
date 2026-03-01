#![doc = include_str!("../../docs/annex.md")]

use crate::{NodeResult, SinexError};
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sinex_primitives::Ulid;
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command as AsyncCommand;
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, warn};

pub mod blob_manager;
pub mod path_validator;

pub use blob_manager::{BlobManager, BlobMetadata};
pub use path_validator::{VerifiedPath, create_secure_temp_path, validate_and_convert_path};

static ANNEX_PROCESS_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

fn annex_process_lock() -> &'static AsyncMutex<()> {
    ANNEX_PROCESS_LOCK.get_or_init(|| AsyncMutex::new(()))
}

fn run_command_blocking(
    mut cmd: Command,
    context: &'static str,
) -> NodeResult<std::process::Output> {
    let _guard = loop {
        if let Ok(guard) = annex_process_lock().try_lock() {
            break guard;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    cmd.output()
        .map_err(|e| SinexError::processing(context).with_source(e))
}

async fn run_command_async(
    mut cmd: AsyncCommand,
    context: &'static str,
) -> NodeResult<std::process::Output> {
    let _guard = annex_process_lock().lock().await;
    cmd.output()
        .await
        .map_err(|e| SinexError::processing(context).with_source(e))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnexConfig {
    pub repo_path: Utf8PathBuf,
    pub num_copies: Option<u8>,
    pub large_files: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnexKey {
    pub key: String,
    pub backend: String,
    pub size: u64,
    pub hash: String,
}

impl AnnexKey {
    pub fn parse(key_str: &str) -> NodeResult<Self> {
        // Parse git-annex key format: BACKEND-s<size>--hash.ext
        // Example: SHA256E-s12345--hash.dat
        let (prefix, hash) = key_str.split_once("--").ok_or_else(|| {
            SinexError::processing(format!(
                "Invalid annex key format (missing '--'): {key_str}"
            ))
        })?;

        let (backend, size_part) = prefix.split_once("-s").ok_or_else(|| {
            SinexError::processing(format!(
                "Invalid size format in annex key (missing '-s'): {key_str}"
            ))
        })?;

        let size = size_part.parse::<u64>().map_err(|e| {
            SinexError::processing(format!("Failed to parse size from annex key: {key_str}"))
                .with_source(e)
        })?;

        Ok(AnnexKey {
            key: key_str.to_string(),
            backend: backend.to_string(),
            size,
            hash: hash.to_string(),
        })
    }
}

struct BatchAddProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl BatchAddProcess {
    async fn spawn(repo_path: &Utf8Path) -> NodeResult<Self> {
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("add")
            .arg("--json")
            .arg("--batch")
            .current_dir(repo_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());

        let mut child = cmd.spawn().map_err(|e| {
            SinexError::processing(
                "Failed to spawn git-annex add --batch. Is git-annex installed and available in PATH?"
            ).with_source(e)
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            SinexError::processing("Missing stdin handle for git-annex add --batch".to_string())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            SinexError::processing("Missing stdout handle for git-annex add --batch".to_string())
        })?;

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    async fn shutdown(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

struct BatchAddState {
    process: Option<BatchAddProcess>,
    disabled: bool,
    disabled_reason: Option<String>,
}

impl BatchAddState {
    fn new() -> Self {
        Self {
            process: None,
            disabled: false,
            disabled_reason: None,
        }
    }

    async fn add(
        &mut self,
        repo_path: &Utf8Path,
        relative_path: &Utf8Path,
    ) -> NodeResult<AnnexKey> {
        if self.disabled {
            let reason = self
                .disabled_reason
                .clone()
                .unwrap_or_else(|| "unknown error".to_string());
            return Err(SinexError::processing(format!(
                "git-annex batch add disabled: {reason}"
            )));
        }

        if self.process.is_none() {
            self.process = Some(BatchAddProcess::spawn(repo_path).await?);
        }

        let _guard = annex_process_lock().lock().await;

        let process = self.process.as_mut().ok_or_else(|| {
            SinexError::processing("git-annex batch process unavailable".to_string())
        })?;

        if let Some(status) = process.child.try_wait().map_err(SinexError::io)? {
            let reason = format!("git-annex batch add exited with {status}");
            self.disable(reason).await;
            return Err(SinexError::processing(
                "git-annex batch add exited unexpectedly".to_string(),
            ));
        }

        let line = format!("{}\n", relative_path.as_str());
        process
            .stdin
            .write_all(line.as_bytes())
            .await
            .map_err(SinexError::io)?;
        process.stdin.flush().await.map_err(SinexError::io)?;

        let mut output_line = String::new();
        loop {
            output_line.clear();
            let bytes = process
                .stdout
                .read_line(&mut output_line)
                .await
                .map_err(SinexError::io)?;
            if bytes == 0 {
                let reason = "git-annex batch add closed stdout".to_string();
                self.disable(reason).await;
                return Err(SinexError::processing(
                    "git-annex batch add terminated unexpectedly".to_string(),
                ));
            }
            if !output_line.trim().is_empty() {
                break;
            }
        }

        let parsed: JsonValue = match serde_json::from_str(output_line.trim()) {
            Ok(parsed) => parsed,
            Err(err) => {
                let reason = format!("git-annex batch add returned non-JSON output: {err}");
                self.disable(reason).await;
                return Err(SinexError::processing(
                    "git-annex batch add returned invalid JSON".to_string(),
                ));
            }
        };

        if parsed.get("success").and_then(|val| val.as_bool()) == Some(false) {
            let errors = parsed
                .get("error-messages")
                .and_then(|val| val.as_array())
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|entry| entry.as_str())
                        .collect::<Vec<_>>()
                        .join("; ")
                });
            let message = errors.unwrap_or_else(|| "unknown batch add error".to_string());
            return Err(SinexError::processing(format!(
                "git-annex batch add failed: {message}"
            )));
        }

        let key = parsed
            .get("key")
            .and_then(|value| value.as_str())
            .ok_or_else(|| SinexError::processing("git-annex batch add missing key".to_string()))?;

        let parsed_key = AnnexKey::parse(key).map_err(|e| {
            SinexError::processing(format!("git-annex batch add returned invalid key: {key}"))
                .with_source(e)
        })?;

        Ok(parsed_key)
    }

    async fn disable(&mut self, reason: String) {
        if !self.disabled {
            warn!(reason = %reason, "Disabling git-annex batch add");
        }
        self.disabled = true;
        self.disabled_reason = Some(reason);
        if let Some(process) = self.process.as_mut() {
            process.shutdown().await;
        }
        self.process = None;
    }
}

impl std::fmt::Debug for BatchAddState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchAddState")
            .field("disabled", &self.disabled)
            .field("process_running", &self.process.is_some())
            .field("disabled_reason", &self.disabled_reason)
            .finish()
    }
}

#[derive(Debug)]
pub struct GitAnnex {
    pub config: AnnexConfig,
    batch_add: AsyncMutex<BatchAddState>,
}

impl GitAnnex {
    pub fn new(config: AnnexConfig) -> NodeResult<Self> {
        // Verify git-annex is available
        which::which("git-annex")
            .map_err(|e| SinexError::processing("git-annex not found in PATH").with_source(e))?;

        // Ensure repository exists; initialize git + git-annex even when the
        // directory already exists (e.g., tempdirs created by tests).
        std::fs::create_dir_all(&config.repo_path).map_err(SinexError::io)?;

        let git_dir = config.repo_path.join(".git");
        if !git_dir.exists() {
            info!(
                "Initializing git repository for annex at {:?}",
                config.repo_path
            );

            let mut git_cmd = Command::new("git");
            git_cmd.arg("init").current_dir(&config.repo_path);
            let git_output =
                run_command_blocking(git_cmd, "Failed to run git init for annex repo")?;
            if !git_output.status.success() {
                return Err(SinexError::processing(format!(
                    "git init failed for annex repo: {}",
                    String::from_utf8_lossy(&git_output.stderr)
                )));
            }
        }

        let annex_dir = git_dir.join("annex");
        if !annex_dir.exists() {
            info!(
                "Initializing git-annex repository at {:?}",
                config.repo_path
            );

            let mut annex_cmd = Command::new("git-annex");
            annex_cmd
                .args(["init", "sinex"])
                .current_dir(&config.repo_path);
            let annex_output =
                run_command_blocking(annex_cmd, "Failed to run git-annex init for annex repo")?;
            if !annex_output.status.success() {
                return Err(SinexError::processing(format!(
                    "git-annex init failed for annex repo: {}",
                    String::from_utf8_lossy(&annex_output.stderr)
                )));
            }
        }

        Ok(GitAnnex {
            config,
            batch_add: AsyncMutex::new(BatchAddState::new()),
        })
    }

    /// Get the repository path
    pub fn repo_path(&self) -> &Utf8Path {
        &self.config.repo_path
    }

    /// Initialize a new git-annex repository
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

    /// Add a file to git-annex and return the annex key
    pub async fn add_file(&self, file_path: &Utf8Path) -> NodeResult<AnnexKey> {
        debug!("Adding file to annex: {:?}", file_path);

        // Allow callers to pass either absolute paths or paths relative to the
        // annex repository root. Resolve to an absolute path for validation so
        // we don't accidentally check the process CWD (which may differ from
        // the repo path for systemd services).
        let resolved_path = if file_path.is_absolute() {
            file_path.to_owned()
        } else {
            self.config.repo_path.join(file_path)
        };

        if !resolved_path.exists() {
            return Err(SinexError::processing(format!(
                "File does not exist: {resolved_path:?}"
            )));
        }

        let (ingest_path, needs_cleanup) = if resolved_path.starts_with(&self.config.repo_path) {
            (resolved_path.clone(), false)
        } else {
            let temp_name = format!("ingest-{}.tmp", Ulid::new());
            let target = self.config.repo_path.join(temp_name);
            tokio::fs::copy(&resolved_path, &target)
                .await
                .map_err(|e| SinexError::io(e))?;
            (target, true)
        };

        let relative_path = ingest_path
            .strip_prefix(&self.config.repo_path)
            .unwrap_or(&ingest_path)
            .to_owned();

        let key = match self.try_batch_add(&relative_path).await {
            Ok(key) => key,
            Err(err) => {
                debug!(error = %err, "git-annex batch add failed; falling back");
                self.add_file_direct(&relative_path, &resolved_path)
                    .await
                    .map_err(|e| {
                        SinexError::processing(format!(
                            "git-annex add fallback failed after batch error: {err}"
                        ))
                        .with_source(e)
                    })?
            }
        };

        if needs_cleanup {
            if let Err(e) = tokio::fs::remove_file(&ingest_path).await {
                warn!(
                    error = %e,
                    path = %ingest_path,
                    "Failed to clean up staged ingest file inside annex repo"
                );
            }
        }

        Ok(key)
    }

    async fn try_batch_add(&self, relative_path: &Utf8Path) -> NodeResult<AnnexKey> {
        let mut batch = self.batch_add.lock().await;
        batch.add(&self.config.repo_path, relative_path).await
    }

    async fn add_file_direct(
        &self,
        relative_path: &Utf8Path,
        resolved_path: &Utf8Path,
    ) -> NodeResult<AnnexKey> {
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("add")
            .arg("--json")
            .arg(relative_path.as_str())
            .current_dir(&self.config.repo_path);
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

        match parse_add_output_for_key(&output.stdout) {
            Some(key) => Ok(key),
            None => self.get_key(relative_path).await,
        }
    }

    /// Get the annex key for a file
    pub async fn get_key(&self, file_path: &Utf8Path) -> NodeResult<AnnexKey> {
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("lookupkey")
            .arg(file_path)
            .current_dir(&self.config.repo_path);
        let output = run_command_async(cmd, "Failed to run git-annex lookupkey").await?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex lookupkey failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let key_str = String::from_utf8(output.stdout)
            .map_err(|e| SinexError::processing("Invalid UTF-8 in annex key").with_source(e))?
            .trim()
            .to_string();

        AnnexKey::parse(&key_str)
    }

    fn resolve_argument(&self, key_or_path: &str) -> (bool, String) {
        let candidate = self.config.repo_path.join(key_or_path);
        if candidate.exists() {
            let rel = candidate
                .strip_prefix(&self.config.repo_path)
                .unwrap_or(&candidate);
            (false, rel.to_string())
        } else {
            (true, key_or_path.to_string())
        }
    }

    /// Ensure content is available locally
    pub async fn get_content(&self, key_or_path: &str) -> NodeResult<()> {
        debug!("Getting content for: {key_or_path}");

        let (is_key, argument) = self.resolve_argument(key_or_path);

        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("get");
        if is_key {
            cmd.arg("--key").arg(&argument);
        } else {
            cmd.arg(&argument);
        }

        cmd.current_dir(&self.config.repo_path);
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

        cmd.current_dir(&self.config.repo_path);
        let output = run_command_async(cmd, "Failed to run git-annex drop").await?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex drop failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        Ok(())
    }

    /// Check filesystem integrity
    pub async fn fsck(
        &self,
        fast: bool,
        incremental: bool,
        key: Option<&str>,
    ) -> NodeResult<String> {
        info!("Running git-annex fsck");

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

        cmd.current_dir(&self.config.repo_path);
        let output = run_command_async(cmd, "Failed to run git-annex fsck").await?;

        let result = String::from_utf8(output.stdout)
            .map_err(|e| SinexError::processing("Invalid UTF-8 in fsck output").with_source(e))?;

        if !output.status.success() {
            warn!(
                "git-annex fsck completed with errors: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(result)
    }

    /// Get repository status information
    pub async fn status(&self) -> NodeResult<String> {
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("status").current_dir(&self.config.repo_path);
        let output = run_command_async(cmd, "Failed to run git-annex status").await?;

        String::from_utf8(output.stdout)
            .map_err(|e| SinexError::processing("Invalid UTF-8 in status output").with_source(e))
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
            .current_dir(&self.config.repo_path);
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

fn parse_add_output_for_key(stdout: &[u8]) -> Option<AnnexKey> {
    let output = std::str::from_utf8(stdout).ok()?;
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parsed: JsonValue = match serde_json::from_str(line) {
            Ok(parsed) => parsed,
            Err(_) => continue,
        };
        let key = parsed.get("key").and_then(|value| value.as_str());
        if let Some(key) = key {
            match AnnexKey::parse(key) {
                Ok(parsed_key) => return Some(parsed_key),
                Err(err) => {
                    warn!(
                        error = %err,
                        raw_key = %key,
                        "Failed to parse annex key from add output"
                    );
                }
            }
        }
    }
    None
}
