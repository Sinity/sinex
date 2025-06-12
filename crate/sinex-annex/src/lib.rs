use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::process::Command as AsyncCommand;
use tracing::{debug, info, warn};

pub mod blob_manager;
pub use blob_manager::{BlobManager, BlobMetadata};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnexConfig {
    pub repo_path: PathBuf,
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
    pub fn parse(key_str: &str) -> Result<Self> {
        // Parse git-annex key format: BACKEND-sizeSTORE--hash.ext
        // Example: SHA256E-s12345--hash.dat
        let parts: Vec<&str> = key_str.split('-').collect();
        if parts.len() < 3 {
            anyhow::bail!("Invalid annex key format: {}", key_str);
        }

        let backend = parts[0].to_string();
        let size_part = &parts[1];
        
        if !size_part.starts_with('s') {
            anyhow::bail!("Invalid size format in annex key: {}", key_str);
        }
        
        let size = size_part[1..].parse::<u64>()
            .context("Failed to parse size from annex key")?;

        let hash = parts[2..].join("-");

        Ok(AnnexKey {
            key: key_str.to_string(),
            backend,
            size,
            hash,
        })
    }
}

#[derive(Debug)]
pub struct GitAnnex {
    config: AnnexConfig,
}

impl GitAnnex {
    pub fn new(config: AnnexConfig) -> Result<Self> {
        // Verify git-annex is available
        which::which("git-annex")
            .context("git-annex not found in PATH")?;

        // Verify repository exists
        if !config.repo_path.exists() {
            anyhow::bail!("Repository path does not exist: {:?}", config.repo_path);
        }

        Ok(GitAnnex { config })
    }

    /// Initialize a new git-annex repository
    pub async fn init(repo_path: &Path, description: Option<&str>) -> Result<()> {
        info!("Initializing git-annex repository at {:?}", repo_path);
        
        // Ensure directory exists
        tokio::fs::create_dir_all(repo_path).await
            .context("Failed to create repository directory")?;

        // Initialize git repository if needed
        let git_dir = repo_path.join(".git");
        if !git_dir.exists() {
            let output = AsyncCommand::new("git")
                .arg("init")
                .current_dir(repo_path)
                .output()
                .await
                .context("Failed to run git init")?;

            if !output.status.success() {
                anyhow::bail!("git init failed: {}", String::from_utf8_lossy(&output.stderr));
            }
        }

        // Initialize git-annex
        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("init").current_dir(repo_path);
        
        if let Some(desc) = description {
            cmd.arg(desc);
        }

        let output = cmd.output().await
            .context("Failed to run git-annex init")?;

        if !output.status.success() {
            anyhow::bail!("git-annex init failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        info!("Successfully initialized git-annex repository");
        Ok(())
    }

    /// Add a file to git-annex and return the annex key
    pub async fn add_file(&self, file_path: &Path) -> Result<AnnexKey> {
        debug!("Adding file to annex: {:?}", file_path);

        if !file_path.exists() {
            anyhow::bail!("File does not exist: {:?}", file_path);
        }

        let output = AsyncCommand::new("git-annex")
            .arg("add")
            .arg(file_path)
            .current_dir(&self.config.repo_path)
            .output()
            .await
            .context("Failed to run git-annex add")?;

        if !output.status.success() {
            anyhow::bail!("git-annex add failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        // Get the annex key for the added file
        self.get_key(file_path).await
    }

    /// Get the annex key for a file
    pub async fn get_key(&self, file_path: &Path) -> Result<AnnexKey> {
        let output = AsyncCommand::new("git-annex")
            .arg("lookupkey")
            .arg(file_path)
            .current_dir(&self.config.repo_path)
            .output()
            .await
            .context("Failed to run git-annex lookupkey")?;

        if !output.status.success() {
            anyhow::bail!("git-annex lookupkey failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        let key_str = String::from_utf8(output.stdout)
            .context("Invalid UTF-8 in annex key")?
            .trim()
            .to_string();

        AnnexKey::parse(&key_str)
    }

    /// Ensure content is available locally
    pub async fn get_content(&self, key_or_path: &str) -> Result<()> {
        debug!("Getting content for: {}", key_or_path);

        let output = AsyncCommand::new("git-annex")
            .arg("get")
            .arg(key_or_path)
            .current_dir(&self.config.repo_path)
            .output()
            .await
            .context("Failed to run git-annex get")?;

        if !output.status.success() {
            anyhow::bail!("git-annex get failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        Ok(())
    }

    /// Drop content if sufficient copies exist elsewhere
    pub async fn drop_content(&self, key_or_path: &str, force: bool) -> Result<()> {
        debug!("Dropping content for: {}", key_or_path);

        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("drop").arg(key_or_path);
        
        if force {
            cmd.arg("--force");
        }

        let output = cmd
            .current_dir(&self.config.repo_path)
            .output()
            .await
            .context("Failed to run git-annex drop")?;

        if !output.status.success() {
            anyhow::bail!("git-annex drop failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        Ok(())
    }

    /// Check filesystem integrity
    pub async fn fsck(&self, fast: bool, incremental: bool) -> Result<String> {
        info!("Running git-annex fsck");

        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("fsck");
        
        if fast {
            cmd.arg("--fast");
        }
        
        if incremental {
            cmd.arg("--incremental");
        }

        let output = cmd
            .current_dir(&self.config.repo_path)
            .output()
            .await
            .context("Failed to run git-annex fsck")?;

        let result = String::from_utf8(output.stdout)
            .context("Invalid UTF-8 in fsck output")?;

        if !output.status.success() {
            warn!("git-annex fsck completed with errors: {}", 
                  String::from_utf8_lossy(&output.stderr));
        }

        Ok(result)
    }

    /// Get repository status information
    pub async fn status(&self) -> Result<String> {
        let output = AsyncCommand::new("git-annex")
            .arg("status")
            .current_dir(&self.config.repo_path)
            .output()
            .await
            .context("Failed to run git-annex status")?;

        String::from_utf8(output.stdout)
            .context("Invalid UTF-8 in status output")
    }

    /// Compute BLAKE3 hash for deduplication
    pub async fn compute_blake3_hash(file_path: &Path) -> Result<String> {
        let content = tokio::fs::read(file_path).await
            .context("Failed to read file for hashing")?;
        
        let hash = blake3::hash(&content);
        Ok(hash.to_hex().to_string())
    }

    /// Configure repository settings
    pub async fn configure(&self) -> Result<()> {
        if let Some(num_copies) = self.config.num_copies {
            self.set_config("annex.numcopies", &num_copies.to_string()).await?;
        }

        if let Some(ref large_files) = self.config.large_files {
            self.set_config("annex.largefiles", large_files).await?;
        }

        Ok(())
    }

    async fn set_config(&self, key: &str, value: &str) -> Result<()> {
        let output = AsyncCommand::new("git")
            .arg("config")
            .arg(key)
            .arg(value)
            .current_dir(&self.config.repo_path)
            .output()
            .await
            .context("Failed to set git config")?;

        if !output.status.success() {
            anyhow::bail!("Failed to set config {}: {}", key, 
                         String::from_utf8_lossy(&output.stderr));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_annex_key_parsing() {
        let key = AnnexKey::parse("SHA256E-s12345--abcdef123456.dat").unwrap();
        assert_eq!(key.backend, "SHA256E");
        assert_eq!(key.size, 12345);
        assert!(key.hash.contains("abcdef123456"));
    }

    #[tokio::test]
    async fn test_blake3_hash() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        tokio::fs::write(&test_file, b"hello world").await.unwrap();
        
        let hash = GitAnnex::compute_blake3_hash(&test_file).await.unwrap();
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64); // BLAKE3 hex string length
    }
}