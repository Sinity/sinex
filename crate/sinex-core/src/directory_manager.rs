use anyhow::{Context, Result};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{info, warn, error};

/// Directory validation and creation with proper permissions
pub struct DirectoryManager {
    required_directories: Vec<DirectoryConfig>,
}

/// Configuration for a required directory
#[derive(Debug, Clone)]
pub struct DirectoryConfig {
    pub path: PathBuf,
    pub description: String,
    pub mode: u32,
    pub create_if_missing: bool,
    pub required: bool,
}

impl DirectoryConfig {
    /// Create a new directory configuration
    pub fn new<P: Into<PathBuf>>(path: P, description: &str) -> Self {
        Self {
            path: path.into(),
            description: description.to_string(),
            mode: 0o755, // Default: rwxr-xr-x
            create_if_missing: true,
            required: true,
        }
    }

    /// Set custom permissions mode
    pub fn with_mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }

    /// Set whether to create the directory if missing
    pub fn with_create_if_missing(mut self, create: bool) -> Self {
        self.create_if_missing = create;
        self
    }

    /// Set whether this directory is required for operation
    pub fn with_required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }
}

impl DirectoryManager {
    /// Create a new directory manager
    pub fn new() -> Self {
        Self {
            required_directories: Vec::new(),
        }
    }

    /// Add a directory to manage
    pub fn add_directory(&mut self, config: DirectoryConfig) {
        self.required_directories.push(config);
    }

    /// Add multiple directories to manage
    pub fn add_directories(&mut self, configs: Vec<DirectoryConfig>) {
        self.required_directories.extend(configs);
    }

    /// Validate and create all required directories
    pub async fn initialize_all(&self) -> Result<()> {
        let mut has_errors = false;
        let mut required_failures = Vec::new();

        for dir_config in &self.required_directories {
            match self.ensure_directory(dir_config).await {
                Ok(_) => {
                    info!(
                        path = %dir_config.path.display(),
                        description = %dir_config.description,
                        "Directory validated successfully"
                    );
                }
                Err(e) => {
                    if dir_config.required {
                        error!(
                            path = %dir_config.path.display(),
                            description = %dir_config.description,
                            error = %e,
                            "Required directory failed validation"
                        );
                        required_failures.push((dir_config.path.clone(), e));
                        has_errors = true;
                    } else {
                        warn!(
                            path = %dir_config.path.display(),
                            description = %dir_config.description,
                            error = %e,
                            "Optional directory failed validation"
                        );
                    }
                }
            }
        }

        if has_errors {
            return Err(anyhow::anyhow!(
                "Failed to initialize {} required directories: {:?}",
                required_failures.len(),
                required_failures.into_iter()
                    .map(|(path, _)| path.display().to_string())
                    .collect::<Vec<_>>()
            ));
        }

        Ok(())
    }

    /// Ensure a single directory exists with correct permissions
    async fn ensure_directory(&self, config: &DirectoryConfig) -> Result<()> {
        let path = &config.path;

        // Check if directory exists
        if !path.exists() {
            if config.create_if_missing {
                info!(
                    path = %path.display(),
                    description = %config.description,
                    "Creating missing directory"
                );
                self.create_directory_with_permissions(path, config.mode).await
                    .with_context(|| format!("Failed to create directory: {}", path.display()))?;
            } else {
                return Err(anyhow::anyhow!(
                    "Required directory does not exist and creation is disabled: {}",
                    path.display()
                ));
            }
        }

        // Validate it's actually a directory
        let metadata = fs::metadata(path).await
            .with_context(|| format!("Failed to read directory metadata: {}", path.display()))?;

        if !metadata.is_dir() {
            return Err(anyhow::anyhow!(
                "Path exists but is not a directory: {}",
                path.display()
            ));
        }

        // Check permissions (Unix only)
        #[cfg(unix)]
        {
            let current_mode = metadata.permissions().mode() & 0o777;
            if current_mode != config.mode {
                warn!(
                    path = %path.display(),
                    current_mode = format!("{:o}", current_mode),
                    expected_mode = format!("{:o}", config.mode),
                    "Directory has unexpected permissions, attempting to fix"
                );
                
                self.set_directory_permissions(path, config.mode).await
                    .with_context(|| format!("Failed to set directory permissions: {}", path.display()))?;
            }
        }

        // Test write access
        self.test_directory_access(path).await
            .with_context(|| format!("Directory access test failed: {}", path.display()))?;

        Ok(())
    }

    /// Create directory with specific permissions
    async fn create_directory_with_permissions(&self, path: &Path, mode: u32) -> Result<()> {
        fs::create_dir_all(path).await?;
        
        #[cfg(unix)]
        {
            self.set_directory_permissions(path, mode).await?;
        }
        
        Ok(())
    }

    /// Set directory permissions (Unix only)
    #[cfg(unix)]
    async fn set_directory_permissions(&self, path: &Path, mode: u32) -> Result<()> {
        let permissions = Permissions::from_mode(mode);
        fs::set_permissions(path, permissions).await?;
        Ok(())
    }

    /// Test directory write access
    async fn test_directory_access(&self, path: &Path) -> Result<()> {
        let test_file = path.join(".sinex_access_test");
        
        // Try to create a test file
        fs::write(&test_file, b"access_test").await
            .with_context(|| "Failed to write test file - directory not writable")?;
        
        // Try to read it back
        let content = fs::read(&test_file).await
            .with_context(|| "Failed to read test file - directory not readable")?;
        
        if content != b"access_test" {
            return Err(anyhow::anyhow!("Test file content mismatch - filesystem corruption?"));
        }
        
        // Clean up test file
        fs::remove_file(&test_file).await
            .with_context(|| "Failed to remove test file - directory cleanup issues")?;
        
        Ok(())
    }

    /// Get standard Sinex directories based on environment
    pub fn get_standard_directories() -> Vec<DirectoryConfig> {
        let data_dir = std::env::var("SINEX_DATA_DIR")
            .or_else(|_| std::env::var("XDG_DATA_HOME").map(|d| format!("{}/sinex", d)))
            .unwrap_or_else(|_| "/var/lib/sinex".to_string());
        
        let tmp_dir = std::env::var("SINEX_TMP_DIR")
            .unwrap_or_else(|_| "/tmp/sinex".to_string());
        
        let log_dir = std::env::var("SINEX_LOG_DIR")
            .or_else(|_| std::env::var("XDG_CACHE_HOME").map(|d| format!("{}/sinex/logs", d)))
            .unwrap_or_else(|_| format!("{}/logs", data_dir));

        vec![
            DirectoryConfig::new(&data_dir, "Main data directory")
                .with_mode(0o755),
            DirectoryConfig::new(format!("{}/events", data_dir), "Event storage")
                .with_mode(0o755),
            DirectoryConfig::new(format!("{}/dlq", data_dir), "Dead letter queue")
                .with_mode(0o755),
            DirectoryConfig::new(&tmp_dir, "Temporary files")
                .with_mode(0o755),
            DirectoryConfig::new(&log_dir, "Log files")
                .with_mode(0o755),
            DirectoryConfig::new(format!("{}/shell", data_dir), "Shell history")
                .with_mode(0o700) // More restrictive for shell data
                .with_required(false), // Optional
        ]
    }
}

impl Default for DirectoryManager {
    fn default() -> Self {
        Self::new()
    }
}