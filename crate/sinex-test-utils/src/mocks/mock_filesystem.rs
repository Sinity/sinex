// Mock filesystem implementation for testing
//
// Provides a controllable filesystem substitute that can simulate:
// - Permission errors
// - Disk full conditions
// - File corruption
// - Network filesystem issues
// - Concurrent access problems


use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

/// Configuration for MockFilesystem behavior
#[derive(Debug, Clone)]
pub struct MockFilesystemConfig {
    /// Maximum number of files
    pub max_files: usize,
    /// Maximum file size in bytes
    pub max_file_size: usize,
    /// Simulated disk capacity
    pub disk_capacity: usize,
    /// Probability of permission errors
    pub permission_error_rate: f64,
    /// Probability of disk full errors
    pub disk_full_rate: f64,
    /// Probability of file corruption
    pub corruption_rate: f64,
    /// Simulated IO latency
    pub io_latency: Duration,
    /// Whether to simulate concurrent access issues
    pub simulate_concurrent_access: bool,
    /// Read-only mode
    pub read_only: bool,
}

impl Default for MockFilesystemConfig {
    fn default() -> Self {
        Self {
            max_files: 10000,
            max_file_size: 10 * 1024 * 1024,   // 10MB
            disk_capacity: 1024 * 1024 * 1024, // 1GB
            permission_error_rate: 0.0,
            disk_full_rate: 0.0,
            corruption_rate: 0.0,
            io_latency: Duration::from_millis(1),
            simulate_concurrent_access: false,
            read_only: false,
        }
    }
}

/// Mock filesystem implementation
pub struct MockFilesystem {
    config: MockFilesystemConfig,
    files: Arc<RwLock<HashMap<PathBuf, MockFile>>>,
    directories: Arc<RwLock<HashMap<PathBuf, MockDirectory>>>,
    disk_usage: Arc<RwLock<usize>>,
    operation_count: Arc<RwLock<usize>>,
    start_time: Instant,
}

#[derive(Debug, Clone)]
struct MockFile {
    path: PathBuf,
    content: Vec<u8>,
    metadata: MockFileMetadata,
    locked: bool,
}

#[derive(Debug, Clone)]
struct MockFileMetadata {
    size: usize,
    created: Instant,
    modified: Instant,
    accessed: Instant,
    permissions: u32,
    is_directory: bool,
}

#[derive(Debug, Clone)]
struct MockDirectory {
    path: PathBuf,
    entries: Vec<PathBuf>,
    metadata: MockFileMetadata,
}

impl MockFilesystem {
    pub fn new(config: MockFilesystemConfig) -> Self {
        let mut directories = HashMap::new();

        // Create root directory
        directories.insert(
            PathBuf::from("/"),
            MockDirectory {
                path: PathBuf::from("/"),
                entries: Vec::new(),
                metadata: MockFileMetadata {
                    size: 0,
                    created: Instant::now(),
                    modified: Instant::now(),
                    accessed: Instant::now(),
                    permissions: 0o755,
                    is_directory: true,
                },
            },
        );

        Self {
            config,
            files: Arc::new(RwLock::new(HashMap::new())),
            directories: Arc::new(RwLock::new(directories)),
            disk_usage: Arc::new(RwLock::new(0)),
            operation_count: Arc::new(RwLock::new(0)),
            start_time: Instant::now(),
        }
    }

    pub async fn create_file(
        &self,
        path: &Path,
        content: &[u8],
    ) -> Result<(), MockFilesystemError> {
        self.increment_operation_count().await;

        if self.config.read_only {
            return Err(MockFilesystemError::PermissionDenied);
        }

        if self.should_fail_permission().await {
            return Err(MockFilesystemError::PermissionDenied);
        }

        if self.should_fail_disk_full().await {
            return Err(MockFilesystemError::DiskFull);
        }

        if content.len() > self.config.max_file_size {
            return Err(MockFilesystemError::FileTooLarge);
        }

        // Check disk capacity
        let current_usage = *self.disk_usage.read().await;
        if current_usage + content.len() > self.config.disk_capacity {
            return Err(MockFilesystemError::DiskFull);
        }

        // Check file limit
        let files = self.files.read().await;
        if files.len() >= self.config.max_files {
            return Err(MockFilesystemError::TooManyFiles);
        }
        drop(files);

        // Simulate IO latency
        tokio::time::sleep(self.config.io_latency).await;

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            self.create_directory(parent).await?;
        }

        // Create file
        let now = Instant::now();
        let file = MockFile {
            path: path.to_path_buf(),
            content: content.to_vec(),
            metadata: MockFileMetadata {
                size: content.len(),
                created: now,
                modified: now,
                accessed: now,
                permissions: 0o644,
                is_directory: false,
            },
            locked: false,
        };

        let mut files = self.files.write().await;
        files.insert(path.to_path_buf(), file);

        // Update disk usage
        let mut disk_usage = self.disk_usage.write().await;
        *disk_usage += content.len();

        Ok(())
    }

    pub async fn read_file(&self, path: &Path) -> Result<Vec<u8>, MockFilesystemError> {
        self.increment_operation_count().await;

        if self.should_fail_permission().await {
            return Err(MockFilesystemError::PermissionDenied);
        }

        // Simulate IO latency
        tokio::time::sleep(self.config.io_latency).await;

        let mut files = self.files.write().await;
        match files.get_mut(path) {
            Some(file) => {
                file.metadata.accessed = Instant::now();

                // Simulate corruption
                if self.should_corrupt_file().await {
                    return Err(MockFilesystemError::FileCorrupted);
                }

                Ok(file.content.clone())
            }
            None => Err(MockFilesystemError::FileNotFound),
        }
    }

    pub async fn write_file(
        &self,
        path: &Path,
        content: &[u8],
    ) -> Result<(), MockFilesystemError> {
        self.increment_operation_count().await;

        if self.config.read_only {
            return Err(MockFilesystemError::PermissionDenied);
        }

        if self.should_fail_permission().await {
            return Err(MockFilesystemError::PermissionDenied);
        }

        if content.len() > self.config.max_file_size {
            return Err(MockFilesystemError::FileTooLarge);
        }

        // Simulate IO latency
        tokio::time::sleep(self.config.io_latency).await;

        let mut files = self.files.write().await;
        match files.get_mut(path) {
            Some(file) => {
                if file.locked && self.config.simulate_concurrent_access {
                    return Err(MockFilesystemError::FileLocked);
                }

                // Check disk capacity change
                let size_diff = content.len() as i64 - file.content.len() as i64;
                if size_diff > 0 {
                    let current_usage = *self.disk_usage.read().await;
                    if current_usage + size_diff as usize > self.config.disk_capacity {
                        return Err(MockFilesystemError::DiskFull);
                    }
                }

                file.content = content.to_vec();
                file.metadata.size = content.len();
                file.metadata.modified = Instant::now();
                file.metadata.accessed = Instant::now();

                // Update disk usage
                let mut disk_usage = self.disk_usage.write().await;
                *disk_usage = (*disk_usage as i64 + size_diff) as usize;

                Ok(())
            }
            None => Err(MockFilesystemError::FileNotFound),
        }
    }

    pub async fn delete_file(&self, path: &Path) -> Result<(), MockFilesystemError> {
        self.increment_operation_count().await;

        if self.config.read_only {
            return Err(MockFilesystemError::PermissionDenied);
        }

        if self.should_fail_permission().await {
            return Err(MockFilesystemError::PermissionDenied);
        }

        // Simulate IO latency
        tokio::time::sleep(self.config.io_latency).await;

        let mut files = self.files.write().await;
        match files.remove(path) {
            Some(file) => {
                // Update disk usage
                let mut disk_usage = self.disk_usage.write().await;
                *disk_usage = disk_usage.saturating_sub(file.metadata.size);

                Ok(())
            }
            None => Err(MockFilesystemError::FileNotFound),
        }
    }

    pub async fn exists(&self, path: &Path) -> bool {
        self.increment_operation_count().await;

        // Simulate IO latency
        tokio::time::sleep(self.config.io_latency).await;

        let files = self.files.read().await;
        let directories = self.directories.read().await;

        files.contains_key(path) || directories.contains_key(path)
    }

    pub async fn create_directory(&self, path: &Path) -> Result<(), MockFilesystemError> {
        self.increment_operation_count().await;

        if self.config.read_only {
            return Err(MockFilesystemError::PermissionDenied);
        }

        if self.should_fail_permission().await {
            return Err(MockFilesystemError::PermissionDenied);
        }

        // Simulate IO latency
        tokio::time::sleep(self.config.io_latency).await;

        let mut directories = self.directories.write().await;

        // Check if directory already exists
        if directories.contains_key(path) {
            return Ok(()); // Directory already exists
        }

        // Create parent directories recursively
        if let Some(parent) = path.parent() {
            if !directories.contains_key(parent) {
                drop(directories);
                Box::pin(self.create_directory(parent)).await?;
                directories = self.directories.write().await;
            }
        }

        let now = Instant::now();
        let directory = MockDirectory {
            path: path.to_path_buf(),
            entries: Vec::new(),
            metadata: MockFileMetadata {
                size: 0,
                created: now,
                modified: now,
                accessed: now,
                permissions: 0o755,
                is_directory: true,
            },
        };

        directories.insert(path.to_path_buf(), directory);

        Ok(())
    }

    pub async fn list_directory(
        &self,
        path: &Path,
    ) -> Result<Vec<PathBuf>, MockFilesystemError> {
        self.increment_operation_count().await;

        if self.should_fail_permission().await {
            return Err(MockFilesystemError::PermissionDenied);
        }

        // Simulate IO latency
        tokio::time::sleep(self.config.io_latency).await;

        let directories = self.directories.read().await;
        let files = self.files.read().await;

        // Check if directory exists
        if !directories.contains_key(path) {
            return Err(MockFilesystemError::DirectoryNotFound);
        }

        let mut entries = Vec::new();

        // Add files in this directory
        for file_path in files.keys() {
            if let Some(parent) = file_path.parent() {
                if parent == path {
                    entries.push(file_path.clone());
                }
            }
        }

        // Add subdirectories
        for dir_path in directories.keys() {
            if let Some(parent) = dir_path.parent() {
                if parent == path {
                    entries.push(dir_path.clone());
                }
            }
        }

        Ok(entries)
    }

    pub async fn get_metadata(
        &self,
        path: &Path,
    ) -> Result<MockFileMetadata, MockFilesystemError> {
        self.increment_operation_count().await;

        if self.should_fail_permission().await {
            return Err(MockFilesystemError::PermissionDenied);
        }

        // Simulate IO latency
        tokio::time::sleep(self.config.io_latency).await;

        let files = self.files.read().await;
        let directories = self.directories.read().await;

        if let Some(file) = files.get(path) {
            Ok(file.metadata.clone())
        } else if let Some(dir) = directories.get(path) {
            Ok(dir.metadata.clone())
        } else {
            Err(MockFilesystemError::FileNotFound)
        }
    }

    pub async fn lock_file(&self, path: &Path) -> Result<(), MockFilesystemError> {
        if !self.config.simulate_concurrent_access {
            return Ok(());
        }

        let mut files = self.files.write().await;
        match files.get_mut(path) {
            Some(file) => {
                if file.locked {
                    Err(MockFilesystemError::FileLocked)
                } else {
                    file.locked = true;
                    Ok(())
                }
            }
            None => Err(MockFilesystemError::FileNotFound),
        }
    }

    pub async fn unlock_file(&self, path: &Path) -> Result<(), MockFilesystemError> {
        if !self.config.simulate_concurrent_access {
            return Ok(());
        }

        let mut files = self.files.write().await;
        match files.get_mut(path) {
            Some(file) => {
                file.locked = false;
                Ok(())
            }
            None => Err(MockFilesystemError::FileNotFound),
        }
    }

    pub async fn get_stats(&self) -> MockFilesystemStats {
        let files = self.files.read().await;
        let directories = self.directories.read().await;
        let disk_usage = *self.disk_usage.read().await;
        let operation_count = *self.operation_count.read().await;

        MockFilesystemStats {
            files_count: files.len(),
            directories_count: directories.len(),
            disk_usage,
            operation_count,
            uptime: self.start_time.elapsed(),
        }
    }

    pub async fn reset(&self) {
        let mut files = self.files.write().await;
        files.clear();

        let mut directories = self.directories.write().await;
        directories.clear();

        // Recreate root directory
        directories.insert(
            PathBuf::from("/"),
            MockDirectory {
                path: PathBuf::from("/"),
                entries: Vec::new(),
                metadata: MockFileMetadata {
                    size: 0,
                    created: Instant::now(),
                    modified: Instant::now(),
                    accessed: Instant::now(),
                    permissions: 0o755,
                    is_directory: true,
                },
            },
        );

        let mut disk_usage = self.disk_usage.write().await;
        *disk_usage = 0;

        let mut operation_count = self.operation_count.write().await;
        *operation_count = 0;
    }

    async fn increment_operation_count(&self) {
        let mut count = self.operation_count.write().await;
        *count += 1;
    }

    async fn should_fail_permission(&self) -> bool {
        fastrand::f64() < self.config.permission_error_rate
    }

    async fn should_fail_disk_full(&self) -> bool {
        fastrand::f64() < self.config.disk_full_rate
    }

    async fn should_corrupt_file(&self) -> bool {
        fastrand::f64() < self.config.corruption_rate
    }

    /// Simulate disk full condition
    pub async fn simulate_disk_full(&self) {
        let mut disk_usage = self.disk_usage.write().await;
        *disk_usage = self.config.disk_capacity;
    }

    /// Simulate permission errors
    pub async fn simulate_permission_errors(&self, rate: f64) {
        // In a real implementation, this would update the config
        // For now, this is a placeholder
    }

    /// Simulate filesystem corruption
    pub async fn simulate_corruption(&self, rate: f64) {
        // In a real implementation, this would update the config
        // For now, this is a placeholder
    }
}

/// Mock filesystem errors
#[derive(Debug, Clone)]
pub enum MockFilesystemError {
    FileNotFound,
    DirectoryNotFound,
    PermissionDenied,
    DiskFull,
    FileTooLarge,
    TooManyFiles,
    FileLocked,
    FileCorrupted,
    IoError,
}

impl std::fmt::Display for MockFilesystemError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MockFilesystemError::FileNotFound => write!(f, "File not found"),
            MockFilesystemError::DirectoryNotFound => write!(f, "Directory not found"),
            MockFilesystemError::PermissionDenied => write!(f, "Permission denied"),
            MockFilesystemError::DiskFull => write!(f, "Disk full"),
            MockFilesystemError::FileTooLarge => write!(f, "File too large"),
            MockFilesystemError::TooManyFiles => write!(f, "Too many files"),
            MockFilesystemError::FileLocked => write!(f, "File locked"),
            MockFilesystemError::FileCorrupted => write!(f, "File corrupted"),
            MockFilesystemError::IoError => write!(f, "IO error"),
        }
    }
}

impl std::error::Error for MockFilesystemError {}

/// Statistics for MockFilesystem
#[derive(Debug, Clone)]
pub struct MockFilesystemStats {
    pub files_count: usize,
    pub directories_count: usize,
    pub disk_usage: usize,
    pub operation_count: usize,
    pub uptime: Duration,
}

/// Test utilities for MockFilesystem
impl MockFilesystem {
    pub fn for_testing() -> Self {
        Self::new(MockFilesystemConfig::default())
    }

    pub fn with_failures(error_rate: f64) -> Self {
        let config = MockFilesystemConfig {
            permission_error_rate: error_rate,
            disk_full_rate: error_rate,
            corruption_rate: error_rate,
            ..Default::default()
        };
        Self::new(config)
    }

    pub fn read_only() -> Self {
        let config = MockFilesystemConfig {
            read_only: true,
            ..Default::default()
        };
        Self::new(config)
    }

    pub fn with_limited_capacity(capacity: usize) -> Self {
        let config = MockFilesystemConfig {
            disk_capacity: capacity,
            ..Default::default()
        };
        Self::new(config)
    }

    pub async fn verify_file_exists(&self, path: &Path) -> bool {
        self.exists(path).await
    }

    pub async fn verify_file_content(&self, path: &Path, expected: &[u8]) -> bool {
        match self.read_file(path).await {
            Ok(content) => content == expected,
            Err(_) => false,
        }
    }

    pub async fn get_file_count(&self) -> usize {
        let files = self.files.read().await;
        files.len()
    }

    pub async fn get_directory_count(&self) -> usize {
        let directories = self.directories.read().await;
        directories.len()
    }

    pub async fn get_disk_usage(&self) -> usize {
        *self.disk_usage.read().await
    }
}
