use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::process::Command as AsyncCommand;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Configuration for a single Git-annex storage location
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageLocation {
    pub id: String,
    pub description: String,
    pub remote_name: String,
    pub url: String,
    pub priority: u8, // 1-10, higher = more preferred
    pub max_capacity_gb: Option<u64>,
    pub cost: u8, // Git-annex cost (higher = more expensive)
    pub enabled: bool,
    pub auto_sync: bool,
}

/// Current status of a storage location
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationStatus {
    pub location_id: String,
    pub is_available: bool,
    pub last_seen: SystemTime,
    pub last_sync: Option<SystemTime>,
    pub disk_usage_gb: Option<f64>,
    pub file_count: Option<u64>,
    pub sync_errors: Vec<SyncError>,
    pub health_score: f32, // 0.0-1.0
}

/// Sync error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncError {
    pub timestamp: SystemTime,
    pub error_type: SyncErrorType,
    pub message: String,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncErrorType {
    NetworkTimeout,
    AuthenticationFailure,
    DiskFull,
    CorruptedData,
    RemoteUnavailable,
    Other,
}

/// Multi-location synchronization coordinator
pub struct MultiLocationCoordinator {
    repo_path: PathBuf,
    locations: HashMap<String, StorageLocation>,
    location_status: HashMap<String, LocationStatus>,
    sync_interval: Duration,
    health_check_interval: Duration,
    max_retry_attempts: u32,
}

impl MultiLocationCoordinator {
    pub fn new(repo_path: PathBuf) -> Self {
        Self {
            repo_path,
            locations: HashMap::new(),
            location_status: HashMap::new(),
            sync_interval: Duration::from_secs(300), // 5 minutes
            health_check_interval: Duration::from_secs(60), // 1 minute
            max_retry_attempts: 3,
        }
    }

    /// Add a storage location to the coordinator
    pub async fn add_location(&mut self, location: StorageLocation) -> Result<()> {
        info!("Adding storage location: {} ({})", location.id, location.description);

        // Add remote to git-annex if it doesn't exist
        self.ensure_remote_exists(&location).await?;

        // Initialize location status
        let status = LocationStatus {
            location_id: location.id.clone(),
            is_available: false,
            last_seen: SystemTime::now(),
            last_sync: None,
            disk_usage_gb: None,
            file_count: None,
            sync_errors: Vec::new(),
            health_score: 0.0,
        };

        self.location_status.insert(location.id.clone(), status);
        self.locations.insert(location.id.clone(), location);

        Ok(())
    }

    /// Remove a storage location
    pub async fn remove_location(&mut self, location_id: &str) -> Result<()> {
        info!("Removing storage location: {}", location_id);

        if let Some(location) = self.locations.get(location_id) {
            // Remove git remote
            let output = AsyncCommand::new("git")
                .arg("remote")
                .arg("remove")
                .arg(&location.remote_name)
                .current_dir(&self.repo_path)
                .output()
                .await
                .context("Failed to remove git remote")?;

            if !output.status.success() {
                warn!("Failed to remove git remote {}: {}", 
                      location.remote_name, 
                      String::from_utf8_lossy(&output.stderr));
            }
        }

        self.locations.remove(location_id);
        self.location_status.remove(location_id);

        Ok(())
    }

    /// Start the multi-location sync daemon
    pub async fn start_sync_daemon(&mut self) -> Result<()> {
        info!("Starting multi-location sync daemon");

        let mut sync_timer = interval(self.sync_interval);
        let mut health_timer = interval(self.health_check_interval);

        loop {
            tokio::select! {
                _ = sync_timer.tick() => {
                    if let Err(e) = self.sync_all_locations().await {
                        error!("Sync failed: {}", e);
                    }
                }
                _ = health_timer.tick() => {
                    if let Err(e) = self.check_all_health().await {
                        error!("Health check failed: {}", e);
                    }
                }
            }
        }
    }

    /// Sync all enabled locations
    pub async fn sync_all_locations(&mut self) -> Result<()> {
        debug!("Starting sync cycle for all locations");

        let location_ids: Vec<String> = self.locations
            .iter()
            .filter(|(_, loc)| loc.enabled && loc.auto_sync)
            .map(|(id, _)| id.clone())
            .collect();

        for location_id in location_ids {
            if let Err(e) = self.sync_location(&location_id).await {
                error!("Failed to sync location {}: {}", location_id, e);
                self.record_sync_error(&location_id, SyncErrorType::Other, &e.to_string()).await;
            }
        }

        Ok(())
    }

    /// Sync a specific location
    pub async fn sync_location(&mut self, location_id: &str) -> Result<()> {
        let location = self.locations.get(location_id)
            .context("Location not found")?
            .clone();

        debug!("Syncing location: {} ({})", location_id, location.description);

        // Check if location is available
        if !self.is_location_available(&location).await? {
            warn!("Location {} is not available for sync", location_id);
            return Ok(());
        }

        // Perform bidirectional sync
        self.push_to_location(&location).await?;
        self.pull_from_location(&location).await?;

        // Update sync timestamp
        if let Some(status) = self.location_status.get_mut(location_id) {
            status.last_sync = Some(SystemTime::now());
            status.is_available = true;
            status.last_seen = SystemTime::now();
        }

        info!("Successfully synced location: {}", location_id);
        Ok(())
    }

    /// Push local changes to a remote location
    async fn push_to_location(&self, location: &StorageLocation) -> Result<()> {
        debug!("Pushing to location: {}", location.id);

        // Git push
        let output = AsyncCommand::new("git")
            .arg("push")
            .arg(&location.remote_name)
            .arg("--all")
            .current_dir(&self.repo_path)
            .output()
            .await
            .context("Failed to git push")?;

        if !output.status.success() {
            anyhow::bail!("Git push failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        // Git-annex copy
        let output = AsyncCommand::new("git-annex")
            .arg("copy")
            .arg("--to")
            .arg(&location.remote_name)
            .arg("--auto")
            .current_dir(&self.repo_path)
            .output()
            .await
            .context("Failed to git-annex copy")?;

        if !output.status.success() {
            anyhow::bail!("Git-annex copy failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        Ok(())
    }

    /// Pull changes from a remote location
    async fn pull_from_location(&self, location: &StorageLocation) -> Result<()> {
        debug!("Pulling from location: {}", location.id);

        // Git fetch
        let output = AsyncCommand::new("git")
            .arg("fetch")
            .arg(&location.remote_name)
            .current_dir(&self.repo_path)
            .output()
            .await
            .context("Failed to git fetch")?;

        if !output.status.success() {
            anyhow::bail!("Git fetch failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        // Git-annex get (selective based on policies)
        let output = AsyncCommand::new("git-annex")
            .arg("get")
            .arg("--from")
            .arg(&location.remote_name)
            .arg("--auto")
            .current_dir(&self.repo_path)
            .output()
            .await
            .context("Failed to git-annex get")?;

        if !output.status.success() {
            anyhow::bail!("Git-annex get failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        Ok(())
    }

    /// Check health of all locations
    async fn check_all_health(&mut self) -> Result<()> {
        debug!("Checking health of all locations");

        let location_ids: Vec<String> = self.locations.keys().cloned().collect();

        for location_id in location_ids {
            if let Err(e) = self.check_location_health(&location_id).await {
                error!("Health check failed for location {}: {}", location_id, e);
            }
        }

        Ok(())
    }

    /// Check health of a specific location
    async fn check_location_health(&mut self, location_id: &str) -> Result<()> {
        let location = self.locations.get(location_id)
            .context("Location not found")?
            .clone();

        debug!("Checking health of location: {}", location_id);

        let is_available = self.is_location_available(&location).await?;
        let disk_usage = if is_available {
            self.get_location_disk_usage(&location).await.ok()
        } else {
            None
        };

        let file_count = if is_available {
            self.get_location_file_count(&location).await.ok()
        } else {
            None
        };

        // Calculate health score
        let health_score = self.calculate_health_score(&location, is_available, &disk_usage).await;

        // Update status
        if let Some(status) = self.location_status.get_mut(location_id) {
            status.is_available = is_available;
            status.disk_usage_gb = disk_usage;
            status.file_count = file_count;
            status.health_score = health_score;
            
            if is_available {
                status.last_seen = SystemTime::now();
            }
        }

        Ok(())
    }

    /// Check if a location is available
    async fn is_location_available(&self, location: &StorageLocation) -> Result<bool> {
        // Simple availability check using git ls-remote
        let output = AsyncCommand::new("git")
            .arg("ls-remote")
            .arg("--heads")
            .arg(&location.url)
            .current_dir(&self.repo_path)
            .output()
            .await;

        match output {
            Ok(result) => Ok(result.status.success()),
            Err(_) => Ok(false),
        }
    }

    /// Get disk usage for a location (if supported)
    async fn get_location_disk_usage(&self, location: &StorageLocation) -> Result<f64> {
        // This would need to be implemented based on the remote type
        // For now, return a placeholder
        debug!("Getting disk usage for location: {}", location.id);
        
        // Try to get info from git-annex
        let output = AsyncCommand::new("git-annex")
            .arg("info")
            .arg(&location.remote_name)
            .current_dir(&self.repo_path)
            .output()
            .await
            .context("Failed to get git-annex info")?;

        if output.status.success() {
            // Parse output for size information (simplified)
            let _info = String::from_utf8_lossy(&output.stdout);
            // This would need proper parsing based on git-annex info output format
            Ok(0.0) // Placeholder
        } else {
            Ok(0.0)
        }
    }

    /// Get file count for a location
    async fn get_location_file_count(&self, location: &StorageLocation) -> Result<u64> {
        // Get file count using git-annex find
        let output = AsyncCommand::new("git-annex")
            .arg("find")
            .arg("--in")
            .arg(&location.remote_name)
            .current_dir(&self.repo_path)
            .output()
            .await
            .context("Failed to count files")?;

        if output.status.success() {
            let file_list = String::from_utf8_lossy(&output.stdout);
            let count = file_list.lines().count() as u64;
            Ok(count)
        } else {
            Ok(0)
        }
    }

    /// Calculate health score for a location
    async fn calculate_health_score(&self, location: &StorageLocation, is_available: bool, disk_usage: &Option<f64>) -> f32 {
        if !is_available {
            return 0.0;
        }

        let mut score = 1.0f32;

        // Factor in disk usage if available
        if let (Some(usage), Some(max_capacity)) = (disk_usage, location.max_capacity_gb) {
            let usage_ratio = usage / (max_capacity as f64);
            if usage_ratio > 0.9 {
                score *= 0.3; // Heavily penalize near-full disks
            } else if usage_ratio > 0.8 {
                score *= 0.7;
            } else if usage_ratio > 0.6 {
                score *= 0.9;
            }
        }

        // Factor in recent sync errors
        if let Some(status) = self.location_status.get(&location.id) {
            let recent_errors = status.sync_errors.iter()
                .filter(|e| e.timestamp.elapsed().unwrap_or(Duration::MAX) < Duration::from_secs(3600))
                .count();
            
            if recent_errors > 0 {
                score *= 0.5; // Penalize recent errors
            }
        }

        // Factor in priority (higher priority = better score)
        score *= (location.priority as f32) / 10.0;

        score.clamp(0.0, 1.0)
    }

    /// Record a sync error
    async fn record_sync_error(&mut self, location_id: &str, error_type: SyncErrorType, message: &str) {
        if let Some(status) = self.location_status.get_mut(location_id) {
            let error = SyncError {
                timestamp: SystemTime::now(),
                error_type,
                message: message.to_string(),
                retry_count: 0,
            };
            
            status.sync_errors.push(error);
            
            // Keep only recent errors (last 24 hours)
            status.sync_errors.retain(|e| {
                e.timestamp.elapsed().unwrap_or(Duration::MAX) < Duration::from_secs(86400)
            });
        }
    }

    /// Ensure a git remote exists for the location
    async fn ensure_remote_exists(&self, location: &StorageLocation) -> Result<()> {
        // Check if remote already exists
        let output = AsyncCommand::new("git")
            .arg("remote")
            .arg("get-url")
            .arg(&location.remote_name)
            .current_dir(&self.repo_path)
            .output()
            .await;

        if output.is_ok() && output.unwrap().status.success() {
            debug!("Remote {} already exists", location.remote_name);
            return Ok(());
        }

        // Add the remote
        let output = AsyncCommand::new("git")
            .arg("remote")
            .arg("add")
            .arg(&location.remote_name)
            .arg(&location.url)
            .current_dir(&self.repo_path)
            .output()
            .await
            .context("Failed to add git remote")?;

        if !output.status.success() {
            anyhow::bail!("Failed to add remote: {}", String::from_utf8_lossy(&output.stderr));
        }

        // Set git-annex cost
        let output = AsyncCommand::new("git")
            .arg("config")
            .arg(&format!("remote.{}.annex-cost", location.remote_name))
            .arg(&location.cost.to_string())
            .current_dir(&self.repo_path)
            .output()
            .await
            .context("Failed to set annex cost")?;

        if !output.status.success() {
            warn!("Failed to set annex cost for {}: {}", 
                  location.remote_name, 
                  String::from_utf8_lossy(&output.stderr));
        }

        info!("Added git remote: {} -> {}", location.remote_name, location.url);
        Ok(())
    }

    /// Get status of all locations
    pub fn get_all_status(&self) -> Vec<LocationStatus> {
        self.location_status.values().cloned().collect()
    }

    /// Get status of a specific location
    pub fn get_location_status(&self, location_id: &str) -> Option<&LocationStatus> {
        self.location_status.get(location_id)
    }

    /// Force sync of a specific location
    pub async fn force_sync(&mut self, location_id: &str) -> Result<()> {
        info!("Force syncing location: {}", location_id);
        self.sync_location(location_id).await
    }

    /// Get the best available location for storing new content
    pub fn get_best_location_for_storage(&self) -> Option<&StorageLocation> {
        self.locations
            .values()
            .filter(|loc| loc.enabled)
            .filter(|loc| {
                self.location_status
                    .get(&loc.id)
                    .map(|status| status.is_available && status.health_score > 0.5)
                    .unwrap_or(false)
            })
            .max_by(|a, b| {
                let score_a = self.location_status.get(&a.id).map(|s| s.health_score).unwrap_or(0.0);
                let score_b = self.location_status.get(&b.id).map(|s| s.health_score).unwrap_or(0.0);
                score_a.partial_cmp(&score_b).unwrap_or(std::cmp::Ordering::Equal)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_multi_location_coordinator_creation() {
        let temp_dir = TempDir::new().unwrap();
        let coordinator = MultiLocationCoordinator::new(temp_dir.path().to_path_buf());
        
        assert_eq!(coordinator.locations.len(), 0);
        assert_eq!(coordinator.location_status.len(), 0);
    }

    #[test]
    fn test_health_score_calculation() {
        let temp_dir = TempDir::new().unwrap();
        let coordinator = MultiLocationCoordinator::new(temp_dir.path().to_path_buf());
        
        let location = StorageLocation {
            id: "test".to_string(),
            description: "Test location".to_string(),
            remote_name: "test-remote".to_string(),
            url: "https://example.com/repo.git".to_string(),
            priority: 8,
            max_capacity_gb: Some(100),
            cost: 100,
            enabled: true,
            auto_sync: true,
        };

        // Test with available location and reasonable disk usage
        let score = tokio_test::block_on(coordinator.calculate_health_score(&location, true, &Some(50.0)));
        assert!(score > 0.5); // Should be good score

        // Test with unavailable location
        let score = tokio_test::block_on(coordinator.calculate_health_score(&location, false, &None));
        assert_eq!(score, 0.0);
    }
}