//! Static fixture management with automatic generation
//!
//! This module provides infrastructure for declarative fixture management where fixtures
//! are automatically generated on first use and cached for subsequent runs.
//!
//! # Design Philosophy
//!
//! Instead of requiring manual fixture generation via CLI tools, fixtures are:
//! - Declared in code with their specifications
//! - Generated automatically on first use
//! - Cached in a versioned format
//! - Verified via checksums
//! - Regenerated when specifications change
//!
//! # Usage Pattern
//!
//! Each crate should declare its own domain-specific fixtures using this infrastructure:
//!
//! ```rust
//! // In sinex-db/src/bench_fixtures.rs
//! use sinex_test_utils::static_fixtures::{FixtureSet, DatasetSize};
//!
//! /// Database-specific benchmark fixtures
//! pub static DB_BENCH_FIXTURES: FixtureSet = FixtureSet::new()
//!     .with_events(DatasetSize::Small, 42)
//!     .with_events(DatasetSize::Medium, 1337)
//!     .with_checkpoints(100);
//!
//! // In sinex-db benchmarks
//! #[cfg(all(test, feature = "bench"))]
//! mod benches {
//!     use super::*;
//!     use crate::bench_fixtures::DB_BENCH_FIXTURES;
//!     
//!     bench_with_db!(bench_query_performance, |ctx: &BenchContext| async move {
//!         ctx.ensure_fixture(&DB_BENCH_FIXTURES, DatasetSize::Medium).await?;
//!         // Run benchmark...
//!     });
//! }
//! ```
//!
//! This keeps fixture definitions close to where they're used while sharing
//! the generation infrastructure.

use crate::fixture_generator::{DatasetConfig, FixtureGenerator};
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use sinex_core_types::{DbPool, SinexError};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Standard dataset sizes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DatasetSize {
    /// Empty dataset (clean database)
    Empty,
    /// Small dataset - 1K events for quick tests
    Small,
    /// Medium dataset - 100K events for integration tests  
    Medium,
    /// Large dataset - 10M events for performance benchmarks
    Large,
    /// Custom size
    Custom(usize),
}

impl DatasetSize {
    pub fn event_count(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Small => 1_000,
            Self::Medium => 100_000,
            Self::Large => 10_000_000,
            Self::Custom(n) => *n,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Empty => "empty".to_string(),
            Self::Small => "small".to_string(),
            Self::Medium => "medium".to_string(),
            Self::Large => "large".to_string(),
            Self::Custom(n) => format!("custom_{}", n),
        }
    }
}

/// Declarative fixture set specification
///
/// Create domain-specific fixtures in your crate:
/// ```rust
/// pub static MY_FIXTURES: FixtureSet = FixtureSet::new()
///     .with_events(DatasetSize::Small, 42)
///     .with_checkpoints(10);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureSet {
    /// Event datasets to generate
    pub events: HashMap<DatasetSize, u64>, // size -> seed
    /// Number of checkpoints to include
    pub checkpoints: usize,
    /// Number of operations to include
    pub operations: usize,
    /// Additional configuration
    pub config: FixtureConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureConfig {
    /// Base directory for fixture storage
    pub base_dir: PathBuf,
    /// Schema version for migration handling
    pub schema_version: String,
    /// Whether to verify checksums on load
    pub verify_checksums: bool,
    /// Maximum age before regeneration (in days)
    pub max_age_days: Option<i64>,
}

impl Default for FixtureConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("target/fixtures"),
            schema_version: "1.0.0".to_string(),
            verify_checksums: true,
            max_age_days: Some(30),
        }
    }
}

impl FixtureSet {
    /// Create a new empty fixture set
    pub fn new() -> Self {
        Self {
            events: HashMap::new(),
            checkpoints: 0,
            operations: 0,
            config: FixtureConfig {
                base_dir: PathBuf::from("target/bench-fixtures"),
                schema_version: "1.0".to_string(),
                verify_checksums: true,
                max_age_days: None,
            },
        }
    }

    /// Add an event dataset with specific size and seed
    pub fn with_events(mut self, size: DatasetSize, seed: u64) -> Self {
        self.events.insert(size, seed);
        self
    }

    /// Set number of checkpoints
    pub fn with_checkpoints(mut self, count: usize) -> Self {
        self.checkpoints = count;
        self
    }

    /// Set number of operations
    pub fn with_operations(mut self, count: usize) -> Self {
        self.operations = count;
        self
    }

    /// Use custom configuration
    pub fn with_config(mut self, config: FixtureConfig) -> Self {
        self.config = config;
        self
    }
}

/// Fixture manifest tracking what's been generated
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureManifest {
    /// When this fixture was generated
    pub generated_at: DateTime<Utc>,
    /// Configuration used to generate
    pub config: DatasetConfig,
    /// Schema version at generation time
    pub schema_version: String,
    /// SHA256 checksum of the generated SQL
    pub checksum: String,
    /// File paths relative to base directory
    pub files: FixtureFiles,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureFiles {
    pub sql: PathBuf,
    pub json: PathBuf,
    pub manifest: PathBuf,
}

/// Global fixture manager for automatic generation and caching
static FIXTURE_MANAGER: Lazy<Arc<Mutex<FixtureManager>>> =
    Lazy::new(|| Arc::new(Mutex::new(FixtureManager::new())));

struct FixtureManager {
    /// Cached manifests by fixture ID
    manifests: HashMap<String, FixtureManifest>,
    /// Base directory for fixtures
    base_dir: PathBuf,
}

impl FixtureManager {
    fn new() -> Self {
        Self {
            manifests: HashMap::new(),
            base_dir: PathBuf::from("target/fixtures"),
        }
    }

    /// Get or generate a fixture
    async fn ensure_fixture(
        &mut self,
        fixture_set: &FixtureSet,
        size: DatasetSize,
    ) -> Result<PathBuf, SinexError> {
        let fixture_id = self.fixture_id(fixture_set, size);
        let fixture_dir = self.base_dir.join(&fixture_id);

        // Check if we have a valid cached fixture
        if let Some(manifest) = self
            .check_cached_fixture(&fixture_id, &fixture_set.config)
            .await?
        {
            return Ok(fixture_dir.join(&manifest.files.sql));
        }

        // Generate new fixture
        let manifest = self.generate_fixture(fixture_set, size).await?;
        self.manifests.insert(fixture_id, manifest.clone());

        Ok(fixture_dir.join(&manifest.files.sql))
    }

    /// Generate a unique fixture ID
    fn fixture_id(&self, fixture_set: &FixtureSet, size: DatasetSize) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();

        // Hash the fixture specification
        hasher.update(size.name().as_bytes());
        hasher.update(fixture_set.config.schema_version.as_bytes());
        if let Some(&seed) = fixture_set.events.get(&size) {
            hasher.update(seed.to_le_bytes());
        }
        hasher.update(fixture_set.checkpoints.to_le_bytes());
        hasher.update(fixture_set.operations.to_le_bytes());

        format!(
            "{}_v{}_{:x}",
            size.name(),
            fixture_set.config.schema_version,
            hasher.finalize()
        )
    }

    /// Check if we have a valid cached fixture
    async fn check_cached_fixture(
        &mut self,
        fixture_id: &str,
        config: &FixtureConfig,
    ) -> Result<Option<FixtureManifest>, SinexError> {
        let fixture_dir = self.base_dir.join(fixture_id);
        let manifest_path = fixture_dir.join("manifest.json");

        if !manifest_path.exists() {
            return Ok(None);
        }

        // Load manifest
        let manifest_data = tokio::fs::read_to_string(&manifest_path).await?;
        let manifest: FixtureManifest = serde_json::from_str(&manifest_data)?;

        // Check schema version
        if manifest.schema_version != config.schema_version {
            return Ok(None); // Need regeneration for new schema
        }

        // Check age if configured
        if let Some(max_age_days) = config.max_age_days {
            let age = Utc::now() - manifest.generated_at;
            if age.num_days() > max_age_days {
                return Ok(None); // Too old, regenerate
            }
        }

        // Verify checksum if configured
        if config.verify_checksums {
            let sql_path = fixture_dir.join(&manifest.files.sql);
            let sql_content = tokio::fs::read_to_string(&sql_path).await?;

            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&sql_content);
            let checksum = format!("{:x}", hasher.finalize());

            if checksum != manifest.checksum {
                return Ok(None); // Checksum mismatch, regenerate
            }
        }

        self.manifests
            .insert(fixture_id.to_string(), manifest.clone());
        Ok(Some(manifest))
    }

    /// Generate a new fixture
    async fn generate_fixture(
        &self,
        fixture_set: &FixtureSet,
        size: DatasetSize,
    ) -> Result<FixtureManifest, SinexError> {
        let fixture_id = self.fixture_id(fixture_set, size);
        let fixture_dir = self.base_dir.join(&fixture_id);

        // Create directory
        tokio::fs::create_dir_all(&fixture_dir).await?;

        // Create dataset config
        let seed = fixture_set.events.get(&size).copied().unwrap_or(42);
        let config = match size {
            DatasetSize::Empty => DatasetConfig {
                name: "empty".to_string(),
                event_count: 0,
                source_count: 0,
                event_type_count: 0,
                payload_sizes: [0; 5],
                time_range_days: 0,
                seed,
                checkpoint_interval: 0,
                operation_count: 0,
            },
            DatasetSize::Small => DatasetConfig::small(),
            DatasetSize::Medium => DatasetConfig::medium(),
            DatasetSize::Large => DatasetConfig::large(),
            DatasetSize::Custom(n) => DatasetConfig {
                name: format!("custom_{}", n),
                event_count: n,
                source_count: 8,
                event_type_count: 5,
                payload_sizes: [100, 500, 1000, 5000, 10000],
                time_range_days: 7,
                seed,
                checkpoint_interval: fixture_set.checkpoints,
                operation_count: fixture_set.operations,
            },
        };

        // Generate fixture
        let mut generator = FixtureGenerator::new(config.clone());
        let metadata = generator.save_dataset(&fixture_dir).await?;

        // Create manifest
        let manifest = FixtureManifest {
            generated_at: metadata.generated_at,
            config,
            schema_version: fixture_set.config.schema_version.clone(),
            checksum: metadata.checksum,
            files: FixtureFiles {
                sql: PathBuf::from(format!("{}.sql", size.name())),
                json: PathBuf::from(format!("{}.json", size.name())),
                manifest: PathBuf::from("manifest.json"),
            },
        };

        // Save manifest
        let manifest_path = fixture_dir.join("manifest.json");
        let manifest_data = serde_json::to_string_pretty(&manifest)?;
        tokio::fs::write(&manifest_path, &manifest_data).await?;

        Ok(manifest)
    }
}

/// Ensure a fixture is available and load it into the database
pub async fn ensure_fixture(
    pool: &DbPool,
    fixture_set: &FixtureSet,
    size: DatasetSize,
) -> Result<(), SinexError> {
    let manager = FIXTURE_MANAGER.clone();
    let sql_path = manager
        .lock()
        .await
        .ensure_fixture(fixture_set, size)
        .await?;

    // Load into database
    crate::fixture_generator::load_dataset(pool, &sql_path).await?;

    Ok(())
}

// For standard fixtures that are useful across crates, see the standard_fixtures module

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;
    use anyhow::Result;

    #[sinex_test]
    fn test_fixture_set_builder() -> Result<()> {
        let fixtures = FixtureSet::new()
            .with_events(DatasetSize::Small, 42)
            .with_events(DatasetSize::Medium, 1337)
            .with_checkpoints(50)
            .with_operations(25);

        assert_eq!(fixtures.events.len(), 2);
        assert_eq!(fixtures.events[&DatasetSize::Small], 42);
        assert_eq!(fixtures.checkpoints, 50);
        assert_eq!(fixtures.operations, 25);
        Ok(())
    }

    #[sinex_test]
    fn test_dataset_size_values() -> Result<()> {
        assert_eq!(DatasetSize::Empty.event_count(), 0);
        assert_eq!(DatasetSize::Small.event_count(), 1_000);
        assert_eq!(DatasetSize::Medium.event_count(), 100_000);
        assert_eq!(DatasetSize::Large.event_count(), 10_000_000);
        assert_eq!(DatasetSize::Custom(555).event_count(), 555);
        Ok(())
    }
}

#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use crate::sinex_bench;

    #[sinex_bench]
    async fn bench_fixture_id_generation() -> anyhow::Result<()> {
        let manager = FixtureManager::new();
        let fixtures = FixtureSet::new()
            .with_events(DatasetSize::Medium, 1337)
            .with_checkpoints(100);

        let id = manager.fixture_id(&fixtures, DatasetSize::Medium);
        divan::black_box(id);
        Ok(())
    }
}
