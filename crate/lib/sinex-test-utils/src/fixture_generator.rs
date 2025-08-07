//! Static fixture dataset generation
//!
//! Generates deterministic, reproducible datasets for benchmarks and tests.
//! All datasets use fixed seeds to ensure reproducibility across runs.

use crate::prelude::*;
use crate::Result;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_db::models::*;
use sinex_db::DbPool;

use camino::Utf8Path;
use sinex_types::error::SinexError;
use std::collections::HashMap;
use std::fs;

/// Configuration for dataset generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetConfig {
    /// Dataset name (e.g., "small", "medium", "large")
    pub name: String,
    /// Number of events to generate
    pub event_count: usize,
    /// Number of different event sources
    pub source_count: usize,
    /// Number of different event types per source
    pub event_type_count: usize,
    /// Payload size distribution [min, p25, p50, p75, max]
    pub payload_sizes: [usize; 5],
    /// Time range for events (in days)
    pub time_range_days: i64,
    /// Random seed for reproducibility
    pub seed: u64,
    /// Checkpoint interval (0 for no checkpoints)
    pub checkpoint_interval: usize,
    /// Number of operations to generate
    pub operation_count: usize,
}

impl DatasetConfig {
    /// Standard small dataset for quick tests
    pub fn small() -> Self {
        Self {
            name: "small".to_string(),
            event_count: 1_000,
            source_count: 4,
            event_type_count: 3,
            payload_sizes: [50, 100, 200, 500, 1000],
            time_range_days: 1,
            seed: 42,
            checkpoint_interval: 100,
            operation_count: 10,
        }
    }

    /// Medium dataset for integration tests
    pub fn medium() -> Self {
        Self {
            name: "medium".to_string(),
            event_count: 100_000,
            source_count: 8,
            event_type_count: 5,
            payload_sizes: [100, 500, 1000, 5000, 10000],
            time_range_days: 7,
            seed: 1337,
            checkpoint_interval: 1000,
            operation_count: 100,
        }
    }

    /// Large dataset for performance benchmarks
    pub fn large() -> Self {
        Self {
            name: "large".to_string(),
            event_count: 10_000_000,
            source_count: 16,
            event_type_count: 10,
            payload_sizes: [200, 1000, 5000, 20000, 100000],
            time_range_days: 30,
            seed: 9999,
            checkpoint_interval: 10000,
            operation_count: 1000,
        }
    }
}

/// Metadata about a generated dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMetadata {
    pub config: DatasetConfig,
    pub generated_at: DateTime<Utc>,
    pub schema_version: String,
    pub checksum: String,
    pub file_size_bytes: u64,
    pub event_count: usize,
    pub checkpoint_count: usize,
    pub operation_count: usize,
}

/// Generator for creating static fixture datasets
pub struct FixtureGenerator {
    config: DatasetConfig,
    rng: fastrand::Rng,
}

impl FixtureGenerator {
    /// Create a new fixture generator with the given configuration
    pub fn new(config: DatasetConfig) -> Self {
        let rng = fastrand::Rng::with_seed(config.seed);
        Self { config, rng }
    }

    /// Generate events for the dataset
    pub fn generate_events(&mut self) -> Vec<Event> {
        let mut events = Vec::with_capacity(self.config.event_count);

        // Pre-generate sources and event types
        let sources = self.generate_sources();
        let event_types = self.generate_event_types();

        // Time distribution
        let start_time = Utc::now() - Duration::days(self.config.time_range_days);
        let time_step =
            Duration::days(self.config.time_range_days) / self.config.event_count as i32;

        for i in 0..self.config.event_count {
            let source = &sources[i % sources.len()];
            let event_type = &event_types[i % event_types.len()];
            let payload_size = self.select_payload_size();
            let timestamp = start_time + time_step * i as i32;

            let event = self.create_event(source, event_type, payload_size, timestamp, i);
            events.push(event);
        }

        events
    }

    /// Generate source names
    fn generate_sources(&self) -> Vec<String> {
        use sinex_types::*;
        let base_sources = vec![
            FileCreatedPayload::SOURCE.to_string(),
            KittyCommandExecutedPayload::SOURCE.to_string(),
            ClipboardCopiedPayload::SOURCE.to_string(),
            HyprlandWindowFocusedPayload::SOURCE.to_string(),
            SystemdUnitStartedPayload::SOURCE.to_string(),
            DbusSignalPayload::SOURCE.to_string(),
        ];

        let mut sources = Vec::with_capacity(self.config.source_count);
        for i in 0..self.config.source_count {
            if i < base_sources.len() {
                sources.push(base_sources[i].clone());
            } else {
                sources.push(format!("fixture_source_{}", i));
            }
        }
        sources
    }

    /// Generate event types
    fn generate_event_types(&self) -> Vec<String> {
        use sinex_types::*;
        let base_types = vec![
            FileCreatedPayload::EVENT_TYPE.to_string(),
            KittyCommandExecutedPayload::EVENT_TYPE.to_string(),
            ClipboardCopiedPayload::EVENT_TYPE.to_string(),
            HyprlandWindowFocusedPayload::EVENT_TYPE.to_string(),
        ];

        let mut types = Vec::with_capacity(self.config.event_type_count);
        for i in 0..self.config.event_type_count {
            if i < base_types.len() {
                types.push(base_types[i].clone());
            } else {
                types.push(format!("fixture.event_{}", i));
            }
        }
        types
    }

    /// Select a payload size based on distribution
    fn select_payload_size(&mut self) -> usize {
        let percentile = self.rng.u32(0..100);
        match percentile {
            0..=24 => self.config.payload_sizes[1],  // p25
            25..=49 => self.config.payload_sizes[2], // p50
            50..=74 => self.config.payload_sizes[3], // p75
            75..=94 => self.config.payload_sizes[4], // max
            _ => self.config.payload_sizes[0],       // min
        }
    }

    /// Create a single event
    fn create_event(
        &mut self,
        source: &str,
        event_type: &str,
        payload_size: usize,
        timestamp: DateTime<Utc>,
        index: usize,
    ) -> Event {
        use sinex_types::*;
        let mut payload = HashMap::new();

        // Add standard fields
        payload.insert("index".to_string(), json!(index));
        payload.insert("dataset".to_string(), json!(self.config.name));
        payload.insert("seed".to_string(), json!(self.config.seed));

        // Add source-specific fields
        match source {
            s if s == FileCreatedPayload::SOURCE.as_str() => {
                payload.insert(
                    "path".to_string(),
                    json!(format!("/fixture/path/{}/file_{}.txt", index / 100, index)),
                );
                payload.insert("operation".to_string(), json!("created"));
            }
            s if s == KittyCommandExecutedPayload::SOURCE.as_str() => {
                let commands = ["ls", "cd", "git", "cargo", "vim", "grep", "find"];
                payload.insert(
                    "command".to_string(),
                    json!(commands[index % commands.len()]),
                );
                payload.insert("exit_code".to_string(), json!(0));
                payload.insert("duration_ms".to_string(), json!(self.rng.u32(10..1000)));
            }
            s if s == ClipboardCopiedPayload::SOURCE.as_str() => {
                payload.insert("format".to_string(), json!("text/plain"));
                payload.insert("source_app".to_string(), json!("fixture_app"));
            }
            _ => {}
        }

        // Add padding data to reach target size
        let current_size = serde_json::to_string(&payload).unwrap().len();
        if current_size < payload_size {
            let padding_size = payload_size - current_size;
            payload.insert("data".to_string(), json!("x".repeat(padding_size)));
        }

        use sinex_types::domain::*;

        Event::builder()
            .source(EventSource::new(source))
            .event_type(EventType::new(event_type))
            .host(HostName::new("fixture_host"))
            .payload(serde_json::to_value(payload).unwrap())
            .ts_orig(Some(timestamp))
            .ingestor_version("fixture_generator/1.0.0".to_string())
            .build()
    }

    /// Generate SQL for the dataset
    pub fn generate_sql(&mut self, events: &[Event]) -> String {
        let mut sql = String::new();

        // Header
        sql.push_str(&format!("-- Sinex Fixture Dataset: {}\n", self.config.name));
        sql.push_str(&format!("-- Generated: {}\n", Utc::now()));
        sql.push_str(&format!("-- Event Count: {}\n", events.len()));
        sql.push_str(&format!("-- Seed: {}\n\n", self.config.seed));

        // Transaction
        sql.push_str("BEGIN;\n\n");

        // Events
        sql.push_str("-- Insert events\n");
        for (i, event) in events.iter().enumerate() {
            if i > 0 && i % 1000 == 0 {
                sql.push_str(&format!("\n-- Progress: {}/{}\n", i, events.len()));
            }

            sql.push_str("INSERT INTO core.events (id, source, event_type, host, payload, ts_ingest, ts_orig, ingestor_version) VALUES (\n");
            if let Some(id) = &event.id {
                sql.push_str(&format!("  '{}',\n", id.as_ulid().to_uuid()));
            } else {
                sql.push_str(&format!("  '{}',\n", Ulid::new().to_uuid()));
            }
            sql.push_str(&format!("  '{}',\n", event.source));
            sql.push_str(&format!("  '{}',\n", event.event_type));
            sql.push_str(&format!("  '{}',\n", event.host));
            sql.push_str(&format!(
                "  '{}',\n",
                serde_json::to_string(&event.payload)
                    .unwrap()
                    .replace('\'', "''")
            ));
            sql.push_str(&format!("  '{}',\n", event.ts_ingest.to_rfc3339()));
            sql.push_str(&format!("  '{}',\n", event.ts_orig.unwrap().to_rfc3339()));
            sql.push_str(&format!(
                "  '{}'\n",
                event.ingestor_version.as_ref().unwrap()
            ));
            sql.push_str(");\n");
        }

        // Checkpoints
        if self.config.checkpoint_interval > 0 {
            sql.push_str("\n-- Insert checkpoints\n");
            let checkpoint_count = events.len() / self.config.checkpoint_interval;

            for i in 0..checkpoint_count {
                let last_event_index = (i + 1) * self.config.checkpoint_interval - 1;
                let last_event_id = &events[last_event_index].id;

                sql.push_str(&format!(
                    "INSERT INTO core.processor_checkpoints (processor_name, last_processed_event_id, processed_count, state) VALUES (\n  'fixture_processor_{}', '{}', {}, '{}'::jsonb\n);\n",
                    i % 3,
                    last_event_id.as_ref().map(|id| id.to_string()).unwrap_or_else(|| "UNKNOWN".to_string()),
                    (i + 1) * self.config.checkpoint_interval,
                    json!({
                        "checkpoint": i,
                        "dataset": self.config.name,
                        "timestamp": Utc::now()
                    })
                ));
            }
        }

        // Operations
        if self.config.operation_count > 0 {
            sql.push_str("\n-- Insert operations\n");
            for i in 0..self.config.operation_count {
                let op_type = ["stage", "replay", "archive"][i % 3];
                let op_id = Ulid::new();

                sql.push_str(&format!(
                    "SELECT core.start_operation('{}', 'fixture_user', '{}'::jsonb);\n",
                    op_type,
                    json!({
                        "operation": i,
                        "dataset": self.config.name
                    })
                ));

                if i % 2 == 0 {
                    sql.push_str(&format!(
                        "SELECT core.complete_operation('{}', '{}'::jsonb);\n",
                        op_id.to_uuid(),
                        json!({"status": "success"})
                    ));
                }
            }
        }

        sql.push_str("\nCOMMIT;\n");
        sql
    }

    /// Generate JSON dataset
    pub fn generate_json(&mut self, events: &[Event]) -> serde_json::Value {
        json!({
            "metadata": {
                "dataset": self.config.name,
                "generated_at": Utc::now(),
                "event_count": events.len(),
                "seed": self.config.seed,
                "schema_version": "1.0.0"
            },
            "events": events
        })
    }

    /// Save dataset to disk
    pub async fn save_dataset(&mut self, output_dir: &Utf8Path) -> Result<DatasetMetadata> {
        // Generate events
        let events = self.generate_events();

        // Create output directory
        let dataset_dir = output_dir.join(&self.config.name);
        fs::create_dir_all(&dataset_dir)
            .map_err(|e| SinexError::io(format!("Failed to create dataset directory: {}", e)))?;

        // Save SQL file
        let sql_path = dataset_dir.join(format!("{}.sql", self.config.name));
        let sql_content = self.generate_sql(&events);
        fs::write(&sql_path, &sql_content)
            .map_err(|e| SinexError::io(format!("Failed to write SQL file: {}", e)))?;

        // Save JSON file
        let json_path = dataset_dir.join(format!("{}.json", self.config.name));
        let json_content = self.generate_json(&events);
        fs::write(&json_path, serde_json::to_string_pretty(&json_content)?)
            .map_err(|e| SinexError::io(format!("Failed to write JSON file: {}", e)))?;

        // Calculate checksum
        let checksum = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&sql_content);
            format!("{:x}", hasher.finalize())
        };

        // Create metadata
        let metadata = DatasetMetadata {
            config: self.config.clone(),
            generated_at: Utc::now(),
            schema_version: "1.0.0".to_string(),
            checksum,
            file_size_bytes: sql_content.len() as u64,
            event_count: events.len(),
            checkpoint_count: events.len() / self.config.checkpoint_interval.max(1),
            operation_count: self.config.operation_count,
        };

        // Save metadata
        let metadata_path = dataset_dir.join(format!("{}.metadata.json", self.config.name));
        fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?)?;

        Ok(metadata)
    }
}

/// Load a dataset from disk
pub async fn load_dataset(pool: &DbPool, dataset_path: &Utf8Path) -> crate::Result<()> {
    let sql_content = fs::read_to_string(dataset_path)
        .map_err(|e| SinexError::io(format!("Failed to read dataset: {}", e)))?;

    // Execute SQL in a transaction
    let mut tx = pool.begin().await?;
    sqlx::query(&sql_content).execute(&mut *tx).await?;
    tx.commit().await?;

    Ok(())
}

/// Verify dataset integrity
pub async fn verify_dataset(pool: &DbPool, metadata_path: &Utf8Path) -> Result<bool> {
    let metadata: DatasetMetadata = serde_json::from_str(&fs::read_to_string(metadata_path)?)?;

    // Check event count
    use sinex_db::repositories::*;
    let event_count = pool.events().count_all().await?;

    if event_count != metadata.event_count as i64 {
        return Ok(false);
    }

    // Check checkpoint count
    let checkpoint_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.processor_checkpoints")
            .fetch_one(pool)
            .await?;

    if checkpoint_count != metadata.checkpoint_count as i64 {
        return Ok(false);
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sinex_test]
    fn test_dataset_configs() {
        let small = DatasetConfig::small();
        assert_eq!(small.event_count, 1_000);
        assert_eq!(small.seed, 42);

        let medium = DatasetConfig::medium();
        assert_eq!(medium.event_count, 100_000);

        let large = DatasetConfig::large();
        assert_eq!(large.event_count, 10_000_000);
    }

    #[sinex_test]
    fn test_deterministic_generation() {
        let config = DatasetConfig::small();

        // Generate twice with same seed
        let mut gen1 = FixtureGenerator::new(config.clone());
        let events1 = gen1.generate_events();

        let mut gen2 = FixtureGenerator::new(config);
        let events2 = gen2.generate_events();

        // Should produce identical events
        assert_eq!(events1.len(), events2.len());
        for (e1, e2) in events1.iter().zip(events2.iter()) {
            assert_eq!(e1.source, e2.source);
            assert_eq!(e1.event_type, e2.event_type);
            assert_eq!(e1.payload, e2.payload);
        }
    }
}

#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use crate::sinex_bench;

    #[sinex_bench]
    fn bench_generate_small_dataset() -> color_eyre::eyre::Result<()> {
        let mut gen = FixtureGenerator::new(DatasetConfig::small());
        let events = gen.generate_events();
        divan::black_box(events);
        Ok(())
    }

    #[sinex_bench]
    fn bench_generate_sql() -> color_eyre::eyre::Result<()> {
        let mut gen = FixtureGenerator::new(DatasetConfig::small());
        let events = gen.generate_events();
        let sql = gen.generate_sql(&events);
        divan::black_box(sql);
        Ok(())
    }
}
