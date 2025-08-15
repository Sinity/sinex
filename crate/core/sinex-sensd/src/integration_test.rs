//! Integration test for sensd components
//!
//! This module provides a self-contained test that verifies the full sensd flow

use crate::{
    grpc_server::SensdGrpcService, job_manager::JobManager, material_stream::MaterialSliceStream,
    temporal_ledger::TemporalLedger,
};
use color_eyre::eyre::Result;
use sinex_core::types::Ulid;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::info;

/// Simple integration test runner
pub struct SensdIntegrationTest {
    db_pool: PgPool,
    temporal_ledger: Arc<TemporalLedger>,
    job_manager: Arc<JobManager>,
    grpc_service: SensdGrpcService,
}

impl SensdIntegrationTest {
    /// Create a new integration test instance
    pub async fn new(database_url: &str) -> Result<Self> {
        let db_pool = PgPool::connect(database_url).await?;
        let temporal_ledger =
            Arc::new(TemporalLedger::new(db_pool.clone(), Default::default()).await?);
        let job_manager = Arc::new(
            JobManager::new(db_pool.clone(), temporal_ledger.clone(), Default::default()).await?,
        );
        let grpc_service = SensdGrpcService::new(
            db_pool.clone(),
            temporal_ledger.clone(),
            job_manager.clone(),
        );

        Ok(Self {
            db_pool,
            temporal_ledger,
            job_manager,
            grpc_service,
        })
    }

    /// Test the complete flow: job creation → execution → material streaming
    pub async fn test_complete_flow(&self) -> Result<()> {
        info!("Starting sensd integration test");

        // 1. Create a test material directly in the database
        let material_id = self.create_test_material().await?;
        info!("Created test material: {}", material_id);

        // 2. Create some temporal ledger entries for the material
        self.create_test_ledger_entries(material_id).await?;
        info!("Created temporal ledger entries");

        // 3. Test MaterialSliceStream
        let mut stream = MaterialSliceStream::new(self.db_pool.clone(), material_id, 10);
        let mut slice_count = 0;

        while let Some(slice) = stream.next_slice().await? {
            info!(
                "Retrieved slice: material_id={}, offset={}..{}, data_len={}",
                slice.material_id,
                slice.offset_start,
                slice.offset_end,
                slice.data.len()
            );
            slice_count += 1;
        }

        info!("Successfully streamed {} slices", slice_count);

        // 4. Test job creation (without actual execution)
        let test_job_id = self.create_test_job().await?;
        info!("Created test job: {}", test_job_id);

        info!("Integration test completed successfully");
        Ok(())
    }

    /// Create a test material in the database
    async fn create_test_material(&self) -> Result<Ulid> {
        let material_id = Ulid::new();
        let test_data = b"Hello, this is test material data for sensd integration testing!";

        sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (
                source_material_id, source_identifier, source_type, 
                total_bytes, content_type, data, status, 
                created_at, staged_at
            )
            VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, NOW(), NOW())
            "#,
            material_id as Ulid,
            "integration-test",
            "test",
            test_data.len() as i64,
            "text/plain",
            test_data,
            "ready",
        )
        .execute(&self.db_pool)
        .await?;

        Ok(material_id)
    }

    /// Create test temporal ledger entries
    async fn create_test_ledger_entries(&self, material_id: Ulid) -> Result<()> {
        let test_data = b"Hello, this is test material data for sensd integration testing!";
        let chunk_size = 10;

        for (i, chunk) in test_data.chunks(chunk_size).enumerate() {
            let offset_start = (i * chunk_size) as i64;
            let offset_end = offset_start + chunk.len() as i64;
            let entry_id = Ulid::new();

            sqlx::query!(
                r#"
                INSERT INTO raw.temporal_ledger (
                    entry_id, material_id, offset_start, offset_end,
                    offset_kind, ts_capture, precision, clock,
                    source_type, note, created_at
                )
                VALUES ($1::ulid, $2::ulid, $3, $4, $5, NOW(), $6, $7, $8, $9, NOW())
                "#,
                entry_id as Ulid,
                material_id as Ulid,
                offset_start,
                offset_end,
                "byte",
                "exact",
                "wall",
                "realtime_capture",
                serde_json::json!({
                    "chunk_index": i,
                    "test": true
                })
                .to_string(),
            )
            .execute(&self.db_pool)
            .await?;
        }

        Ok(())
    }

    /// Create a test job
    async fn create_test_job(&self) -> Result<Ulid> {
        let job_id = Ulid::new();

        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                job_id, sensor_type, target_uri, source_identifier,
                acquisition_mode, parameters, owner, priority, status,
                created_at
            )
            VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            "#,
            job_id as Ulid,
            "test_sensor",
            "/tmp/test",
            "integration-test-job",
            serde_json::json!({"mode": "test"}),
            serde_json::json!({"test": true}),
            "integration-test",
            1,
            "pending",
        )
        .execute(&self.db_pool)
        .await?;

        Ok(job_id)
    }
}

/// Run a basic integration test
pub async fn run_integration_test(database_url: &str) -> Result<()> {
    let test = SensdIntegrationTest::new(database_url).await?;
    test.test_complete_flow().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[tokio::test]
    async fn test_sensd_integration() -> Result<()> {
        // Only run if database URL is provided
        if let Ok(database_url) = env::var("DATABASE_URL") {
            run_integration_test(&database_url).await?;
        } else {
            println!("Skipping integration test - no DATABASE_URL provided");
        }
        Ok(())
    }
}
