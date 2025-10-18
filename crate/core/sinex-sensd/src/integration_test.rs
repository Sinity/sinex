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
    /// Create a new integration test instance from an existing pool
    pub async fn with_pool(db_pool: PgPool) -> Result<Self> {
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

    /// Create a new integration test instance (connects using the provided URL)
    pub async fn new(database_url: &str) -> Result<Self> {
        let db_pool = PgPool::connect(database_url).await?;
        Self::with_pool(db_pool).await
    }

    /// Test the complete flow: job creation → execution → material streaming
    pub async fn test_complete_flow(&self) -> Result<()> {
        info!("Starting sensd integration test");

        let test_data = b"Hello, this is test material data for sensd integration testing!";

        // Prepare a temporary annex root so the blob reader can find the data
        let annex_dir = tempfile::tempdir()?;
        std::env::set_var("SINEX_ANNEX_PATH", annex_dir.path());

        // 1. Create a test material directly in the database
        let material_id = self
            .create_test_material(annex_dir.path(), test_data)
            .await?;
        info!("Created test material: {}", material_id);

        // 2. Create some temporal ledger entries for the material
        self.create_test_ledger_entries(material_id, test_data)
            .await?;
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

        // Best effort cleanup of the temporary annex path env var
        std::env::remove_var("SINEX_ANNEX_PATH");
        Ok(())
    }

    /// Create a test material in the database
    async fn create_test_material(
        &self,
        annex_root: &std::path::Path,
        test_data: &[u8],
    ) -> Result<Ulid> {
        let material_id = Ulid::new();
        let blob_id = Ulid::new();
        let annex_backend = "SHA256";
        let content_hash = format!(
            "integration-test-{}",
            material_id.to_string().to_lowercase()
        );

        sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (
                id, source_identifier, material_kind, 
                status, timing_info_type, metadata,
                staged_at
            )
            VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, NOW())
            "#,
            material_id.to_uuid(),
            "integration-test",
            "annex",
            "completed",
            "realtime",
            serde_json::json!({
                "test": true,
                "data_size": test_data.len()
            }),
        )
        .execute(&self.db_pool)
        .await?;

        sqlx::query!(
            r#"
            INSERT INTO core.blobs (
                id,
                annex_backend,
                content_hash,
                size_bytes,
                checksum_blake3,
                original_filename,
                mime_type,
                metadata
            ) VALUES (
                $1::uuid::ulid,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8
            )
            "#,
            blob_id.to_uuid(),
            annex_backend,
            &content_hash,
            test_data.len() as i64,
            Option::<String>::None,
            "integration-test.bin",
            Option::<String>::None,
            serde_json::json!({ "test": true }),
        )
        .execute(&self.db_pool)
        .await?;

        sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET optional_blob_id = $1::uuid::ulid
            WHERE id = $2::uuid::ulid
            "#,
            blob_id.to_uuid(),
            material_id.to_uuid()
        )
        .execute(&self.db_pool)
        .await?;

        // Create a fake annex object that matches the expected layout
        let annex_key = format!("{}-s{}--{}", annex_backend, test_data.len(), content_hash);

        let objects_root = annex_root.join(".git").join("annex").join("objects");
        let annex_path = if content_hash.len() >= 4 {
            objects_root
                .join(&content_hash[0..2])
                .join(&content_hash[2..4])
                .join(&annex_key)
                .join(&annex_key)
        } else {
            objects_root.join(&annex_key)
        };

        if let Some(parent) = annex_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&annex_path, test_data)?;

        Ok(material_id)
    }

    /// Create test temporal ledger entries
    async fn create_test_ledger_entries(&self, material_id: Ulid, test_data: &[u8]) -> Result<()> {
        let chunk_size = 10;

        for (i, chunk) in test_data.chunks(chunk_size).enumerate() {
            let offset_start = (i * chunk_size) as i64;
            let offset_end = offset_start + chunk.len() as i64;
            let entry_id = Ulid::new();

            sqlx::query!(
                r#"
                INSERT INTO raw.temporal_ledger (
                    id, source_material_id, offset_start, offset_end,
                    offset_kind, ts_capture, precision, clock,
                    source_type
                )
                VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, $4, $5, NOW(), $6, $7, $8)
                "#,
                entry_id.to_uuid(),
                material_id.to_uuid(),
                offset_start,
                offset_end,
                "byte",
                "exact",
                "wall",
                "realtime_capture",
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
                id, sensor_type, target_uri, config, priority, status, updated_at
            )
            VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, NOW())
            "#,
            job_id.to_uuid(),
            "test_sensor",
            format!("/tmp/test-{}", job_id),
            serde_json::json!({"test": true, "mode": "test", "source_identifier": "integration-test-job"}),
            1,
            "active",
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

/// Run a basic integration test against an existing pool
pub async fn run_integration_test_with_pool(db_pool: PgPool) -> Result<()> {
    let test = SensdIntegrationTest::with_pool(db_pool).await?;
    test.test_complete_flow().await
}
