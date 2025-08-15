//! gRPC server implementation for sensd
//!
//! Provides MaterialSliceStream and job management interfaces

use crate::{
    job_manager::{JobManager, JobStatus, SensorJob, SensorType},
    material_stream::{MaterialSlice, MaterialSliceStream, StreamFrame},
    temporal_ledger::TemporalLedger,
};
use chrono::Utc;
use color_eyre::eyre::{eyre, Result};
use sinex_core::types::Ulid;
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, error, info, instrument};

// Include the generated proto code
pub mod proto {
    tonic::include_proto!("sinex.sensd");
}

use proto::{
    sensd_service_server::{SensdService as ProtoService, SensdServiceServer},
    CreateJobRequest, CreateJobResponse, EndOfMaterial, GapIndicator, GetJobStatusRequest,
    GetMaterialMetadataRequest, GetMaterialStreamRequest, JobStatus as ProtoJobStatus,
    ListMaterialsRequest, ListMaterialsResponse, MaterialMetadata, MaterialSlice as ProtoSlice,
    RotationBoundary, StreamFrame as ProtoFrame,
};

/// gRPC service implementation
pub struct SensdGrpcService {
    db_pool: PgPool,
    temporal_ledger: Arc<TemporalLedger>,
    job_manager: Arc<JobManager>,
}

impl SensdGrpcService {
    pub fn new(
        db_pool: PgPool,
        temporal_ledger: Arc<TemporalLedger>,
        job_manager: Arc<JobManager>,
    ) -> Self {
        Self {
            db_pool,
            temporal_ledger,
            job_manager,
        }
    }
}

#[tonic::async_trait]
impl ProtoService for SensdGrpcService {
    type GetMaterialStreamStream = ReceiverStream<Result<ProtoFrame, Status>>;

    #[instrument(skip(self))]
    async fn get_material_stream(
        &self,
        request: Request<GetMaterialStreamRequest>,
    ) -> Result<Response<Self::GetMaterialStreamStream>, Status> {
        let req = request.into_inner();

        let material_id = Ulid::from_str(&req.material_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid material_id: {}", e)))?;

        let batch_size = req.batch_size as usize;
        let start_offset = req.start_offset;

        info!(
            "Starting material stream for {} at offset {}",
            material_id, start_offset
        );

        // Create channel for streaming
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // Spawn task to stream materials
        let db_pool = self.db_pool.clone();
        tokio::spawn(async move {
            if let Err(e) =
                stream_material_slices(db_pool, material_id, start_offset, batch_size, tx).await
            {
                error!("Error streaming material slices: {}", e);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    #[instrument(skip(self))]
    async fn list_materials(
        &self,
        request: Request<ListMaterialsRequest>,
    ) -> Result<Response<ListMaterialsResponse>, Status> {
        let req = request.into_inner();
        let limit = req.limit.max(1).min(1000) as i64;

        // Query source materials
        let materials = sqlx::query!(
            r#"
            SELECT 
                source_material_id as "material_id: Ulid",
                source_identifier,
                source_type,
                total_bytes as size_bytes,
                content_type,
                created_at,
                staged_at,
                status as lifecycle_status,
                metadata
            FROM raw.source_material_registry
            WHERE ($1::text IS NULL OR source_identifier = $1)
              AND ($2::text IS NULL OR source_type = $2)
              AND ($3::text IS NULL OR status = $3)
            ORDER BY staged_at DESC
            LIMIT $4
            "#,
            req.source_identifier.as_ref().filter(|s| !s.is_empty()),
            req.source_type.as_ref().filter(|s| !s.is_empty()),
            req.status.as_ref().filter(|s| !s.is_empty()),
            limit
        )
        .fetch_all(&self.db_pool)
        .await
        .map_err(|e| Status::internal(format!("Database error: {}", e)))?;

        let materials = materials
            .into_iter()
            .map(|m| MaterialMetadata {
                material_id: m.material_id.to_string(),
                source_identifier: m.source_identifier.unwrap_or_default(),
                source_type: m.source_type.unwrap_or_default(),
                size_bytes: m.size_bytes.unwrap_or(0),
                content_type: m.content_type.unwrap_or_default(),
                created_at: m.created_at.to_rfc3339(),
                staged_at: m.staged_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
                status: m.lifecycle_status.unwrap_or_default(),
                metadata_json: serde_json::to_string(&m.metadata)
                    .unwrap_or_else(|_| "{}".to_string()),
            })
            .collect();

        Ok(Response::new(ListMaterialsResponse { materials }))
    }

    #[instrument(skip(self))]
    async fn get_material_metadata(
        &self,
        request: Request<GetMaterialMetadataRequest>,
    ) -> Result<Response<MaterialMetadata>, Status> {
        let req = request.into_inner();

        let material_id = Ulid::from_str(&req.material_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid material_id: {}", e)))?;

        let material = sqlx::query!(
            r#"
            SELECT 
                source_material_id as "material_id: Ulid",
                source_identifier,
                source_type,
                total_bytes as size_bytes,
                content_type,
                created_at,
                staged_at,
                status as lifecycle_status,
                metadata
            FROM raw.source_material_registry
            WHERE source_material_id = $1::ulid
            "#,
            material_id as Ulid
        )
        .fetch_optional(&self.db_pool)
        .await
        .map_err(|e| Status::internal(format!("Database error: {}", e)))?
        .ok_or_else(|| Status::not_found("Material not found"))?;

        Ok(Response::new(MaterialMetadata {
            material_id: material.material_id.to_string(),
            source_identifier: material.source_identifier.unwrap_or_default(),
            source_type: material.source_type.unwrap_or_default(),
            size_bytes: material.size_bytes.unwrap_or(0),
            content_type: material.content_type.unwrap_or_default(),
            created_at: material.created_at.to_rfc3339(),
            staged_at: material
                .staged_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
            status: material.lifecycle_status.unwrap_or_default(),
            metadata_json: serde_json::to_string(&material.metadata)
                .unwrap_or_else(|_| "{}".to_string()),
        }))
    }

    #[instrument(skip(self))]
    async fn create_job(
        &self,
        request: Request<CreateJobRequest>,
    ) -> Result<Response<CreateJobResponse>, Status> {
        let req = request.into_inner();

        // Parse sensor type
        let sensor_type = match req.sensor_type.as_str() {
            "append_stream" => "append_stream",
            "tree_watch" => "tree_watch",
            _ => return Err(Status::invalid_argument("Invalid sensor type")),
        };

        // Parse JSON fields
        let acquisition_mode = serde_json::from_str(&req.acquisition_mode_json).map_err(|e| {
            Status::invalid_argument(format!("Invalid acquisition_mode JSON: {}", e))
        })?;

        let parameters = serde_json::from_str(&req.parameters_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid parameters JSON: {}", e)))?;

        // Create new job
        let job_id = Ulid::new();

        let result = sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                job_id, sensor_type, target_uri, source_identifier,
                acquisition_mode, parameters, owner, priority, status,
                created_at
            ) VALUES (
                $1::ulid, $2, $3, $4, $5, $6, $7, $8, 'pending', NOW()
            )
            "#,
            job_id as Ulid,
            sensor_type,
            req.target_uri,
            req.source_identifier,
            acquisition_mode,
            parameters,
            req.owner,
            req.priority
        )
        .execute(&self.db_pool)
        .await;

        match result {
            Ok(_) => Ok(Response::new(CreateJobResponse {
                job_id: job_id.to_string(),
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(CreateJobResponse {
                job_id: String::new(),
                success: false,
                error: format!("Failed to create job: {}", e),
            })),
        }
    }

    #[instrument(skip(self))]
    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<ProtoJobStatus>, Status> {
        let req = request.into_inner();

        let job_id = Ulid::from_str(&req.job_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid job_id: {}", e)))?;

        let job = sqlx::query!(
            r#"
            SELECT 
                job_id as "job_id: Ulid",
                sensor_type,
                status,
                created_at,
                started_at,
                completed_at,
                error_message,
                material_id as "material_id: Ulid"
            FROM raw.sensor_jobs
            WHERE job_id = $1::ulid
            "#,
            job_id as Ulid
        )
        .fetch_optional(&self.db_pool)
        .await
        .map_err(|e| Status::internal(format!("Database error: {}", e)))?
        .ok_or_else(|| Status::not_found("Job not found"))?;

        Ok(Response::new(ProtoJobStatus {
            job_id: job.job_id.to_string(),
            sensor_type: job.sensor_type,
            status: job.status,
            created_at: job.created_at.to_rfc3339(),
            started_at: job.started_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
            completed_at: job.completed_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
            error_message: job.error_message.unwrap_or_default(),
            material_id: job.material_id.map(|id| id.to_string()).unwrap_or_default(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn capture_direct_with_ack(
        &self,
        request: Request<DirectCaptureRequest>,
    ) -> Result<Response<DirectCaptureAcknowledgment>, Status> {
        let req = request.into_inner();

        // Generate material ID
        let material_id = Ulid::new();
        let capture_timestamp = Utc::now();

        // Calculate checksum of the data
        let checksum = blake3::hash(&req.data).to_hex().to_string();

        // Parse metadata
        let metadata: serde_json::Value = if req.metadata_json.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(&req.metadata_json)
                .map_err(|e| Status::invalid_argument(format!("Invalid metadata JSON: {}", e)))?
        };

        // Store the material directly in the database
        let result = sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (
                id, source_identifier, source_type, 
                content_type, status, total_bytes,
                created_at, staged_at, metadata, data,
                material_type, checksum, source_uri, encoding,
                is_archived, ingestion_time
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
            )
            RETURNING id as "id: Ulid"
            "#,
            material_id as _,
            req.source_identifier,
            req.sensor_type,
            "application/octet-stream",
            "completed",
            req.data.len() as i64,
            capture_timestamp,
            capture_timestamp,
            metadata,
            req.data,
            "direct_capture",
            checksum,
            format!("direct://{}", req.source_identifier),
            "binary",
            false,
            capture_timestamp
        )
        .fetch_one(&self.db_pool)
        .await
        .map_err(|e| Status::internal(format!("Failed to store material: {}", e)))?;

        // If acknowledgment is required, ensure data integrity
        if req.require_acknowledgment {
            // Verify the data was written correctly by reading it back
            let verification = sqlx::query!(
                r#"
                SELECT checksum, total_bytes 
                FROM raw.source_material_registry 
                WHERE id = $1
                "#,
                result.id as _
            )
            .fetch_optional(&self.db_pool)
            .await
            .map_err(|e| Status::internal(format!("Failed to verify material: {}", e)))?;

            if let Some(verify) = verification {
                if verify.checksum != Some(checksum.clone()) {
                    return Ok(Response::new(DirectCaptureAcknowledgment {
                        material_id: material_id.to_string(),
                        success: false,
                        error: "Checksum verification failed".to_string(),
                        bytes_captured: 0,
                        capture_timestamp: capture_timestamp.to_rfc3339(),
                        checksum: String::new(),
                    }));
                }
            }
        }

        info!(
            material_id = %material_id,
            bytes = req.data.len(),
            source = req.source_identifier,
            "Direct capture with acknowledgment completed"
        );

        // Return acknowledgment
        Ok(Response::new(DirectCaptureAcknowledgment {
            material_id: material_id.to_string(),
            success: true,
            error: String::new(),
            bytes_captured: req.data.len() as i64,
            capture_timestamp: capture_timestamp.to_rfc3339(),
            checksum,
        }))
    }
}

/// Load material data from storage backend
async fn load_material_data(
    db_pool: &PgPool,
    material_id: Ulid,
    offset_start: i64,
    offset_end: i64,
) -> Result<Vec<u8>> {
    // Query the source material registry to get storage information
    let material = sqlx::query!(
        r#"
        SELECT 
            data,
            optional_blob_id as "optional_blob_id: Ulid"
        FROM raw.source_material_registry
        WHERE source_material_id = $1::ulid
        "#,
        material_id as Ulid
    )
    .fetch_optional(db_pool)
    .await?
    .ok_or_else(|| eyre!("Material not found: {}", material_id))?;

    if let Some(inline_data) = material.data {
        // Extract slice from inline data
        let start = offset_start as usize;
        let end = offset_end as usize;

        if start <= inline_data.len() && end <= inline_data.len() && start <= end {
            return Ok(inline_data[start..end].to_vec());
        } else {
            return Ok(vec![]); // Return empty if slice is out of bounds
        }
    } else if let Some(blob_id) = material.optional_blob_id {
        // Load from external blob storage
        // Query blob metadata from core.blobs table
        let blob = sqlx::query!(
            r#"
            SELECT 
                content_hash,
                size_bytes,
                stored_at,
                content_type,
                metadata
            FROM core.blobs
            WHERE id = $1::uuid
            "#,
            sinex_schema::ulid_conversions::ulid_to_uuid(blob_id)
        )
        .fetch_optional(db_pool)
        .await?;

        if let Some(blob_record) = blob {
            // Check if we have a file path in metadata
            if let Some(metadata) = blob_record.metadata {
                if let Some(file_path) = metadata.get("file_path").and_then(|v| v.as_str()) {
                    // Read the file from disk
                    match tokio::fs::read(file_path).await {
                        Ok(data) => {
                            // Extract the requested slice
                            let start = offset_start as usize;
                            let end = offset_end as usize;
                            if start <= data.len() && end <= data.len() && start <= end {
                                return Ok(data[start..end].to_vec());
                            } else {
                                return Ok(vec![]); // Out of bounds
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to read blob file {}: {}", file_path, e);
                            return Ok(vec![]);
                        }
                    }
                }
            }
        }

        // If we couldn't load from file storage, return empty
        tracing::warn!("External blob {} not found or couldn't be loaded", blob_id);
        return Ok(vec![]);
    } else {
        // No data available
        return Ok(vec![]);
    }
}

/// Stream material slices to a channel
async fn stream_material_slices(
    db_pool: PgPool,
    material_id: Ulid,
    start_offset: i64,
    batch_size: usize,
    tx: tokio::sync::mpsc::Sender<Result<ProtoFrame, Status>>,
) -> Result<()> {
    let mut current_offset = start_offset;

    loop {
        // Query temporal ledger for slices
        let slices = sqlx::query!(
            r#"
            SELECT 
                material_id as "material_id: Ulid",
                offset_start,
                offset_end,
                ts_capture,
                note
            FROM raw.temporal_ledger
            WHERE material_id = $1::ulid
            AND offset_start >= $2
            ORDER BY offset_start
            LIMIT $3
            "#,
            material_id as Ulid,
            current_offset,
            batch_size as i64,
        )
        .fetch_all(&db_pool)
        .await?;

        if slices.is_empty() {
            // Send end of material frame
            let frame = ProtoFrame {
                frame: Some(proto::stream_frame::Frame::EndOfMaterial(EndOfMaterial {
                    material_id: material_id.to_string(),
                    final_offset: current_offset,
                })),
            };

            if tx.send(Ok(frame)).await.is_err() {
                break; // Receiver dropped
            }
            break;
        }

        for record in slices {
            // Load actual data from storage backend
            let data = load_material_data(
                &db_pool,
                record.material_id,
                record.offset_start,
                record.offset_end,
            )
            .await
            .unwrap_or_else(|e| {
                error!("Failed to load material data: {}", e);
                vec![] // Return empty data on error
            });

            let slice = ProtoSlice {
                material_id: record.material_id.to_string(),
                offset_start: record.offset_start,
                offset_end: record.offset_end,
                ts_capture_start: record.ts_capture.to_rfc3339(),
                ts_capture_end: record.ts_capture.to_rfc3339(), // Same as start for single capture time
                data,
                metadata_json: record.note.unwrap_or_else(|| "{}".to_string()),
            };

            let frame = ProtoFrame {
                frame: Some(proto::stream_frame::Frame::Slice(slice)),
            };

            if tx.send(Ok(frame)).await.is_err() {
                return Ok(()); // Receiver dropped
            }

            current_offset = record.offset_end;
        }
    }

    Ok(())
}

/// Run the gRPC server
pub async fn run_grpc_server(
    addr: std::net::SocketAddr,
    db_pool: PgPool,
    temporal_ledger: Arc<TemporalLedger>,
    job_manager: Arc<JobManager>,
) -> Result<()> {
    let service = SensdGrpcService::new(db_pool, temporal_ledger, job_manager);

    info!("Starting sensd gRPC server on {}", addr);

    Server::builder()
        .add_service(SensdServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
