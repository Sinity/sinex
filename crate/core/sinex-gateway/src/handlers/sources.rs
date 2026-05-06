//! Source material RPC handlers.
//!
//! Handlers for `sources.stage`, `sources.list`, `sources.show`, and
//! `sources.coverage` — the CLI-driven source material inventory surface.

use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial;
use sinex_primitives::domain::{SourceMaterialFormat, SourceMaterialTimingInfoType};
use sinex_primitives::rpc::sources::{
    SourceAnnotations, SourceCoverageEntry, SourceMaterialDetail, SourceMaterialMetadataContract,
    SourceMaterialStatistics, SourceMaterialSummary, SourceOrigin, SourcesCoverageRequest,
    SourcesCoverageResponse, SourcesListRequest, SourcesListResponse, SourcesShowRequest,
    SourcesShowResponse, SourcesStageRequest, SourcesStageResponse, TemporalEvidenceSummary,
};
use sinex_primitives::{Result, SinexError};
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;
use uuid::Uuid;

// ── Query row structs (sqlx FromRow) ──────────────────────────

#[derive(Debug, FromRow)]
struct MaterialListRow {
    id: Uuid,
    material_kind: String,
    source_identifier: String,
    status: String,
    timing_info_type: String,
    metadata: serde_json::Value,
    staged_at: OffsetDateTime,
    staged_by: Option<String>,
    total_bytes: Option<i64>,
    mime_type: Option<String>,
}

#[derive(Debug, FromRow)]
struct CoverageRow {
    source_identifier: String,
    material_kind: String,
    earliest_ts: Option<OffsetDateTime>,
    latest_ts: Option<OffsetDateTime>,
    material_count: i64,
}

// ── sources.stage ──────────────────────────────────────────────

pub async fn handle_sources_stage(pool: &PgPool, params: Value) -> Result<Value> {
    let req: SourcesStageRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid sources.stage request").with_std_error(&error)
    })?;

    let path = std::path::Path::new(&req.file_path);
    if !path.exists() {
        return Err(SinexError::not_found(format!(
            "File not found: {}",
            req.file_path
        )));
    }
    if !path.is_file() {
        return Err(SinexError::validation(format!(
            "Not a regular file: {}",
            req.file_path
        )));
    }

    let file_size = tokio::fs::metadata(path).await.map(|m| m.len() as i64).ok();

    let canonical = std::path::absolute(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .to_string();

    let format = req
        .format
        .unwrap_or_else(|| SourceMaterialFormat::infer_from_path(&canonical));
    if matches!(
        format,
        SourceMaterialFormat::Directory | SourceMaterialFormat::Repository
    ) {
        return Err(SinexError::validation(
            "sources.stage only accepts regular-file material formats",
        )
        .with_context("format", format.to_string()));
    }
    let timing = req
        .timing_info_type
        .unwrap_or(SourceMaterialTimingInfoType::Intrinsic);
    let mut contract = SourceMaterialMetadataContract::new(format, timing);
    contract.origin = Some(SourceOrigin {
        source_uri: Some(canonical.clone()),
        ..SourceOrigin::default()
    });
    contract.annotations = Some(SourceAnnotations {
        reason: req.reason.clone(),
        tags: req.tags.clone(),
        ..SourceAnnotations::default()
    });
    contract.statistics = Some(SourceMaterialStatistics {
        total_bytes: file_size,
        ..SourceMaterialStatistics::default()
    });

    let material = SourceMaterial::file(&canonical).with_metadata_contract(contract.clone());

    let mut record = pool
        .source_materials()
        .register_material(material)
        .await
        .map_err(|error| {
            SinexError::processing("Failed to register source material")
                .with_context("file_path", &canonical)
                .with_std_error(&error)
        })?;

    // Persist file size if we have it (registration doesn't set total_bytes).
    if let Some(size) = file_size {
        sqlx::query!(
            "UPDATE raw.source_material_registry SET total_bytes = $1 WHERE id = $2",
            size,
            record.id
        )
        .execute(pool)
        .await
        .map_err(|error| {
            SinexError::database("Failed to persist staged source material size")
                .with_context("material_id", record.id.to_string())
                .with_std_error(&error)
        })?;
        record.total_bytes = Some(size);
    }

    let response = SourcesStageResponse {
        material_id: record.id.to_string(),
        source_identifier: record.source_identifier,
        total_bytes: record.total_bytes,
        contract,
    };

    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("Failed to serialize sources.stage response")
            .with_std_error(&error)
    })
}

// ── sources.list ───────────────────────────────────────────────

pub async fn handle_sources_list(pool: &PgPool, params: Value) -> Result<Value> {
    let req: SourcesListRequest = super::parse_default_on_null(params).map_err(|error| {
        SinexError::serialization("Invalid sources.list request").with_std_error(&error)
    })?;

    let limit = req.limit.unwrap_or(100).clamp(1, 1000);
    let rows = sqlx::query_as!(
        MaterialListRow,
        r#"
        SELECT
            sm.id as "id!",
            sm.material_kind,
            sm.source_identifier,
            sm.status,
            sm.timing_info_type,
            sm.metadata,
            sm.staged_at as "staged_at!",
            sm.staged_by,
            sm.total_bytes,
            b.mime_type
        FROM raw.source_material_registry sm
        LEFT JOIN core.blobs b ON b.id = sm.optional_blob_id
        WHERE ($1::text IS NULL OR sm.status = $1)
        ORDER BY sm.staged_at DESC
        LIMIT $2
        "#,
        req.status.as_deref(),
        limit
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to list source materials").with_std_error(&error)
    })?;

    let materials = rows
        .into_iter()
        .map(|row| SourceMaterialSummary {
            format: SourceMaterialMetadataContract::from_metadata(&row.metadata)
                .map(|contract| contract.format),
            contract_version: SourceMaterialMetadataContract::from_metadata(&row.metadata)
                .map(|contract| contract.version),
            id: row.id.to_string(),
            material_kind: row.material_kind,
            source_identifier: row.source_identifier,
            status: row.status,
            timing_info_type: row.timing_info_type,
            staged_at: Some(row.staged_at.to_string()),
            staged_by: row.staged_by,
            size_bytes: row.total_bytes,
            mime_type: row.mime_type,
        })
        .collect();

    let response = SourcesListResponse { materials };

    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("Failed to serialize sources.list response")
            .with_std_error(&error)
    })
}

// ── sources.show ───────────────────────────────────────────────

pub async fn handle_sources_show(pool: &PgPool, params: Value) -> Result<Value> {
    let req: SourcesShowRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid sources.show request").with_std_error(&error)
    })?;

    let material_id = Uuid::parse_str(&req.material_id).map_err(|error| {
        SinexError::validation("Invalid material_id UUID")
            .with_context("material_id", &req.material_id)
            .with_std_error(&error)
    })?;

    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await
        .map_err(|error| {
            SinexError::database("Failed to fetch source material").with_std_error(&error)
        })?
        .ok_or_else(|| {
            SinexError::not_found(format!("Source material not found: {material_id}"))
        })?;

    let event_count = sqlx::query_scalar!(
        r#"SELECT COUNT(*) as "count!: i64" FROM core.events WHERE source_material_id = $1"#,
        material_id
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let temporal_evidence = query_temporal_evidence(pool, material_id).await?;
    let contract = SourceMaterialMetadataContract::from_metadata(&record.metadata);

    let detail = SourceMaterialDetail {
        id: record.id.to_string(),
        material_kind: record.material_kind,
        source_identifier: record.source_identifier,
        status: record.status,
        timing_info_type: record.timing_info_type,
        metadata: record.metadata,
        contract,
        temporal_evidence: Some(temporal_evidence),
        staged_at: Some(record.staged_at.to_string()),
        start_time: record.start_time.map(|ts| ts.to_string()),
        end_time: record.end_time.map(|ts| ts.to_string()),
        staged_by: record.staged_by,
        staged_on_host: record.staged_on_host,
        optional_blob_id: record.optional_blob_id.map(|id| id.to_string()),
        total_bytes: record.total_bytes,
        event_count: Some(event_count),
    };

    let response = SourcesShowResponse { material: detail };

    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("Failed to serialize sources.show response")
            .with_std_error(&error)
    })
}

async fn query_temporal_evidence(
    pool: &PgPool,
    material_id: Uuid,
) -> Result<TemporalEvidenceSummary> {
    let row = sqlx::query!(
        r#"
        SELECT
            COUNT(*)::bigint as "ledger_entries!",
            COALESCE(
                array_remove(array_agg(DISTINCT source_type ORDER BY source_type), NULL),
                ARRAY[]::text[]
            ) as "source_types!"
        FROM raw.temporal_ledger
        WHERE source_material_id = $1
        "#,
        material_id
    )
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to summarize source material temporal evidence")
            .with_std_error(&error)
    })?;

    Ok(TemporalEvidenceSummary {
        ledger_entries: row.ledger_entries,
        source_types: row.source_types,
    })
}

// ── sources.coverage ───────────────────────────────────────────

pub async fn handle_sources_coverage(pool: &PgPool, params: Value) -> Result<Value> {
    let _req: SourcesCoverageRequest = super::parse_default_on_null(params).map_err(|error| {
        SinexError::serialization("Invalid sources.coverage request").with_std_error(&error)
    })?;

    let rows = sqlx::query_as!(
        CoverageRow,
        r#"
        SELECT
            sm.source_identifier,
            sm.material_kind,
            MIN(sm.start_time) as "earliest_ts: _",
            MAX(sm.end_time) as "latest_ts: _",
            COUNT(*) as "material_count!"
        FROM raw.source_material_registry sm
        GROUP BY sm.source_identifier, sm.material_kind
        ORDER BY sm.source_identifier
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to compute source material coverage").with_std_error(&error)
    })?;

    let sources = rows
        .into_iter()
        .map(|row| SourceCoverageEntry {
            source_identifier: row.source_identifier,
            material_kind: row.material_kind,
            earliest_ts: row.earliest_ts.map(|ts| ts.to_string()),
            latest_ts: row.latest_ts.map(|ts| ts.to_string()),
            event_count: None,
            material_count: Some(row.material_count),
        })
        .collect();

    let response = SourcesCoverageResponse { sources };

    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("Failed to serialize sources.coverage response")
            .with_std_error(&error)
    })
}
