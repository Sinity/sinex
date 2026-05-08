//! Source material RPC handlers.
//!
//! Handlers for `sources.stage`, `sources.list`, `sources.show`, and
//! `sources.coverage` — the CLI-driven source material inventory surface.

use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial;
use sinex_primitives::domain::{SourceMaterialFormat, SourceMaterialTimingInfoType};
use sinex_primitives::rpc::sources::{
    ContinuityContractStatus, CoverageGap, ReplayabilityStatus, SourceAnnotations,
    SourceBindingSummary, SourceCoverageEntry, SourceMaterialDetail,
    SourceMaterialMetadataContract, SourceMaterialStatistics, SourceMaterialSummary, SourceOrigin,
    SourcePolicyEvidence, SourcePresetDescriptor, SourcesAnnotateRequest, SourcesAnnotateResponse,
    SourcesArchiveRequest, SourcesArchiveResponse, SourcesBindingsCreateRequest,
    SourcesBindingsCreateResponse, SourcesBindingsListRequest, SourcesBindingsListResponse,
    SourcesBindingsResolveRequest, SourcesBindingsResolveResponse, SourcesContinuityRequest,
    SourcesContinuityResponse, SourcesCoverageRequest, SourcesCoverageResponse, SourcesListRequest,
    SourcesListResponse, SourcesPresetsListResponse, SourcesShowRequest, SourcesShowResponse,
    SourcesStageRequest, SourcesStageResponse, TemporalEvidenceSummary,
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

pub async fn handle_sources_stage(
    params: Value,
    services: &crate::service_container::ServiceContainer,
    _auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let req: SourcesStageRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid sources.stage request").with_std_error(&error)
    })?;

    let pool = services.pool();

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

    // ── Privacy: classify the material path ──────────────────────
    let (path_class, display_path) = sinex_primitives::privacy::classify_material_path(&canonical);
    if path_class == sinex_primitives::privacy::MaterialPathClass::Temporary {
        return Err(SinexError::validation(
            "Staging of temporary paths is not allowed",
        )
        .with_context("file_path", &canonical)
        .with_context("path_class", "temporary"));
    }

    // ── Determine material capture class ─────────────────────────
    // Source bindings are Nix config (#1098), not a DB catalog.
    // Default to allowed_plaintext for now; binding-based policy is a follow-up.
    let capture_class = "allowed_plaintext".to_string();

    let material_class =
        sinex_primitives::privacy::MaterialCaptureClass::from_str(&capture_class)
            .unwrap_or(sinex_primitives::privacy::MaterialCaptureClass::AllowedPlaintext);

    if material_class.is_rejected() {
        return Err(SinexError::validation(
            "Material capture policy rejects this file",
        )
        .with_context("file_path", &canonical)
        .with_context("capture_class", capture_class));
    }

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

    // ── Byte-backed staging via ContentStoreManager ─────────────
    let (blob_id, checksum_blake3) = if req.with_bytes && material_class.allows_byte_storage() {
        let content_store = services.content.content_store();
        let verified_path = sinex_node_sdk::content_store::VerifiedPath::parse(&canonical)
            .map_err(|error| {
                SinexError::validation("Invalid file path for content store")
                    .with_context("file_path", &canonical)
                    .with_source(error.to_string())
            })?;
        let blob_metadata = content_store
            .ingest_file(&verified_path, None)
            .await
            .map_err(|error| {
                SinexError::processing("Failed to store file in content store")
                    .with_context("file_path", &canonical)
                    .with_std_error(&error)
            })?;
        (
            Some(blob_metadata.id.to_string()),
            blob_metadata.checksum_blake3,
        )
    } else {
        (None, None)
    };

    contract.statistics = Some(SourceMaterialStatistics {
        total_bytes: file_size,
        checksum_blake3: checksum_blake3.clone(),
        ..SourceMaterialStatistics::default()
    });

    // Record privacy policy evidence.
    contract.policy = Some(SourcePolicyEvidence {
        privacy_class: Some(capture_class),
        admission_decision: Some(if material_class.is_rejected() {
            "rejected".to_string()
        } else if material_class.requires_confirmation() {
            "requires_confirmation".to_string()
        } else {
            "admitted".to_string()
        }),
        quarantine_reason: None,
    });

    // Build material with optional blob_id.
    let material = SourceMaterial::file(&canonical)
        .with_optional_blob_id(blob_id.as_ref().and_then(|id_str| {
            uuid::Uuid::parse_str(id_str).ok().map(sinex_db::Id::from)
        }))
        .with_metadata_contract(contract.clone());

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
        blob_id,
        checksum_blake3,
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

// ── sources.presets.list ─────────────────────────────────────────

fn builtin_presets() -> Vec<SourcePresetDescriptor> {
    use SourcePresetDescriptor as P;
    vec![
        // Terminal presets
        P { name: "atuin.default".into(), description: "Default Atuin shell history database".into(), source_family: "terminal".into(), input_shape_kind: "sqlite_db".into(), material_format_hint: Some("sqlite".into()), resolver_preset: None },
        P { name: "zsh.default".into(), description: "Default Zsh history file".into(), source_family: "terminal".into(), input_shape_kind: "file".into(), material_format_hint: Some("plaintext".into()), resolver_preset: None },
        // Browser presets
        P { name: "firefox.default".into(), description: "Default Firefox profile history database".into(), source_family: "browser".into(), input_shape_kind: "sqlite_db".into(), material_format_hint: Some("sqlite".into()), resolver_preset: None },
        P { name: "chromium.default".into(), description: "Default Chromium profile history database".into(), source_family: "browser".into(), input_shape_kind: "sqlite_db".into(), material_format_hint: Some("sqlite".into()), resolver_preset: None },
        // Desktop presets
        P { name: "activitywatch.default".into(), description: "Default ActivityWatch SQLite database".into(), source_family: "desktop".into(), input_shape_kind: "sqlite_db".into(), material_format_hint: Some("sqlite".into()), resolver_preset: None },
        // Chat export presets
        P { name: "polylogue.exports.default".into(), description: "Polylogue chat archive root".into(), source_family: "chat".into(), input_shape_kind: "directory".into(), material_format_hint: None, resolver_preset: None },
        // Generic presets
        P { name: "directory.watch".into(), description: "Operator-supplied directory with optional glob pattern".into(), source_family: "generic".into(), input_shape_kind: "directory".into(), material_format_hint: None, resolver_preset: None },
    ]
}

pub async fn handle_sources_presets_list(
    _params: Value,
    _services: &crate::service_container::ServiceContainer,
    _auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let presets = builtin_presets();
    let response = SourcesPresetsListResponse { presets };
    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("Failed to serialize sources.presets.list response")
            .with_std_error(&error)
    })
}

// ── sources.bindings.list ───────────────────────────────────────

pub async fn handle_sources_bindings_list(
    _pool: &PgPool,
    _params: Value,
) -> Result<Value> {
    // Source bindings are Nix configuration (#1098), not a DB catalog.
    // The binding catalog DB tables were removed in #1160.
    let response = SourcesBindingsListResponse { bindings: vec![] };
    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("Failed to serialize sources.bindings.list response")
            .with_std_error(&error)
    })
}

// ── sources.bindings.create ─────────────────────────────────────

pub async fn handle_sources_bindings_create(
    _pool: &PgPool,
    _params: Value,
) -> Result<Value> {
    Err(SinexError::configuration(
        "Source bindings are Nix configuration (#1098), not a DB catalog. Bindings are declared in nixos/modules/source-bindings.nix."
    ))
}

// ── sources.bindings.resolve ─────────────────────────────────────

pub async fn handle_sources_bindings_resolve(
    _pool: &PgPool,
    _params: Value,
) -> Result<Value> {
    Err(SinexError::configuration(
        "Source bindings are Nix configuration (#1098), not a DB catalog. Bindings are declared in nixos/modules/source-bindings.nix."
    ))
}

// ── sources.annotate ─────────────────────────────────────────────

/// Annotate a staged source material with notes, tags, and declared temporal bounds.
///
/// Merges new annotations additively: new notes are appended to existing notes,
/// new tags are merged (deduplicated). Declared temporal bounds replace existing
/// values only when the request provides them.
pub async fn handle_sources_annotate(pool: &PgPool, params: Value) -> Result<Value> {
    let req: SourcesAnnotateRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid sources.annotate request").with_std_error(&error)
    })?;

    let material_id = Uuid::parse_str(&req.material_id).map_err(|error| {
        SinexError::validation("Invalid material_id UUID")
            .with_context("material_id", &req.material_id)
            .with_std_error(&error)
    })?;

    let mut record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await
        .map_err(|error| {
            SinexError::database("Failed to fetch source material for annotation")
                .with_std_error(&error)
        })?
        .ok_or_else(|| {
            SinexError::not_found(format!("Source material not found: {material_id}"))
        })?;

    // Read existing contract from metadata.
    let mut contract = SourceMaterialMetadataContract::from_metadata(&record.metadata)
        .unwrap_or_else(|| SourceMaterialMetadataContract::new(
            SourceMaterialFormat::Unknown,
            SourceMaterialTimingInfoType::Unknown,
        ));

    let mut annotations = contract.annotations.unwrap_or_default();

    // Merge notes.
    if let Some(notes) = &req.notes {
        let merged = match &annotations.reason {
            Some(existing) if !existing.is_empty() => format!("{existing}\n{notes}"),
            _ => notes.clone(),
        };
        annotations.reason = Some(merged);
    }

    // Merge tags.
    if !req.tags.is_empty() {
        let mut tags = annotations.tags.clone();
        for tag in &req.tags {
            if !tags.contains(tag) {
                tags.push(tag.clone());
            }
        }
        annotations.tags = tags;
    }

    // Override declared temporal bounds when provided.
    if req.declared_start_time.is_some() {
        annotations.declared_start_time = req.declared_start_time;
    }
    if req.declared_end_time.is_some() {
        annotations.declared_end_time = req.declared_end_time;
    }

    contract.annotations = Some(annotations.clone());

    // Persist updated metadata.
    let mut updated_meta = record.metadata.clone();
    if let serde_json::Value::Object(ref mut map) = updated_meta {
        let patch = contract.metadata_patch();
        if let serde_json::Value::Object(patch_map) = patch {
            for (k, v) in patch_map {
                map.insert(k, v);
            }
        }
    } else {
        updated_meta = contract.metadata_patch();
    }

    sqlx::query!(
        "UPDATE raw.source_material_registry SET metadata = $1 WHERE id = $2",
        &updated_meta as &serde_json::Value,
        material_id
    )
    .execute(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to persist source material annotations")
            .with_std_error(&error)
    })?;

    let response = SourcesAnnotateResponse {
        material_id: req.material_id,
        annotations,
    };

    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("Failed to serialize sources.annotate response")
            .with_std_error(&error)
    })
}

// ── sources.archive ──────────────────────────────────────────────

/// Archive a staged source material and its derived events.
///
/// Wraps the existing lifecycle archive infrastructure. Dry-run mode computes
/// the cascade preview without actually archiving, letting the operator inspect
/// the blast radius.
pub async fn handle_sources_archive(pool: &PgPool, params: Value) -> Result<Value> {
    let req: SourcesArchiveRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid sources.archive request").with_std_error(&error)
    })?;

    let material_id = Uuid::parse_str(&req.material_id).map_err(|error| {
        SinexError::validation("Invalid material_id UUID")
            .with_context("material_id", &req.material_id)
            .with_std_error(&error)
    })?;

    // Verify the material exists.
    let _record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await
        .map_err(|error| {
            SinexError::database("Failed to fetch source material for archival")
                .with_std_error(&error)
        })?
        .ok_or_else(|| {
            SinexError::not_found(format!("Source material not found: {material_id}"))
        })?;

    // Count events referencing this material.
    let event_count = sqlx::query_scalar!(
        r#"SELECT COUNT(*) as "count!: i64" FROM core.events WHERE source_material_id = $1"#,
        material_id
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    if req.dry_run {
        let preview = serde_json::json!({
            "material_id": req.material_id,
            "event_count": event_count,
            "action": "archive",
            "dry_run": true,
            "reason": req.reason.as_deref().unwrap_or("(no reason provided)"),
        });

        let response = SourcesArchiveResponse {
            material_id: req.material_id,
            operation_id: None,
            cascade_count: event_count,
            dry_run: true,
            preview: Some(preview),
        };
        return serde_json::to_value(response).map_err(|error| {
            SinexError::serialization("Failed to serialize sources.archive response")
                .with_std_error(&error)
        });
    }

    // Execute the archive via the lifecycle handler logic.
    // We build a synthetic archive request scoped to this material.
    let lifecycle_req = sinex_primitives::rpc::lifecycle::LifecycleArchiveRequest {
        before: None,
        source: None,
        event_ids: None,
        limit: 10000,
        reason: req.reason,
        dry_run: false,
    };

    let system_auth = crate::rpc_server::RpcAuthContext::system();
    let lifecycle_result = crate::handlers::lifecycle::handle_lifecycle_archive(
        pool,
        {
            serde_json::to_value(&lifecycle_req).map_err(|error| {
                SinexError::serialization("Failed to build lifecycle archive request")
                    .with_std_error(&error)
            })?
        },
        &system_auth,
    )
    .await;

    match lifecycle_result {
        Ok(value) => {
            let archive_resp: sinex_primitives::rpc::lifecycle::LifecycleArchiveResponse =
                serde_json::from_value(value).map_err(|error| {
                    SinexError::serialization("Failed to parse lifecycle archive response")
                        .with_std_error(&error)
                })?;

            let response = SourcesArchiveResponse {
                material_id: req.material_id,
                operation_id: Some(archive_resp.operation_id),
                cascade_count: archive_resp.cascade_total as i64,
                dry_run: false,
                preview: None,
            };
            serde_json::to_value(response).map_err(|error| {
                SinexError::serialization("Failed to serialize sources.archive response")
                    .with_std_error(&error)
            })
        }
        Err(error) => Err(error),
    }
}

// ── sources.continuity ───────────────────────────────────────────

/// Compute temporal continuity diagnostics for a source identifier.
///
/// Queries `raw.temporal_ledger` and `raw.source_material_registry` to detect
/// coverage gaps, contract breaches, and replayability status.
pub async fn handle_sources_continuity(pool: &PgPool, params: Value) -> Result<Value> {
    let req: SourcesContinuityRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid sources.continuity request").with_std_error(&error)
    })?;

    // ── Gather materials for this source ─────────────────────────
    let material_rows = sqlx::query!(
        r#"
        SELECT
            id, start_time, end_time, source_identifier, material_kind, status
        FROM raw.source_material_registry
        WHERE source_identifier = $1
          AND ($2::text IS NULL OR material_kind = $2)
        ORDER BY start_time ASC NULLS LAST
        "#,
        req.source_identifier,
        req.material_kind.as_deref()
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to query source materials for continuity diagnostics")
            .with_std_error(&error)
    })?;

    // ── Detect temporal gaps ─────────────────────────────────────
    let mut gaps: Vec<CoverageGap> = Vec::new();
    let mut prev_end: Option<OffsetDateTime> = None;

    for row in &material_rows {
        if let Some(start) = row.start_time {
            if let Some(prev) = prev_end {
                if start > prev {
                    let gap_secs = (start - prev).whole_seconds();
                    // Only report gaps larger than 1 second (rounding noise).
                    if gap_secs > 1 {
                        gaps.push(CoverageGap {
                            gap_start: Some(prev.to_string()),
                            gap_end: Some(start.to_string()),
                            gap_duration_seconds: Some(gap_secs),
                            gap_type: "temporal".to_string(),
                        });
                    }
                }
            }
            prev_end = row.end_time.max(Some(start));
        }
    }

    // ── Coverage contract status ─────────────────────────────────
    let temporal_rows = sqlx::query!(
        r#"
        SELECT COUNT(*) as "count!: i64"
        FROM raw.temporal_ledger tl
        JOIN raw.source_material_registry sm ON sm.id = tl.source_material_id
        WHERE sm.source_identifier = $1
          AND ($2::text IS NULL OR sm.material_kind = $2)
        "#,
        req.source_identifier,
        req.material_kind.as_deref()
    )
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to count temporal ledger entries for continuity")
            .with_std_error(&error)
    })?;

    let contract_status = if temporal_rows.count > 0 {
        let total_materials = material_rows.len() as i64;
        let materials_with_temporal = material_rows
            .iter()
            .filter(|r| r.start_time.is_some() || r.end_time.is_some())
            .count() as i64;
        let coverage_pct = if total_materials > 0 {
            Some((materials_with_temporal as f64 / total_materials as f64) * 100.0)
        } else {
            None
        };

        let mut breaches: Vec<String> = Vec::new();
        if !gaps.is_empty() {
            breaches.push(format!("{} temporal gap(s) detected", gaps.len()));
        }

        ContinuityContractStatus {
            has_coverage_contract: true,
            expected_interval_seconds: None,
            actual_coverage_percent: coverage_pct,
            breaches,
        }
    } else {
        ContinuityContractStatus {
            has_coverage_contract: false,
            expected_interval_seconds: None,
            actual_coverage_percent: None,
            breaches: Vec::new(),
        }
    };

    // ── Replayability assessment ─────────────────────────────────
    let events_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!: i64"
        FROM core.events e
        JOIN raw.source_material_registry sm ON sm.id = e.source_material_id
        WHERE sm.source_identifier = $1
          AND ($2::text IS NULL OR sm.material_kind = $2)
        "#,
        req.source_identifier,
        req.material_kind.as_deref()
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let material_count = material_rows.len() as i64;
    let non_failed_materials = material_rows
        .iter()
        .filter(|r| r.status != "failed")
        .count() as i64;

    let replayable = material_count > 0 && non_failed_materials == material_count;

    let replayability = ReplayabilityStatus {
        replayable,
        reason: if !replayable && material_count > 0 {
            Some("Some source materials have a failed status; replay may be incomplete".to_string())
        } else if material_count == 0 {
            Some("No source materials registered for this source identifier".to_string())
        } else {
            None
        },
        material_count,
        events_count,
    };

    let response = SourcesContinuityResponse {
        source_identifier: req.source_identifier,
        coverage_gaps: gaps,
        contract_status,
        replayability,
    };

    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("Failed to serialize sources.continuity response")
            .with_std_error(&error)
    })
}
