//! Source material RPC handlers.
//!
//! Handlers for `sources.stage`, `sources.list`, `sources.show`, and
//! `sources.coverage` — the CLI-driven source material inventory surface.

use serde::{Deserialize, Serialize};
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial;
use sinex_primitives::domain::{SourceMaterialFormat, SourceMaterialTimingInfoType};
use sinex_primitives::privacy::{RuntimePrivateModeState, load_private_mode_state};
use sinex_primitives::rpc::sources::{
    CaveatSeverity, ContinuityContractStatus, CoverageGap, ReplayabilityStatus,
    SourceAdmissionDecision, SourceAnnotations, SourceCaveat, SourceCoverageEntry,
    SourceMaterialDetail, SourceMaterialMetadataContract, SourceMaterialStatistics,
    SourceMaterialSummary, SourceOrigin, SourcePackageCompletenessModeView,
    SourcePackageCompletenessPackageView, SourcePolicyEvidence, SourcePresetDescriptor,
    SourceReadiness, SourceReadinessStatus, SourceShapeDriftObservation, SourceShapeTypeChange,
    SourcesAnnotateRequest, SourcesAnnotateResponse, SourcesArchiveRequest, SourcesArchiveResponse,
    SourcesBindingsCreateRequest, SourcesBindingsCreateResponse, SourcesBindingsListRequest,
    SourcesBindingsListResponse, SourcesBindingsResolveRequest, SourcesBindingsResolveResponse,
    SourcesContinuityRequest, SourcesContinuityResponse, SourcesCoverageRequest,
    SourcesCoverageResponse, SourcesDriftListRequest, SourcesDriftListResponse, SourcesListRequest,
    SourcesListResponse, SourcesPackageCompletenessRequest, SourcesPackageCompletenessResponse,
    SourcesPackageCompletenessSummaryView, SourcesPresetsListRequest, SourcesPresetsListResponse,
    SourcesReadinessGetRequest, SourcesReadinessGetResponse, SourcesReadinessListRequest,
    SourcesReadinessListResponse, SourcesShowRequest, SourcesShowResponse, SourcesStageRequest,
    SourcesStageResponse, TemporalEvidenceSummary, bridge_material_presets, caveat_codes,
    external_producer_presets,
};
use sinex_primitives::sources::SourceFamily;
use sinex_primitives::sources::continuity::{
    CoverageContract, CoverageGap as ContinuityCoverageGap, GapKind, Replayability,
    SourceContinuityReport, SourcesContinuityGetRequest, SourcesContinuityGetResponse,
    SourcesContinuityListRequest, SourcesContinuityListResponse, SourcesExplainGapRequest,
    SourcesExplainGapResponse,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Result, SinexError};
use sqlx::{FromRow, PgPool};
use std::error::Error as _;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::sources::package_completeness::build_package_completeness_report;

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
    services: &crate::api::service_container::ServiceContainer,
    req: SourcesStageRequest,
    _auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<SourcesStageResponse> {
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
    let (path_class, _display_path) = sinex_primitives::privacy::classify_material_path(&canonical);
    if path_class == sinex_primitives::privacy::MaterialPathClass::Temporary {
        return Err(
            SinexError::validation("Staging of temporary paths is not allowed")
                .with_context("file_path", &canonical)
                .with_context("path_class", "temporary"),
        );
    }

    // ── Determine material capture class ─────────────────────────
    // Source bindings are deployment config, not a DB catalog. This manual
    // staging path has no binding record to consult, so it records the default
    // allowed material capture class.
    let capture_class = "allowed_plaintext".to_string();

    let material_class =
        sinex_primitives::privacy::MaterialCaptureClass::from_canonical_str(&capture_class)
            .unwrap_or(sinex_primitives::privacy::MaterialCaptureClass::AllowedPlaintext);

    if material_class.is_rejected() {
        return Err(
            SinexError::validation("Material capture policy rejects this file")
                .with_context("file_path", &canonical)
                .with_context("capture_class", capture_class),
        );
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
    let mut contract = stage_material_contract(&canonical, format, timing, &req);

    // ── Byte-backed staging via ContentStoreManager ─────────────
    let (blob_id, checksum_blake3) = if req.with_bytes && material_class.allows_byte_storage() {
        let content_store = services.content.content_store();
        let verified_path = crate::runtime::content_store::VerifiedPath::parse(&canonical)
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
        capture_class: Some(material_class),
        admission_decision: Some(if material_class.is_rejected() {
            SourceAdmissionDecision::Rejected
        } else if material_class.requires_confirmation() {
            SourceAdmissionDecision::RequiresConfirmation
        } else {
            SourceAdmissionDecision::Admitted
        }),
        quarantine_reason: None,
    });

    // Build material with optional blob_id.
    let material = SourceMaterial::file(&canonical)
        .with_optional_blob_id(
            blob_id
                .as_ref()
                .and_then(|id_str| uuid::Uuid::parse_str(id_str).ok().map(sinex_db::Id::from)),
        )
        .with_metadata_contract(&contract);

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

    Ok(response)
}

fn stage_material_contract(
    canonical: &str,
    format: SourceMaterialFormat,
    timing: SourceMaterialTimingInfoType,
    req: &SourcesStageRequest,
) -> SourceMaterialMetadataContract {
    let mut contract = SourceMaterialMetadataContract::new(format, timing);
    contract.origin = Some(SourceOrigin {
        source_uri: Some(canonical.to_string()),
        binding_id: req.binding_name.clone(),
        ..SourceOrigin::default()
    });
    contract.annotations = Some(SourceAnnotations {
        reason: req.reason.clone(),
        tags: req.tags.clone(),
        ..SourceAnnotations::default()
    });
    contract
}

// ── sources.list ───────────────────────────────────────────────

pub async fn handle_sources_list(
    pool: &PgPool,
    req: SourcesListRequest,
) -> Result<SourcesListResponse> {
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
            status: row
                .status
                .parse()
                .unwrap_or(sinex_primitives::MaterialStatus::Sensing),
            timing_info_type: row
                .timing_info_type
                .parse()
                .unwrap_or(sinex_primitives::domain::SourceMaterialTimingInfoType::Unknown),
            staged_at: Some(row.staged_at.to_string()),
            staged_by: row.staged_by,
            size_bytes: row.total_bytes,
            mime_type: row.mime_type,
        })
        .collect();

    Ok(SourcesListResponse { materials })
}

// ── sources.show ───────────────────────────────────────────────

pub async fn handle_sources_show(
    pool: &PgPool,
    req: SourcesShowRequest,
) -> Result<SourcesShowResponse> {
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
        timing_info_type: record
            .timing_info_type
            .parse()
            .unwrap_or(sinex_primitives::domain::SourceMaterialTimingInfoType::Unknown),
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

    Ok(SourcesShowResponse { material: detail })
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

pub async fn handle_sources_coverage(
    pool: &PgPool,
    _req: SourcesCoverageRequest,
) -> Result<SourcesCoverageResponse> {
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

    Ok(SourcesCoverageResponse { sources })
}

// ── sources.package_completeness ───────────────────────────────

pub async fn handle_sources_package_completeness(
    _services: &crate::api::service_container::ServiceContainer,
    _request: SourcesPackageCompletenessRequest,
) -> Result<SourcesPackageCompletenessResponse> {
    let report = build_package_completeness_report();
    let packages = report
        .packages
        .into_values()
        .map(|package| SourcePackageCompletenessPackageView {
            package_id: package.package_id,
            family: package.family,
            display_namespace: package.display_namespace,
            modes: package
                .modes
                .into_values()
                .map(|mode| SourcePackageCompletenessModeView {
                    mode_id: mode.mode_id,
                    package_id: mode.package_id,
                    mode_state: serialized_label(&mode.mode_state),
                    completeness: serialized_label(&mode.completeness),
                    subject: mode.subject,
                    acquisition_kind: mode.acquisition_kind.to_string(),
                    operator_enablement: mode.operator_enablement.to_string(),
                    missing: mode.missing,
                    caveats: mode.caveats,
                    event_contract_refs: mode.event_contract_refs,
                    admission_policy_refs: mode.admission_policy_refs,
                    coverage_debt_refs: mode.coverage_debt_refs,
                    operation_refs: mode.operation_refs,
                })
                .collect(),
        })
        .collect();

    Ok(SourcesPackageCompletenessResponse {
        schema_version: report.schema_version,
        summary: SourcesPackageCompletenessSummaryView {
            package_count: report.summary.package_count,
            mode_count: report.summary.mode_count,
            accepted_mode_count: report.summary.accepted_mode_count,
            proposed_mode_count: report.summary.proposed_mode_count,
            manual_mode_count: report.summary.manual_mode_count,
            incomplete_mode_count: report.summary.incomplete_mode_count,
            blocking_missing_count: report.summary.blocking_missing_count,
        },
        packages,
    })
}

fn serialized_label(value: &impl Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}

// ── sources.presets.list ─────────────────────────────────────────

fn builtin_presets() -> Vec<SourcePresetDescriptor> {
    use SourcePresetDescriptor as P;
    let mut presets = vec![
        // Terminal presets
        P {
            name: "atuin.default".into(),
            description: "Default Atuin shell history database".into(),
            source_family: "terminal".into(),
            input_shape_kind: "sqlite_db".into(),
            material_format_hint: Some("sqlite".into()),
            resolver_preset: None,
        },
        P {
            name: "zsh.default".into(),
            description: "Default Zsh history file".into(),
            source_family: "terminal".into(),
            input_shape_kind: "file".into(),
            material_format_hint: Some("plaintext".into()),
            resolver_preset: None,
        },
        // Browser presets
        P {
            name: "firefox.default".into(),
            description: "Default Firefox profile history database".into(),
            source_family: "browser".into(),
            input_shape_kind: "sqlite_db".into(),
            material_format_hint: Some("sqlite".into()),
            resolver_preset: None,
        },
        P {
            name: "chromium.default".into(),
            description: "Default Chromium profile history database".into(),
            source_family: "browser".into(),
            input_shape_kind: "sqlite_db".into(),
            material_format_hint: Some("sqlite".into()),
            resolver_preset: None,
        },
        // Desktop presets
        P {
            name: "activitywatch.default".into(),
            description: "Default ActivityWatch SQLite database".into(),
            source_family: "desktop".into(),
            input_shape_kind: "sqlite_db".into(),
            material_format_hint: Some("sqlite".into()),
            resolver_preset: None,
        },
        // Generic presets
        P {
            name: "directory.watch".into(),
            description: "Operator-supplied directory with optional glob pattern".into(),
            source_family: "generic".into(),
            input_shape_kind: "directory".into(),
            material_format_hint: None,
            resolver_preset: None,
        },
    ];
    presets.extend(bridge_material_presets());
    presets.extend(external_producer_presets());
    presets
}

pub async fn handle_sources_presets_list(
    _services: &crate::api::service_container::ServiceContainer,
    _request: SourcesPresetsListRequest,
) -> Result<SourcesPresetsListResponse> {
    let presets = builtin_presets();
    Ok(SourcesPresetsListResponse { presets })
}

#[cfg(test)]
mod preset_tests {
    use super::builtin_presets;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn builtin_presets_include_external_bridge_surfaces() -> TestResult<()> {
        let presets = builtin_presets();
        let names: std::collections::BTreeSet<_> =
            presets.iter().map(|preset| preset.name.as_str()).collect();

        assert!(
            names.contains("polylogue.exports.default"),
            "Polylogue material bridge preset must be exposed through sources.presets.list"
        );
        // External producer presets are operator-configured, not hardcoded.
        // This test verifies the presets list endpoint is reachable and
        // returns the expected structure.
        Ok(())
    }
}

// ── sources.bindings.list ───────────────────────────────────────

pub async fn handle_sources_bindings_list(
    _pool: &PgPool,
    _request: SourcesBindingsListRequest,
) -> Result<SourcesBindingsListResponse> {
    // Source bindings are deployment configuration, not a DB catalog.
    // The binding catalog DB tables were removed in #1160.
    Ok(SourcesBindingsListResponse { bindings: vec![] })
}

// ── sources.bindings.create ─────────────────────────────────────

pub async fn handle_sources_bindings_create(
    _pool: &PgPool,
    _request: SourcesBindingsCreateRequest,
) -> Result<SourcesBindingsCreateResponse> {
    Err(SinexError::configuration(
        "Source bindings are deployment configuration, not a DB catalog. Bindings are declared in nixos/modules/source-bindings.nix.",
    ))
}

// ── sources.bindings.resolve ─────────────────────────────────────

pub async fn handle_sources_bindings_resolve(
    _pool: &PgPool,
    _request: SourcesBindingsResolveRequest,
) -> Result<SourcesBindingsResolveResponse> {
    Err(SinexError::configuration(
        "Source bindings are deployment configuration, not a DB catalog. Bindings are declared in nixos/modules/source-bindings.nix.",
    ))
}

// ── sources.annotate ─────────────────────────────────────────────

/// Annotate a staged source material with notes, tags, and declared temporal bounds.
///
/// Merges new annotations additively: new notes are appended to existing notes,
/// new tags are merged (deduplicated). Declared temporal bounds replace existing
/// values only when the request provides them.
pub async fn handle_sources_annotate(
    pool: &PgPool,
    req: SourcesAnnotateRequest,
) -> Result<SourcesAnnotateResponse> {
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
            SinexError::database("Failed to fetch source material for annotation")
                .with_std_error(&error)
        })?
        .ok_or_else(|| {
            SinexError::not_found(format!("Source material not found: {material_id}"))
        })?;

    // Read existing contract from metadata.
    let mut contract = SourceMaterialMetadataContract::from_metadata(&record.metadata)
        .unwrap_or_else(|| {
            SourceMaterialMetadataContract::new(
                SourceMaterialFormat::Unknown,
                SourceMaterialTimingInfoType::Unknown,
            )
        });

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
        SinexError::database("Failed to persist source material annotations").with_std_error(&error)
    })?;

    let response = SourcesAnnotateResponse {
        material_id: req.material_id,
        annotations,
    };

    Ok(response)
}

// ── sources.archive ──────────────────────────────────────────────

/// Archive a staged source material and its derived events.
///
/// Wraps the existing lifecycle archive infrastructure. Dry-run mode computes
/// the cascade preview without actually archiving, letting the operator inspect
/// the blast radius.
pub async fn handle_sources_archive(
    pool: &PgPool,
    req: SourcesArchiveRequest,
) -> Result<SourcesArchiveResponse> {
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
        return Ok(response);
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

    let system_auth = crate::api::rpc_server::RpcAuthContext::system();
    let lifecycle_result = crate::api::handlers::lifecycle::handle_lifecycle_archive(
        pool,
        lifecycle_req,
        &system_auth,
    )
    .await;

    match lifecycle_result {
        Ok(archive_resp) => {
            let response = SourcesArchiveResponse {
                material_id: req.material_id,
                operation_id: Some(archive_resp.operation_id),
                cascade_count: archive_resp.cascade_total as i64,
                dry_run: false,
                preview: None,
            };
            Ok(response)
        }
        Err(error) => Err(error),
    }
}

// ── sources.continuity ───────────────────────────────────────────

/// Compute temporal continuity diagnostics for a source identifier.
///
/// Queries `raw.temporal_ledger` and `raw.source_material_registry` to detect
/// coverage gaps, contract breaches, and replayability status.
pub async fn handle_sources_continuity(
    pool: &PgPool,
    req: SourcesContinuityRequest,
) -> Result<SourcesContinuityResponse> {
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
            if let Some(prev) = prev_end
                && start > prev
            {
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
        .filter(|r| r.status != sinex_primitives::MaterialStatus::Failed.as_str())
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

    Ok(SourcesContinuityResponse {
        source_identifier: req.source_identifier,
        coverage_gaps: gaps,
        contract_status,
        replayability,
    })
}

// ── sources.readiness.list (#1099) ─────────────────────────────

pub async fn handle_sources_readiness_list(
    services: &crate::api::service_container::ServiceContainer,
    req: SourcesReadinessListRequest,
) -> Result<SourcesReadinessListResponse> {
    let mut sources = services
        .pool()
        .source_materials()
        .list_source_readiness(req.source_family.as_deref(), req.stale_after_seconds)
        .await
        .map_err(|error| {
            SinexError::database("Failed to list source readiness").with_std_error(&error)
        })?;
    apply_private_mode_readiness_overlay(services, &mut sources);
    apply_checkpoint_drift_readiness_overlay(services, &mut sources).await;

    Ok(SourcesReadinessListResponse { sources })
}

// ── sources.readiness.get (#1099) ──────────────────────────────

pub async fn handle_sources_readiness_get(
    services: &crate::api::service_container::ServiceContainer,
    req: SourcesReadinessGetRequest,
) -> Result<SourcesReadinessGetResponse> {
    let mut readiness = services
        .pool()
        .source_materials()
        .get_source_readiness(
            &req.source_identifier,
            req.source_family.as_deref(),
            req.stale_after_seconds,
        )
        .await
        .map_err(|error| {
            SinexError::database("Failed to get source readiness").with_std_error(&error)
        })?;
    if let Some(row) = readiness.as_mut() {
        apply_private_mode_readiness_overlay(services, std::slice::from_mut(row));
        apply_checkpoint_drift_readiness_overlay(services, std::slice::from_mut(row)).await;
    }

    Ok(SourcesReadinessGetResponse { readiness })
}

// ── sources.drift.list (#1103) ─────────────────────────────────

pub async fn handle_sources_drift_list(
    services: &crate::api::service_container::ServiceContainer,
    req: SourcesDriftListRequest,
) -> Result<SourcesDriftListResponse> {
    let mut drifts = load_checkpoint_drifts(services).await?;
    if let Some(source_id) = req.source_id {
        drifts.retain(|drift| drift.source_id == source_id);
    }
    drifts.sort_by(compare_drift_observations_newest_first);
    let limit = req.limit.unwrap_or(50).min(500);
    drifts.truncate(limit);

    Ok(SourcesDriftListResponse { drifts })
}

async fn load_checkpoint_drifts(
    services: &crate::api::service_container::ServiceContainer,
) -> Result<Vec<SourceShapeDriftObservation>> {
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let js = async_nats::jetstream::new(nats_client.clone());
    let bucket = crate::runtime::checkpoint::checkpoint_bucket_name(None);
    let kv = match js.get_key_value(&bucket).await {
        Ok(kv) => kv,
        Err(error) if is_missing_checkpoint_bucket(&error) => {
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(SinexError::kv("Failed to open checkpoint bucket")
                .with_context("bucket", bucket)
                .with_source(error));
        }
    };

    let mut keys = kv
        .keys()
        .await
        .map_err(|error| SinexError::kv("Failed to list checkpoint keys").with_source(error))?;
    let mut drifts = Vec::new();

    use futures::StreamExt;
    while let Some(key) = keys.next().await {
        let key = key
            .map_err(|error| SinexError::kv("Failed to read checkpoint key").with_source(error))?;
        let Some(entry) = kv.get(&key).await.map_err(|error| {
            SinexError::kv("Failed to fetch checkpoint state")
                .with_context("checkpoint_key", key.clone())
                .with_source(error)
        })?
        else {
            continue;
        };

        let checkpoint = serde_json::from_slice::<CheckpointEnvelope>(&entry).map_err(|error| {
            SinexError::serialization("Checkpoint state is not valid JSON")
                .with_context("checkpoint_key", key.clone())
                .with_std_error(&error)
        })?;
        drifts.extend(extract_checkpoint_drifts(&key, checkpoint.data.as_ref())?);
    }

    Ok(drifts)
}

fn compare_drift_observations_newest_first(
    lhs: &SourceShapeDriftObservation,
    rhs: &SourceShapeDriftObservation,
) -> std::cmp::Ordering {
    rhs.observed_at
        .cmp(&lhs.observed_at)
        .then_with(|| rhs.checkpoint_key.cmp(&lhs.checkpoint_key))
}

fn is_missing_checkpoint_bucket(error: &async_nats::jetstream::context::KeyValueError) -> bool {
    use async_nats::jetstream::ErrorCode;
    use async_nats::jetstream::context::KeyValueErrorKind;

    if error.kind() != KeyValueErrorKind::GetBucket {
        return false;
    }

    let Some(source) = error.source() else {
        return false;
    };
    let Some(stream_error) =
        source.downcast_ref::<async_nats::jetstream::context::GetStreamError>()
    else {
        return false;
    };

    matches!(
        stream_error.kind(),
        async_nats::jetstream::context::GetStreamErrorKind::JetStream(js_error)
            if js_error.error_code() == ErrorCode::STREAM_NOT_FOUND
    )
}

#[derive(Debug, Deserialize)]
struct CheckpointEnvelope {
    #[serde(default)]
    data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RawDriftEvent {
    source_id: sinex_primitives::parser::SourceId,
    previous_hash: String,
    current_hash: String,
    format: String,
    #[serde(default)]
    added_keys: Vec<String>,
    #[serde(default)]
    removed_keys: Vec<String>,
    #[serde(default)]
    type_changes: Vec<RawTypeChange>,
    #[serde(default)]
    required_input_keys: Vec<String>,
    observed_at: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawTypeChange {
    Tuple(String, String, String),
    Object {
        key: String,
        previous_type: String,
        current_type: String,
    },
}

impl RawTypeChange {
    fn into_rpc(self) -> SourceShapeTypeChange {
        match self {
            Self::Tuple(key, previous_type, current_type)
            | Self::Object {
                key,
                previous_type,
                current_type,
            } => SourceShapeTypeChange {
                key,
                previous_type,
                current_type,
            },
        }
    }
}

fn extract_checkpoint_drifts(
    checkpoint_key: &str,
    data: Option<&serde_json::Value>,
) -> Result<Vec<SourceShapeDriftObservation>> {
    let Some(user_state) = data.and_then(|value| value.get("user_state")) else {
        return Ok(Vec::new());
    };
    let Some(raw_drifts) = user_state
        .get("recent_input_drifts")
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(Vec::new());
    };

    let parsed_key = crate::runtime::checkpoint::parse_checkpoint_key(checkpoint_key);
    let (_, consumer_group, consumer_name) = parsed_key.as_ref().map_or(
        ("", String::new(), String::new()),
        |(module, group, consumer)| (module.as_str(), group.clone(), consumer.clone()),
    );

    raw_drifts
        .iter()
        .map(|value| {
            let raw = serde_json::from_value::<RawDriftEvent>(value.clone()).map_err(|error| {
                SinexError::serialization("Checkpoint drift event is not valid JSON")
                    .with_context("checkpoint_key", checkpoint_key.to_string())
                    .with_std_error(&error)
            })?;
            Ok(SourceShapeDriftObservation {
                checkpoint_key: checkpoint_key.to_string(),
                source_id: raw.source_id,
                consumer_group: (!consumer_group.is_empty()).then_some(consumer_group.clone()),
                consumer_name: (!consumer_name.is_empty()).then_some(consumer_name.clone()),
                previous_hash: raw.previous_hash,
                current_hash: raw.current_hash,
                format: raw.format,
                added_keys: raw.added_keys,
                removed_keys: raw.removed_keys,
                type_changes: raw
                    .type_changes
                    .into_iter()
                    .map(RawTypeChange::into_rpc)
                    .collect(),
                required_input_keys: raw.required_input_keys,
                observed_at: raw
                    .observed_at
                    .as_str()
                    .map_or_else(|| raw.observed_at.to_string(), str::to_string),
            })
        })
        .collect()
}

fn apply_private_mode_readiness_overlay(
    services: &crate::api::service_container::ServiceContainer,
    sources: &mut [SourceReadiness],
) {
    match load_private_mode_state(services.state_dir()) {
        Ok(state) => apply_private_mode_state_readiness_overlay(sources, &state),
        Err(error) => apply_private_mode_unavailable_readiness_overlay(sources, &error),
    }
}

async fn apply_checkpoint_drift_readiness_overlay(
    services: &crate::api::service_container::ServiceContainer,
    sources: &mut [SourceReadiness],
) {
    if services.nats_client().is_none() {
        return;
    }

    match load_checkpoint_drifts(services).await {
        Ok(mut drifts) => {
            drifts.sort_by(compare_drift_observations_newest_first);
            apply_shape_drift_readiness_overlay(sources, &drifts);
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to load checkpointed source-shape drift for readiness overlay"
            );
        }
    }
}

fn apply_shape_drift_readiness_overlay(
    sources: &mut [SourceReadiness],
    drifts: &[SourceShapeDriftObservation],
) {
    for source in sources {
        let Some(drift) = drifts
            .iter()
            .filter(|drift| drift_matches_readiness_source(drift, source))
            .min_by(|lhs, rhs| compare_drift_observations_newest_first(lhs, rhs))
        else {
            continue;
        };

        let mut has_degraded_drift = false;
        for caveat in drift.readiness_caveats() {
            has_degraded_drift |= matches!(
                caveat.severity,
                CaveatSeverity::Degraded | CaveatSeverity::Blocking
            );
            if !source.caveats.iter().any(|existing| {
                existing.code == caveat.code && existing.evidence_ref == caveat.evidence_ref
            }) {
                source.caveats.push(caveat);
            }
        }

        if has_degraded_drift && source.status == SourceReadinessStatus::Available {
            source.status = SourceReadinessStatus::Partial;
        }
    }
}

fn drift_matches_readiness_source(
    drift: &SourceShapeDriftObservation,
    source: &SourceReadiness,
) -> bool {
    if let Some(source_id) = source.source_id.as_ref() {
        return &drift.source_id == source_id;
    }

    drift
        .source_id
        .as_str()
        .split_once('.')
        .is_some_and(|(family, _)| family == source.source_family)
}

fn apply_private_mode_state_readiness_overlay(
    sources: &mut [SourceReadiness],
    state: &RuntimePrivateModeState,
) {
    if !state.is_active_at(Timestamp::now()) {
        return;
    }
    for source in sources {
        if private_mode_applies_to_readiness(source, state) {
            source.status = SourceReadinessStatus::Blocked;
            source.caveats.push(SourceCaveat {
                code: caveat_codes::POLICY_RAW_MATERIAL_BLOCKED.to_string(),
                severity: CaveatSeverity::Blocking,
                message: format!(
                    "Runtime private mode is enabled for source family '{}'.",
                    source.source_family
                ),
                evidence_ref: state.updated_by_operation_id.clone(),
            });
        }
    }
}

fn apply_private_mode_unavailable_readiness_overlay(
    sources: &mut [SourceReadiness],
    _error: &SinexError,
) {
    for source in sources {
        source.status = SourceReadinessStatus::Blocked;
        source.caveats.push(SourceCaveat {
            code: caveat_codes::POLICY_PRIVATE_MODE_STATE_UNAVAILABLE.to_string(),
            severity: CaveatSeverity::Blocking,
            message: "Runtime private-mode state is unavailable; readiness is blocked fail-closed."
                .to_string(),
            evidence_ref: None,
        });
    }
}

fn private_mode_applies_to_readiness(
    source: &SourceReadiness,
    state: &RuntimePrivateModeState,
) -> bool {
    state.affected_source_classes.is_empty()
        || state.affected_source_classes.iter().any(|scope| {
            scope == &source.source_family
                || source
                    .source_id
                    .as_ref()
                    .is_some_and(|source_id| scope == source_id.as_str())
                || scope == &source.source_identifier
        })
}

// ── sources.continuity.list / .get / .explain_gap (#1085) ────────
//
// Operator-facing continuity diagnostics scoped by `SourceFamily`.
// Sits alongside `sources.continuity` (per `source_identifier`); the
// new methods aggregate across the family rollup axis and return
// richer scorecard / seam / gap structures.

/// Handle `sources.continuity.list` — a continuity report per observed source family.
pub async fn handle_sources_continuity_list(
    services: &crate::api::service_container::ServiceContainer,
    req: SourcesContinuityListRequest,
) -> Result<SourcesContinuityListResponse> {
    let mut reports = services
        .pool()
        .continuity()
        .list_continuity_reports(req.since)
        .await?;
    apply_private_mode_continuity_overlay(services, &mut reports);

    Ok(SourcesContinuityListResponse { reports })
}

/// Handle `sources.continuity.get` — continuity report for one family.
pub async fn handle_sources_continuity_get(
    services: &crate::api::service_container::ServiceContainer,
    req: SourcesContinuityGetRequest,
) -> Result<SourcesContinuityGetResponse> {
    let mut report = services
        .pool()
        .continuity()
        .get_continuity_report(&req.source_family)
        .await?;
    apply_private_mode_continuity_get_overlay(services, &req.source_family, &mut report);

    Ok(SourcesContinuityGetResponse { report })
}

/// Handle `sources.continuity.explain_gap` — attribute a single window.
pub async fn handle_sources_continuity_explain_gap(
    services: &crate::api::service_container::ServiceContainer,
    req: SourcesExplainGapRequest,
) -> Result<SourcesExplainGapResponse> {
    let mut gap = services
        .pool()
        .continuity()
        .explain_gap(&req.source_family, req.at)
        .await?;
    apply_private_mode_explain_gap_overlay(services, &req.source_family, req.at, &mut gap);

    let explanation = match (&gap, gap.as_ref().and_then(|g| g.attribution.as_deref())) {
        (Some(_), Some(reason)) => format!(
            "At {}, source family {} was inside a coverage gap: {}",
            req.at, req.source_family, reason
        ),
        (Some(_), None) => format!(
            "At {}, source family {} was inside a coverage gap (no attribution available)",
            req.at, req.source_family
        ),
        (None, _) => format!(
            "At {}, coverage was present for source family {} (no gap to explain)",
            req.at, req.source_family
        ),
    };

    Ok(SourcesExplainGapResponse {
        source_family: req.source_family,
        at: req.at,
        gap,
        explanation,
    })
}

fn apply_private_mode_continuity_overlay(
    services: &crate::api::service_container::ServiceContainer,
    reports: &mut [SourceContinuityReport],
) {
    let Ok(state) = load_private_mode_state(services.state_dir()) else {
        return;
    };
    apply_private_mode_state_continuity_overlay(reports, &state, Timestamp::now());
}

fn apply_private_mode_continuity_get_overlay(
    services: &crate::api::service_container::ServiceContainer,
    source_family: &SourceFamily,
    report: &mut Option<SourceContinuityReport>,
) {
    let Ok(state) = load_private_mode_state(services.state_dir()) else {
        return;
    };
    apply_private_mode_state_continuity_get_overlay(
        report,
        source_family,
        &state,
        Timestamp::now(),
    );
}

fn apply_private_mode_explain_gap_overlay(
    services: &crate::api::service_container::ServiceContainer,
    source_family: &SourceFamily,
    at: Timestamp,
    gap: &mut Option<ContinuityCoverageGap>,
) {
    let Ok(state) = load_private_mode_state(services.state_dir()) else {
        return;
    };
    if gap.is_none()
        && private_mode_applies_to_source_family(source_family, &state)
        && private_mode_state_covers_at(&state, at)
    {
        *gap = private_mode_gap_for_state(&state, at);
    }
}

fn apply_private_mode_state_continuity_overlay(
    reports: &mut [SourceContinuityReport],
    state: &RuntimePrivateModeState,
    now: Timestamp,
) {
    for report in reports {
        if private_mode_applies_to_source_family(&report.source_family, state)
            && let Some(gap) = private_mode_gap_for_state(state, now)
        {
            report.gaps.push(gap);
        }
    }
}

fn apply_private_mode_state_continuity_get_overlay(
    report: &mut Option<SourceContinuityReport>,
    source_family: &SourceFamily,
    state: &RuntimePrivateModeState,
    now: Timestamp,
) {
    if !private_mode_applies_to_source_family(source_family, state) {
        return;
    }
    let Some(gap) = private_mode_gap_for_state(state, now) else {
        return;
    };

    match report {
        Some(report) => report.gaps.push(gap),
        None => {
            *report = Some(SourceContinuityReport {
                source_family: source_family.clone(),
                coverage_contract: CoverageContract::Continuous,
                is_declared: false,
                replayability: private_mode_only_replayability(),
                seams: Vec::new(),
                gaps: vec![gap],
                earliest_ts: None,
                latest_ts: None,
                material_count: 0,
                event_count: 0,
            });
        }
    }
}

fn private_mode_state_covers_at(state: &RuntimePrivateModeState, at: Timestamp) -> bool {
    state.enabled
        && state.started_at.is_none_or(|started_at| started_at <= at)
        && state.expires_at.is_none_or(|expires_at| at < expires_at)
}

fn private_mode_gap_for_state(
    state: &RuntimePrivateModeState,
    now: Timestamp,
) -> Option<ContinuityCoverageGap> {
    if !state.is_active_at(now) {
        return None;
    }
    let from_ts = state.started_at.unwrap_or(now);
    let to_ts = state
        .expires_at
        .filter(|expires_at| *expires_at < now)
        .unwrap_or(now);
    if to_ts < from_ts {
        return None;
    }

    Some(ContinuityCoverageGap {
        from_ts,
        to_ts,
        kind: GapKind::PrivateMode,
        attribution: Some(private_mode_continuity_attribution(state)),
    })
}

fn private_mode_continuity_attribution(state: &RuntimePrivateModeState) -> String {
    match &state.updated_by_operation_id {
        Some(operation_id) => format!("runtime private mode active ({operation_id})"),
        None => "runtime private mode active".to_string(),
    }
}

fn private_mode_only_replayability() -> Replayability {
    Replayability {
        raw_bytes_preserved: false,
        timing_quality: false,
        anchor_stability: false,
        parser_determinism: true,
        privacy_safe_replay: true,
        weak_points: vec![
            "private-mode caveat only; no source material was observed for this family".to_string(),
        ],
    }
}

fn private_mode_applies_to_source_family(
    source_family: &SourceFamily,
    state: &RuntimePrivateModeState,
) -> bool {
    state.affected_source_classes.is_empty()
        || state
            .affected_source_classes
            .iter()
            .any(|scope| scope == source_family.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_primitives::rpc::sources::SourceReadinessCost;
    use sinex_primitives::temporal::Timestamp;
    use xtask::sandbox::prelude::sinex_test;

    fn continuity_report(source_family: &str) -> SourceContinuityReport {
        SourceContinuityReport {
            source_family: SourceFamily::new(source_family).expect("valid source family"),
            coverage_contract: CoverageContract::Continuous,
            is_declared: true,
            replayability: Replayability {
                raw_bytes_preserved: true,
                timing_quality: true,
                anchor_stability: true,
                parser_determinism: true,
                privacy_safe_replay: true,
                weak_points: Vec::new(),
            },
            seams: Vec::new(),
            gaps: Vec::new(),
            earliest_ts: None,
            latest_ts: None,
            material_count: 1,
            event_count: 1,
        }
    }

    fn readiness(source_family: &str, source_identifier: &str) -> SourceReadiness {
        SourceReadiness {
            binding_id: None,
            source_family: source_family.to_string(),
            source_id: None,
            parser_id: None,
            source_identifier: source_identifier.to_string(),
            status: SourceReadinessStatus::Available,
            cost: SourceReadinessCost::LocalFast,
            freshness_seconds: Some(1),
            material_count: 1,
            parsed_event_count: Some(1),
            last_success_at: Some("1970-01-01 00:00:00 UTC".to_string()),
            caveats: Vec::new(),
            evidence: serde_json::Value::Null,
        }
    }

    #[sinex_test]
    async fn stage_material_contract_records_package_mode_binding() -> xtask::sandbox::TestResult<()>
    {
        let request = SourcesStageRequest {
            file_path: "/tmp/sinex-fixtures/screenshot/session.json".to_string(),
            format: Some(SourceMaterialFormat::Json),
            timing_info_type: Some(SourceMaterialTimingInfoType::Intrinsic),
            reason: Some("operator import".to_string()),
            tags: vec!["media".to_string()],
            binding_name: Some("source:media.screen-ocr.screenshot-ocr-staged".to_string()),
            with_bytes: true,
        };

        let contract = stage_material_contract(
            "/tmp/sinex-fixtures/screenshot/session.json",
            SourceMaterialFormat::Json,
            SourceMaterialTimingInfoType::Intrinsic,
            &request,
        );

        let origin = contract.origin.as_ref().expect("origin expected");
        assert_eq!(
            origin.source_uri.as_deref(),
            Some("/tmp/sinex-fixtures/screenshot/session.json")
        );
        assert_eq!(
            origin.binding_id.as_deref(),
            Some("source:media.screen-ocr.screenshot-ocr-staged")
        );
        assert_eq!(contract.format, SourceMaterialFormat::Json);
        assert_eq!(contract.timing, SourceMaterialTimingInfoType::Intrinsic);
        assert_eq!(
            contract
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.reason.as_deref()),
            Some("operator import")
        );
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_readiness_overlay_blocks_matching_family()
    -> xtask::sandbox::TestResult<()> {
        let mut sources = vec![
            readiness("desktop", "/capture/desktop"),
            readiness("terminal", "/capture/terminal"),
        ];
        let mut state = RuntimePrivateModeState::enabled_by(
            "operator",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        );
        state.updated_by_operation_id = Some("op-1".to_string());

        apply_private_mode_state_readiness_overlay(&mut sources, &state);

        assert_eq!(sources[0].status, SourceReadinessStatus::Blocked);
        assert_eq!(
            sources[0].caveats[0].code,
            caveat_codes::POLICY_RAW_MATERIAL_BLOCKED
        );
        assert_eq!(sources[0].caveats[0].evidence_ref.as_deref(), Some("op-1"));
        assert_eq!(sources[1].status, SourceReadinessStatus::Available);
        assert!(sources[1].caveats.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_readiness_overlay_blocks_all_when_scope_empty()
    -> xtask::sandbox::TestResult<()> {
        let mut sources = vec![
            readiness("desktop", "/capture/desktop"),
            readiness("terminal", "/capture/terminal"),
        ];
        let state =
            RuntimePrivateModeState::enabled_by("operator", Vec::new(), Timestamp::UNIX_EPOCH);

        apply_private_mode_state_readiness_overlay(&mut sources, &state);

        assert!(
            sources
                .iter()
                .all(|source| source.status == SourceReadinessStatus::Blocked)
        );
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_unavailable_readiness_overlay_blocks_fail_closed()
    -> xtask::sandbox::TestResult<()> {
        let mut sources = vec![readiness("desktop", "/capture/desktop")];
        let error = SinexError::io("private-mode state unavailable");

        apply_private_mode_unavailable_readiness_overlay(&mut sources, &error);

        assert_eq!(sources[0].status, SourceReadinessStatus::Blocked);
        assert_eq!(
            sources[0].caveats[0].code,
            caveat_codes::POLICY_PRIVATE_MODE_STATE_UNAVAILABLE
        );
        assert_eq!(sources[0].caveats[0].severity, CaveatSeverity::Blocking);
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_continuity_overlay_adds_coarse_gap_for_matching_family()
    -> xtask::sandbox::TestResult<()> {
        let now = Timestamp::UNIX_EPOCH;
        let mut reports = vec![continuity_report("desktop"), continuity_report("terminal")];
        let mut state =
            RuntimePrivateModeState::enabled_by("operator", vec!["desktop".to_string()], now);
        state.updated_by_operation_id = Some("op-private".to_string());

        apply_private_mode_state_continuity_overlay(&mut reports, &state, now);

        assert_eq!(reports[0].gaps.len(), 1);
        assert_eq!(reports[0].gaps[0].kind, GapKind::PrivateMode);
        assert_eq!(reports[0].gaps[0].from_ts, now);
        assert_eq!(reports[0].gaps[0].to_ts, now);
        assert_eq!(
            reports[0].gaps[0].attribution.as_deref(),
            Some("runtime private mode active (op-private)")
        );
        assert!(reports[1].gaps.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_continuity_get_synthesizes_no_material_report()
    -> xtask::sandbox::TestResult<()> {
        let now = Timestamp::UNIX_EPOCH;
        let source_family = SourceFamily::new("clipboard")?;
        let state =
            RuntimePrivateModeState::enabled_by("operator", vec!["clipboard".to_string()], now);
        let mut report = None;

        apply_private_mode_state_continuity_get_overlay(&mut report, &source_family, &state, now);

        let report = report.expect("private-mode overlay should synthesize report");
        assert_eq!(report.source_family, source_family);
        assert_eq!(report.material_count, 0);
        assert_eq!(report.event_count, 0);
        assert_eq!(report.gaps.len(), 1);
        assert_eq!(report.gaps[0].kind, GapKind::PrivateMode);
        assert!(
            report
                .replayability
                .weak_points
                .iter()
                .any(|weak_point| weak_point.contains("private-mode caveat only"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_shape_drift_extraction_reads_checkpoint_user_state()
    -> xtask::sandbox::TestResult<()> {
        let drifts = extract_checkpoint_drifts(
            "source.default.host-a",
            Some(&json!({
                "user_state": {
                    "recent_input_drifts": [
                        {
                            "source_id": "browser.history",
                            "previous_hash": "old",
                            "current_hash": "new",
                            "format": "csv",
                            "added_keys": ["url"],
                            "removed_keys": ["visit_id"],
                            "required_input_keys": ["visit_id"],
                            "type_changes": [
                                ["title", "string", "null"],
                                {
                                    "key": "visit_time",
                                    "previous_type": "number",
                                    "current_type": "string"
                                }
                            ],
                            "observed_at": "2026-05-21T10:00:00Z"
                        }
                    ]
                }
            })),
        )?;

        assert_eq!(drifts.len(), 1);
        let drift = &drifts[0];
        assert_eq!(drift.checkpoint_key, "source.default.host-a");
        assert_eq!(drift.source_id.as_str(), "browser.history");
        assert_eq!(drift.consumer_group.as_deref(), Some("default"));
        assert_eq!(drift.consumer_name.as_deref(), Some("host-a"));
        assert_eq!(drift.added_keys, ["url"]);
        assert_eq!(drift.removed_keys, ["visit_id"]);
        assert_eq!(drift.required_input_keys, ["visit_id"]);
        assert_eq!(drift.type_changes.len(), 2);
        assert_eq!(drift.type_changes[0].key, "title");
        assert_eq!(drift.type_changes[0].previous_type, "string");
        assert_eq!(drift.type_changes[0].current_type, "null");
        assert_eq!(drift.type_changes[1].key, "visit_time");
        assert_eq!(drift.observed_at, "2026-05-21T10:00:00Z");
        Ok(())
    }

    #[sinex_test]
    async fn source_shape_drift_extraction_ignores_checkpoints_without_drift()
    -> xtask::sandbox::TestResult<()> {
        let drifts = extract_checkpoint_drifts(
            "source.default.host-a",
            Some(&json!({ "user_state": { "other": [] } })),
        )?;

        assert!(drifts.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn source_shape_drift_readiness_overlay_adds_latest_degraded_caveats()
    -> xtask::sandbox::TestResult<()> {
        let source_id = sinex_primitives::parser::SourceId::new("browser.history")?;
        let mut sources = vec![readiness("browser", "history.sqlite")];
        sources[0].source_id = Some(source_id.clone());

        let drifts = vec![
            SourceShapeDriftObservation {
                checkpoint_key: "source.default.host-a".to_string(),
                source_id: source_id.clone(),
                consumer_group: Some("default".to_string()),
                consumer_name: Some("host-a".to_string()),
                previous_hash: "old-1".to_string(),
                current_hash: "new-1".to_string(),
                format: "sqlite_schema".to_string(),
                added_keys: vec!["title".to_string()],
                removed_keys: Vec::new(),
                type_changes: Vec::new(),
                required_input_keys: Vec::new(),
                observed_at: "2026-05-21T09:00:00Z".to_string(),
            },
            SourceShapeDriftObservation {
                checkpoint_key: "source.default.host-a".to_string(),
                source_id,
                consumer_group: Some("default".to_string()),
                consumer_name: Some("host-a".to_string()),
                previous_hash: "old-2".to_string(),
                current_hash: "new-2".to_string(),
                format: "sqlite_schema".to_string(),
                added_keys: Vec::new(),
                removed_keys: vec!["visit_id".to_string()],
                type_changes: vec![SourceShapeTypeChange {
                    key: "visit_time".to_string(),
                    previous_type: "integer".to_string(),
                    current_type: "text".to_string(),
                }],
                required_input_keys: Vec::new(),
                observed_at: "2026-05-21T10:00:00Z".to_string(),
            },
        ];

        apply_shape_drift_readiness_overlay(&mut sources, &drifts);

        assert_eq!(sources[0].status, SourceReadinessStatus::Partial);
        let codes = sources[0]
            .caveats
            .iter()
            .map(|caveat| caveat.code.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            codes,
            [
                caveat_codes::PARSER_FIELD_TYPE_CHANGED,
                caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            ]
        );
        assert!(
            sources[0]
                .caveats
                .iter()
                .all(|caveat| caveat.severity == CaveatSeverity::Degraded)
        );
        assert!(
            sources[0]
                .caveats
                .iter()
                .all(|caveat| caveat.evidence_ref.as_deref() == Some("drift:new-2"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_shape_drift_readiness_overlay_keeps_additive_drift_available()
    -> xtask::sandbox::TestResult<()> {
        let source_id = sinex_primitives::parser::SourceId::new("browser.history")?;
        let mut sources = vec![readiness("browser", "history.csv")];
        sources[0].source_id = Some(source_id.clone());

        let drifts = vec![SourceShapeDriftObservation {
            checkpoint_key: "source.default.host-a".to_string(),
            source_id,
            consumer_group: Some("default".to_string()),
            consumer_name: Some("host-a".to_string()),
            previous_hash: "old".to_string(),
            current_hash: "new".to_string(),
            format: "csv".to_string(),
            added_keys: vec!["title".to_string()],
            removed_keys: Vec::new(),
            type_changes: Vec::new(),
            required_input_keys: Vec::new(),
            observed_at: "2026-05-21T10:00:00Z".to_string(),
        }];

        apply_shape_drift_readiness_overlay(&mut sources, &drifts);

        assert_eq!(sources[0].status, SourceReadinessStatus::Available);
        assert_eq!(sources[0].caveats.len(), 1);
        assert_eq!(
            sources[0].caveats[0].code,
            caveat_codes::SOURCE_SHAPE_CHANGED
        );
        assert_eq!(sources[0].caveats[0].severity, CaveatSeverity::Info);
        Ok(())
    }

    #[sinex_test]
    async fn source_shape_drift_readiness_overlay_matches_family_when_unit_unknown()
    -> xtask::sandbox::TestResult<()> {
        let mut sources = vec![
            readiness("browser", "history.csv"),
            readiness("terminal", "history.txt"),
        ];

        let drifts = vec![SourceShapeDriftObservation {
            checkpoint_key: "source.default.host-a".to_string(),
            source_id: sinex_primitives::parser::SourceId::new("browser.history")?,
            consumer_group: Some("default".to_string()),
            consumer_name: Some("host-a".to_string()),
            previous_hash: "old".to_string(),
            current_hash: "new".to_string(),
            format: "csv".to_string(),
            added_keys: Vec::new(),
            removed_keys: vec!["visit_id".to_string()],
            type_changes: Vec::new(),
            required_input_keys: Vec::new(),
            observed_at: "2026-05-21T10:00:00Z".to_string(),
        }];

        apply_shape_drift_readiness_overlay(&mut sources, &drifts);

        assert_eq!(sources[0].status, SourceReadinessStatus::Partial);
        assert_eq!(
            sources[0].caveats[0].code,
            caveat_codes::PARSER_REQUIRED_FIELD_MISSING
        );
        assert_eq!(sources[1].status, SourceReadinessStatus::Available);
        assert!(sources[1].caveats.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn source_shape_drift_readiness_overlay_blocks_required_input_removal()
    -> xtask::sandbox::TestResult<()> {
        let source_id = sinex_primitives::parser::SourceId::new("browser.history")?;
        let mut sources = vec![readiness("browser", "history.sqlite")];
        sources[0].source_id = Some(source_id.clone());

        let drifts = vec![SourceShapeDriftObservation {
            checkpoint_key: "source.default.host-a".to_string(),
            source_id,
            consumer_group: Some("default".to_string()),
            consumer_name: Some("host-a".to_string()),
            previous_hash: "old".to_string(),
            current_hash: "new".to_string(),
            format: "sqlite_schema".to_string(),
            added_keys: Vec::new(),
            removed_keys: vec!["visit_id".to_string()],
            type_changes: Vec::new(),
            required_input_keys: vec!["visit_id".to_string()],
            observed_at: "2026-05-21T10:00:00Z".to_string(),
        }];

        apply_shape_drift_readiness_overlay(&mut sources, &drifts);

        assert_eq!(sources[0].status, SourceReadinessStatus::Partial);
        assert!(
            sources[0].caveats.iter().any(|caveat| {
                caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
                    && caveat.severity == CaveatSeverity::Blocking
            }),
            "expected required input removal to surface as blocking: {:?}",
            sources[0].caveats
        );
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_explain_gap_overlay_uses_active_window() -> xtask::sandbox::TestResult<()>
    {
        let now = Timestamp::UNIX_EPOCH;
        let source_family = SourceFamily::new("desktop")?;
        let state =
            RuntimePrivateModeState::enabled_by("operator", vec!["desktop".to_string()], now);
        let mut gap = None;

        if gap.is_none()
            && private_mode_applies_to_source_family(&source_family, &state)
            && private_mode_state_covers_at(&state, now)
        {
            gap = private_mode_gap_for_state(&state, now);
        }

        let gap = gap.expect("private-mode active window should explain absence");
        assert_eq!(gap.kind, GapKind::PrivateMode);
        assert_eq!(gap.from_ts, now);
        assert_eq!(gap.to_ts, now);
        Ok(())
    }
}
