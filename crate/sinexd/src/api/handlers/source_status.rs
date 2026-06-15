//! Operator-facing source status handler.

use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::rpc::source_status::{
    SourceStatus, SourcesStatusRequest, SourcesStatusResponse, SourcesStatusViewRequest,
};
use sinex_primitives::source_contracts::{
    SourceContract, SourceRuntimeBinding, all_source_contracts, source_runtime_bindings,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, CaveatView, CoverageGapView,
    SourceCoverageContinuity, SourceCoverageListView, SourceCoverageReadiness, SourceCoverageView,
    SourcePrivacyPosture, ViewEnvelope,
};
use sqlx::FromRow;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use time::OffsetDateTime;

type Result<T> = std::result::Result<T, SinexError>;

#[derive(Debug, FromRow)]
struct SourceEventAggregateRow {
    source: String,
    event_type: String,
    event_count: i64,
    last_event_at: Option<OffsetDateTime>,
}

#[derive(Debug, FromRow)]
struct SourceMaterialAggregateRow {
    source_identifier: String,
    material_count: i64,
    last_material_at: Option<OffsetDateTime>,
}

/// List registered sources with run, health, and recent-emission stats.
///
/// Mirrors `handle_automata_status` (`automata.status`) for the source-side
/// surface; filtered to `manifest_type = 'source'`.
pub async fn handle_sources_status(
    pool: &PgPool,
    request: SourcesStatusRequest,
) -> Result<SourcesStatusResponse> {
    let stale_after = Duration::from_secs(request.stale_after_secs);
    let recent_window = Duration::from_secs(request.recent_window_secs);
    let sources = pool
        .state()
        .list_sources_status(stale_after, recent_window)
        .await
        .map_err(|e| SinexError::database("Failed to list sources status").with_std_error(&e))?
        .into_iter()
        .map(|row| SourceStatus {
            module_name: row.module_name,
            version: row.version,
            description: row.description,
            manifest_status: row.manifest_status.unwrap_or_default(),
            live: row.live,
            service_name: row.service_name,
            instance_id: row.instance_id,
            module_run_id: row.module_run_id,
            host: row.host,
            run_status: row.run_status,
            started_at: row.started_at,
            last_heartbeat_at: row.last_heartbeat_at,
            current_health: row.current_health,
            health_changed_at: row.health_changed_at,
            health_reason: row.health_reason,
            recent_output_count: row.recent_output_count,
            last_output_at: row.last_output_at,
        })
        .collect();

    let response = SourcesStatusResponse {
        generated_at: Timestamp::now(),
        stale_after_secs: request.stale_after_secs,
        recent_window_secs: request.recent_window_secs,
        sources,
    };

    Ok(response)
}

pub async fn handle_sources_status_view(
    pool: &PgPool,
    _request: SourcesStatusViewRequest,
) -> Result<ViewEnvelope<SourceCoverageListView>> {
    let event_rows = sqlx::query_as!(
        SourceEventAggregateRow,
        r#"
        SELECT
            source,
            event_type,
            COUNT(*)::bigint as "event_count!",
            MAX(ts_orig) as "last_event_at: _"
        FROM core.events
        GROUP BY source, event_type
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to compute source event coverage").with_std_error(&error)
    })?;

    let material_rows = sqlx::query_as!(
        SourceMaterialAggregateRow,
        r#"
        SELECT
            source_identifier,
            COUNT(*)::bigint as "material_count!",
            MAX(COALESCE(end_time, start_time, staged_at)) as "last_material_at: _"
        FROM raw.source_material_registry
        GROUP BY source_identifier
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to compute source material coverage").with_std_error(&error)
    })?;

    let mut event_aggregates = HashMap::<(String, String), SourceEventAggregateRow>::new();
    for row in event_rows {
        event_aggregates.insert((row.source.clone(), row.event_type.clone()), row);
    }
    let material_aggregates: HashMap<String, SourceMaterialAggregateRow> = material_rows
        .into_iter()
        .map(|row| (row.source_identifier.clone(), row))
        .collect();

    let bindings: Vec<&'static SourceRuntimeBinding> = source_runtime_bindings().collect();
    let mut contracts: Vec<&'static SourceContract> = all_source_contracts().collect();
    contracts.sort_by_key(|contract| contract.id);

    let mut views = Vec::with_capacity(contracts.len());
    for contract in contracts {
        let source_bindings: Vec<_> = bindings
            .iter()
            .copied()
            .filter(|binding| binding.source_id == contract.id)
            .collect();
        views.push(source_coverage_view(
            contract,
            &source_bindings,
            &event_aggregates,
            &material_aggregates,
        ));
    }

    Ok(ViewEnvelope::new(
        "sinexd.sources.status.view",
        SourceCoverageListView::new(views),
    ))
}

fn source_coverage_view(
    contract: &SourceContract,
    bindings: &[&SourceRuntimeBinding],
    event_aggregates: &HashMap<(String, String), SourceEventAggregateRow>,
    material_aggregates: &HashMap<String, SourceMaterialAggregateRow>,
) -> SourceCoverageView {
    let mut event_count = 0i64;
    let mut last_event_at = None;
    let event_types: Vec<String> = contract
        .event_types
        .iter()
        .map(|(source, event_type)| {
            if let Some(row) = event_aggregates.get(&(source.to_string(), event_type.to_string())) {
                event_count += row.event_count;
                last_event_at = max_timestamp(last_event_at, row.last_event_at);
            }
            format!("{source}/{event_type}")
        })
        .collect();

    let material = material_aggregates.get(contract.id);
    let material_count = material.map_or(0, |row| row.material_count);
    let last_material_at = material.and_then(|row| row.last_material_at.map(Timestamp::from));
    let last_event_at = last_event_at.map(Timestamp::from);
    let live_binding_count = bindings.iter().filter(|binding| !binding.proposed).count();
    let proposed_binding_count = bindings.len().saturating_sub(live_binding_count);
    let has_live_binding = live_binding_count > 0;
    let has_material = material_count > 0;
    let has_events = event_count > 0;

    let mut gaps = Vec::new();
    let mut caveats = Vec::new();
    if bindings.is_empty() {
        gaps.push(CoverageGapView {
            kind: "missing_binding".to_string(),
            message: "source contract has no runtime binding".to_string(),
        });
    }
    if !has_material {
        gaps.push(CoverageGapView {
            kind: "missing_material".to_string(),
            message: "no source material is directly registered under this source id".to_string(),
        });
        caveats.push(CaveatView {
            id: "source.material.match.v0_exact_id".to_string(),
            message: "v0 material coverage only counts source_material_registry rows whose source_identifier exactly equals the source id".to_string(),
            ref_: None,
        });
    }
    if !has_events {
        gaps.push(CoverageGapView {
            kind: "missing_events".to_string(),
            message: "no live events match the contract's declared source/event_type pairs"
                .to_string(),
        });
    }

    let readiness = if !has_live_binding && proposed_binding_count > 0 {
        SourceCoverageReadiness::Proposed
    } else if !has_live_binding {
        SourceCoverageReadiness::MissingBinding
    } else if !has_material {
        SourceCoverageReadiness::MissingMaterial
    } else if !has_events {
        SourceCoverageReadiness::MissingEvents
    } else {
        SourceCoverageReadiness::Ready
    };
    let continuity = match (has_material, has_events) {
        (true, true) => SourceCoverageContinuity::Active,
        (true, false) => SourceCoverageContinuity::MaterialOnly,
        (false, true) => SourceCoverageContinuity::EventOnly,
        (false, false) if bindings.is_empty() => SourceCoverageContinuity::Unknown,
        (false, false) => SourceCoverageContinuity::Gapped,
    };
    let privacy_context = bindings
        .iter()
        .copied()
        .find(|binding| !binding.proposed)
        .or_else(|| bindings.first().copied())
        .map_or("none".to_string(), |binding| {
            format!("{:?}", binding.privacy_context).to_ascii_lowercase()
        });

    SourceCoverageView {
        source_id: contract.id.to_string(),
        namespace: contract.namespace.to_string(),
        event_types,
        readiness,
        continuity,
        last_material_at,
        last_event_at,
        material_count,
        event_count,
        binding_count: bindings.len(),
        live_binding_count,
        proposed_binding_count,
        gaps,
        caveats,
        privacy: SourcePrivacyPosture {
            tier: format!("{:?}", contract.privacy_tier).to_ascii_lowercase(),
            context: privacy_context,
            proposed: live_binding_count == 0 && proposed_binding_count > 0,
        },
        actions: source_actions(contract.id),
    }
}

fn max_timestamp(
    current: Option<OffsetDateTime>,
    candidate: Option<OffsetDateTime>,
) -> Option<OffsetDateTime> {
    match (current, candidate) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (None, Some(b)) => Some(b),
        (Some(a), None) => Some(a),
        (None, None) => None,
    }
}

fn source_actions(source_id: &str) -> Vec<ActionAvailability> {
    vec![
        ActionAvailability::read(
            "sources.readiness",
            "Readiness",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint(format!("sinexctl sources readiness {source_id}"))
        .with_rpc_method("sources.readiness.get"),
        ActionAvailability::read(
            "sources.coverage",
            "Coverage",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint("sinexctl sources coverage")
        .with_rpc_method("sources.coverage"),
    ]
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use sinex_primitives::privacy::ProcessingContext;
    use sinex_primitives::source_contracts::{
        AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
        RetentionPolicy, RunnerPack, RuntimeShape, SourceBuildImpact, SubjectRef,
    };
    use xtask::sandbox::sinex_test;

    static CONTRACT: SourceContract = SourceContract {
        id: "fixture.source",
        namespace: "fixture",
        event_types: &[("fixture", "fixture.event")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_scope: AccessScope::StagedExport,
    };

    static BINDING: SourceRuntimeBinding = SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:fixture.source"),
        "fixture.source",
        "fixture",
    )
    .implementation("sinexd")
    .adapter("StaticFileAdapter")
    .output_event_type("fixture.event")
    .privacy_context(ProcessingContext::Command)
    .resource_profile(ResourceProfile::BoundedFile)
    .source_id("fixture.source")
    .runner_pack(RunnerPack::SinexdSource)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .build_impact(SourceBuildImpact::ZERO)
    .build();

    #[sinex_test]
    async fn source_coverage_view_marks_ready_when_catalog_material_and_events_exist()
    -> xtask::TestResult<()> {
        let now = OffsetDateTime::now_utc();
        let mut events = HashMap::new();
        events.insert(
            ("fixture".to_string(), "fixture.event".to_string()),
            SourceEventAggregateRow {
                source: "fixture".to_string(),
                event_type: "fixture.event".to_string(),
                event_count: 3,
                last_event_at: Some(now),
            },
        );
        let mut materials = HashMap::new();
        materials.insert(
            "fixture.source".to_string(),
            SourceMaterialAggregateRow {
                source_identifier: "fixture.source".to_string(),
                material_count: 2,
                last_material_at: Some(now),
            },
        );

        let view = source_coverage_view(&CONTRACT, &[&BINDING], &events, &materials);

        assert_eq!(view.readiness, SourceCoverageReadiness::Ready);
        assert_eq!(view.continuity, SourceCoverageContinuity::Active);
        assert_eq!(view.event_count, 3);
        assert_eq!(view.material_count, 2);
        assert!(view.gaps.is_empty());
        assert_eq!(view.privacy.tier, "sensitive");
        assert_eq!(view.privacy.context, "command");
        assert!(
            view.actions
                .iter()
                .any(|action| action.id == "sources.readiness")
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_coverage_view_surfaces_missing_material_caveat() -> xtask::TestResult<()> {
        let events = HashMap::new();
        let materials = HashMap::new();

        let view = source_coverage_view(&CONTRACT, &[&BINDING], &events, &materials);

        assert_eq!(view.readiness, SourceCoverageReadiness::MissingMaterial);
        assert_eq!(view.continuity, SourceCoverageContinuity::Gapped);
        assert!(view.gaps.iter().any(|gap| gap.kind == "missing_material"));
        assert!(
            view.caveats
                .iter()
                .any(|caveat| caveat.id == "source.material.match.v0_exact_id")
        );
        Ok(())
    }
}
