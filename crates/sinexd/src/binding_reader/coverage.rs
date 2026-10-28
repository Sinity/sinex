//! A read outcome is evidence even when no source record changed.
use crate::backfill::Totals;
use crate::material_access::Access;
use crate::writer::{Priority, WriterHandle};
use sinex_model::event_types::coverage::{CoverageCompleteness, CoverageState, SourceCoverage};
use sinex_model::{Class, RunId, Timestamp};
use sinex_source_core::Binding;
use sinex_sources::sources::coverage;
use sinex_store::{Blobs, bindings};
use sqlx::PgConnection;
use std::sync::Arc;

pub(super) fn outcome(
    binding: &Binding,
    started_at: Timestamp,
    result: &Result<Totals, String>,
) -> SourceCoverage {
    let (state, reason, events, failures) = match result {
        Ok(totals) if totals.failures == 0 => (
            CoverageState::RecordedUnknownCoverage,
            Some(if totals.events_offered == 0 {
                "input checked successfully; no new events".into()
            } else {
                "input read successfully; temporal coverage is not declared".into()
            }),
            totals.events_offered as u64,
            0,
        ),
        Ok(totals) => (
            CoverageState::Partial,
            Some("some source records failed to parse".into()),
            totals.events_offered as u64,
            totals.failures as u64,
        ),
        Err(error) => (CoverageState::Missing, Some(error.clone()), 0, 1),
    };
    SourceCoverage {
        observation: uuid::Uuid::now_v7().to_string(),
        source: binding.source.clone(),
        binding: binding.name.clone(),
        host: binding.settings.host.clone(),
        started_at,
        ended_at: Timestamp::now(),
        state,
        reason,
        completeness: CoverageCompleteness::Unknown,
        bounded_by: "read attempt; source coverage interval is unknown".into(),
        events,
        failures,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn flush(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    run: RunId,
    access: Access<'_>,
    pending: &mut Vec<SourceCoverage>,
    results: &mut Vec<(String, Result<Totals, String>)>,
) {
    for observation in pending.drain(..) {
        let source = observation.source.clone();
        if let Err(error) = record(conn, blobs, writer, run, access, observation).await {
            eprintln!("sinexd: source coverage evidence for {source}: {error}");
            results.push((source, Err(format!("source coverage evidence: {error}"))));
        }
    }
}

async fn record(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    run: RunId,
    access: Access<'_>,
    observation: SourceCoverage,
) -> Result<(), String> {
    let binding = format!("source.observation:{}", observation.binding);
    let acquired = coverage::observation_material(observation.clone());
    bindings::save(conn, &bindings::Stored {
        binding: binding.clone(), source: coverage::OBSERVATION.name.into(),
        input: acquired.input.locator.clone(),
        settings: serde_json::json!({"host": acquired.input.host}),
    }).await.map_err(|error| error.to_string())?;
    let floor = sinex_store::privacy::load(conn, [observation.source.as_str(), coverage::OBSERVATION.name].into_iter())
        .await.map_err(|error| error.to_string())?
        .floors.values().copied().max().unwrap_or_default();
    let declared = sinex_sources::sources::specs().into_iter()
        .filter(|spec| spec.name == observation.source)
        .map(|spec| spec.class).max().unwrap_or_default();
    let settings = bindings::load(conn, &observation.binding).await.map_err(|error| error.to_string())?
        .and_then(|saved| serde_json::from_value::<sinex_source_core::Settings>(saved.settings).ok())
        .and_then(|settings| settings.class).unwrap_or_default();
    let class = Class::Open.max(floor).max(declared).max(settings);
    let material = crate::material_access::MaterialAccess::new(blobs.clone(), class, access)?;
    let admitted = crate::intake::Intake {
        writer, source: Arc::new(coverage::observation_source()), binding: &binding, run,
        material, check: Arc::new(|| Ok(())), priority: Priority::Live, atomic: true,
        registered: None,
    }.submit(acquired, serde_json::Value::Null, &mut Totals::default()).await?;
    if admitted.outcome.failures != 0 || admitted.outcome.admitted != 1 {
        return Err("read outcome was not admitted as one coverage event".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
