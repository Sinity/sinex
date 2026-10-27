//! Records written through the API (library requests and scans, forecasts):
//! one material, stored and then admitted in one batch.
use crate::writer::{Priority, WriterHandle};
use serde_json::Value;
use sinex_model::{Class, MaterialId, RunId};
use sinex_sources::{Limits, Source, drivers};
use sinex_store::{Blobs, Outcome, bindings};
use sqlx::PgConnection;
use std::sync::Arc;

pub(crate) struct Submission {
    pub outcome: Outcome,
    pub material: Option<MaterialId>,
}

fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// Store and admit what `acquired` holds as one batch of `binding`.
pub(crate) async fn submit(
    conn: &mut PgConnection,
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
    acquired: drivers::Acquired,
) -> Result<Submission, String> {
    if let Some(reason) = &acquired.observation.unavailable {
        return Err(format!("request records are unavailable: {reason}"));
    }
    bindings::save(
        conn,
        &bindings::Stored {
            binding: binding.to_owned(),
            source: source.spec().name.into(),
            input: acquired.input.locator.clone(),
            settings: serde_json::json!({"host": acquired.input.host}),
        },
    )
    .await
    .map_err(error)?;
    let floor = sinex_store::privacy::load(conn, std::iter::once(source.spec().name))
        .await
        .map_err(error)?
        .floors
        .get(source.spec().name)
        .copied()
        .unwrap_or_default();
    let class = source.spec().class.max(floor);
    if class == Class::Sealed {
        return Err("request records cannot be stored sealed".into());
    }
    let batch = {
        let (binding, blobs) = (binding.to_owned(), blobs.clone());
        tokio::task::spawn_blocking(move || {
            prepare(source.as_ref(), &binding, acquired, &blobs, class)
        })
        .await
        .map_err(error)??
    };
    let material = match &batch.kind {
        sinex_model::BatchKind::Source(progress) => progress.material.as_ref().map(|m| m.id),
        _ => unreachable!("drivers build source batches"),
    };
    let outcome = writer.submit(Priority::Live, batch).await.map_err(error)?;
    Ok(Submission { outcome, material })
}

/// Store the bytes and build the one batch.
fn prepare(
    source: &dyn Source,
    binding: &str,
    acquired: drivers::Acquired,
    blobs: &Blobs,
    class: Class,
) -> Result<sinex_model::Batch, String> {
    let prepared = drivers::prepare(
        source,
        binding,
        RunId::new(),
        acquired,
        Value::Null,
        &crate::material_sink::Sink(blobs.clone()),
        Limits {
            max_events: usize::MAX,
            max_bytes: usize::MAX,
        },
    )
    .map_err(error)?;
    let mut batches = prepared.batches().map_err(error)?;
    let mut batch = batches
        .next()
        .ok_or("request records produced no batch")?
        .map_err(error)?;
    if batches.next().is_some() {
        return Err("request records must fit one batch".into());
    }
    if batch
        .records
        .iter()
        .any(|record| matches!(record, sinex_model::RecordOutcome::Failure { .. }))
    {
        return Err("request records failed to read back".into());
    }
    for record in &mut batch.records {
        if let sinex_model::RecordOutcome::Events { events, .. } = record {
            for event in events {
                event.class = event.class.max(class);
            }
        }
    }
    Ok(batch)
}
