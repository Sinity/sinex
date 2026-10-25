//! Records written through the API (library requests and scans, forecasts):
//! one material, stored and then admitted in one batch, under the source's
//! guard so no excision of the source falls between its exclusions and the
//! admission.
use crate::excision::exclusions::Exclusions;
use crate::writer::{Priority, WriterHandle};
use serde_json::Value;
use sinex_model::{BatchKind, Class, MaterialId, RunId};
use sinex_sources::{Limits, Source, drivers};
use sinex_store::{Blobs, Outcome, bindings};
use std::sync::Arc;

pub(crate) struct Authored {
    pub outcome: Outcome,
    pub material: Option<MaterialId>,
}

fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// Store and admit what `acquired` holds as one batch of `binding`.
pub(crate) async fn submit(
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
    acquired: drivers::Acquired,
) -> Result<Authored, String> {
    let mut guard = writer.source_guard(source.spec().name).await?;
    let checked = async {
        if let Some(reason) = &acquired.observation.unavailable {
            return Err(format!("authored records are unavailable: {reason}"));
        }
        bindings::save(
            guard.connection(),
            &bindings::Stored {
                binding: binding.to_owned(),
                source: source.spec().name.into(),
                input: acquired.input.locator.clone(),
                settings: serde_json::json!({"host": acquired.input.host}),
            },
        )
        .await
        .map_err(error)?;
        let exclusions = Exclusions::load(guard.connection(), source.spec().name).await?;
        let floor =
            sinex_store::privacy::load(guard.connection(), std::iter::once(source.spec().name))
                .await
                .map_err(error)?
                .floors
                .get(source.spec().name)
                .copied()
                .unwrap_or_default();
        let class = source.spec().class.max(floor);
        if class == Class::Sealed {
            return Err("authored records cannot be stored sealed".into());
        }
        Ok((exclusions, class))
    }
    .await;
    let (exclusions, class) = match checked {
        Ok(value) => value,
        Err(e) => {
            guard.release().await?;
            return Err(e);
        }
    };
    let prepared = {
        let (source, binding, blobs) = (source.clone(), binding.to_owned(), blobs.clone());
        let exclusions = Arc::new(exclusions);
        let filter = exclusions.clone();
        tokio::task::spawn_blocking(move || {
            prepare(source.as_ref(), &binding, acquired, &blobs, &filter, class)
        })
        .await
        .map_err(error)
        .and_then(|batch| batch)
        .map(|batch| (batch, exclusions))
    };
    let (mut batch, exclusions) = match prepared {
        Ok(value) => value,
        Err(e) => {
            guard.release().await?;
            return Err(e);
        }
    };
    let cited = batch
        .events()
        .flat_map(|event| event.cites.iter().cloned())
        .collect::<Vec<_>>();
    if let Err(e) = exclusions.check_cited(&cited) {
        guard.release().await?;
        return Err(e);
    }
    let BatchKind::Source(progress) = &mut batch.kind else {
        unreachable!("drivers build source batches")
    };
    progress.exclusions = Some(exclusions.version().clone());
    let material = progress.material.as_ref().map(|m| m.id);
    let outcome = writer
        .guarded_slot(Priority::Live)
        .await
        .map_err(error)?
        .admit(batch, guard, Arc::new(|| Ok(())))
        .await
        .map_err(error)?;
    Ok(Authored { outcome, material })
}

/// Apply exclusions, store the bytes and build the one batch.
fn prepare(
    source: &dyn Source,
    binding: &str,
    acquired: drivers::Acquired,
    blobs: &Blobs,
    exclusions: &Exclusions,
    class: Class,
) -> Result<sinex_model::Batch, String> {
    let acquired = exclusions.filter(source, acquired, &Value::Null, class, false)?;
    let prepared = drivers::prepare(
        source,
        binding,
        RunId::new(),
        acquired,
        Value::Null,
        &crate::one_shot::Sink(blobs.clone()),
        Limits {
            max_events: usize::MAX,
            max_bytes: usize::MAX,
        },
    )
    .map_err(error)?;
    let mut batches = prepared.batches().map_err(error)?;
    let mut batch = batches
        .next()
        .ok_or("authored records produced no batch")?
        .map_err(error)?;
    if batches.next().is_some() {
        return Err("authored records must fit one batch".into());
    }
    if batch
        .records
        .iter()
        .any(|record| matches!(record, sinex_model::RecordOutcome::Failure { .. }))
    {
        return Err("authored records failed to read back".into());
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
