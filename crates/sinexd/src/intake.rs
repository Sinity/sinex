//! One path from an acquisition to the writer, for bindings and API requests.
use crate::backfill::Totals;
use crate::writer::{AdmissionCheck, Priority, WriterHandle};
use serde_json::Value;
use sinex_model::{BatchKind, Class, MaterialId, RecordOutcome, RunId};
use sinex_sources::{Limits, Source, drivers};
use sinex_store::{Blobs, Outcome, bindings};
use sqlx::PgConnection;
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct Submission {
    pub outcome: Outcome,
    pub material: Option<MaterialId>,
}

pub(crate) struct Intake<'a> {
    pub writer: &'a WriterHandle,
    pub source: &'a dyn Source,
    pub binding: &'a str,
    pub run: RunId,
    pub sink: &'a dyn drivers::MaterialSink,
    pub class: Class,
    pub sealing: Option<&'a crate::material_sink::SealingSink>,
    pub unlock: Option<&'a crate::sealed_access::Unlock>,
    pub check: AdmissionCheck,
    pub priority: Priority,
    /// Requests commit together and refuse parser failures before admission.
    pub atomic: bool,
    /// The material and its data key are already registered.
    pub registered: bool,
}

impl Intake<'_> {
    pub(crate) async fn submit(
        &self,
        acquired: drivers::Acquired,
        carry: Value,
        totals: &mut Totals,
    ) -> Result<Submission, String> {
        (self.check)()?;
        let material = acquired.material.as_ref().map(|m| m.id);
        let prepared = drivers::prepare(
            self.source,
            self.binding,
            self.run,
            acquired,
            carry,
            self.sink,
            if self.atomic {
                Limits {
                    max_events: usize::MAX,
                    max_bytes: usize::MAX,
                }
            } else {
                Limits::default()
            },
        )
        .map_err(|e| e.to_string())?;
        totals.poll(prepared.material_len());
        let mut batches = prepared.batches().map_err(|e| e.to_string())?;
        let mut outcome = Outcome::default();
        while let Some(batch) = batches.next() {
            let mut batch = batch.map_err(|e| e.to_string())?;
            if self.atomic {
                if batches.next().is_some() {
                    return Err("request records must commit in one batch".into());
                }
                if batch
                    .records
                    .iter()
                    .any(|r| matches!(r, RecordOutcome::Failure { .. }))
                {
                    return Err("request records failed to read back".into());
                }
            }
            for record in &mut batch.records {
                if let RecordOutcome::Events { events, .. } = record {
                    for event in events {
                        event.class = event.class.max(self.class);
                    }
                }
            }
            if let Some(sealing) = self.sealing {
                sealing.seal(std::slice::from_mut(&mut batch))?;
                if self.registered
                    && let BatchKind::Source(progress) = &mut batch.kind
                {
                    progress.data_keys.clear();
                }
            }
            let check = self.check.clone();
            let unlock = self.unlock.cloned();
            let check: AdmissionCheck = Arc::new(move || {
                check()?;
                if let Some(unlock) = &unlock {
                    unlock.check()?;
                }
                Ok(())
            });
            check()?;
            let events = batch.event_count();
            let started = Instant::now();
            let admitted = self
                .writer
                .submit_checked(self.priority, batch, check)
                .await
                .map_err(|e| e.to_string())?;
            totals.batch(events, started.elapsed());
            totals.add_outcome(&admitted);
            outcome.admitted += admitted.admitted;
            outcome.repeats += admitted.repeats;
            outcome.replaced += admitted.replaced;
            outcome.older += admitted.older;
            outcome.hidden += admitted.hidden;
            outcome.superseded += admitted.superseded;
            outcome.failures += admitted.failures;
            outcome.dirty_keys += admitted.dirty_keys;
            outcome.replay_archived += admitted.replay_archived;
            if let Some((start, end)) = admitted.commit_seq {
                outcome.commit_seq =
                    Some((outcome.commit_seq.map_or(start, |(first, _)| first), end));
            }
            // Streaming readers hold only one batch's results at a time.
            if self.atomic {
                outcome.admitted_ids = admitted.admitted_ids;
            }
        }
        Ok(Submission { outcome, material })
    }
}

/// Record an authenticated API acquisition, then use the ordinary submit path.
pub(crate) async fn request(
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
    .map_err(|e| e.to_string())?;
    let class = request_class(conn, source.as_ref()).await?;
    Intake {
        writer,
        source: source.as_ref(),
        binding,
        run: RunId::new(),
        sink: &crate::material_sink::Sink(blobs.clone()),
        class,
        sealing: None,
        unlock: None,
        check: Arc::new(|| Ok(())),
        priority: Priority::Live,
        atomic: true,
        registered: false,
    }
    .submit(acquired, Value::Null, &mut Totals::default())
    .await
}

/// API requests refuse sealed storage.
pub(crate) async fn request_class(
    conn: &mut PgConnection,
    source: &dyn Source,
) -> Result<Class, String> {
    let floor = sinex_store::privacy::load(conn, std::iter::once(source.spec().name))
        .await
        .map_err(|e| e.to_string())?
        .floors
        .get(source.spec().name)
        .copied()
        .unwrap_or_default();
    let class = source.spec().class.max(floor);
    if class == Class::Sealed {
        return Err("request records cannot be stored sealed".into());
    }
    Ok(class)
}

#[cfg(test)]
mod tests;
