//! One path from an acquisition to the writer, for bindings and API requests.
use crate::backfill::Totals;
use crate::writer::{AdmissionCheck, Priority, WriterHandle};
use serde_json::Value;
use sinex_model::{Class, EventPayload, MaterialId, RecordOutcome, RunId};
use sinex_source_core::{Limits, Source, drivers};
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
    pub source: Arc<dyn Source>,
    pub binding: &'a str,
    pub host: Option<&'a str>,
    pub run: RunId,
    pub material: crate::material_access::MaterialAccess,
    pub check: AdmissionCheck,
    pub priority: Priority,
    /// Requests commit together and refuse parser failures before admission.
    pub atomic: bool,
    /// The material and its data key are already registered.
    pub registered: Option<MaterialId>,
}

impl Intake<'_> {
    pub(crate) async fn submit(
        &self,
        acquired: drivers::Acquired,
        carry: Value,
        totals: &mut Totals,
    ) -> Result<Submission, String> {
        self.submit_using(acquired, carry, totals, |batch, check| async move {
            let result = if self.atomic {
                self.writer
                    .submit_checked(self.priority, batch, check)
                    .await
            } else {
                self.writer.submit_source(self.priority, batch, check).await
            };
            result.map_err(|e| e.to_string())
        })
        .await
    }

    pub(crate) async fn submit_checkpoint(
        &self,
        acquired: drivers::Acquired,
        checkpoint: sinex_store::signals::Commit,
    ) -> Result<Submission, String> {
        let mut checkpoint = Some(checkpoint);
        self.submit_using(
            acquired,
            Value::Null,
            &mut Totals::default(),
            |batch, check| {
                let checkpoint = checkpoint.take();
                async move {
                    check()?;
                    let mut checkpoint =
                        checkpoint.ok_or("signal transition spans multiple admission batches")?;
                    for decision in &mut checkpoint.decisions {
                        let id = decision.payload["delivery_id"].as_str();
                        let handoff = batch
                            .events()
                            .find(|event| {
                                event.event_type
                                    == sinex_model::event_types::signal::HandoffRecorded::EVENT_TYPE
                                    && event.payload["delivery_id"].as_str() == id
                            })
                            .ok_or("signal decision lacks its physical handoff record")?;
                        if let sinex_model::Origin::Derived { parents } = &mut decision.origin {
                            parents.push(handoff.id);
                        }
                    }
                    self.writer
                        .submit_signals(batch, checkpoint)
                        .await
                        .map_err(|error| error.to_string())
                }
            },
        )
        .await
    }

    async fn submit_using<F, A>(
        &self,
        acquired: drivers::Acquired,
        carry: Value,
        totals: &mut Totals,
        mut admit: F,
    ) -> Result<Submission, String>
    where
        F: FnMut(sinex_model::Batch, AdmissionCheck) -> A,
        A: std::future::Future<Output = Result<Outcome, String>>,
    {
        let check = self.material.admission_check(self.check.clone());
        check()?;
        let material = acquired.material.as_ref().map(|m| m.id);
        let work = Preparation {
            source: self.source.clone(),
            binding: self.binding.to_owned(),
            host: self.host.map(str::to_owned),
            run: self.run,
            material: self.material.clone(),
            atomic: self.atomic,
            registered: self.registered,
            check: check.clone(),
        };
        let mut batches =
            blocking::Batches::start(self.priority, move |send| work.run(acquired, carry, send))
                .await?;
        let cancelled = batches.cancellation_check();
        let check: AdmissionCheck = Arc::new(move || {
            cancelled()?;
            check()
        });
        let mut outcome = Outcome::default();
        let result = async {
            while let Some(parsed) = batches.next().await {
                let parsed = parsed?;
                let mut permit = parsed.permit;
                if let Some(length) = parsed.material_length {
                    totals.poll(length);
                }
                totals.parse(parsed.elapsed);
                let batch = parsed.batch;

                check()?;
                let events = batch.event_count();
                let started = Instant::now();
                // The first writer check starts service; the answer confirms the commit.
                let service_started = Arc::new(std::sync::Mutex::new(None));
                let service_clock = service_started.clone();
                let admission_check = check.clone();
                let writer_check: AdmissionCheck = Arc::new(move || {
                    service_clock
                        .lock()
                        .map_err(|e| e.to_string())?
                        .get_or_insert_with(Instant::now);
                    admission_check()
                });
                let admitted = admit(batch, writer_check).await?;
                let elapsed = started.elapsed();
                let queue = service_started
                    .lock()
                    .map_err(|e| e.to_string())?
                    .map_or(elapsed, |service| service.duration_since(started));
                totals.writer(queue, elapsed.saturating_sub(queue));
                totals.batch(events, elapsed);
                totals.add_outcome(&admitted);
                outcome += &admitted;
                // Streaming submissions do not keep every committed event ID.
                if self.atomic {
                    outcome.admitted_ids = admitted.admitted_ids;
                }
                permit.committed();
            }
            Ok(Submission { outcome, material })
        }
        .await;
        batches.finish().await?;
        result
    }
}

/// Record an authenticated API acquisition, then use the ordinary submit path.
pub(crate) async fn request(
    conn: &mut PgConnection,
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
    host: &str,
    acquired: drivers::Acquired,
) -> Result<Submission, String> {
    request_inner(conn, writer, blobs, source, binding, host, acquired, None).await
}

pub(crate) async fn request_checkpoint(
    conn: &mut PgConnection,
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
    host: &str,
    acquired: drivers::Acquired,
    checkpoint: sinex_store::signals::Commit,
) -> Result<Submission, String> {
    request_inner(
        conn,
        writer,
        blobs,
        source,
        binding,
        host,
        acquired,
        Some(checkpoint),
    )
    .await
}

async fn request_inner(
    conn: &mut PgConnection,
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
    host: &str,
    acquired: drivers::Acquired,
    checkpoint: Option<sinex_store::signals::Commit>,
) -> Result<Submission, String> {
    let spec = source.spec();
    let version = spec.parser_version.to_string();
    let run = sinex_store::runs::start(
        conn,
        &sinex_store::runs::Manifest {
            name: spec.name,
            kind: "source",
            version: &version,
            build: crate::build_identity::current()?,
            emits: spec.event_types,
        },
        host,
    )
    .await
    .map_err(|error| format!("starting request run: {error}"))?;
    let result = async {
        if let Some(reason) = &acquired.observation.unavailable {
            return Err(format!("request records are unavailable: {reason}"));
        }
        bindings::save(
            conn,
            &bindings::Stored {
                binding: binding.to_owned(),
                source: source.spec().name.into(),
                input: acquired.input.locator.clone(),
                settings: serde_json::json!({"host": host}),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        let class = request_class(conn, source.as_ref()).await?.max(
            checkpoint
                .as_ref()
                .map_or(Class::Open, |commit| commit.class),
        );
        let material = crate::material_access::MaterialAccess::new(
            blobs.clone(),
            class,
            crate::material_access::Access::default(),
        )?;
        let intake = Intake {
            writer,
            source,
            binding,
            host: Some(host),
            run,
            material,
            check: Arc::new(|| Ok(())),
            priority: Priority::Live,
            atomic: true,
            registered: None,
        };
        match checkpoint {
            Some(checkpoint) => intake.submit_checkpoint(acquired, checkpoint).await,
            None => {
                intake
                    .submit(acquired, Value::Null, &mut Totals::default())
                    .await
            }
        }
    }
    .await;
    let ended = sinex_store::runs::end(conn, run)
        .await
        .map_err(|error| format!("ending request run {run}: {error}"));
    match (result, ended) {
        (result, Ok(())) => result,
        (Ok(_), Err(error)) => Err(format!(
            "request records committed for run {run}, but finalization failed: {error}"
        )),
        (Err(error), Err(ending)) => Err(format!("{error}; {ending}")),
    }
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

struct Preparation {
    source: Arc<dyn Source>,
    binding: String,
    host: Option<String>,
    run: RunId,
    material: crate::material_access::MaterialAccess,
    atomic: bool,
    registered: Option<MaterialId>,
    check: AdmissionCheck,
}

impl Preparation {
    fn run(
        self,
        acquired: drivers::Acquired,
        carry: Value,
        send: blocking::BatchSender,
    ) -> Result<(), String> {
        let started = Instant::now();
        (self.check)()?;
        let prepared = drivers::prepare(
            self.source.as_ref(),
            &self.binding,
            self.run,
            acquired,
            carry,
            self.material.sink(),
            if self.atomic {
                Limits {
                    max_events: usize::MAX,
                    max_bytes: usize::MAX,
                }
            } else {
                Limits::default()
            },
        )
        .map_err(|e| e.to_string())?
        .with_host(self.host.clone());
        let mut material_length = Some(prepared.material_len());
        let mut batches = prepared.batches().map_err(|e| e.to_string())?;
        let mut setup = started.elapsed();
        loop {
            // The permit covers parsing, waiting in the channel, and the commit answer.
            let Some(permit) = send.permit() else {
                return Ok(());
            };
            (self.check)()?;
            let started = Instant::now();
            let Some(batch) = batches.next() else {
                return Ok(());
            };
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
            self.material.prepare_batch(&mut batch, self.registered)?;
            (self.check)()?;
            let elapsed = started.elapsed() + std::mem::take(&mut setup);
            if !send.send(blocking::Parsed {
                batch,
                elapsed,
                material_length: material_length.take(),
                permit,
            }) {
                return Ok(());
            }
        }
    }
}

pub(crate) mod blocking;

#[cfg(test)]
mod tests;
