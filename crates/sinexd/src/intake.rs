//! One path from an acquisition to the writer, for bindings and API requests.
use crate::stage::Totals;
use crate::writer::{AdmissionCheck, Priority, StageCheckpoint, WriterHandle};
use crate::{Error, Result};
use serde_json::Value;
use sinex_model::{Class, MaterialId, RecordOutcome, RunId};
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
    /// Readings per admission batch; the drain's default when unset.
    pub batch_events: Option<u64>,
    /// The material and its data key are already registered.
    pub registered: Option<MaterialId>,
}

impl Intake<'_> {
    pub(crate) async fn submit(
        &self,
        acquired: drivers::Acquired,
        carry: Value,
        totals: &mut Totals,
    ) -> Result<Submission> {
        self.submit_using(acquired, carry, totals, |batch, check, stage| async move {
            let result = if self.atomic {
                self.writer
                    .submit_checked(self.priority, batch, check)
                    .await
            } else {
                self.writer
                    .submit_source(self.priority, batch, check, stage)
                    .await
            };
            result.map_err(Error::from)
        })
        .await
    }

    pub(crate) async fn submit_checkpoint(
        &self,
        acquired: drivers::Acquired,
        checkpoint: sinex_store::signals::Commit,
    ) -> Result<Submission> {
        let mut checkpoint = Some(checkpoint);
        self.submit_using(
            acquired,
            Value::Null,
            &mut Totals::default(),
            |batch, check, _| {
                let checkpoint = checkpoint.take();
                async move {
                    check()?;
                    let checkpoint = checkpoint.ok_or_else(|| {
                        Error::internal("signal transition spans multiple admission batches")
                    })?;
                    Ok(self.writer.submit_signals(Some(batch), checkpoint).await?)
                }
            },
        )
        .await
    }

    /// Submit planned library curation records, verified in admission.
    pub(crate) async fn submit_curation(&self, acquired: drivers::Acquired) -> Result<Submission> {
        self.submit_using(
            acquired,
            Value::Null,
            &mut Totals::default(),
            |batch, check, _| async move {
                self.writer
                    .submit_curation(self.priority, batch, check)
                    .await
                    .map_err(Error::from)
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
    ) -> Result<Submission>
    where
        F: FnMut(sinex_model::Batch, AdmissionCheck, Option<StageCheckpoint>) -> A,
        A: std::future::Future<Output = Result<Outcome>>,
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
            batch_events: self.batch_events,
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
        // The material's framed records, and how many of them are committed.
        let mut material_framed = None;
        let mut committed_records = 0u64;
        let result = async {
            while let Some(parsed) = batches.next().await {
                let parsed = parsed?;
                let mut permit = parsed.permit;
                if let Some(length) = parsed.material_length {
                    totals.poll(length);
                }
                let mut removed_now = 0;
                if let Some((framed, removed)) = parsed.framed {
                    totals.framed(framed, removed);
                    material_framed = Some(framed);
                    removed_now = removed;
                }
                totals.parse(parsed.elapsed);
                let batch = parsed.batch;
                totals.records(&batch);
                let records = batch.records.len() as u64;
                // A staged binding's accounting once this batch commits, short
                // of admission's decisions, which the writer adds in the
                // batch's transaction.
                let stage = totals.committed.as_ref().map(|committed| {
                    let mut accounting = committed.accounting.clone();
                    accounting.framed += records;
                    accounting.removed += removed_now;
                    accounting.records(&batch.records);
                    accounting.readings += batch.event_count() as u64;
                    StageCheckpoint {
                        operation: committed.operation,
                        accounting,
                    }
                });

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
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert_with(Instant::now);
                    admission_check()
                });
                let admitted = admit(batch, writer_check, stage.clone()).await?;
                let elapsed = started.elapsed();
                let queue = service_started
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .map_or(elapsed, |service| service.duration_since(started));
                totals.writer(queue, elapsed.saturating_sub(queue));
                totals.batch(events, elapsed);
                totals.add_outcome(&admitted);
                if let (Some(committed), Some(stage)) = (totals.committed.as_mut(), stage) {
                    committed.accounting = stage.accounting;
                    crate::stage::decided(&mut committed.accounting, &admitted);
                }
                committed_records += records;
                outcome += &admitted;
                // Streaming submissions do not keep every committed event ID.
                if self.atomic {
                    outcome.admitted_ids = admitted.admitted_ids;
                }
                permit.committed();
            }
            // All of the material is committed: the driver's count replaces
            // the committed records it framed.
            if let (Some(committed), Some(framed)) = (totals.committed.as_mut(), material_framed) {
                committed.accounting.framed =
                    committed.accounting.framed - committed_records + framed;
            }
            Ok::<_, Error>(Submission { outcome, material })
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
) -> Result<Submission> {
    request_inner(
        conn,
        writer,
        blobs,
        source,
        binding,
        host,
        acquired,
        Admission::Plain,
    )
    .await
}

/// [`request`] for planned library curation records: admitted only if they
/// still place as planned.
pub(crate) async fn request_curation(
    conn: &mut PgConnection,
    writer: &WriterHandle,
    blobs: &Blobs,
    source: Arc<dyn Source>,
    binding: &str,
    host: &str,
    acquired: drivers::Acquired,
) -> Result<Submission> {
    request_inner(
        conn,
        writer,
        blobs,
        source,
        binding,
        host,
        acquired,
        Admission::Curation,
    )
    .await
}

/// What a request's admission must also hold.
enum Admission {
    Plain,
    Signals(sinex_store::signals::Commit),
    Curation,
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
) -> Result<Submission> {
    request_inner(
        conn,
        writer,
        blobs,
        source,
        binding,
        host,
        acquired,
        Admission::Signals(checkpoint),
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
    admission: Admission,
) -> Result<Submission> {
    crate::request_deadline::finish_read(conn).await?;
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
    .map_err(|error| Error::from(error).within("starting request run"))?;
    let result = async {
        if let Some(reason) = &acquired.observation.unavailable {
            return Err(Error::internal(format!(
                "request records are unavailable: {reason}"
            )));
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
        .await?;
        let class = request_class(conn, source.as_ref())
            .await?
            .max(match &admission {
                Admission::Signals(commit) => commit.class,
                _ => Class::Open,
            });
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
            batch_events: None,
            registered: None,
        };
        match admission {
            Admission::Signals(checkpoint) => intake.submit_checkpoint(acquired, checkpoint).await,
            Admission::Curation => Ok(intake.submit_curation(acquired).await?),
            Admission::Plain => Ok(intake
                .submit(acquired, Value::Null, &mut Totals::default())
                .await?),
        }
    }
    .await;
    let ended = sinex_store::runs::end(conn, run)
        .await
        .map_err(|error| Error::from(error).within(format!("ending request run {run}")));
    match (result, ended) {
        (result, Ok(())) => result,
        (Ok(_), Err(error)) => Err(error.with_note(format!(
            "request records committed for run {run}, but finalization failed"
        ))),
        (Err(error), Err(ending)) => Err(error.with_note(ending)),
    }
}

/// API requests refuse sealed storage.
pub(crate) async fn request_class(conn: &mut PgConnection, source: &dyn Source) -> Result<Class> {
    let floor = sinex_store::privacy::load(conn, std::iter::once(source.spec().name))
        .await?
        .floors
        .get(source.spec().name)
        .copied()
        .unwrap_or_default();
    let class = source.spec().class.max(floor);
    if class == Class::Sealed {
        return Err(Error::refused("request records cannot be stored sealed"));
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
    batch_events: Option<u64>,
    registered: Option<MaterialId>,
    check: AdmissionCheck,
}

impl Preparation {
    fn run(
        self,
        acquired: drivers::Acquired,
        carry: Value,
        send: blocking::BatchSender,
    ) -> Result<()> {
        let started = Instant::now();
        (self.check)()?;
        let mut framed = acquired
            .material
            .as_ref()
            .map(|m| (m.records.len() as u64, m.removed.len() as u64));
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
                let default = Limits::default();
                Limits {
                    max_events: self.batch_events.map_or(default.max_events, |n| {
                        usize::try_from(n).unwrap_or(usize::MAX)
                    }),
                    ..default
                }
            },
        )?
        .with_host(self.host.clone());
        let mut material_length = Some(prepared.material_len());
        let mut batches = prepared.batches()?;
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
            let mut batch = batch?;
            if self.atomic {
                if batches.next().is_some() {
                    return Err(Error::internal("request records must commit in one batch"));
                }
                if batch
                    .records
                    .iter()
                    .any(|r| matches!(r, RecordOutcome::Failure { .. }))
                {
                    return Err(Error::internal("request records failed to read back"));
                }
            }
            self.material.prepare_batch(&mut batch, self.registered)?;
            (self.check)()?;
            let elapsed = started.elapsed() + std::mem::take(&mut setup);
            if !send.send(blocking::Parsed {
                batch,
                elapsed,
                material_length: material_length.take(),
                framed: framed.take(),
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
