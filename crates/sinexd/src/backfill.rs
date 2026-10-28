//! Read configured sources to the end through the writer, reporting counts and
//! rates. The same driver, drain and admission path the daemon uses; only the
//! scheduling differs (read until caught up, then stop).
//!
//! Every `--read FORMAT=PATH` is planned by its format into bindings; each
//! binding is recorded with its settings (so a replay reads it the same way)
//! and its driver state is rebuilt from the materials it stored, so a
//! second run reads only what changed.
use crate::material_access::Sink;
use crate::reader_context::ReaderContext;
use crate::writer;
use sinex_model::RunId;
use sinex_source_core::{Binding, InputFormat, Settings, Source, drivers};
use sinex_sources::sources;
use sinex_store::{bindings, positions, runs};
use sqlx::PgConnection;
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub(crate) struct Options {
    pub database: String,
    pub store: PathBuf,
    /// `format=path`, one per input; see [`sources::formats`].
    pub reads: Vec<String>,
    /// The zone sources with local times were written in (IANA name); the
    /// system zone when unset. Recorded with each binding.
    pub zone: Option<String>,
    /// A source option as `key=value` (a clipboard spool's `selection`).
    pub options: Vec<String>,
    pub host: String,
    /// Print one JSON object (per-source totals, peak memory) instead of
    /// lines.
    pub json: bool,
    pub identity_file: Option<PathBuf>,
}

/// The formats `--read FORMAT=PATH` accepts.
fn format_names() -> Vec<&'static str> {
    sources::formats()
        .iter()
        .filter(|k| k.plan.is_some())
        .map(|k| k.name)
        .collect()
}

#[derive(Default)]
pub(crate) struct Totals {
    polls: usize,
    materials: usize,
    material_bytes: usize,
    batches: usize,
    pub(crate) events_offered: usize,
    pub(crate) admitted: usize,
    repeats: usize,
    pub(crate) replaced: usize,
    older: usize,
    hidden: usize,
    superseded: usize,
    failures: usize,
    dirty_keys: usize,
    admit_time: Duration,
    poll_time: Duration,
    parse_time: Duration,
    queue_time: Duration,
    service_time: Duration,
    wall: Duration,
    files_stored: u64,
    pass_decoded_bytes: u64,
    pass_record_bytes: u64,
}

impl std::fmt::Display for Totals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rate = |n: usize, d: Duration| {
            if d.is_zero() {
                0.0
            } else {
                n as f64 / d.as_secs_f64()
            }
        };
        write!(
            f,
            "polls {} | materials {} ({} bytes) | batches {} | events offered {} | admitted {} | repeats {} | replaced {} | older {} | hidden {} | superseded {} | failures {} | dirty keys {} | wall {:.2}s | admission {:.2}s | {:.0} events/s through admission, {:.0} events/s end to end",
            self.polls,
            self.materials,
            self.material_bytes,
            self.batches,
            self.events_offered,
            self.admitted,
            self.repeats,
            self.replaced,
            self.older,
            self.hidden,
            self.superseded,
            self.failures,
            self.dirty_keys,
            self.wall.as_secs_f64(),
            self.admit_time.as_secs_f64(),
            rate(self.events_offered, self.admit_time),
            rate(self.events_offered, self.wall),
        )
    }
}

pub(crate) async fn run(o: Options) -> Result<(), String> {
    let unlock = o
        .identity_file
        .as_ref()
        .map(|path| {
            let text =
                std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let identity = sinex_sealing::parse_identity(text.trim()).map_err(|e| e.to_string())?;
            crate::material_access::sealing::Unlock::new(identity, Duration::from_secs(480 * 60))
        })
        .transpose()?;
    let mut settings = Settings::new(o.host.clone());
    settings.zone = o.zone.clone();
    settings.options = sinex_sources::live::options(&o.options)?;
    let mut planned: Vec<(&'static InputFormat, Binding)> = Vec::new();
    for spec in &o.reads {
        let (name, path) = spec
            .split_once('=')
            .ok_or_else(|| format!("--read {spec}: expected format=path"))?;
        let format = sources::format(name).ok_or_else(|| {
            format!(
                "unknown format {name}; one of {}",
                format_names().join(", ")
            )
        })?;
        let plan = format
            .plan
            .ok_or_else(|| format!("{name} is not read from a path"))?;
        for b in plan(&PathBuf::from(path), &settings)? {
            planned.push((format, b));
        }
    }
    let mut ctx = ReaderContext::open(
        &o.database,
        &o.store,
        sinex_model::time_settings::TimeSettings::operator_default(),
    )
    .await?;
    let sink = Sink(ctx.blobs.clone());
    let emits: Vec<&'static str> = {
        let mut types: Vec<&'static str> = planned
            .iter()
            .filter_map(|(format, b)| (format.specs)().into_iter().find(|s| s.parser == b.parser))
            .flat_map(|s| s.event_types.iter().copied())
            .collect();
        types.sort_unstable();
        types.dedup();
        types
    };
    let manifest = runs::Manifest {
        name: "sinexd backfill",
        kind: "source",
        version: env!("CARGO_PKG_VERSION"),
        build: option_env!("SINEX_BUILD").unwrap_or("dev"),
        emits: &emits,
    };
    let run = runs::start(&mut ctx.read_conn, &manifest, &o.host)
        .await
        .map_err(|e| e.to_string())?;
    // Totals per source, in the order sources were first read.
    let mut per_source: Vec<(String, Totals)> = Vec::new();
    let mut readers = planned
        .into_iter()
        .map(|(format, b)| crate::binding_reader::BindingReader::new(format, b))
        .collect::<Result<Vec<_>, _>>()?;
    let (_stop, stop) = tokio::sync::watch::channel(false);
    let results = crate::binding_reader::read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        run,
        &sink,
        &mut readers,
        stop,
        crate::material_access::Access {
            recipient: None,
            unlock: unlock.as_ref(),
        },
        // A backfill reports every failure once it has read everything.
        &mut |_, _| {},
    )
    .await;
    let mut failures = Vec::new();
    for (source, result) in results {
        match result {
            Ok(totals) => match per_source.iter_mut().find(|(s, _)| *s == source) {
                Some((_, t)) => t.add(totals),
                None => per_source.push((source, totals)),
            },
            Err(e) => failures.push(format!("{source}: {e}")),
        }
    }
    if o.json {
        let sources: serde_json::Map<String, serde_json::Value> = per_source
            .iter()
            .map(|(s, t)| (s.clone(), t.to_json()))
            .collect();
        println!(
            "{}",
            serde_json::json!({"sources": sources, "peak_rss_mb": crate::peak_rss_mb()})
        );
    } else {
        for (source, totals) in &per_source {
            println!("{source} {totals}");
        }
    }
    runs::end(&mut ctx.read_conn, run)
        .await
        .map_err(|e| e.to_string())?;
    ctx.close().await?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

/// Store `b`'s source, input and settings under its name.
pub(crate) async fn record(read_conn: &mut PgConnection, b: &Binding) -> Result<(), String> {
    bindings::save(
        read_conn,
        &bindings::Stored {
            binding: b.name.clone(),
            source: b.source.clone(),
            input: b.input.display().to_string(),
            settings: serde_json::to_value(&b.settings).map_err(|e| e.to_string())?,
        },
    )
    .await
    .map_err(|e| e.to_string())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drive(
    read_conn: &mut PgConnection,
    handle: &writer::WriterHandle,
    run: RunId,
    source: std::sync::Arc<dyn Source>,
    binding: &str,
    sink: &Sink,
    access: crate::material_access::Access<'_>,
    minimum_class: sinex_model::Class,
    reader: &mut crate::binding_reader::polled::PolledReader,
    stop: tokio::sync::watch::Receiver<bool>,
) -> Result<Totals, String> {
    let mut totals = Totals::default();
    let started = Instant::now();
    // The source's class: its declaration, raised by the operator's floor.
    // A sealed source seals each material and its events as they are read.
    let floor = sinex_store::privacy::load(read_conn, std::iter::once(source.spec().name))
        .await
        .map_err(|e| e.to_string())?
        .floors
        .get(source.spec().name)
        .copied()
        .unwrap_or_default();
    let effective_class = source.spec().class.max(floor).max(minimum_class);
    let material =
        crate::material_access::MaterialAccess::new(sink.0.clone(), effective_class, access)
            .map_err(|e| format!("{}: {e}", source.spec().name))?;
    let checked_stop = stop.clone();
    let check: std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync> =
        std::sync::Arc::new(move || {
            if *checked_stop.borrow() {
                return Err("capture cancelled".into());
            }
            Ok(())
        });
    loop {
        if *stop.borrow() {
            return Err("capture cancelled".into());
        }
        let (cursor, carry) = positions::load(read_conn, binding)
            .await
            .map_err(|e| e.to_string())?
            .unwrap_or((serde_json::Value::Null, serde_json::Value::Null));
        let poll_started = Instant::now();
        let mut acquired = reader.acquire(cursor.clone()).await?;
        totals.poll_time += poll_started.elapsed();
        drivers::note_previous_through(&cursor, &mut acquired);
        let carry = material.carry(read_conn, binding, carry, &acquired).await?;
        if let Some(reason) = &acquired.observation.unavailable {
            return Err(format!("{binding}: {reason}"));
        }
        let had_material = acquired.material.is_some();
        check()?;
        let registered = material
            .reopen(read_conn, source.as_ref(), &mut acquired)
            .await?;
        crate::intake::Intake {
            writer: handle,
            source: source.clone(),
            binding,
            run,
            material: material.clone(),
            check: check.clone(),
            priority: crate::writer::Priority::Bulk,
            atomic: false,
            registered,
        }
        .submit(acquired, carry, &mut totals)
        .await?;
        if !had_material {
            break;
        }
    }
    totals.wall = started.elapsed();
    Ok(totals)
}

impl Totals {
    pub(crate) fn poll(&mut self, length: Option<usize>) {
        self.polls += 1;
        if let Some(length) = length {
            self.materials += 1;
            self.material_bytes += length;
        }
    }

    pub(crate) fn parse(&mut self, elapsed: Duration) {
        self.parse_time += elapsed;
    }
    pub(crate) fn writer(&mut self, queue: Duration, service: Duration) {
        self.queue_time += queue;
        self.service_time += service;
    }
    pub(crate) fn batch(&mut self, events: usize, elapsed: Duration) {
        self.batches += 1;
        self.events_offered += events;
        self.admit_time += elapsed;
    }

    pub(crate) fn add_outcome(&mut self, out: &sinex_store::Outcome) {
        self.admitted += out.admitted;
        self.repeats += out.repeats;
        self.replaced += out.replaced;
        self.older += out.older;
        self.hidden += out.hidden;
        self.superseded += out.superseded;
        self.failures += out.failures;
        self.dirty_keys += out.dirty_keys;
    }

    pub(crate) fn finish(&mut self, started: Instant) {
        self.wall = started.elapsed();
    }

    pub(crate) fn pass_work(
        &mut self,
        work: &sinex_source_core::shared_pass::ArchivePassStats,
        captured: bool,
    ) {
        self.files_stored += u64::from(captured);
        self.pass_decoded_bytes += work.decoded_bytes;
        self.pass_record_bytes += work.record_bytes_stored;
    }
    pub(crate) fn to_json(&self) -> serde_json::Value {
        let per_s = |n: usize, d: Duration| n as f64 / d.as_secs_f64().max(1e-9);
        serde_json::json!({
            "polls": self.polls,
            "files_stored": self.files_stored,
            "pass_decoded_bytes": self.pass_decoded_bytes,
            "pass_record_bytes": self.pass_record_bytes,
            "materials": self.materials,
            "material_bytes": self.material_bytes,
            "batches": self.batches,
            "events_offered": self.events_offered,
            "admitted": self.admitted,
            "repeats": self.repeats,
            "replaced": self.replaced,
            "older": self.older,
            "hidden": self.hidden,
            "superseded": self.superseded,
            "failures": self.failures,
            "dirty_keys": self.dirty_keys,
            "wall_s": self.wall.as_secs_f64(),
            "admission_s": self.admit_time.as_secs_f64(),
            "poll_s": self.poll_time.as_secs_f64(),
            "parse_s": self.parse_time.as_secs_f64(),
            "queue_s": self.queue_time.as_secs_f64(),
            "service_s": self.service_time.as_secs_f64(),
            "admission_events_per_s": per_s(self.events_offered, self.admit_time),
            "events_per_s": per_s(self.events_offered, self.wall),
            "mb_per_s": self.material_bytes as f64 / 1e6 / self.wall.as_secs_f64().max(1e-9),
        })
    }

    pub(crate) fn add(&mut self, o: Totals) {
        self.polls += o.polls;
        self.files_stored += o.files_stored;
        self.pass_decoded_bytes += o.pass_decoded_bytes;
        self.pass_record_bytes += o.pass_record_bytes;
        self.materials += o.materials;
        self.material_bytes += o.material_bytes;
        self.batches += o.batches;
        self.events_offered += o.events_offered;
        self.admitted += o.admitted;
        self.repeats += o.repeats;
        self.replaced += o.replaced;
        self.older += o.older;
        self.hidden += o.hidden;
        self.superseded += o.superseded;
        self.failures += o.failures;
        self.dirty_keys += o.dirty_keys;
        self.admit_time += o.admit_time;
        self.poll_time += o.poll_time;
        self.parse_time += o.parse_time;
        self.queue_time += o.queue_time;
        self.service_time += o.service_time;
        self.wall += o.wall;
    }
}
