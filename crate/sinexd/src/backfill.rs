//! Read configured sources to the end through the writer, reporting counts and
//! rates. The same driver, drain and admission path the daemon uses; only the
//! scheduling differs (read until caught up, then stop).
//!
//! Every `--source KIND=PATH` is planned by its kind into bindings; each
//! binding is recorded with its settings (so a replay reads it the same way)
//! and its driver's memory is rebuilt from the materials it stored, so a
//! second run reads only what changed.
use crate::context::{Context, Sink, stored_chain};
use crate::writer::{self, Priority};
use sinex_model::RunId;
use sinex_sources::{Binding, Kind, Limits, Settings, Source, drivers, sources};
use sinex_store::{bindings, positions, runs};
use sqlx::PgConnection;
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub struct Options {
    pub database: String,
    pub store: PathBuf,
    /// `kind=path`, one per input; see [`sources::kinds`].
    pub sources: Vec<String>,
    /// The zone sources with local times were written in (IANA name); the
    /// system zone when unset. Recorded with each binding.
    pub zone: Option<String>,
    /// A source option as `key=value` (a clipboard spool's `selection`).
    pub options: Vec<String>,
    pub host: String,
}

/// The kinds `--source KIND=PATH` accepts.
pub fn kind_names() -> Vec<&'static str> {
    sources::kinds()
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
    events_offered: usize,
    admitted: usize,
    repeats: usize,
    replaced: usize,
    older: usize,
    hidden: usize,
    superseded: usize,
    failures: usize,
    dirty_keys: usize,
    admit_time: Duration,
    wall: Duration,
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

pub async fn run(o: Options) -> Result<(), String> {
    let mut settings = Settings::new(o.host.clone());
    settings.zone = o.zone.clone();
    settings.options = sinex_sources::live::options(&o.options)?;
    let mut planned: Vec<(&'static Kind, Binding)> = Vec::new();
    for spec in &o.sources {
        let (name, path) = spec
            .split_once('=')
            .ok_or_else(|| format!("--source {spec}: expected kind=path"))?;
        let kind = sources::kind(name).ok_or_else(|| {
            format!(
                "unknown source kind {name}; one of {}",
                kind_names().join(", ")
            )
        })?;
        let plan = kind
            .plan
            .ok_or_else(|| format!("{name} is not read from a path"))?;
        for b in plan(&PathBuf::from(path), &settings)? {
            planned.push((kind, b));
        }
    }
    let mut ctx = Context::open(&o.database, &o.store).await?;
    let sink = Sink(ctx.blobs.clone());
    let emits: Vec<&'static str> = {
        let mut types: Vec<&'static str> = planned
            .iter()
            .filter_map(|(kind, b)| (kind.specs)().into_iter().find(|s| s.parser == b.parser))
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
    let run = runs::start(&mut ctx.aux, &manifest, &o.host)
        .await
        .map_err(|e| e.to_string())?;
    // Totals per source, in the order sources were first read.
    let mut per_source: Vec<(String, Totals)> = Vec::new();
    for (kind, b) in &planned {
        let totals = read_binding(&mut ctx, run, &sink, kind, b).await?;
        match per_source.iter_mut().find(|(s, _)| *s == b.source) {
            Some((_, t)) => t.add(totals),
            None => per_source.push((b.source.clone(), totals)),
        }
    }
    for (source, totals) in &per_source {
        println!("{source} {totals}");
    }
    runs::end(&mut ctx.aux, run)
        .await
        .map_err(|e| e.to_string())?;
    ctx.close().await
}

/// Record `b`, rebuild its driver's memory from what it stored, and read it
/// to the end.
pub(crate) async fn read_binding(
    ctx: &mut Context,
    run: RunId,
    sink: &Sink,
    kind: &Kind,
    b: &Binding,
) -> Result<Totals, String> {
    let source = (kind.source)(b)?;
    let reader_for = kind
        .reader
        .ok_or_else(|| format!("{} is not read by a driver", kind.name))?;
    let mut reader = reader_for(b)?;
    record(&mut ctx.aux, b).await?;
    reader.remember(&stored(ctx, &b.name).await?);
    drive(
        &mut ctx.aux,
        &ctx.writer,
        run,
        source.as_ref(),
        &b.name,
        sink,
        |cursor| reader.poll(cursor),
    )
    .await
}

/// Store `b`'s source, input and settings under its name.
pub(crate) async fn record(aux: &mut PgConnection, b: &Binding) -> Result<(), String> {
    bindings::save(
        aux,
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

/// The stored bytes the binding's driver read last, for its memory.
pub(crate) async fn stored(
    ctx: &mut Context,
    binding: &str,
) -> Result<Vec<(sinex_model::MaterialId, Vec<u8>)>, String> {
    let cursor = positions::load(&mut ctx.aux, binding)
        .await
        .map_err(|e| e.to_string())?
        .map(|(cursor, _)| cursor)
        .unwrap_or(serde_json::Value::Null);
    stored_chain(&mut ctx.aux, &ctx.blobs, &cursor).await
}

pub(crate) async fn drive(
    aux: &mut PgConnection,
    handle: &writer::WriterHandle,
    run: RunId,
    source: &dyn Source,
    binding: &str,
    sink: &Sink,
    mut poll: impl FnMut(&serde_json::Value) -> Result<drivers::Acquired, drivers::DriverError>,
) -> Result<Totals, String> {
    let mut totals = Totals::default();
    let started = Instant::now();
    let private = sinex_protocol::private_mode::PrivateMode::default();
    // The source's class: its declaration, raised by the operator's floor.
    // A sealed source seals each material and its events as they are read.
    let floor = sinex_store::privacy::load(aux, std::iter::once(source.spec().name))
        .await
        .map_err(|e| e.to_string())?
        .floors
        .get(source.spec().name)
        .copied()
        .unwrap_or_default();
    let sealing = if source.spec().class.max(floor) == sinex_model::Class::Sealed {
        let recipient = crate::context::class_key()?.ok_or_else(|| {
            format!(
                "{}: its class is sealed, but no class key is configured (SINEX_CLASS_KEY_FILE)",
                source.spec().name
            )
        })?;
        Some(crate::context::SealingSink::new(sink.0.clone(), recipient))
    } else {
        None
    };
    let material_sink: &dyn drivers::MaterialSink = match &sealing {
        Some(s) => s,
        None => sink,
    };
    loop {
        // Private mode: capture nothing, and say so rather than report a gap.
        if private.on() {
            return Err(format!(
                "private mode is on ({}); nothing was read",
                private.path().display()
            ));
        }
        let (cursor, carry) = positions::load(aux, binding)
            .await
            .map_err(|e| e.to_string())?
            .unwrap_or((serde_json::Value::Null, serde_json::Value::Null));
        let acquired = poll(&cursor).map_err(|e| e.to_string())?;
        totals.polls += 1;
        if let Some(reason) = &acquired.observation.unavailable {
            return Err(format!("{binding}: {reason}"));
        }
        let had_material = acquired.material.as_ref().map(|m| m.bytes.len());
        let mut batches = drivers::batches(
            source,
            binding,
            run,
            acquired,
            carry,
            material_sink,
            Limits::default(),
        )
        .map_err(|e| e.to_string())?;
        if let Some(s) = &sealing {
            s.seal(aux, &mut batches).await?;
        }
        if let Some(bytes) = had_material {
            totals.materials += 1;
            totals.material_bytes += bytes;
        }
        for batch in batches {
            totals.batches += 1;
            totals.events_offered += batch.event_count();
            let t = Instant::now();
            let out = handle
                .submit(Priority::Bulk, batch)
                .await
                .map_err(|e| e.to_string())?;
            totals.admit_time += t.elapsed();
            totals.admitted += out.admitted;
            totals.repeats += out.repeats;
            totals.replaced += out.replaced;
            totals.older += out.older;
            totals.hidden += out.hidden;
            totals.superseded += out.superseded;
            totals.failures += out.failures;
            totals.dirty_keys += out.dirty_keys;
        }
        if had_material.is_none() {
            break;
        }
    }
    totals.wall = started.elapsed();
    Ok(totals)
}

impl Totals {
    fn add(&mut self, o: Totals) {
        self.polls += o.polls;
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
        self.wall += o.wall;
    }
}
