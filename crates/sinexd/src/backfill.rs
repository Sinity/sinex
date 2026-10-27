//! Read configured sources to the end through the writer, reporting counts and
//! rates. The same driver, drain and admission path the daemon uses; only the
//! scheduling differs (read until caught up, then stop).
//!
//! Every `--read FORMAT=PATH` is planned by its format into bindings; each
//! binding is recorded with its settings (so a replay reads it the same way)
//! and its driver state is rebuilt from the materials it stored, so a
//! second run reads only what changed.
mod pipeline;
use crate::material_sink::Sink;
use crate::reader_context::ReaderContext;
use crate::writer;
use sinex_model::RunId;
use sinex_sources::{Binding, InputFormat, Settings, Source, drivers, sources};
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
            let identity =
                sinex_store::sealed::parse_identity(text.trim()).map_err(|e| e.to_string())?;
            crate::sealed_access::Unlock::new(identity, Duration::from_secs(480 * 60))
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
    let mut ctx = ReaderContext::open(&o.database, &o.store).await?;
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
        crate::binding_reader::Access {
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
    source: &dyn Source,
    binding: &str,
    sink: &Sink,
    access: crate::sealed_access::Access<'_>,
    minimum_class: sinex_model::Class,
    reader: &mut dyn sinex_sources::Reader,
    stop: tokio::sync::watch::Receiver<bool>,
) -> Result<Totals, String> {
    let mut totals = Totals::default();
    let started = Instant::now();
    let private = sinex_transport::private_mode::PrivateMode::default();
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
    let sealing = if effective_class == sinex_model::Class::Sealed {
        let recipient = match access.recipient {
            Some(recipient) => Some(recipient.clone()),
            None => crate::material_sink::class_key()?,
        };
        let recipient = recipient.ok_or_else(|| {
            format!(
                "{}: its class is sealed, but no class key is configured (SINEX_CLASS_KEY_FILE)",
                source.spec().name
            )
        })?;
        Some(crate::material_sink::SealingSink::new(
            sink.0.clone(),
            recipient,
        ))
    } else {
        None
    };
    let checked_stop = stop.clone();
    // Checked for every buffer read, so the switch is looked at only every
    // quarter second.
    let checked_private = std::sync::Mutex::new(sinex_transport::private_mode::Cached::new(
        sinex_transport::private_mode::PrivateMode::default(),
        std::time::Duration::from_millis(250),
    ));
    let check: std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync> =
        std::sync::Arc::new(move || {
            let private = checked_private
                .lock()
                .map_or(true, |mut cached| cached.on());
            if *checked_stop.borrow() || private {
                return Err("capture cancelled or private mode is on".into());
            }
            Ok(())
        });
    loop {
        // Private mode: capture nothing, and say so rather than report a gap.
        if private.on() {
            return Err(format!(
                "private mode is on ({}); nothing was read",
                private.path().display()
            ));
        }
        if *stop.borrow() {
            return Err("capture cancelled".into());
        }
        let (cursor, carry) = positions::load(read_conn, binding)
            .await
            .map_err(|e| e.to_string())?
            .unwrap_or((serde_json::Value::Null, serde_json::Value::Null));
        let mut acquired = reader.poll(&cursor).map_err(|e| e.to_string())?;
        drivers::note_previous_through(&cursor, &mut acquired);
        if let Some(unlock) = access.unlock {
            unlock.check()?;
        }
        if let (Some(sealing), Some(material)) = (&sealing, &acquired.material)
            && let Some(drivers::StoredBytes::Blob {
                name,
                records: Some(_),
                ..
            }) = &material.stored
            && sink.0.is_sealed(name).map_err(|e| e.to_string())?
        {
            let unlock = access
                .unlock
                .ok_or("stored sealed material needs an operator unlock")?;
            let key = unlock.data_key(read_conn, material.id).await?;
            sealing.restore_key(name.clone(), key)?;
        }
        let carry = match (&sealing, acquired.material.is_some()) {
            (Some(s), true) => open_carry(read_conn, binding, &carry, s, access.unlock).await?,
            _ => carry,
        };
        if let Some(reason) = &acquired.observation.unavailable {
            return Err(format!("{binding}: {reason}"));
        }
        let had_material = acquired.material.is_some();
        check()?;
        let registered = pipeline::reopen_stored(
            read_conn,
            &sink.0,
            source,
            &mut acquired,
            sealing.as_ref(),
            access.unlock,
        )
        .await?;
        let material_sink: &dyn drivers::MaterialSink = match &sealing {
            Some(sealing) => sealing,
            None => sink,
        };
        crate::intake::Intake {
            writer: handle,
            source,
            binding,
            run,
            sink: material_sink,
            class: effective_class,
            sealing: sealing.as_ref(),
            unlock: access.unlock,
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
        work: &sinex_sources::shared_pass::ArchivePassStats,
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
        self.wall += o.wall;
    }
}

pub(crate) async fn open_carry(
    conn: &mut PgConnection,
    binding: &str,
    carry: &serde_json::Value,
    sealing: &crate::material_sink::SealingSink,
    unlock: Option<&crate::sealed_access::Unlock>,
) -> Result<serde_json::Value, String> {
    if let Ok(value) = sealing.open_carry(carry) {
        return Ok(value);
    }
    let reference = sinex_store::sealed::key_ref(carry).ok_or("invalid sealed parser carry")?;
    let unlock = unlock.ok_or("sealed parser carry requires explicit operator unlock")?;
    let material = sinex_store::sealed::material_of_key(conn, reference, binding)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("sealed parser carry has no key of this binding")?;
    let key = unlock.data_key(conn, material).await?;
    let value = sinex_store::sealed::unseal_payload(carry, &key.key).map_err(|e| e.to_string())?;
    unlock.check()?;
    Ok(value)
}
