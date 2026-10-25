//! Read configured sources to the end through the writer, reporting counts and
//! rates. The same driver, drain and admission path the daemon uses; only the
//! scheduling differs (read until caught up, then stop).
//!
//! Every `--source KIND=PATH` is planned by its kind into bindings; each
//! binding is recorded with its settings (so a replay reads it the same way)
//! and its driver state is rebuilt from the materials it stored, so a
//! second run reads only what changed.
mod pipeline;
use crate::one_shot::{OneShot, Sink};
use crate::writer::{self, Priority};
use sinex_model::RunId;
use sinex_sources::{Binding, InputKind, Limits, Settings, Source, drivers, sources};
use sinex_store::{bindings, positions, runs};
use sqlx::PgConnection;
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub struct Options {
    pub database: String,
    pub store: PathBuf,
    /// `kind=path`, one per input; see [`sources::input_kinds`].
    pub sources: Vec<String>,
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

/// The kinds `--source KIND=PATH` accepts.
fn kind_names() -> Vec<&'static str> {
    sources::input_kinds()
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
    archive_source_opens: u64,
    archive_captures: u64,
    archive_decoded_bytes: u64,
    archive_record_bytes: u64,
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
    let mut planned: Vec<(&'static InputKind, Binding)> = Vec::new();
    for spec in &o.sources {
        let (name, path) = spec
            .split_once('=')
            .ok_or_else(|| format!("--source {spec}: expected kind=path"))?;
        let kind = sources::input_kind(name).ok_or_else(|| {
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
    let mut ctx = OneShot::open(&o.database, &o.store).await?;
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
    let run = runs::start(&mut ctx.read_conn, &manifest, &o.host)
        .await
        .map_err(|e| e.to_string())?;
    // Totals per source, in the order sources were first read.
    let mut per_source: Vec<(String, Totals)> = Vec::new();
    let mut bound = planned
        .into_iter()
        .map(|(kind, b)| crate::binding_reader::Bound::new(kind, b))
        .collect::<Result<Vec<_>, _>>()?;
    let (_stop, stop) = tokio::sync::watch::channel(false);
    let results = crate::binding_reader::read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        run,
        &sink,
        &mut bound,
        stop,
        crate::binding_reader::Access {
            recipient: None,
            unlock: unlock.as_ref(),
        },
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
            None => crate::one_shot::class_key()?,
        };
        let recipient = recipient.ok_or_else(|| {
            format!(
                "{}: its class is sealed, but no class key is configured (SINEX_CLASS_KEY_FILE)",
                source.spec().name
            )
        })?;
        Some(crate::one_shot::SealingSink::new(sink.0.clone(), recipient))
    } else {
        None
    };
    let checked_stop = stop.clone();
    let check: std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync> =
        std::sync::Arc::new(move || {
            if *checked_stop.borrow() || sinex_transport::private_mode::PrivateMode::default().on()
            {
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
        let mut stopping = stop.clone();
        let mut guard = tokio::select! {
            guard=handle.source_guard(source.spec().name)=>guard?,
            _=stopping.wait_for(|stopped|*stopped)=>return Err("capture cancelled".into()),
        };
        let before = async {
            let position = positions::load(guard.connection(), binding)
                .await
                .map_err(|e| e.to_string())?
                .unwrap_or((serde_json::Value::Null, serde_json::Value::Null));
            let exclusions = crate::excision::exclusions::Exclusions::load(
                guard.connection(),
                source.spec().name,
            )
            .await?;
            Ok::<_, String>((position, exclusions))
        }
        .await;
        let ((cursor, carry), exclusions) = match before {
            Ok(value) => value,
            Err(error) => {
                guard.release().await?;
                return Err(error);
            }
        };
        let mut acquired = match reader.poll(&cursor) {
            Err(error) => {
                guard.release().await?;
                return Err(error.to_string());
            }
            Ok(acquired) => acquired,
        };
        drivers::note_previous_through(&cursor, &mut acquired);
        let prepared = async {
            if let Some(unlock) = access.unlock {
                unlock.check()?;
            }
            if let (Some(sealing), Some(material)) = (&sealing, &acquired.material)
                && let Some(drivers::StoredBytes::FramedReader(stored)) = &material.stored
                && sink.0.is_sealed(&stored.blob).map_err(|e| e.to_string())?
            {
                let unlock = access
                    .unlock
                    .ok_or("stored sealed material needs an operator unlock")?;
                let key = unlock.material_key(read_conn, material.id).await?;
                sealing.restore_key(stored.blob.clone(), key)?;
            }
            let carry = if acquired.material.is_some() {
                match &sealing {
                    Some(s) => open_carry(read_conn, binding, &carry, s, access.unlock).await?,
                    None => carry,
                }
            } else {
                carry
            };
            if let Some(reason) = &acquired.observation.unavailable {
                return Err(format!("{binding}: {reason}"));
            }
            Ok::<_, String>(carry)
        }
        .await;
        let carry = match prepared {
            Ok(carry) => carry,
            Err(error) => {
                guard.release().await?;
                return Err(error);
            }
        };
        let had_material = acquired.material.is_some();
        let pipeline = pipeline::Pipeline {
            writer: handle,
            blobs: &sink.0,
            source,
            binding,
            run,
            class: effective_class,
            sealing: sealing.as_ref(),
            unlock: access.unlock,
            check: check.clone(),
        };
        pipeline
            .submit(guard, exclusions, carry, acquired, &mut totals)
            .await?;
        if !had_material {
            break;
        }
    }
    totals.wall = started.elapsed();
    Ok(totals)
}

/// Submit one acquisition through the same drain and writer in every binding reader.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn submit(
    handle: &writer::WriterHandle,
    run: RunId,
    source: &dyn Source,
    binding: &str,
    material_sink: &dyn drivers::MaterialSink,
    sealing: Option<&crate::one_shot::SealingSink>,
    unlock: Option<&crate::sealed_access::Unlock>,
    minimum_class: sinex_model::Class,
    acquired: drivers::Acquired,
    carry: serde_json::Value,
    totals: &mut Totals,
) -> Result<(), String> {
    totals.polls += 1;
    // Batches are submitted as they are drained, so memory holds one
    // batch at a time, never a whole material's events.
    let stored_material = acquired.material.as_ref().and_then(|material| {
        matches!(
            &material.stored,
            Some(drivers::StoredBytes::FramedReader(_))
        )
        .then_some(material.id)
    });
    let prepared = drivers::prepare(
        source,
        binding,
        run,
        acquired,
        carry,
        material_sink,
        Limits::default(),
    )
    .map_err(|e| e.to_string())?;
    let had_material = prepared.material_len();
    if let Some(bytes) = had_material {
        totals.materials += 1;
        totals.material_bytes += bytes;
    }
    for batch in prepared.batches().map_err(|e| e.to_string())? {
        let mut one = [batch.map_err(|e| e.to_string())?];
        for record in &mut one[0].records {
            if let sinex_model::RecordOutcome::Events { events, .. } = record {
                for event in events {
                    event.class = event.class.max(minimum_class);
                }
            }
        }
        if let Some(s) = sealing {
            s.seal(&mut one)?;
            if let (Some(material), sinex_model::BatchKind::Source(progress)) =
                (stored_material, &mut one[0].kind)
            {
                // Admission already owned this key. An authorized cached read
                // cannot publish it again after excision destroyed it.
                progress
                    .material_keys
                    .retain(|key| key.material != material);
            }
        }
        if let Some(unlock) = unlock {
            unlock.check()?;
        }
        let [batch] = one;
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
    Ok(())
}

impl Totals {
    fn add_outcome(&mut self, out: sinex_store::Outcome) {
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

    pub(crate) fn archive_work(
        &mut self,
        work: &sinex_sources::drivers::export_archive::group::Work,
        captured: bool,
    ) {
        self.archive_source_opens += u64::from(captured);
        self.archive_captures += u64::from(captured);
        self.archive_decoded_bytes += work.decoded_bytes;
        self.archive_record_bytes += work.record_bytes_stored;
    }
    pub(crate) fn to_json(&self) -> serde_json::Value {
        let per_s = |n: usize, d: Duration| n as f64 / d.as_secs_f64().max(1e-9);
        serde_json::json!({
            "polls": self.polls,
            "archive_source_opens": self.archive_source_opens,
            "archive_captures": self.archive_captures,
            "archive_decoded_bytes": self.archive_decoded_bytes,
            "archive_record_bytes": self.archive_record_bytes,
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
        self.archive_source_opens += o.archive_source_opens;
        self.archive_captures += o.archive_captures;
        self.archive_decoded_bytes += o.archive_decoded_bytes;
        self.archive_record_bytes += o.archive_record_bytes;
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
    sealing: &crate::one_shot::SealingSink,
    unlock: Option<&crate::sealed_access::Unlock>,
) -> Result<serde_json::Value, String> {
    if let Ok(value) = sealing.open_carry(carry) {
        return Ok(value);
    }
    let reference = sinex_store::sealed::key_ref(carry).ok_or("invalid sealed parser carry")?;
    let unlock = unlock.ok_or("sealed parser carry requires explicit operator unlock")?;
    let material:uuid::Uuid=sqlx::query_scalar("SELECT k.material_id FROM privacy.material_keys k JOIN raw.materials m ON m.id=k.material_id JOIN raw.inputs i ON i.id=m.input_id WHERE k.key_ref=$1 AND i.binding=$2")
        .bind(reference).bind(binding).fetch_optional(&mut *conn).await.map_err(|e|e.to_string())?.ok_or("sealed parser carry has no key of this binding")?;
    let key = unlock
        .material_key(conn, sinex_model::MaterialId(material))
        .await?;
    let value = sinex_store::sealed::unseal_payload(carry, &key.key).map_err(|e| e.to_string())?;
    unlock.check()?;
    Ok(value)
}
