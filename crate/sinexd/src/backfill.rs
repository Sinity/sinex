//! Read configured sources to the end through the writer, reporting counts and
//! rates. The same driver, drain and admission path the daemon uses; only the
//! scheduling differs (read until caught up, then stop).
use crate::keys::Registered;
use crate::writer::{self, Priority};
use sinex_model::RunId;
use sinex_sources::{Limits, Source, drivers};
use sinex_store::{Blobs, positions, runs};
use sqlx::{Connection, PgConnection};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct Options {
    pub database: String,
    pub store: PathBuf,
    pub atuin: Option<PathBuf>,
    pub zsh: Option<PathBuf>,
    pub host: String,
}

struct Sink(Blobs);
impl drivers::MaterialSink for Sink {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.0.put(bytes).map_err(|e| e.to_string())
    }
}

#[derive(Default)]
struct Totals {
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
    let mut aux = PgConnection::connect(&o.database)
        .await
        .map_err(|e| e.to_string())?;
    sinex_schema::apply(&mut aux)
        .await
        .map_err(|e| e.to_string())?;
    let sink = Sink(Blobs::open(&o.store).map_err(|e| e.to_string())?);
    let writer_conn = PgConnection::connect(&o.database)
        .await
        .map_err(|e| e.to_string())?;
    let (handle, queues) = writer::channels();
    let writer_task = tokio::spawn(writer::run(
        o.database.clone(),
        writer_conn,
        queues,
        Arc::new(Registered(sinex_automata::keyed())),
    ));
    let manifest = runs::Manifest {
        name: "sinexd backfill",
        kind: "source",
        version: env!("CARGO_PKG_VERSION"),
        build: option_env!("SINEX_BUILD").unwrap_or("dev"),
        emits: &["command.executed", "command.imported"],
    };
    let run = runs::start(&mut aux, &manifest, &o.host)
        .await
        .map_err(|e| e.to_string())?;

    if let Some(path) = &o.atuin {
        let source = sinex_sources::sources::atuin::source();
        let driver = sinex_sources::sources::atuin::driver(path.clone(), o.host.clone());
        let mut memory = drivers::sqlite_rows::RowMemory::default();
        let totals = drive(
            &mut aux,
            &handle,
            run,
            &source,
            &format!("atuin@{}", o.host),
            &sink,
            |cursor| driver.poll(cursor, &mut memory),
        )
        .await?;
        println!("atuin  {totals}");
    }
    if let Some(path) = &o.zsh {
        let source = sinex_sources::sources::zsh::source(o.host.clone());
        let driver = sinex_sources::sources::zsh::driver(path.clone(), o.host.clone());
        let totals = drive(
            &mut aux,
            &handle,
            run,
            &source,
            &format!("zsh@{}", o.host),
            &sink,
            |cursor| driver.poll(cursor),
        )
        .await?;
        println!("zsh    {totals}");
    }
    runs::end(&mut aux, run).await.map_err(|e| e.to_string())?;
    drop(handle);
    writer_task.await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn drive(
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
    loop {
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
        let batches = drivers::batches(
            source,
            binding,
            run,
            acquired,
            carry,
            sink,
            Limits::default(),
        )
        .map_err(|e| e.to_string())?;
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
