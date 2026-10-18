//! Read configured sources to the end through the writer, reporting counts and
//! rates. The same driver, drain and admission path the daemon uses; only the
//! scheduling differs (read until caught up, then stop).
//!
//! A driver's memory of what it already stored is rebuilt from the stored
//! materials before the first read, so a second run reads only what changed.
use crate::context::{Context, Sink, stored_chain};
use crate::writer::{self, Priority};
use sinex_model::RunId;
use sinex_sources::{Limits, Source, drivers};
use sinex_store::{positions, runs};
use sqlx::PgConnection;
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub struct Options {
    pub database: String,
    pub store: PathBuf,
    pub atuin: Option<PathBuf>,
    pub zsh: Option<PathBuf>,
    /// A Hyprland capture spool (`sinex-session capture hyprland`).
    pub hyprland_spool: Option<PathBuf>,
    /// A journald capture spool (`sinexd capture journald`).
    pub journald_spool: Option<PathBuf>,
    pub host: String,
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
    let mut ctx = Context::open(&o.database, &o.store).await?;
    let sink = Sink(ctx.blobs.clone());
    let manifest = runs::Manifest {
        name: "sinexd backfill",
        kind: "source",
        version: env!("CARGO_PKG_VERSION"),
        build: option_env!("SINEX_BUILD").unwrap_or("dev"),
        emits: &[
            "command.executed",
            "command.imported",
            "window.focused",
            "window.opened",
            "window.closed",
            "window.moved",
            "window.title_changed",
            "workspace.switched",
            "monitor.focused",
            "desktop.other",
            "journal.entry",
        ],
    };
    let run = runs::start(&mut ctx.aux, &manifest, &o.host)
        .await
        .map_err(|e| e.to_string())?;

    if let Some(path) = &o.atuin {
        let source = sinex_sources::sources::atuin::source();
        let driver = sinex_sources::sources::atuin::driver(path.clone(), o.host.clone());
        let binding = format!("atuin@{}", o.host);
        let chain = stored(&mut ctx, &binding).await?;
        let mut memory = drivers::sqlite_rows::RowMemory::rebuild(chain.iter().map(Vec::as_slice));
        let totals = drive(
            &mut ctx.aux,
            &ctx.writer,
            run,
            &source,
            &binding,
            &sink,
            |cursor| driver.poll(cursor, &mut memory),
        )
        .await?;
        println!("atuin  {totals}");
    }
    if let Some(path) = &o.zsh {
        let source = sinex_sources::sources::zsh::source(o.host.clone());
        let driver = sinex_sources::sources::zsh::driver(path.clone(), o.host.clone());
        let binding = format!("zsh@{}", o.host);
        let chain = stored(&mut ctx, &binding).await?;
        let mut memory = drivers::append_file::AppendMemory::rebuild(
            chain.iter().map(Vec::as_slice),
            sinex_sources::sources::zsh::frame,
        );
        let totals = drive(
            &mut ctx.aux,
            &ctx.writer,
            run,
            &source,
            &binding,
            &sink,
            |cursor| driver.poll(cursor, &mut memory),
        )
        .await?;
        println!("zsh    {totals}");
    }
    if let Some(spool) = &o.hyprland_spool {
        let source = sinex_sources::sources::hyprland::source();
        let driver = sinex_sources::sources::hyprland::driver(spool.clone(), o.host.clone());
        let totals = drive(
            &mut ctx.aux,
            &ctx.writer,
            run,
            &source,
            &format!("hyprland@{}", o.host),
            &sink,
            |cursor| driver.poll(cursor),
        )
        .await?;
        println!("hyprland {totals}");
    }
    if let Some(spool) = &o.journald_spool {
        let source = sinex_sources::sources::journald::source();
        let driver = sinex_sources::sources::journald::driver(spool.clone(), o.host.clone());
        let totals = drive(
            &mut ctx.aux,
            &ctx.writer,
            run,
            &source,
            &format!("journald@{}", o.host),
            &sink,
            |cursor| driver.poll(cursor),
        )
        .await?;
        println!("journald {totals}");
    }
    runs::end(&mut ctx.aux, run)
        .await
        .map_err(|e| e.to_string())?;
    ctx.close().await
}

/// The stored bytes the binding's driver read last, for its memory.
async fn stored(ctx: &mut Context, binding: &str) -> Result<Vec<Vec<u8>>, String> {
    let cursor = positions::load(&mut ctx.aux, binding)
        .await
        .map_err(|e| e.to_string())?
        .map(|(cursor, _)| cursor)
        .unwrap_or(serde_json::Value::Null);
    stored_chain(&mut ctx.aux, &ctx.blobs, &cursor).await
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
