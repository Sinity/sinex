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
    /// A file-watch capture spool (`sinex-session capture files`).
    pub file_watch_spool: Option<PathBuf>,
    /// ActivityWatch's `aw-server-rust` database.
    pub activitywatch: Option<PathBuf>,
    /// Local repositories whose commits to read.
    pub repositories: Vec<PathBuf>,
    /// Export and directory sources, as `kind=path` (see [`EXPORT_KINDS`]).
    pub sources: Vec<String>,
    /// The zone WeeChat logs were written in; the system zone when unset.
    pub zone: Option<String>,
    pub host: String,
}

/// The kinds `--source kind=path` accepts. A directory given for a kind that
/// reads one file is read file by file, each its own binding.
pub const EXPORT_KINDS: &[&str] = &[
    "sinnix-lake",
    "chromium",
    "takeout-chrome",
    "spotify",
    "raindrop",
    "notes",
    "mbox",
    "maildir",
    "weechat",
    "takeout",
    "calendar",
    "goodreads",
    "messenger",
    "sleep-as-android",
    "wykop",
    "reddit",
    "samsung-health",
    "samsung-sms",
    "raw-log",
    "steering",
];

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
            "file.changed",
            "window.span",
            "presence.span",
            "browser.tab_active",
            "activitywatch.other",
            "git.commit",
            "browser.visit",
            "browser.url",
            "browser.history_entry",
            "music.played",
            "bookmark.saved",
            "note.document",
            "mail.message",
            "chat.message",
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
    if let Some(spool) = &o.file_watch_spool {
        let source = sinex_sources::sources::file_watch::source();
        let driver = sinex_sources::sources::file_watch::driver(spool.clone(), o.host.clone());
        let totals = drive(
            &mut ctx.aux,
            &ctx.writer,
            run,
            &source,
            &format!("file-watch@{}", o.host),
            &sink,
            |cursor| driver.poll(cursor),
        )
        .await?;
        println!("file-watch {totals}");
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
    if let Some(path) = &o.activitywatch {
        let source = sinex_sources::sources::activitywatch::source();
        let driver = sinex_sources::sources::activitywatch::driver(path.clone(), o.host.clone());
        let binding = format!("activitywatch@{}", o.host);
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
        println!("activitywatch {totals}");
    }
    for path in &o.repositories {
        let source = sinex_sources::sources::repository::source();
        let driver = sinex_sources::sources::repository::driver(path.clone(), o.host.clone());
        let binding = format!("repository:{}@{}", path.display(), o.host);
        let totals = drive(
            &mut ctx.aux,
            &ctx.writer,
            run,
            &source,
            &binding,
            &sink,
            |cursor| driver.poll(cursor),
        )
        .await?;
        println!("{} {totals}", path.display());
    }
    for spec in &o.sources {
        let (kind, path) = spec
            .split_once('=')
            .ok_or_else(|| format!("--source {spec}: expected kind=path"))?;
        let totals = export(&mut ctx, run, &sink, kind, PathBuf::from(path), &o).await?;
        println!("{kind} {totals}");
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

/// The files to read for `path`: itself, or a directory's files in name order
/// (recursively when `recursive`), keeping those `keep` accepts.
fn files(
    path: &std::path::Path,
    recursive: bool,
    keep: fn(&std::path::Path) -> bool,
) -> Vec<PathBuf> {
    if path.is_file() {
        return vec![path.to_path_buf()];
    }
    let mut out = Vec::new();
    let mut dirs = vec![path.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let p = entry.path();
            if p.is_dir() {
                if recursive {
                    dirs.push(p);
                }
            } else if keep(&p) {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// Read one `--source kind=path` to the end: one binding per file, profile or
/// directory, each driver's memory rebuilt from what it stored before.
async fn export(
    ctx: &mut Context,
    run: RunId,
    sink: &Sink,
    kind: &str,
    path: PathBuf,
    o: &Options,
) -> Result<Totals, String> {
    use sinex_sources::sources::{
        chromium, mail, notes, raindrop, spotify, takeout_chrome, weechat,
    };
    let host = o.host.clone();
    let mut totals = Totals::default();
    let binding = |name: &str| format!("{kind}:{name}@{host}");
    let any = |_: &std::path::Path| true;
    match kind {
        "chromium" => {
            for (profile, history) in chromium::profiles(&path) {
                for table in ["visits", "urls"] {
                    let (source, driver): (Box<dyn Source>, _) = if table == "visits" {
                        (
                            Box::new(chromium::visits()),
                            chromium::visits_driver(history.clone(), host.clone()),
                        )
                    } else {
                        (
                            Box::new(chromium::urls()),
                            chromium::urls_driver(history.clone(), host.clone()),
                        )
                    };
                    let name = binding(&format!("{profile}/{table}"));
                    let chain = stored(ctx, &name).await?;
                    let mut memory =
                        drivers::sqlite_rows::RowMemory::rebuild(chain.iter().map(Vec::as_slice));
                    totals.add(
                        drive(
                            &mut ctx.aux,
                            &ctx.writer,
                            run,
                            source.as_ref(),
                            &name,
                            sink,
                            |c| driver.poll(c, &mut memory),
                        )
                        .await?,
                    );
                }
            }
        }
        "takeout-chrome" | "spotify" | "raindrop" | "mbox" | "goodreads" | "messenger"
        | "sleep-as-android" | "wykop" | "reddit" | "samsung-health" | "samsung-sms" => {
            use sinex_sources::sources::{
                goodreads, messenger, reddit, samsung_health, samsung_sms, sleep_as_android, wykop,
            };
            let keep: fn(&std::path::Path) -> bool = match kind {
                "wykop" => wykop::is_own,
                "reddit" | "samsung-health" => |p| p.extension().is_some_and(|e| e == "zip"),
                "samsung-sms" => |p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("SMS_") && n.ends_with(".csv"))
                },
                _ => any,
            };
            let recursive = matches!(kind, "samsung-health" | "samsung-sms");
            for file in files(&path, recursive, keep) {
                let name = binding(&file.display().to_string());
                let (source, driver): (Box<dyn Source>, _) = match kind {
                    "samsung-health" => (
                        Box::new(samsung_health::source()),
                        samsung_health::driver(file, host.clone()),
                    ),
                    "samsung-sms" => (
                        Box::new(samsung_sms::source()),
                        samsung_sms::driver(file, host.clone()),
                    ),
                    "wykop" => (Box::new(wykop::source()), wykop::driver(file, host.clone())),
                    "reddit" => (
                        Box::new(reddit::source()),
                        reddit::driver(file, host.clone()),
                    ),
                    "goodreads" => (
                        Box::new(goodreads::source()),
                        goodreads::driver(file, host.clone()),
                    ),
                    "messenger" => (
                        Box::new(messenger::source()),
                        messenger::driver(file, host.clone()),
                    ),
                    "sleep-as-android" => (
                        Box::new(sleep_as_android::source()),
                        sleep_as_android::driver(file, host.clone()),
                    ),
                    "takeout-chrome" => (
                        Box::new(takeout_chrome::source()),
                        takeout_chrome::driver(file, host.clone()),
                    ),
                    "spotify" => (
                        Box::new(spotify::source()),
                        spotify::driver(file, host.clone()),
                    ),
                    "raindrop" => (
                        Box::new(raindrop::source()),
                        raindrop::driver(file, host.clone()),
                    ),
                    _ => (
                        Box::new(mail::mbox_source()),
                        mail::mbox_driver(file, host.clone()),
                    ),
                };
                totals.add(
                    drive(
                        &mut ctx.aux,
                        &ctx.writer,
                        run,
                        source.as_ref(),
                        &name,
                        sink,
                        |c| driver.poll(c),
                    )
                    .await?,
                );
            }
        }
        // Every Takeout product source over each archive, plus the calendars
        // in it; each source its own binding, so each keeps its own cursor.
        "takeout" | "calendar" => {
            use sinex_sources::sources::{calendar, google_takeout as g};
            let zone = weechat::zone(o.zone.as_deref())?;
            for file in files(&path, false, any) {
                let mut readers: Vec<(Box<dyn Source>, drivers::export_archive::ExportArchive)> =
                    vec![(
                        Box::new(calendar::source(zone.clone())),
                        calendar::driver(file.clone(), host.clone()),
                    )];
                if kind == "takeout" {
                    readers.extend([
                        (
                            Box::new(g::activity::my_activity()) as Box<dyn Source>,
                            g::activity::my_activity_driver(file.clone(), host.clone()),
                        ),
                        (
                            Box::new(g::activity::youtube()),
                            g::activity::youtube_driver(file.clone(), host.clone()),
                        ),
                        (
                            Box::new(g::keep::source()),
                            g::keep::driver(file.clone(), host.clone()),
                        ),
                        (
                            Box::new(g::tasks::source()),
                            g::tasks::driver(file.clone(), host.clone()),
                        ),
                        (
                            Box::new(g::purchases::play_store()),
                            g::purchases::play_store_driver(file.clone(), host.clone()),
                        ),
                        (
                            Box::new(g::purchases::purchases()),
                            g::purchases::purchases_driver(file.clone(), host.clone()),
                        ),
                    ]);
                }
                for (source, driver) in readers {
                    let name = binding(&format!("{}:{}", source.spec().name, file.display()));
                    let t = drive(
                        &mut ctx.aux,
                        &ctx.writer,
                        run,
                        source.as_ref(),
                        &name,
                        sink,
                        |c| driver.poll(c),
                    )
                    .await?;
                    println!("  {} {t}", source.spec().name);
                    totals.add(t);
                }
            }
        }
        "raw-log" => {
            use sinex_sources::sources::operator;
            let source = operator::raw_log(weechat::zone(o.zone.as_deref())?);
            let driver = operator::raw_log_driver(path.clone(), host.clone());
            let name = binding(&path.display().to_string());
            let chain = stored(ctx, &name).await?;
            let mut memory = drivers::append_file::AppendMemory::rebuild(
                chain.iter().map(Vec::as_slice),
                operator::frame_log,
            );
            totals.add(
                drive(&mut ctx.aux, &ctx.writer, run, &source, &name, sink, |c| {
                    driver.poll(c, &mut memory)
                })
                .await?,
            );
        }
        "steering" => {
            use sinex_sources::sources::operator;
            for table in ["commitments", "activities", "reviews"] {
                let source = operator::steering(table).expect("a known table");
                let driver = operator::steering_driver(path.clone(), host.clone(), table);
                let name = binding(&format!("{}/{table}", path.display()));
                let chain = stored(ctx, &name).await?;
                let mut memory =
                    drivers::sqlite_rows::RowMemory::rebuild(chain.iter().map(Vec::as_slice));
                totals.add(
                    drive(&mut ctx.aux, &ctx.writer, run, &source, &name, sink, |c| {
                        driver.poll(c, &mut memory)
                    })
                    .await?,
                );
            }
        }
        "notes" | "maildir" => {
            let (source, driver): (Box<dyn Source>, _) = if kind == "notes" {
                (
                    Box::new(notes::source()),
                    notes::driver(path.clone(), host.clone()),
                )
            } else {
                (
                    Box::new(mail::maildir_source()),
                    mail::maildir_driver(path.clone(), host.clone()),
                )
            };
            // A restart reads the directory again; admission suppresses the
            // repeats.
            let mut memory = drivers::dir_snapshot::DirMemory::default();
            let name = binding(&path.display().to_string());
            totals.add(
                drive(
                    &mut ctx.aux,
                    &ctx.writer,
                    run,
                    source.as_ref(),
                    &name,
                    sink,
                    |c| driver.poll(c, &mut memory),
                )
                .await?,
            );
        }
        "weechat" => {
            let source = weechat::source(weechat::zone(o.zone.as_deref())?);
            let logs = files(&path, true, |p| {
                p.extension()
                    .is_some_and(|e| e == "log" || e == "weechatlog")
            });
            for file in logs {
                let driver = weechat::driver(file.clone(), host.clone());
                let name = binding(&file.display().to_string());
                let chain = stored(ctx, &name).await?;
                let mut memory = drivers::append_file::AppendMemory::rebuild(
                    chain.iter().map(Vec::as_slice),
                    weechat::SPEC.framer,
                );
                totals.add(
                    drive(&mut ctx.aux, &ctx.writer, run, &source, &name, sink, |c| {
                        driver.poll(c, &mut memory)
                    })
                    .await?,
                );
            }
        }
        "sinnix-lake" => {
            // `path` is the lake root (`/realm`); each lane's directories are
            // its configuration (spec `port-sinnix.md` §3.1).
            use sinex_sources::sources::sinnix_lake;
            for lane in sinnix_lake::LANES {
                let source = sinnix_lake::source(lane);
                let mut lane_totals = Totals::default();
                for file in sinnix_lake::day_files(&path, lane) {
                    let driver = sinnix_lake::driver(file.clone(), host.clone());
                    let name = binding(&file.display().to_string());
                    let chain = stored(ctx, &name).await?;
                    let mut memory = drivers::append_file::AppendMemory::rebuild(
                        chain.iter().map(Vec::as_slice),
                        lane.spec.framer,
                    );
                    lane_totals.add(
                        drive(&mut ctx.aux, &ctx.writer, run, &source, &name, sink, |c| {
                            driver.poll(c, &mut memory)
                        })
                        .await?,
                    );
                }
                eprintln!(
                    "sinnix-lake {}: {} materials, {} bytes, {} events offered, {} admitted, {} repeats, {} failures",
                    lane.spec.name,
                    lane_totals.materials,
                    lane_totals.material_bytes,
                    lane_totals.events_offered,
                    lane_totals.admitted,
                    lane_totals.repeats,
                    lane_totals.failures,
                );
                totals.add(lane_totals);
            }
            // Sequence gaps per lane: numbers between the first and last seen
            // with no record. Facts about Sinnix's capture, not errors here.
            let gaps = sqlx::query!(
                r#"SELECT payload->>'lane' AS "lane!", payload->>'host' AS "host!",
                          count(DISTINCT (payload->>'seq')::bigint) AS "seen!",
                          min((payload->>'seq')::bigint) AS "first!",
                          max((payload->>'seq')::bigint) AS "last!"
                   FROM core.events WHERE event_type = 'capture.observed'
                   GROUP BY 1, 2 ORDER BY 1, 2"#
            )
            .fetch_all(&mut ctx.aux)
            .await
            .map_err(|e| e.to_string())?;
            for g in gaps {
                eprintln!(
                    "sinnix-lake gaps {}@{}: {} seen in {}..={}, {} numbers without a record",
                    g.lane,
                    g.host,
                    g.seen,
                    g.first,
                    g.last,
                    (g.last - g.first + 1) - g.seen
                );
            }
        }
        other => {
            return Err(format!(
                "unknown source kind {other}; one of {}",
                EXPORT_KINDS.join(", ")
            ));
        }
    }
    Ok(totals)
}
