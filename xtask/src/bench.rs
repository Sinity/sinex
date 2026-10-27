//! `xtask bench`: backfill and derivation measured on a fixed sample of the
//! operator's real inputs instead of all of them.
//!
//! The inputs are listed, one per line, in `sources.tsv` under the project
//! home's `analysis/bench/` (operator data never enters Git):
//!
//! ```text
//! format <TAB> path <TAB> files|whole
//! ```
//!
//! A `files` input is a directory whose files stand alone (a day of casts, a
//! log directory): the sample takes a stratified subset of its files. A
//! `whole` input must stay intact (an export with its own layout, a SQLite
//! database, an archive): the sample takes all of it. Each input's format is a
//! stratum.
//!
//! `bench sample` writes the manifest. `bench run` copies the sample once,
//! starts its own PostgreSQL with TimescaleDB (on /realm for `disk`, on
//! /dev/shm for `memory`), backfills each input separately with a memory cap
//! (one process per input, so each has its own peak memory), runs every
//! derivation once, and records periodic host-load observations. Full-input
//! estimates explicitly assume linear work per input byte; one sample does
//! not calibrate backfill or derivation. Failed commands remain in the output,
//! but only complete runs enter benchmark history, under `completed-backfill`.
//! Earlier `backfill` records did not check completeness. Every command it runs
//! runs inside its AgentCTL job and holds the real-data lock.
//!
//! `bench synthetic` measures admission alone: it backfills a generated shell
//! history of small events into its own PostgreSQL and records the rate under
//! `synthetic-admission`.
mod cases;
mod measurements;

use devkit::bench::{self, Candidate, LoadMonitor, Manifest};
use devkit::history::Store;
use serde_json::{Value, json};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

fn bench_home() -> PathBuf {
    env::var_os("SINEX_BENCH_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/realm/project/sinex/bench"))
}

fn analysis() -> PathBuf {
    PathBuf::from("/realm/project/sinex/analysis/bench")
}

fn manifest_path() -> PathBuf {
    analysis().join("sinex-sample.json")
}

const LOCK: &str = "/realm/tmp/work/realdata.lock";

#[derive(Debug, clap::Subcommand)]
pub enum Bench {
    /// Draw the sample from `sources.tsv` and write its manifest.
    Sample {
        /// Target size per `files` input, in MB.
        #[arg(long, default_value_t = 400)]
        mb: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
    },
    /// Backfill and derive over the sample, and record it.
    Run {
        /// `disk` puts PostgreSQL and the content store on /realm like the
        /// deployment; `memory` puts them in /dev/shm.
        #[arg(long, default_value = "disk", value_parser = ["disk", "memory"])]
        storage: String,
        /// Memory cap per backfill process (systemd `MemoryMax`). A backfill
        /// that exceeds it is killed and reported, not the desktop.
        #[arg(long, default_value = "6G")]
        memory_max: String,
    },
    /// Backfill a synthetic zsh history of `events` commands and record
    /// admission's rate: a fixed stream of small events, so runs compare
    /// across commits.
    Synthetic {
        #[arg(long, default_value_t = 1_000_000)]
        events: u64,
        /// `disk` or `memory`, as for `run`.
        #[arg(long, default_value = "disk", value_parser = ["disk", "memory"])]
        storage: String,
        #[arg(long, default_value = "6G")]
        memory_max: String,
        /// A PostgreSQL setting over the deployment's, `name=value`;
        /// repeatable. Recorded with the run.
        #[arg(long = "postgres")]
        settings: Vec<String>,
        /// Parent directory for retained run artifacts.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Check that an older sinexd refuses the resulting schema.
        #[arg(long)]
        older_binary: Option<PathBuf>,
        /// Populate and consume this many live keyed journal events before measurement.
        #[arg(long, default_value_t = 0)]
        prefill_keys: u64,
        /// After the measured workload, compare publication lookup and erasure costs.
        #[arg(long)]
        publication_probe: bool,
        /// Measure admission while search and publication consume it.
        #[arg(long)]
        consumers: bool,
        /// Add keyed journal records and variable payload widths.
        #[arg(long)]
        mixed: bool,
    },
    /// Exercise repeats, hidden readings, known-field replacements and derivations.
    Cases {
        #[arg(long, default_value_t = 10_000)]
        events: u64,
        #[arg(long)]
        output: PathBuf,
    },
    #[command(flatten)]
    Shared(devkit::cli::Bench),
}

pub fn execute(command: Bench) -> ExitCode {
    let result = match command {
        Bench::Sample { mb, seed } => sample(mb * 1_000_000, seed),
        Bench::Run {
            storage,
            memory_max,
        } => run(&storage, &memory_max),
        Bench::Synthetic {
            events,
            storage,
            memory_max,
            settings,
            output,
            older_binary,
            prefill_keys,
            publication_probe,
            consumers,
            mixed,
        } => synthetic(SyntheticOptions {
            events,
            storage,
            memory_max,
            settings,
            output,
            older_binary,
            prefill_keys,
            publication_probe,
            consumers,
            mixed,
        }),
        Bench::Cases { events, output } => cases::run(events, &output),
        Bench::Shared(b) => return devkit::cli::bench("sinex", b),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bench: {e}");
            ExitCode::FAILURE
        }
    }
}

struct Input {
    format: String,
    path: PathBuf,
    whole: bool,
    /// The input's directory under the sample home: its format, numbered
    /// when a format is listed more than once.
    label: String,
}

fn inputs() -> Result<Vec<Input>, String> {
    let path = analysis().join("sources.tsv");
    let text = fs::read_to_string(&path).map_err(|e| {
        format!(
            "{}: {e} (one `format<TAB>path<TAB>files|whole` per line)",
            path.display()
        )
    })?;
    let mut out: Vec<Input> = Vec::new();
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        let [format, path, mode] = f[..] else {
            return Err(format!("{}: bad line `{line}`", path.display()));
        };
        let whole = match mode {
            "whole" => true,
            "files" => false,
            other => return Err(format!("mode is files or whole, not {other}")),
        };
        let n = out.iter().filter(|i| i.format == format).count();
        out.push(Input {
            format: format.into(),
            path: PathBuf::from(path),
            whole,
            label: if n == 0 {
                format.into()
            } else {
                format!("{format}-{n}")
            },
        });
    }
    Ok(out)
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let Ok(kind) = e.file_type() else { continue };
        if kind.is_dir() {
            walk(&e.path(), out);
        } else if kind.is_file() {
            out.push(e.path());
        }
    }
}

/// An input's files, placed under `<label>/` (a single-file input keeps
/// its file name).
fn files_of(input: &Input) -> Vec<Candidate> {
    let mut files = Vec::new();
    if input.path.is_file() {
        files.push(input.path.clone());
    } else {
        walk(&input.path, &mut files);
    }
    files
        .into_iter()
        .filter_map(|path| {
            let size = fs::metadata(&path).ok()?.len();
            let rel = if input.path.is_file() {
                PathBuf::from(path.file_name()?)
            } else {
                path.strip_prefix(&input.path).ok()?.to_path_buf()
            };
            Some(Candidate {
                rel: Path::new(&input.label).join(rel),
                path,
                stratum: input.label.clone(),
                size,
                grows: false,
            })
        })
        .collect()
}

/// Where an input's sample is handed to `--read`.
fn source_path(home: &Path, input: &Input) -> PathBuf {
    let dir = home.join(&input.label);
    if input.path.is_file() {
        dir.join(input.path.file_name().unwrap_or_default())
    } else {
        dir
    }
}

fn sample(per_input: u64, seed: u64) -> Result<(), String> {
    let mut all = Vec::new();
    let mut chosen = Vec::new();
    for input in inputs()? {
        let files = files_of(&input);
        if input.whole {
            chosen.extend(files.iter().cloned());
        } else {
            chosen.extend(bench::select(&files, per_input, seed));
        }
        all.extend(files);
    }
    let manifest = Manifest::build("sinex", &all, &chosen, per_input, seed, 1.0)
        .map_err(|e| format!("hash the sample: {e}"))?;
    manifest
        .write(&manifest_path())
        .map_err(|e| format!("write {}: {e}", manifest_path().display()))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "manifest": manifest_path(),
            "id": manifest.id(),
            "files": manifest.entries.len(),
            "representativeness": manifest.representativeness(),
        }))
        .unwrap_or_default()
    );
    Ok(())
}

fn sinexd_binary() -> Result<PathBuf, String> {
    sinexd_binary_measured().map(|(binary, _)| binary)
}

fn sinexd_binary_measured() -> Result<(PathBuf, Value), String> {
    let schema_started = Instant::now();
    // The query macros check against the schema database at compile time,
    // as `xtask check` does; the server stops on drop if this started it.
    let (server, databases) =
        crate::database::prepare(Path::new(".")).map_err(|e| format!("schema database: {e}"))?;
    let schema_setup_s = schema_started.elapsed().as_secs_f64();
    let compile_started = Instant::now();
    let ok = devkit::cargo_command()
        .args(["build", "--locked", "--release", "-p", "sinexd"])
        .envs(crate::database::env(&server, &databases))
        .status()
        .map_err(|e| e.to_string())?
        .success();
    if !ok {
        return Err("building sinexd failed".into());
    }
    Ok((
        crate::target_dir().join("release/sinexd"),
        json!({
            "compile_s": compile_started.elapsed().as_secs_f64(), "schema_setup_s": schema_setup_s
        }),
    ))
}

/// A throwaway PostgreSQL cluster for one bench run, stopped when dropped.
struct Cluster {
    data: PathBuf,
    socket: PathBuf,
    stopped: bool,
}

impl Cluster {
    /// Start a cluster with the deployment's settings (`nix/module.nix`),
    /// then `settings` (`name=value`) over them.
    fn start(dir: &Path, fsync: bool, settings: &[String]) -> Result<Cluster, String> {
        let data = dir.join("data");
        let socket = dir.to_path_buf();
        fs::create_dir_all(dir).map_err(|e| e.to_string())?;
        let ok = Command::new("initdb")
            .args([
                "--no-sync",
                "--no-locale",
                "--auth=trust",
                "-U",
                "sinex",
                "-D",
            ])
            .arg(&data)
            .stdout(std::process::Stdio::null())
            .status()
            .map_err(|e| format!("initdb: {e}"))?
            .success();
        if !ok {
            return Err("initdb failed".into());
        }
        let mut configuration = measurements::postgres_configuration(fsync, settings)?;
        let directory = format!("\"{}\"", socket.to_string_lossy().replace('"', "\"\""));
        configuration.push_str(&format!(
            "listen_addresses = ''\nunix_socket_directories = '{}'\n",
            directory.replace('\\', "\\\\").replace('\'', "''")
        ));
        fs::write(data.join("postgresql.auto.conf"), configuration).map_err(|e| e.to_string())?;
        let ok = Command::new("pg_ctl")
            .arg("-D")
            .arg(&data)
            .arg("-l")
            .arg(dir.join("log"))
            .args(["-w", "start"])
            .stdout(std::process::Stdio::null())
            .status()
            .map_err(|e| format!("pg_ctl: {e}"))?
            .success();
        if !ok {
            return Err(format!(
                "PostgreSQL did not start; see {}",
                dir.join("log").display()
            ));
        }
        let cluster = Cluster {
            data,
            socket,
            stopped: false,
        };
        let ok = Command::new("createdb")
            .arg("-h")
            .arg(&cluster.socket)
            .args(["-U", "sinex", "sinex"])
            .status()
            .map_err(|e| e.to_string())?
            .success();
        if !ok {
            return Err("createdb failed".into());
        }
        Ok(cluster)
    }

    fn stop(&mut self) -> Result<f64, String> {
        if self.stopped {
            return Ok(0.0);
        }
        let started = Instant::now();
        let output = Command::new("pg_ctl")
            .arg("-D")
            .arg(&self.data)
            .args(["-m", "fast", "-w", "-t", "3600", "stop"])
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "benchmark PostgreSQL shutdown failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        self.stopped = true;
        Ok(started.elapsed().as_secs_f64())
    }

    fn url(&self) -> String {
        measurements::socket_url(&self.socket)
    }

    /// Run `sql` in the bench database and return its unaligned output.
    fn psql(&self, sql: &str) -> Option<String> {
        let out = Command::new("psql")
            .arg("-h")
            .arg(&self.socket)
            .args(["-U", "sinex", "-v", "ON_ERROR_STOP=1", "-Atc", sql, "sinex"])
            .output()
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    }

    fn database_mb(&self) -> Option<f64> {
        let out = Command::new("psql")
            .arg("-h")
            .arg(&self.socket)
            .args([
                "-U",
                "sinex",
                "-Atc",
                "select pg_database_size('sinex')",
                "sinex",
            ])
            .output()
            .ok()?;
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse::<f64>()
            .ok()
            .map(|b| b / 1e6)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if !self.stopped
            && let Err(error) = self.stop()
        {
            eprintln!("{error}");
        }
    }
}

/// Runs `sinexd <args> --json` under a memory cap and the real-data lock
/// inside the admitted AgentCTL job, and returns its JSON line.
fn sinexd(binary: &Path, memory_max: &str, args: &[&std::ffi::OsStr]) -> Result<Value, String> {
    sinexd_recorded(binary, memory_max, args, None)
}

fn sinexd_recorded(
    binary: &Path,
    memory_max: &str,
    args: &[&std::ffi::OsStr],
    out: Option<&Path>,
) -> Result<Value, String> {
    let mut command = Command::new("systemd-run");
    if let Some(out) = out {
        command.env("SINEX_PRIVATE_MODE", out.join("private-mode"));
    }
    let output = command
        .env("SINEX_BENCH_TIMINGS", "1")
        .args(["--user", "--scope", "--quiet", "-p"])
        .arg(format!("MemoryMax={memory_max}"))
        .args(["-p", "MemorySwapMax=0", "--", "flock", "-o", LOCK])
        .arg(binary)
        .args(args)
        .arg("--json")
        .output()
        .map_err(|e| e.to_string())?;
    if let Some(out) = out {
        fs::write(out.join("backfill.stdout"), &output.stdout).map_err(|e| e.to_string())?;
        fs::write(out.join("backfill.stderr"), &output.stderr).map_err(|e| e.to_string())?;
    }
    let parsed = String::from_utf8_lossy(&output.stdout)
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str::<Value>(l).ok());
    match parsed {
        Some(v) if output.status.success() => Ok(v),
        _ => Err(format!(
            "{} ({}): {}",
            output.status,
            if output.status.code().is_none() {
                "killed; over the memory cap?"
            } else {
                "failed"
            },
            String::from_utf8_lossy(&output.stderr)
                .lines()
                .last()
                .unwrap_or_default()
        )),
    }
}

/// Bytes free in the filesystem holding `path`.
fn available(path: &Path) -> Option<u64> {
    let out = Command::new("df")
        .args(["--output=avail", "-B1"])
        .arg(path)
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .nth(1)?
        .trim()
        .parse()
        .ok()
}

fn dir_mb(path: &Path) -> f64 {
    let mut files = Vec::new();
    walk(path, &mut files);
    files
        .iter()
        .filter_map(|f| fs::metadata(f).ok())
        .map(|m| m.len())
        .sum::<u64>() as f64
        / 1e6
}

/// An empty output directory for one run: on /realm for `disk`, in /dev/shm
/// for `memory` when it has room for about four times `input_bytes`.
fn output_dir(storage: &str, input_bytes: u64) -> Result<PathBuf, String> {
    let out = match storage {
        "disk" => bench_home().join("out"),
        "memory" => PathBuf::from("/dev/shm/sinex-bench"),
        other => return Err(format!("storage is disk or memory, not {other}")),
    };
    // The database and content store take a few times the input; filling
    // /dev/shm would push the desktop into swap.
    if storage == "memory"
        && available(Path::new("/dev/shm")).is_some_and(|free| free < input_bytes * 4)
    {
        return Err(format!(
            "/dev/shm has less than four times the input ({:.0} MB) free; use --storage disk",
            input_bytes as f64 / 1e6
        ));
    }
    let out = measurements::run_directory(&out)?;
    Ok(out)
}

fn apply_schema(binary: &Path, url: &str) -> Result<(), String> {
    let schema = Command::new(binary)
        .args(["admin", "schema", "--database", url])
        .output()
        .map_err(|e| e.to_string())?;
    if !schema.status.success() {
        return Err(format!(
            "sinexd admin schema: {}",
            String::from_utf8_lossy(&schema.stderr)
        ));
    }
    Ok(())
}

struct SyntheticOptions {
    events: u64,
    storage: String,
    memory_max: String,
    settings: Vec<String>,
    output: Option<PathBuf>,
    older_binary: Option<PathBuf>,
    prefill_keys: u64,
    publication_probe: bool,
    consumers: bool,
    mixed: bool,
}

fn synthetic(options: SyntheticOptions) -> Result<(), String> {
    let SyntheticOptions {
        events,
        storage,
        memory_max,
        settings,
        output,
        older_binary,
        prefill_keys,
        publication_probe,
        consumers,
        mixed,
    } = options;
    let out = match output {
        Some(parent) => measurements::run_directory(&parent)?,
        None => output_dir(
            &storage,
            events.saturating_mul(if mixed { 1024 } else { 48 }),
        )?,
    };
    if bench::storage_of(&out) != storage {
        return Err(format!(
            "--storage {storage} does not match output directory {} ({})",
            out.display(),
            bench::storage_of(&out)
        ));
    }
    if storage == "memory"
        && available(&out).is_some_and(|free| {
            free < events
                .saturating_add(prefill_keys)
                .saturating_mul(if mixed { 4096 } else { 192 })
        })
    {
        return Err(
            "insufficient memory-filesystem space for the generated workload; use --storage disk"
                .into(),
        );
    }
    let harness_started_unix_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs_f64();
    let build = Instant::now();
    let (built_binary, build_times) = sinexd_binary_measured()?;
    let build_s = build.elapsed().as_secs_f64();
    let binary = out.join("sinexd");
    fs::copy(&built_binary, &binary).map_err(|e| e.to_string())?;
    let setup = Instant::now();
    let inputs = measurements::synthetic_inputs(&out, events, mixed)?;
    let mut cluster = Cluster::start(&out.join("pg"), storage == "disk", &settings)?;
    let url = cluster.url();
    apply_schema(&binary, &url)?;
    if let Some(older) = older_binary {
        let output = Command::new(older)
            .args(["admin", "schema", "--database", &url])
            .output()
            .map_err(|e| e.to_string())?;
        fs::write(out.join("older-binary.stderr"), &output.stderr).map_err(|e| e.to_string())?;
        let error = String::from_utf8_lossy(&output.stderr);
        if output.status.success()
            || !error.contains("database is at schema version")
            || !error.contains("this build declares version")
        {
            return Err("older binary did not refuse the new schema with a version error".into());
        }
    }
    cluster
        .psql("CREATE EXTENSION pg_stat_statements")
        .ok_or("pg_stat_statements is not available")?;
    if prefill_keys > 0 {
        let prefill = out.join("prefill");
        fs::create_dir(&prefill).map_err(|e| e.to_string())?;
        let input = measurements::journal_input(&prefill, prefill_keys, "prefill")?;
        measurements::with_consumers(
            &binary,
            &cluster,
            &out,
            &[input],
            measurements::ConsumerCounts::all_new(prefill_keys),
            &memory_max,
        )?;
        for name in [
            "daemon.log",
            "nats.log",
            "sources.json",
            "consumer-positions.json",
            "nats.json",
        ] {
            fs::rename(out.join(name), prefill.join(name)).map_err(|e| e.to_string())?;
        }
        measurements::save(
            &prefill,
            "before-measurement.json",
            &measurements::snapshot(&cluster)?,
        )?;
    }
    let effective = measurements::settings(&cluster)?;
    measurements::save(&out, "settings.json", &effective)?;
    measurements::save(&out, "environment.json", &measurements::environment(&out))?;
    let setup_s = setup.elapsed().as_secs_f64();
    cluster
        .psql("SELECT pg_stat_statements_reset()")
        .ok_or("resetting statement statistics failed")?;
    let before = measurements::snapshot(&cluster)?;
    measurements::save(&out, "before.json", &before)?;
    let load = LoadMonitor::start()?;
    let started = Instant::now();
    let execution = if consumers {
        measurements::with_consumers(
            &binary,
            &cluster,
            &out,
            &inputs,
            measurements::ConsumerCounts::all_new(
                events
                    .checked_add(prefill_keys)
                    .ok_or("event count overflow")?,
            ),
            &memory_max,
        )
    } else {
        let store = out.join("store");
        let mut args: Vec<std::ffi::OsString> = ["backfill", "--database", &url, "--store"]
            .into_iter()
            .map(Into::into)
            .collect();
        args.push(store.into_os_string());
        args.extend(["--host".into(), "bench".into()]);
        for input in &inputs {
            args.extend([
                "--read".into(),
                format!("{}={}", input.format, input.path.display()).into(),
            ]);
        }
        let args: Vec<_> = args.iter().map(|s| s.as_os_str()).collect();
        sinexd_recorded(&binary, &memory_max, &args, Some(&out))
    };
    let execution_s = started.elapsed().as_secs_f64();
    let load = load.finish()?;
    let statements = measurements::statements(&cluster)?;
    let after = measurements::snapshot(&cluster)?;
    measurements::save(&out, "statements.json", &statements)?;
    measurements::save(&out, "after.json", &after)?;
    measurements::save(&out, "load.json", &load.to_json())?;
    let result = match execution {
        Ok(result) => result,
        Err(error) => {
            measurements::save(
                &out,
                "failure.json",
                &json!({"error": error, "build_s": build_s, "setup_s": setup_s, "execution_s": execution_s}),
            )?;
            return Err(format!("{error}; artifacts: {}", out.display()));
        }
    };
    measurements::save(&out, "result.json", &result)?;
    let stages = measurements::stage_summary(&out)?;
    let totals = if result["sources"].is_object() {
        measurements::source_totals(&result)
    } else {
        stages["admission"]["totals"].clone()
    };
    let correctness = measurements::correctness(
        &cluster,
        events
            .checked_add(prefill_keys)
            .ok_or("event count overflow")?,
    )?;
    let complete = correctness["complete"] == true
        && (consumers || totals["admitted"].as_u64() == Some(events));
    let batches = totals["batches"].as_u64();
    let normalized = measurements::normalize_statements(&statements, batches);
    measurements::save(&out, "statements-per-batch.json", &normalized)?;
    let mut metrics = json!({
        "harness_started_unix_s": harness_started_unix_s,
        "complete": complete, "events": events, "prefill_keys": prefill_keys, "offered": totals["events_offered"],
        "admitted": correctness["events"].as_u64().and_then(|n| n.checked_sub(prefill_keys)), "batches": batches,
        "build_s": build_s, "build": build_times, "setup_s": setup_s, "execution_s": execution_s,
        "wall_s": totals["wall_s"], "admission_s": totals["admission_s"],
        "admission_events_per_s": totals["admission_events_per_s"],
        "events_per_s": events as f64 / execution_s,
        "peak_rss_mb": result["peak_rss_mb"], "source_totals": totals, "stages": stages,
        "consumers": result["consumers"], "correctness": correctness,
        "database_mb": cluster.database_mb(), "memory_max": memory_max,
        "postgres_overrides": settings, "effective_postgres": effective,
        "workload": if mixed { "mixed-command-journal" } else { "zsh" },
        "batch_limits": {"events": 10000, "bytes": 16777216},
        "wal_bytes": measurements::wal_delta(&before, &after),
        "wal_bytes_per_event": measurements::wal_delta(&before, &after).map(|b| b as f64 / events.max(1) as f64),
        "search_trigram_bytes": measurements::relation_size(&after, "search_text_trigram", "bytes"),
        "search_tsv_bytes": measurements::relation_size(&after, "search_text_tsv", "bytes"),
        "live_equivalence_bytes": measurements::relation_size(&after, "live_equivalence", "total_bytes"),
        "published_bytes": measurements::relation_size(&after, "published", "total_bytes"),
        "artifacts": out,
    });
    measurements::save(&out, "metrics.json", &metrics)?;
    if consumers {
        metrics["query_probes"] = measurements::query_probes(&cluster, &out)?;
    }
    if publication_probe {
        metrics["publication_probe"] = measurements::publication_probe(&cluster, &out)?;
    }
    metrics["teardown_s"] = match cluster.stop() {
        Ok(seconds) => json!(seconds),
        Err(error) => {
            measurements::save(
                &out,
                "failure.json",
                &json!({"error": error, "metrics": metrics}),
            )?;
            return Err(error);
        }
    };
    measurements::save(&out, "metrics.json", &metrics)?;
    let id = if complete {
        let store = Store::open().map_err(|e| e.to_string())?;
        let checkout = env::current_dir().map_err(|e| e.to_string())?;
        Some(
            bench::record(
                &store,
                &checkout,
                "sinex",
                if consumers {
                    "synthetic-consumers"
                } else {
                    "synthetic-admission"
                },
                &format!("{}-{events}", if mixed { "mixed" } else { "zsh" }),
                &out,
                &load,
                &metrics,
            )
            .map_err(|e| e.to_string())?,
        )
    } else {
        None
    };
    println!(
        "{}",
        json!({"run": id, "storage": bench::storage_of(&out),
        "load_checks_passed": load.checks_passed(), "metrics": metrics})
    );
    if complete {
        Ok(())
    } else {
        Err(format!(
            "synthetic correctness checks failed; artifacts: {}",
            out.display()
        ))
    }
}

fn run(storage: &str, memory_max: &str) -> Result<(), String> {
    let manifest = Manifest::read(&manifest_path()).map_err(|e| {
        format!(
            "{}: {e} (run `xtask bench sample` first)",
            manifest_path().display()
        )
    })?;
    let inputs = inputs()?;
    let home = bench_home().join("sample");
    let fresh = manifest
        .materialize(&home)
        .map_err(|e| format!("materialize the sample: {e}"))?;
    eprintln!(
        "sample {} ({} files, {:.0} MB){}",
        manifest.id(),
        manifest.entries.len(),
        manifest.bytes() as f64 / 1e6,
        if fresh { ", copied" } else { "" }
    );
    let out = output_dir(storage, manifest.bytes())?;
    let binary = sinexd_binary()?;
    let load = LoadMonitor::start()?;
    let mut cluster = Cluster::start(&out.join("pg"), storage == "disk", &[])?;
    let url = cluster.url();
    let store_dir = out.join("store");
    apply_schema(&binary, &url)?;
    let shape = manifest.representativeness();
    let scale_of = |label: &str| -> Option<f64> {
        shape["strata"]
            .as_array()?
            .iter()
            .find(|s| s["stratum"] == label)
            .and_then(|s| {
                let corpus = s["corpus"]["bytes"].as_f64()?;
                let sample = s["sample"]["bytes"].as_f64()?;
                (sample > 0.0).then_some(corpus / sample)
            })
    };
    let mut inputs_out = serde_json::Map::new();
    let mut estimates = Vec::new();
    for input in &inputs {
        let read = format!("{}={}", input.format, source_path(&home, input).display());
        let args: Vec<&std::ffi::OsStr> = [
            "backfill".as_ref(),
            "--database".as_ref(),
            url.as_ref(),
            "--store".as_ref(),
            store_dir.as_os_str(),
            "--host".as_ref(),
            "bench".as_ref(),
            "--read".as_ref(),
            read.as_ref(),
        ]
        .to_vec();
        let scale = scale_of(&input.label);
        let result = match sinexd(&binary, memory_max, &args) {
            Ok(v) => {
                let wall = source_wall_seconds(&v);
                let estimate =
                    bench::linear_estimate(wall, &std::collections::BTreeMap::new(), scale);
                estimates.push(estimate.clone());
                json!({"ok": true, "scale": scale, "wall_s": wall,
                       "linear_estimate": estimate, "result": v})
            }
            Err(e) => {
                estimates.push(bench::linear_estimate(
                    None,
                    &std::collections::BTreeMap::new(),
                    scale,
                ));
                json!({"ok": false, "error": e})
            }
        };
        eprintln!("{} {}", input.label, result);
        inputs_out.insert(input.label.clone(), result);
    }
    let derive = sinexd(
        &binary,
        memory_max,
        &[
            "derive".as_ref(),
            "--database".as_ref(),
            url.as_ref(),
            "--store".as_ref(),
            store_dir.as_os_str(),
            "--host".as_ref(),
            "bench".as_ref(),
        ],
    );
    let scale = shape["scale"].as_f64();
    let derive_complete = derive.is_ok();
    let derive = match derive {
        Ok(value) => {
            estimates.push(bench::linear_estimate(
                value["wall_s"].as_f64(),
                &std::collections::BTreeMap::new(),
                scale,
            ));
            value
        }
        Err(error) => {
            estimates.push(bench::linear_estimate(
                None,
                &std::collections::BTreeMap::new(),
                scale,
            ));
            json!({"error": error})
        }
    };
    let complete = benchmark_complete(&inputs_out, derive_complete);
    let mut metrics = json!({
        "complete": complete,
        "inputs": inputs_out,
        "derive": derive,
        "database_mb": cluster.database_mb(),
        "store_mb": dir_mb(&store_dir),
        "sample_input_file_bytes": manifest.bytes(),
        "memory_max": memory_max,
        "linear_estimate": combined_estimate(&estimates),
    });
    metrics["teardown_s"] = json!(cluster.stop()?);
    let load = load.finish()?;
    let id = if complete {
        let store = Store::open().map_err(|e| e.to_string())?;
        let checkout = env::current_dir().map_err(|e| e.to_string())?;
        Some(
            bench::record(
                &store,
                &checkout,
                "sinex",
                "completed-backfill",
                &manifest.id(),
                &out,
                &load,
                &metrics,
            )
            .map_err(|e| e.to_string())?,
        )
    } else {
        None
    };
    println!(
        "{}",
        json!({"run": id, "storage": bench::storage_of(&out),
               "load_checks_passed": load.checks_passed(), "load": load.to_json(), "metrics": metrics})
    );
    if storage == "memory" {
        let _ = fs::remove_dir_all(&out);
    }
    if complete {
        Ok(())
    } else {
        Err("benchmark incomplete: no completed-run record was saved; see inputs and derive results".into())
    }
}

fn benchmark_complete(inputs: &serde_json::Map<String, Value>, derive_complete: bool) -> bool {
    derive_complete && !inputs.is_empty() && inputs.values().all(|input| input["ok"] == true)
}

/// Sum only when every successful source reported a finite wall time.
fn source_wall_seconds(result: &Value) -> Option<f64> {
    let sources = result["sources"].as_object()?;
    if sources.is_empty() {
        return None;
    }
    sources.values().try_fold(0.0, |total, source| {
        let seconds = source["wall_s"]
            .as_f64()
            .filter(|n| n.is_finite() && *n >= 0.0)?;
        let next = total + seconds;
        next.is_finite().then_some(next)
    })
}

fn combined_estimate(parts: &[Value]) -> Value {
    let seconds = if parts.is_empty() {
        None
    } else {
        parts.iter().try_fold(0.0, |total, part| {
            let seconds = part["estimate"]["seconds"].as_f64()?;
            let next = total + seconds;
            (next.is_finite() && seconds >= 0.0).then_some(next)
        })
    };
    json!({"method": "sum_of_input_byte_linear_estimates", "calibrated": false,
        "estimated_seconds": seconds, "components": parts,
        "scope": "Source backfill wall times plus derivation, excluding setup and teardown.",
        "limitation": "A missing or failed component makes the total unknown; input bytes do not measure decoded, hashed or physically written bytes."})
}

#[cfg(test)]
mod tests;
