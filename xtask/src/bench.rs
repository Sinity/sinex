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
use devkit::bench::{self, Candidate, LoadMonitor, Manifest};
use devkit::history::Store;
use serde_json::{Value, json};
use std::io::Write as _;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
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
        #[arg(long, default_value = "disk")]
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
        #[arg(long, default_value = "disk")]
        storage: String,
        #[arg(long, default_value = "6G")]
        memory_max: String,
        /// A PostgreSQL setting over the deployment's, `name=value`;
        /// repeatable. Recorded with the run.
        #[arg(long = "postgres")]
        settings: Vec<String>,
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
        } => synthetic(events, &storage, &memory_max, &settings),
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
    // The query macros check against the schema database at compile time,
    // as `xtask check` does; the server stops on drop if this started it.
    let (server, databases) =
        crate::database::prepare(Path::new(".")).map_err(|e| format!("schema database: {e}"))?;
    let ok = devkit::cargo_command()
        .args(["build", "--locked", "--release", "-p", "sinexd"])
        .envs(crate::database::env(&server, &databases))
        .status()
        .map_err(|e| e.to_string())?
        .success();
    if !ok {
        return Err("building sinexd failed".into());
    }
    Ok(crate::target_dir().join("release/sinexd"))
}

/// A throwaway PostgreSQL cluster for one bench run, stopped when dropped.
struct Cluster {
    data: PathBuf,
    socket: PathBuf,
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
        let mut options = format!(
            "-k {} -c listen_addresses= -c shared_preload_libraries=timescaledb,pg_stat_statements \
             -c timescaledb.enable_chunk_skipping=on -c timescaledb.telemetry_level=off -c fsync={} \
             -c wal_compression=lz4",
            socket.display(),
            if fsync { "on" } else { "off" }
        );
        for setting in settings {
            if !setting.contains('=') || setting.contains(char::is_whitespace) {
                return Err(format!("--postgres {setting}: expected name=value"));
            }
            options.push_str(" -c ");
            options.push_str(setting);
        }
        let ok = Command::new("pg_ctl")
            .arg("-D")
            .arg(&data)
            .arg("-l")
            .arg(dir.join("log"))
            .args(["-w", "-o", &options, "start"])
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
        let cluster = Cluster { data, socket };
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

    fn url(&self) -> String {
        format!(
            "postgresql:///sinex?host={}&user=sinex",
            self.socket.display()
        )
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
        let _ = Command::new("pg_ctl")
            .arg("-D")
            .arg(&self.data)
            .args(["-m", "fast", "stop"])
            .stdout(std::process::Stdio::null())
            .status();
    }
}

/// Runs `sinexd <args> --json` under a memory cap and the real-data lock
/// inside the admitted AgentCTL job, and returns its JSON line.
fn sinexd(binary: &Path, memory_max: &str, args: &[&std::ffi::OsStr]) -> Result<Value, String> {
    let output = Command::new("systemd-run")
        .args(["--user", "--scope", "--quiet", "-p"])
        .arg(format!("MemoryMax={memory_max}"))
        .args(["-p", "MemorySwapMax=0", "--", "flock", "-o", LOCK])
        .arg(binary)
        .args(args)
        .arg("--json")
        .output()
        .map_err(|e| e.to_string())?;
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
    if out.exists() {
        fs::remove_dir_all(&out).map_err(|e| e.to_string())?;
    }
    fs::create_dir_all(&out).map_err(|e| e.to_string())?;
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

/// A zsh extended history of `events` distinct commands a minute apart: the
/// spacing of real shell history, so the parser's per-second carry stays
/// realistic.
fn synthetic_history(events: u64) -> Vec<u8> {
    let mut text = Vec::with_capacity(usize::try_from(events).unwrap_or(0) * 48);
    for i in 0..events {
        let start = 1_600_000_000 + i * 60;
        let _ = writeln!(text, ": {start}:0;echo synthetic {i}");
    }
    text
}

fn synthetic(
    events: u64,
    storage: &str,
    memory_max: &str,
    settings: &[String],
) -> Result<(), String> {
    let history = synthetic_history(events);
    let out = output_dir(storage, history.len() as u64)?;
    let input = out.join("zsh_history");
    fs::write(&input, &history).map_err(|e| format!("{}: {e}", input.display()))?;
    drop(history);
    let binary = sinexd_binary()?;
    let load = LoadMonitor::start()?;
    let cluster = Cluster::start(&out.join("pg"), storage == "disk", settings)?;
    let url = cluster.url();
    apply_schema(&binary, &url)?;
    cluster
        .psql("CREATE EXTENSION pg_stat_statements")
        .ok_or("pg_stat_statements is not available")?;
    let store_dir = out.join("store");
    let read = format!("zsh={}", input.display());
    let result = sinexd(
        &binary,
        memory_max,
        &[
            "backfill".as_ref(),
            "--database".as_ref(),
            url.as_ref(),
            "--store".as_ref(),
            store_dir.as_os_str(),
            "--host".as_ref(),
            "bench".as_ref(),
            "--read".as_ref(),
            read.as_ref(),
        ],
    );
    let database_mb = cluster.database_mb();
    // Where admission's time went, by statement.
    let statements = cluster.psql(
        "SELECT coalesce(json_agg(s), '[]') FROM (SELECT calls, round(total_exec_time) AS ms, rows, \
         left(regexp_replace(query, '\\s+', ' ', 'g'), 200) AS query FROM pg_stat_statements \
         ORDER BY total_exec_time DESC LIMIT 20) s",
    );
    drop(cluster);
    let load = load.finish()?;
    let result = result?;
    let totals = &result["sources"]["terminal.zsh-history"];
    let complete = totals["admitted"].as_u64() == Some(events);
    let metrics = json!({
        "complete": complete,
        "events": events,
        "admitted": totals["admitted"],
        "batches": totals["batches"],
        "wall_s": totals["wall_s"],
        "admission_s": totals["admission_s"],
        "admission_events_per_s": totals["admission_events_per_s"],
        "events_per_s": totals["events_per_s"],
        "peak_rss_mb": result["peak_rss_mb"],
        "database_mb": database_mb,
        "memory_max": memory_max,
        "postgres": settings,
    });
    let id = if complete {
        let store = Store::open().map_err(|e| e.to_string())?;
        let checkout = env::current_dir().map_err(|e| e.to_string())?;
        Some(
            bench::record(
                &store,
                &checkout,
                "sinex",
                "synthetic-admission",
                &format!("zsh-{events}"),
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
               "load_checks_passed": load.checks_passed(), "load": load.to_json(), "metrics": metrics,
               "statements": statements.and_then(|s| serde_json::from_str::<Value>(&s).ok())})
    );
    if storage == "memory" {
        let _ = fs::remove_dir_all(&out);
    }
    if complete {
        Ok(())
    } else {
        Err(format!(
            "admitted {} of {events} synthetic events; no record was saved",
            totals["admitted"]
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
    let cluster = Cluster::start(&out.join("pg"), storage == "disk", &[])?;
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
    let metrics = json!({
        "complete": complete,
        "inputs": inputs_out,
        "derive": derive,
        "database_mb": cluster.database_mb(),
        "store_mb": dir_mb(&store_dir),
        "sample_input_file_bytes": manifest.bytes(),
        "memory_max": memory_max,
        "linear_estimate": combined_estimate(&estimates),
    });
    drop(cluster);
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
