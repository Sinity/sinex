//! `xtask bench`: backfill and derivation measured on a frozen sample of the
//! operator's real inputs instead of all of them.
//!
//! The inputs are listed, one per line, in `sources.tsv` under the project
//! home's `analysis/bench/` (operator data never enters Git):
//!
//! ```text
//! kind <TAB> path <TAB> files|whole
//! ```
//!
//! A `files` input is a directory whose files stand alone (a day of casts, a
//! log directory): the sample takes a stratified subset of its files. A
//! `whole` input must stay intact (an export with its own layout, a SQLite
//! database, an archive): the sample takes all of it. Each input's kind is a
//! stratum.
//!
//! `bench sample` writes the manifest. `bench run` copies the sample once,
//! starts its own PostgreSQL with TimescaleDB (on /realm for `disk`, on
//! /dev/shm for `memory`), backfills each input separately with a memory cap
//! (one process per input, so each has its own peak memory), runs every
//! derivation once, records the result with the host's load, and predicts
//! the full inputs' time from each input's own rate. Every command it runs
//! runs inside its AgentCTL job and holds the real-data lock.
use devkit::bench::{self, Candidate, HostLoad, Manifest};
use devkit::history::Store;
use serde_json::{Value, json};
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
    kind: String,
    path: PathBuf,
    whole: bool,
    /// The input's directory under the sample home: its kind, numbered when
    /// a kind is listed more than once.
    label: String,
}

fn inputs() -> Result<Vec<Input>, String> {
    let path = analysis().join("sources.tsv");
    let text = fs::read_to_string(&path).map_err(|e| {
        format!(
            "{}: {e} (one `kind<TAB>path<TAB>files|whole` per line)",
            path.display()
        )
    })?;
    let mut out: Vec<Input> = Vec::new();
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        let [kind, path, mode] = f[..] else {
            return Err(format!("{}: bad line `{line}`", path.display()));
        };
        let whole = match mode {
            "whole" => true,
            "files" => false,
            other => return Err(format!("mode is files or whole, not {other}")),
        };
        let n = out.iter().filter(|i| i.kind == kind).count();
        out.push(Input {
            kind: kind.into(),
            path: PathBuf::from(path),
            whole,
            label: if n == 0 {
                kind.into()
            } else {
                format!("{kind}-{n}")
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

/// Where an input's sample is handed to `--source`.
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
    let ok = Command::new("cargo")
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
    fn start(dir: &Path, fsync: bool) -> Result<Cluster, String> {
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
        let options = format!(
            "-k {} -c listen_addresses= -c shared_preload_libraries=timescaledb \
             -c timescaledb.enable_chunk_skipping=on -c timescaledb.telemetry_level=off -c fsync={}",
            socket.display(),
            if fsync { "on" } else { "off" }
        );
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
    let out = match storage {
        "disk" => bench_home().join("out"),
        "memory" => PathBuf::from("/dev/shm/sinex-bench"),
        other => return Err(format!("storage is disk or memory, not {other}")),
    };
    // The database and content store take a few times the input; filling
    // /dev/shm would push the desktop into swap.
    if storage == "memory"
        && available(Path::new("/dev/shm")).is_some_and(|free| free < manifest.bytes() * 4)
    {
        return Err(format!(
            "/dev/shm has less than four times the sample ({:.0} MB) free; use --storage disk",
            manifest.bytes() as f64 / 1e6
        ));
    }
    if out.exists() {
        fs::remove_dir_all(&out).map_err(|e| e.to_string())?;
    }
    let binary = sinexd_binary()?;
    let load = HostLoad::now();
    let cluster = Cluster::start(&out.join("pg"), storage == "disk")?;
    let url = cluster.url();
    let store_dir = out.join("store");
    let schema = Command::new(&binary)
        .args(["admin", "schema", "--database", &url])
        .output()
        .map_err(|e| e.to_string())?;
    if !schema.status.success() {
        return Err(format!(
            "sinexd admin schema: {}",
            String::from_utf8_lossy(&schema.stderr)
        ));
    }
    let shape = manifest.representativeness();
    let scale_of = |label: &str| -> f64 {
        shape["strata"]
            .as_array()
            .and_then(|s| s.iter().find(|s| s["stratum"] == label))
            .and_then(|s| {
                let c = s["corpus"]["bytes"].as_f64()?;
                let m = s["sample"]["bytes"].as_f64()?;
                (m > 0.0).then_some(c / m)
            })
            .unwrap_or(1.0)
    };
    let mut inputs_out = serde_json::Map::new();
    let mut predicted = 0.0;
    for input in &inputs {
        let source = format!("{}={}", input.kind, source_path(&home, input).display());
        let args: Vec<&std::ffi::OsStr> = [
            "backfill".as_ref(),
            "--database".as_ref(),
            url.as_ref(),
            "--store".as_ref(),
            store_dir.as_os_str(),
            "--host".as_ref(),
            "bench".as_ref(),
            "--source".as_ref(),
            source.as_ref(),
        ]
        .to_vec();
        let scale = scale_of(&input.label);
        let result = match sinexd(&binary, memory_max, &args) {
            Ok(v) => {
                let wall: f64 = v["sources"]
                    .as_object()
                    .map(|m| m.values().filter_map(|t| t["wall_s"].as_f64()).sum())
                    .unwrap_or(0.0);
                predicted += wall * scale;
                json!({"ok": true, "scale": scale, "wall_s": wall,
                       "predicted_s": wall * scale, "result": v})
            }
            Err(e) => json!({"ok": false, "error": e}),
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
    let scale = shape["scale"].as_f64().unwrap_or(1.0);
    let derive = match derive {
        Ok(v) => {
            predicted += v["wall_s"].as_f64().unwrap_or(0.0) * scale;
            v
        }
        Err(e) => json!({"error": e}),
    };
    let metrics = json!({
        "inputs": inputs_out,
        "derive": derive,
        "database_mb": cluster.database_mb(),
        "store_mb": dir_mb(&store_dir),
        "sample_mb": manifest.bytes() as f64 / 1e6,
        "memory_max": memory_max,
        "prediction": {"seconds": predicted, "scale": scale},
    });
    drop(cluster);
    let store = Store::open().map_err(|e| e.to_string())?;
    let checkout = env::current_dir().unwrap_or_default();
    let id = bench::record(
        &store,
        &checkout,
        "sinex",
        "backfill",
        &manifest.id(),
        &out,
        load,
        &metrics,
    )
    .map_err(|e| e.to_string())?;
    println!(
        "{}",
        json!({"run": id, "storage": bench::storage_of(&out), "quiet": load.quiet(),
               "metrics": metrics})
    );
    if storage == "memory" {
        let _ = fs::remove_dir_all(&out);
    }
    Ok(())
}
