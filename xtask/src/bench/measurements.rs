use super::*;
use std::collections::BTreeMap;
use std::io::{BufWriter, Read, Write as _};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Stdio};
use std::time::Duration;

pub(super) fn save(out: &Path, name: &str, value: &Value) -> Result<(), String> {
    let path = out.join(name);
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
    fs::write(&path, bytes).map_err(|e| format!("{}: {e}", path.display()))
}

pub(super) fn run_directory(parent: &Path) -> Result<PathBuf, String> {
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?;
    let out = parent.join(format!(
        "{}-{}-{}",
        timestamp.as_secs(),
        timestamp.subsec_nanos(),
        std::process::id()
    ));
    fs::create_dir(&out).map_err(|e| e.to_string())?;
    fs::canonicalize(out).map_err(|e| e.to_string())
}

fn postgres_value(value: &Value) -> String {
    match value {
        Value::Bool(true) => "on".into(),
        Value::Bool(false) => "off".into(),
        Value::String(value) => value.clone(),
        value => value.to_string(),
    }
}

pub(super) fn postgres_configuration(disk: bool, overrides: &[String]) -> Result<String, String> {
    let defaults: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("../../../nix/postgres.json"))
            .map_err(|e| e.to_string())?;
    let mut settings: BTreeMap<String, String> = defaults
        .into_iter()
        .map(|(name, value)| (name, postgres_value(&value)))
        .collect();
    for (name, value) in [
        ("shared_preload_libraries", "timescaledb,pg_stat_statements"),
        ("timescaledb.enable_chunk_skipping", "on"),
        ("timescaledb.telemetry_level", "off"),
        ("track_io_timing", "on"),
        ("track_wal_io_timing", "on"),
        ("pg_stat_statements.track_planning", "on"),
        ("fsync", if disk { "on" } else { "off" }),
    ] {
        settings.insert(name.into(), value.into());
    }
    for setting in overrides {
        let (name, value) = setting
            .split_once('=')
            .ok_or_else(|| format!("--postgres {setting}: expected name=value"))?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
            || value.is_empty()
            || value.contains(['\n', '\r', '\0'])
        {
            return Err(format!("invalid PostgreSQL setting: {setting}"));
        }
        settings.insert(name.into(), value.into());
    }
    if disk {
        for name in ["fsync", "full_page_writes", "synchronous_commit"] {
            if !matches!(
                settings
                    .get(name)
                    .map(|s| s.to_ascii_lowercase())
                    .as_deref(),
                Some("on" | "true" | "yes" | "1")
            ) {
                return Err(format!("disk benchmarks require {name}=on"));
            }
        }
    }
    Ok(settings
        .into_iter()
        .map(|(name, value)| {
            format!(
                "{name} = '{}'\n",
                value.replace('\\', "\\\\").replace('\'', "''")
            )
        })
        .collect())
}

pub(super) fn query(cluster: &Cluster, sql: &str) -> Result<Value, String> {
    let output = Command::new("psql")
        .arg("-h")
        .arg(&cluster.socket)
        .args([
            "-U",
            "sinex",
            "-v",
            "ON_ERROR_STOP=1",
            "-qAtc",
            sql,
            "sinex",
        ])
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "benchmark statistics: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|e| format!("benchmark statistics: {e}"))
}

pub(super) fn settings(cluster: &Cluster) -> Result<Value, String> {
    query(
        cluster,
        "SELECT json_agg(s ORDER BY name) FROM (SELECT name, setting, unit, context, source, pending_restart, reset_val FROM pg_settings) s",
    )
}

pub(super) fn snapshot(cluster: &Cluster) -> Result<Value, String> {
    let mut snapshot = query(
        cluster,
        "SELECT json_build_object(
      'extensions', (SELECT json_agg(e) FROM pg_extension e),
      'at', clock_timestamp(), 'wal', (SELECT to_jsonb(w) FROM pg_stat_wal w),
      'io', (SELECT json_agg(i) FROM pg_stat_io i),
      'database', (SELECT to_jsonb(d) FROM pg_stat_database d WHERE datname=current_database()),
      'statements_info', (SELECT to_jsonb(s) FROM pg_stat_statements_info s),
      'tables', (SELECT json_agg(t) FROM pg_stat_user_tables t),
      'indexes', (SELECT json_agg(i) FROM pg_stat_user_indexes i),
      'relations', (SELECT json_agg(r) FROM (SELECT n.nspname AS schema, c.relname AS name,
         c.relkind, pg_relation_size(c.oid) AS bytes,
         CASE WHEN c.relkind IN ('r','m') THEN pg_total_relation_size(c.oid) END AS total_bytes
         FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname IN ('core','raw','audit','privacy','_timescaledb_internal')
           AND c.relkind IN ('r','i','m')) r))",
    )?;
    let host: serde_json::Map<String, Value> = [
        "stat",
        "meminfo",
        "diskstats",
        "pressure/cpu",
        "pressure/memory",
        "pressure/io",
    ]
    .into_iter()
    .map(|name| {
        (
            name.into(),
            json!(fs::read_to_string(Path::new("/proc").join(name)).ok()),
        )
    })
    .collect();
    snapshot["host"] = json!(host);
    Ok(snapshot)
}

pub(super) fn statements(cluster: &Cluster) -> Result<Value, String> {
    query(
        cluster,
        "SELECT coalesce(json_agg(s ORDER BY total_exec_time DESC), '[]') FROM pg_stat_statements s WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())",
    )
}

pub(super) fn normalize_statements(statements: &Value, batches: Option<u64>) -> Value {
    let normalized: Vec<_> = statements
        .as_array()
        .into_iter()
        .flatten()
        .map(|statement| {
            let mut statement = statement.clone();
            statement["exec_ms_per_offered_batch"] = match (
                statement["total_exec_time"].as_f64(),
                batches.filter(|b| *b > 0),
            ) {
                (Some(ms), Some(batches)) => json!(ms / batches as f64),
                _ => Value::Null,
            };
            statement
        })
        .collect();
    json!(normalized)
}

pub(super) fn wal_delta(before: &Value, after: &Value) -> Option<u64> {
    if before["wal"]["stats_reset"] != after["wal"]["stats_reset"] {
        return None;
    }
    after["wal"]["wal_bytes"]
        .as_u64()?
        .checked_sub(before["wal"]["wal_bytes"].as_u64()?)
}

pub(super) fn environment(out: &Path) -> Value {
    let command = |name: &str, args: &[&str]| -> Option<String> {
        let output = Command::new(name).args(args).output().ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    json!({"head": command("git", &["rev-parse", "HEAD"]), "worktree": env::current_dir().ok(),
        "dirty": command("git", &["status", "--porcelain"]),
        "kernel": command("uname", &["-a"]), "cpu": command("lscpu", &["--json"]),
        "mount": command("findmnt", &["--json", "--target", &out.to_string_lossy()]),
        "postgres": command("postgres", &["--version"]), "nats": command("nats-server", &["--version"]),
        "binary_sha256": command("sha256sum", &[&out.join("sinexd").to_string_lossy()]),
        "job_id": env::var("AGENTCTL_JOB_ID").ok(),
        "job_head": env::var("AGENTCTL_CHECKOUT_HEAD").ok(),
        "meminfo": fs::read_to_string("/proc/meminfo").ok(),
    })
}

pub(super) struct SyntheticInput {
    pub format: &'static str,
    pub source: &'static str,
    pub path: PathBuf,
}

pub(super) fn synthetic_inputs(
    out: &Path,
    events: u64,
    mixed: bool,
) -> Result<Vec<SyntheticInput>, String> {
    let commands = if mixed { events / 3 } else { events };
    let path = out.join("zsh_history");
    let mut history = BufWriter::new(fs::File::create(&path).map_err(|e| e.to_string())?);
    for i in 0..commands {
        if mixed && i % 2 == 0 {
            writeln!(history, "echo untimed synthetic {i}").map_err(|e| e.to_string())?;
            continue;
        }
        writeln!(history, ": {}:0;echo synthetic {i}", 1_600_000_000 + i * 60)
            .map_err(|e| e.to_string())?;
    }
    history.flush().map_err(|e| e.to_string())?;
    let mut inputs = vec![SyntheticInput {
        format: "zsh",
        source: "terminal.zsh-history",
        path,
    }];
    if mixed {
        inputs.push(journal_input(out, events - commands, "measurement")?);
    }
    Ok(inputs)
}

pub(super) fn journal_input(
    out: &Path,
    events: u64,
    namespace: &str,
) -> Result<SyntheticInput, String> {
    let path = out.join("journal");
    fs::create_dir(&path).map_err(|e| e.to_string())?;
    let file = fs::File::create(path.join("000000000000.ndjson")).map_err(|e| e.to_string())?;
    let mut file = BufWriter::new(file);
    for i in 0..events {
        // Multiplication permutes the keys so ordered input does not make
        // equivalence-index insertion sequential.
        let key = i.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let message = format!(
            "synthetic café record {i} {}",
            "payload ".repeat((i % 257) as usize)
        );
        let entry = json!({"__CURSOR": format!("{namespace}:s={key:016x};i={i}"),
                "__REALTIME_TIMESTAMP": (1_600_000_000_000_000u64 + i * 1_000_000).to_string(), "MESSAGE": message, "PRIORITY": "6"});
        writeln!(file, "{}", json!({"received_ns": (1_600_000_000_000_000_000u64 + i * 1_000_000_000).to_string(), "line": entry.to_string()})).map_err(|e| e.to_string())?;
    }
    file.flush().map_err(|e| e.to_string())?;
    Ok(SyntheticInput {
        format: "journald",
        source: "machine.journald",
        path,
    })
}

pub(super) fn source_totals(result: &Value) -> Value {
    let Some(sources) = result["sources"].as_object() else {
        return Value::Null;
    };
    if sources.is_empty() {
        return Value::Null;
    }
    let mut totals = serde_json::Map::new();
    for key in [
        "admitted",
        "batches",
        "events_offered",
        "wall_s",
        "admission_s",
    ] {
        let values: Option<Vec<f64>> = sources.values().map(|s| s[key].as_f64()).collect();
        if let Some(values) = values {
            totals.insert(key.into(), json!(values.iter().sum::<f64>()));
        }
    }
    // Counts retain integer JSON representations for correctness checks.
    for key in ["admitted", "batches", "events_offered"] {
        let count: Option<u64> = sources
            .values()
            .try_fold(0u64, |n, s| n.checked_add(s[key].as_u64()?));
        if let Some(count) = count {
            totals.insert(key.into(), json!(count));
        }
    }
    if let (Some(n), Some(s)) = (
        totals.get("admitted").and_then(Value::as_f64),
        totals
            .get("admission_s")
            .and_then(Value::as_f64)
            .filter(|s| *s > 0.0),
    ) {
        totals.insert("admission_events_per_s".into(), json!(n / s));
    }
    json!(totals)
}

pub(super) fn correctness(cluster: &Cluster, events: u64) -> Result<Value, String> {
    let mut counts = query(
        cluster,
        "SELECT json_build_object('events', (SELECT count(*) FROM core.events),
        'commit_events', (SELECT count(*) FROM core.commit_log WHERE event_id IS NOT NULL),
        'last_commit', (SELECT value FROM core.counters WHERE name='commit_seq'),
        'source_counts', (SELECT coalesce(sum(events),0) FROM core.source_counts),
        'run_events', (SELECT coalesce(sum(events),0) FROM core.runs),
        'failures', (SELECT count(*) FROM raw.failures),
        'positions', (SELECT json_agg(p) FROM raw.positions p),
        'consumers', (SELECT json_agg(p) FROM core.consumer_positions p),
        'search_rows', (SELECT count(*) FROM core.search_text))",
    )?;
    counts["complete"] = json!(
        counts["events"].as_u64() == Some(events)
            && counts["commit_events"].as_u64() == Some(events)
            && counts["last_commit"].as_u64() == Some(events)
            && counts["source_counts"].as_u64() == Some(events)
            && counts["run_events"].as_u64() == Some(events)
            && counts["failures"].as_u64() == Some(0)
    );
    Ok(counts)
}

struct Process(Child, Option<String>);
impl Drop for Process {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none()
            && let Some(unit) = &self.1
        {
            let _ = Command::new("systemctl")
                .args(["--user", "kill", "--signal=SIGTERM", unit])
                .status();
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn scope_memory(unit: &str, binary: &Path) -> Option<(f64, u64)> {
    let output = Command::new("systemctl")
        .args([
            "--user",
            "show",
            &format!("{unit}.scope"),
            "--property=ControlGroup",
            "--value",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let group = String::from_utf8(output.stdout).ok()?;
    let group = group.trim();
    if group.is_empty() {
        return None;
    }
    let directory = Path::new("/sys/fs/cgroup").join(group.trim_start_matches('/'));
    let memory = fs::read_to_string(directory.join("memory.peak"))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    for pid in fs::read_to_string(directory.join("cgroup.procs"))
        .ok()?
        .lines()
    {
        let process = Path::new("/proc").join(pid);
        if fs::read_link(process.join("exe")).ok().as_deref() != Some(binary) {
            continue;
        }
        let status = fs::read_to_string(process.join("status")).ok()?;
        let rss = status
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))?
            .split_whitespace()
            .next()?
            .parse::<f64>()
            .ok()?
            / 1024.0;
        return Some((rss, memory));
    }
    None
}

fn free_port() -> Result<u16, String> {
    TcpListener::bind(("127.0.0.1", 0))
        .and_then(|s| s.local_addr())
        .map(|a| a.port())
        .map_err(|e| e.to_string())
}

fn monitor(port: u16) -> Result<Value, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;
    write!(
        stream,
        "GET /jsz?accounts=true&streams=true HTTP/1.0\r\nHost: localhost\r\n\r\n"
    )
    .map_err(|e| e.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| e.to_string())?;
    let body = response
        .split_once("\r\n\r\n")
        .ok_or("NATS monitor returned no body")?
        .1;
    serde_json::from_str(body).map_err(|e| e.to_string())
}

#[derive(Clone, Copy)]
pub(super) struct ConsumerCounts {
    pub live: u64,
    pub events: u64,
    pub messages: u64,
}
impl ConsumerCounts {
    pub fn all_new(events: u64) -> Self {
        Self {
            live: events,
            events,
            messages: events,
        }
    }
}

pub(super) fn with_consumers(
    binary: &Path,
    cluster: &Cluster,
    out: &Path,
    inputs: &[SyntheticInput],
    expected: ConsumerCounts,
    memory_max: &str,
) -> Result<Value, String> {
    let port = free_port()?;
    let monitoring = free_port()?;
    let nats_log = fs::File::create(out.join("nats.log")).map_err(|e| e.to_string())?;
    let mut nats = Process(
        Command::new("nats-server")
            .args([
                "-js",
                "-a",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-m",
                &monitoring.to_string(),
                "-sd",
            ])
            .arg(out.join("nats"))
            .stdout(Stdio::null())
            .stderr(nats_log)
            .spawn()
            .map_err(|e| e.to_string())?,
        None,
    );
    while monitor(monitoring).is_err() {
        if let Some(status) = nats.0.try_wait().map_err(|e| e.to_string())? {
            return Err(format!("NATS exited: {status}"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let catalog = Command::new(binary)
        .args(["admin", "catalog"])
        .output()
        .map_err(|e| e.to_string())?;
    if !catalog.status.success() {
        return Err("sinexd admin catalog failed".into());
    }
    let catalog: Value = serde_json::from_slice(&catalog.stdout).map_err(|e| e.to_string())?;
    let derivations: serde_json::Map<String, Value> = catalog["derivations"]
        .as_array()
        .ok_or("catalog has no derivations")?
        .iter()
        .filter_map(|d| {
            d["name"]
                .as_str()
                .map(|name| (name.to_owned(), json!(false)))
        })
        .collect();
    let sources: serde_json::Map<String, Value> = inputs
        .iter()
        .map(|input| {
            (
                input.source.into(),
                json!({"enable": true, "paths": [input.path], "format": input.format}),
            )
        })
        .collect();
    save(
        out,
        "sources.json",
        &json!({"sources": sources, "derivations": derivations}),
    )?;
    let runtime = out.join("runtime");
    fs::create_dir_all(&runtime).map_err(|e| e.to_string())?;
    let daemon_log = fs::File::create(out.join("daemon.log")).map_err(|e| e.to_string())?;
    let unit = format!(
        "sinex-bench-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos()
    );
    let started = Instant::now();
    let mut daemon = Process(
        Command::new("systemd-run")
            .args([
                "--user",
                "--scope",
                "--quiet",
                "--unit",
                &unit,
                "-p",
                &format!("MemoryMax={memory_max}"),
                "-p",
                "MemorySwapMax=0",
                "--",
            ])
            .arg(binary)
            .args([
                "run",
                "--database",
                &cluster.url(),
                "--nats",
                &format!("nats://127.0.0.1:{port}"),
                "--host",
                "bench",
                "--subject-root",
                "bench",
                "--store",
            ])
            .arg(out.join("store"))
            .arg("--socket")
            .arg(runtime.join("api.sock"))
            .arg("--tokens")
            .arg(&runtime)
            .arg("--sources")
            .arg(out.join("sources.json"))
            .env("SINEX_PRIVATE_MODE", runtime.join("private-mode"))
            .env("SINEX_BENCH_TIMINGS", "1")
            .stdout(daemon_log.try_clone().map_err(|e| e.to_string())?)
            .stderr(daemon_log)
            .spawn()
            .map_err(|e| e.to_string())?,
        Some(format!("{unit}.scope")),
    );
    let measured = (|| {
        let mut admission_s = None;
        let mut search_s = None;
        let mut publication_s = None;
        let mut samples = Vec::new();
        let mut peak_rss_mb: Option<f64> = None;
        let mut memory_peak_bytes: Option<u64> = None;
        let mut last_count = 0;
        let mut last_progress = Instant::now();
        loop {
            if let Some(status) = daemon.0.try_wait().map_err(|e| e.to_string())? {
                return Err(format!("benchmark daemon exited: {status}"));
            }
            let positions = query(cluster, "SELECT json_build_object('admitted', (SELECT coalesce(sum(events),0) FROM core.source_counts),
                'final_commit', (SELECT value FROM core.counters WHERE name='commit_seq'),
                'search', coalesce((SELECT commit_seq FROM core.consumer_positions WHERE consumer='read.search_text'),0),
                'publisher', coalesce((SELECT commit_seq FROM core.consumer_positions WHERE consumer='publisher'),0))")?;
            let elapsed = started.elapsed().as_secs_f64();
            if let Some((rss, memory)) = scope_memory(&unit, binary) {
                peak_rss_mb = Some(peak_rss_mb.unwrap_or(0.0).max(rss));
                memory_peak_bytes = Some(memory_peak_bytes.unwrap_or(0).max(memory));
            }
            let count = positions["admitted"].as_u64().unwrap_or(0)
                + positions["search"].as_u64().unwrap_or(0)
                + positions["publisher"].as_u64().unwrap_or(0);
            if count != last_count {
                last_count = count;
                last_progress = Instant::now();
            }
            if last_progress.elapsed() > Duration::from_secs(600) {
                save(out, "consumer-positions.json", &json!(samples))?;
                return Err(
                    "no admission or consumer progress for ten minutes; see daemon.log".into(),
                );
            }
            let full = positions["admitted"].as_u64() == Some(expected.live);
            if full && admission_s.is_none() {
                admission_s = Some(elapsed);
            }
            if full && positions["search"] == positions["final_commit"] && search_s.is_none() {
                search_s = Some(elapsed);
            }
            if full
                && positions["publisher"] == positions["final_commit"]
                && publication_s.is_none()
            {
                publication_s = Some(elapsed);
            }
            samples.push(json!({"seconds": elapsed, "positions": positions}));
            if admission_s.is_some() && search_s.is_some() && publication_s.is_some() {
                save(out, "consumer-positions.json", &json!(samples))?;
                let nats = monitor(monitoring)?;
                save(out, "nats.json", &nats)?;
                let messages = nats["account_details"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .flat_map(|a| a["stream_detail"].as_array().into_iter().flatten())
                    .find(|s| s["name"] == "BENCH_CONFIRMED")
                    .and_then(|s| s["state"]["messages"].as_u64());
                if messages.is_none() {
                    std::thread::sleep(Duration::from_millis(200));
                    continue;
                }
                if messages != Some(expected.messages) {
                    return Err(format!(
                        "confirmed stream contains {messages:?} messages, expected {}",
                        expected.messages
                    ));
                }
                let correctness = correctness(cluster, expected.events)?;
                if correctness["search_rows"].as_u64() != Some(expected.events) {
                    return Err("search did not retain all generated rows".into());
                }
                return Ok(
                    json!({"peak_rss_mb": peak_rss_mb, "scope_memory_peak_bytes": memory_peak_bytes,
                        "consumers": {"admission_s": admission_s, "search_s": search_s, "publisher_s": publication_s,
                    "search_catch_up_s": search_s.zip(admission_s).map(|(s,a)| s-a),
                    "publisher_catch_up_s": publication_s.zip(admission_s).map(|(p,a)| p-a),
                    "acknowledged_events": messages, "position_resolution_s": 0.2}}),
                );
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    })();
    let stopped = Command::new("systemctl")
        .args([
            "--user",
            "kill",
            "--signal=SIGTERM",
            &format!("{unit}.scope"),
        ])
        .status()
        .map_err(|e| e.to_string())?;
    if !stopped.success() && measured.is_ok() {
        return Err("stopping benchmark scope failed".into());
    }
    let status = daemon.0.wait().map_err(|e| e.to_string())?;
    if !status.success() && measured.is_ok() {
        return Err(format!("benchmark daemon shutdown failed: {status}"));
    }
    if nats.0.try_wait().map_err(|e| e.to_string())?.is_none() {
        let signalled = Command::new("kill")
            .args(["-TERM", &nats.0.id().to_string()])
            .status()
            .map_err(|e| e.to_string())?;
        if !signalled.success() && measured.is_ok() {
            return Err("stopping benchmark NATS failed".into());
        }
        let status = nats.0.wait().map_err(|e| e.to_string())?;
        if !status.success() && measured.is_ok() {
            return Err(format!("benchmark NATS shutdown failed: {status}"));
        }
    }
    measured
}

pub(super) fn stage_summary(out: &Path) -> Result<Value, String> {
    let mut batches = Vec::new();
    let mut publishers = Vec::new();
    let mut search = Vec::new();
    for name in ["daemon.log", "backfill.stderr", "source.stderr"] {
        let path = out.join(name);
        match fs::read_to_string(&path) {
            Ok(text) => {
                for line in text.lines() {
                    if let Ok(record) = serde_json::from_str::<Value>(line) {
                        if record["admission_batch"].is_object() {
                            batches.push(record["admission_batch"].clone());
                        }
                        if record["search_batch"].is_object() {
                            search.push(record["search_batch"].clone());
                        }
                        if record["publisher_page"].is_object() {
                            let mut page = record["publisher_page"].clone();
                            let timings: serde_json::Map<String, Value> =
                                ["read", "acknowledge", "record", "commit"]
                                    .into_iter()
                                    .map(|stage| {
                                        (stage.into(), page[format!("{stage}_ms")].clone())
                                    })
                                    .collect();
                            page["timings_ms"] = json!(timings);
                            publishers.push(page);
                        }
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("{}: {error}", path.display())),
        }
    }
    save(
        out,
        "batch-timings.json",
        &json!({"admission": batches, "search": search, "publisher": publishers}),
    )?;
    Ok(
        json!({"admission": summarize_batches(&batches), "search": summarize_batches(&search), "publisher": summarize_batches(&publishers)}),
    )
}

pub(super) fn summarize_batches(batches: &[Value]) -> Value {
    let mut groups: BTreeMap<u64, BTreeMap<String, Vec<f64>>> = BTreeMap::new();
    for batch in batches {
        let Some(events) = batch["events"].as_u64() else {
            continue;
        };
        for (stage, duration) in batch["timings_ms"].as_object().into_iter().flatten() {
            if let Some(ms) = duration.as_f64().filter(|n| n.is_finite() && *n >= 0.0) {
                groups
                    .entry(events)
                    .or_default()
                    .entry(stage.clone())
                    .or_default()
                    .push(ms);
            }
        }
    }
    let groups: Vec<_> = groups
        .into_iter()
        .map(|(events, stages)| {
            let stages: serde_json::Map<String, Value> = stages
                .into_iter()
                .map(|(stage, mut samples)| {
                    samples.sort_by(f64::total_cmp);
                    let percentile = |fraction: f64| {
                        samples[(samples.len() as f64 * fraction).ceil() as usize - 1]
                    };
                    (
                        stage,
                        json!({"samples": samples.len(), "total_ms": samples.iter().sum::<f64>(),
                "mean_ms": samples.iter().sum::<f64>() / samples.len() as f64,
                "p50_ms": percentile(0.5), "p95_ms": percentile(0.95)}),
                    )
                })
                .collect();
            json!({"events_per_batch": events, "stages": stages})
        })
        .collect();
    let mut totals = serde_json::Map::new();
    if !batches.is_empty() {
        totals.insert("batches".into(), json!(batches.len()));
        for (input, output) in [
            ("events", "events_offered"),
            ("admitted", "admitted"),
            ("repeats", "repeats"),
            ("replaced", "replaced"),
            ("hidden", "hidden"),
            ("older", "older"),
            ("superseded", "superseded"),
            ("failures", "failures"),
        ] {
            let total: Option<u64> = batches
                .iter()
                .try_fold(0u64, |n, b| n.checked_add(b[input].as_u64()?));
            totals.insert(output.into(), json!(total));
        }
    }
    json!({"reported_batches": batches.len(), "totals": totals, "by_batch_size": groups})
}

pub(super) fn relation_size(snapshot: &Value, relation: &str, field: &str) -> Option<u64> {
    snapshot["relations"]
        .as_array()?
        .iter()
        .find(|row| row["schema"] == "core" && row["name"] == relation)?[field]
        .as_u64()
}

pub(super) fn query_probes(cluster: &Cluster, out: &Path) -> Result<Value, String> {
    let partial = query(cluster, "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='core' AND table_name='search_text' AND column_name='trigram_indexed')")?.as_bool().ok_or("missing search policy")?;
    let mut observations = Vec::new();
    for (kind, pattern) in [
        ("command", "(^| )echo synthetic 12345"),
        ("journal", "(^| )synthetic café record 12345"),
    ] {
        let from = if partial {
            "SELECT s.event_id FROM core.visible_search_text s WHERE s.trigram_indexed AND s.text ~ $1::text UNION ALL SELECT s.event_id FROM core.visible_search_text s WHERE NOT s.trigram_indexed AND s.text ~ $1::text"
        } else {
            "SELECT s.event_id FROM core.visible_search_text s WHERE s.text ~ $1::text"
        };
        let sql = format!("SELECT count(*) FROM ({from}) matches");
        let literal = sql.replace("$1::text", &format!("'{pattern}'::text"));
        let prefix =
            "BEGIN READ ONLY; SET LOCAL ROLE sinex_reader; SET LOCAL sinex.grants='guarded'; ";
        let generic = query(
            cluster,
            &format!("{prefix}EXPLAIN (GENERIC_PLAN, FORMAT JSON) {sql}; ROLLBACK"),
        )?;
        let custom = query(
            cluster,
            &format!("{prefix}EXPLAIN (FORMAT JSON) {literal}; ROLLBACK"),
        )?;
        let mut samples = Vec::new();
        for _ in 0..3 {
            samples.push(query(
                cluster,
                &format!(
                    "{prefix}EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) {literal}; ROLLBACK"
                ),
            )?);
        }
        let count = query(
            cluster,
            &format!("{prefix}SELECT to_jsonb(matches) FROM ({literal}) matches; ROLLBACK"),
        )?;
        observations.push(json!({"kind": kind, "pattern": pattern, "matches": count,
            "generic": generic, "custom": custom, "samples": samples}));
    }
    let result = json!({"partial_index": partial, "observations": observations,
        "scope": "Protected search-view prefix branches after consumer completion; planning is non-executing and latency uses read-only execution plans."});
    save(out, "query-probes.json", &result)?;
    Ok(result)
}

pub(super) fn publication_probe(cluster: &Cluster, out: &Path) -> Result<Value, String> {
    let load = LoadMonitor::start()?;
    let script = out.join("publication-probe.py");
    fs::write(&script, include_str!("publication.py")).map_err(|e| e.to_string())?;
    let output = Command::new("python3")
        .arg(&script)
        .arg(cluster.url())
        .arg(out)
        .arg(env::current_dir().map_err(|e| e.to_string())?)
        .output()
        .map_err(|e| e.to_string())?;
    let load = load.finish()?;
    save(out, "publication-probe-load.json", &load.to_json())?;
    fs::write(out.join("publication-probe.stdout"), output.stdout).map_err(|e| e.to_string())?;
    fs::write(out.join("publication-probe.stderr"), output.stderr).map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "publication probe failed; see {}",
            out.join("publication-probe.stderr").display()
        ));
    }
    let result = fs::read(out.join("publication-probe.json")).map_err(|e| e.to_string())?;
    serde_json::from_slice(&result).map_err(|e| e.to_string())
}

pub(super) fn socket_url(path: &Path) -> String {
    use std::os::unix::ffi::OsStrExt;
    let encoded: String = path
        .as_os_str()
        .as_bytes()
        .iter()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || b"/-_.~".contains(byte) {
                char::from(*byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect();
    format!("postgresql:///sinex?host={encoded}&user=sinex")
}
