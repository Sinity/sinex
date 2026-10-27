use super::*;
use measurements::{ConsumerCounts, query, save};

pub(super) fn run(events: u64, parent: &Path) -> Result<(), String> {
    let out = measurements::run_directory(parent)?;
    if bench::storage_of(&out) != "disk" {
        return Err("case benchmarks require disk storage".into());
    }
    let (built, build) = sinexd_binary_measured()?;
    let binary = out.join("sinexd");
    fs::copy(built, &binary).map_err(|e| e.to_string())?;
    let mut cluster = Cluster::start(&out.join("pg"), true, &[])?;
    apply_schema(&binary, &cluster.url())?;
    cluster
        .psql("CREATE EXTENSION pg_stat_statements")
        .ok_or("pg_stat_statements is not available")?;
    save(&out, "settings.json", &measurements::settings(&cluster)?)?;
    save(&out, "environment.json", &measurements::environment(&out))?;
    let load = LoadMonitor::start()?;
    let started = Instant::now();
    let mut phases = Vec::new();
    for (name, changed) in [("new", false), ("repeat", false), ("replace", true)] {
        let phase = out.join(name);
        fs::create_dir(&phase).map_err(|e| e.to_string())?;
        let input = phase.join("takeout.zip");
        let generator = phase.join("generate.py");
        fs::write(&generator, include_str!("cases.py")).map_err(|e| e.to_string())?;
        let generated = Command::new("python3")
            .arg(generator)
            .arg(&input)
            .arg(events.to_string())
            .arg(if changed { "changed" } else { "complete" })
            .status()
            .map_err(|e| e.to_string())?;
        if !generated.success() {
            return Err("generating case input failed".into());
        }
        if changed && events > 0 {
            let id = query(
                &cluster,
                "SELECT to_jsonb(id::text) FROM core.events WHERE event_type='video.watched' ORDER BY id LIMIT 1",
            )?;
            let output = Command::new(&binary)
                .args([
                    "admin",
                    "hide",
                    &format!("sinex:ev/{}", id.as_str().ok_or("missing hide target")?),
                    "--database",
                    &cluster.url(),
                    "--store",
                ])
                .arg(out.join("store"))
                .output()
                .map_err(|e| e.to_string())?;
            fs::write(phase.join("hide.stdout"), output.stdout).map_err(|e| e.to_string())?;
            fs::write(phase.join("hide.stderr"), output.stderr).map_err(|e| e.to_string())?;
            if !output.status.success() {
                return Err("hiding synthetic case event failed".into());
            }
        }
        cluster
            .psql("SELECT pg_stat_statements_reset()")
            .ok_or("resetting statistics failed")?;
        let before = measurements::snapshot(&cluster)?;
        let result = sinexd_recorded(
            &binary,
            "6G",
            &[
                "backfill".as_ref(),
                "--database".as_ref(),
                cluster.url().as_ref(),
                "--store".as_ref(),
                out.join("store").as_os_str(),
                "--host".as_ref(),
                "bench".as_ref(),
                "--read".as_ref(),
                format!("takeout={}", input.display()).as_ref(),
            ],
            Some(&phase),
        )?;
        let totals = &result["sources"]["google.youtube-history"];
        let admitted = if changed {
            events.saturating_sub(1)
        } else if name == "repeat" {
            0
        } else {
            events
        };
        let repeats = if name == "repeat" { events } else { 0 };
        let hidden = u64::from(changed && events > 0);
        if totals["admitted"].as_u64() != Some(admitted)
            || totals["repeats"].as_u64() != Some(repeats)
            || totals["hidden"].as_u64() != Some(hidden)
            || (changed && totals["replaced"].as_u64() != Some(admitted))
        {
            save(&phase, "result.json", &result)?;
            return Err(format!(
                "incorrect {name} outcomes; see {}",
                phase.display()
            ));
        }
        let expected = counts(&cluster)?;
        let consumers = measurements::with_consumers(&binary, &cluster, &out, &[], expected, "6G")?;
        retain_consumer_logs(&out, &phase)?;
        let after = measurements::snapshot(&cluster)?;
        save(&phase, "before.json", &before)?;
        save(&phase, "after.json", &after)?;
        save(
            &phase,
            "statements.json",
            &measurements::statements(&cluster)?,
        )?;
        save(&phase, "result.json", &result)?;
        if changed {
            let known = query(
                &cluster,
                "SELECT count(*) FROM core.events e WHERE e.event_type='video.watched' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) AND e.payload->>'title' IS NOT NULL AND e.payload->>'channel'='Synthetic channel'",
            )?;
            if known.as_u64() != Some(admitted) {
                return Err("replacement lost known title or channel values".into());
            }
        }
        phases.push(json!({"name": name, "result": result, "consumers": consumers, "stages": measurements::stage_summary(&phase)?}));
    }
    let phase = out.join("derived");
    fs::create_dir(&phase).map_err(|e| e.to_string())?;
    let inputs = measurements::synthetic_inputs(&phase, 1, false)?;
    sinexd_recorded(
        &binary,
        "6G",
        &[
            "backfill".as_ref(),
            "--database".as_ref(),
            cluster.url().as_ref(),
            "--store".as_ref(),
            out.join("store").as_os_str(),
            "--host".as_ref(),
            "bench".as_ref(),
            "--read".as_ref(),
            format!("zsh={}", inputs[0].path.display()).as_ref(),
        ],
        Some(&phase),
    )?;
    fs::rename(phase.join("backfill.stdout"), phase.join("source.stdout"))
        .map_err(|e| e.to_string())?;
    fs::rename(phase.join("backfill.stderr"), phase.join("source.stderr"))
        .map_err(|e| e.to_string())?;
    let result = sinexd_recorded(
        &binary,
        "6G",
        &[
            "derive".as_ref(),
            "--database".as_ref(),
            cluster.url().as_ref(),
            "--store".as_ref(),
            out.join("store").as_os_str(),
            "--host".as_ref(),
            "bench".as_ref(),
        ],
        Some(&phase),
    )?;
    let derived = query(
        &cluster,
        "SELECT json_build_object('events',count(*),'missing_parents',count(*) FILTER(WHERE EXISTS(SELECT 1 FROM unnest(e.parents) p(id) WHERE NOT EXISTS(SELECT 1 FROM core.events parent WHERE parent.id=p.id)))) FROM core.events e WHERE e.parents IS NOT NULL",
    )?;
    if derived["events"].as_u64().unwrap_or(0) == 0
        || derived["missing_parents"].as_u64() != Some(0)
    {
        return Err("derivation case did not retain valid parent links".into());
    }
    let expected = counts(&cluster)?;
    let consumers = measurements::with_consumers(&binary, &cluster, &out, &[], expected, "6G")?;
    retain_consumer_logs(&out, &phase)?;
    phases.push(json!({"name": "derived", "result": result, "parents": derived, "consumers": consumers, "stages": measurements::stage_summary(&phase)?}));
    let execution_s = started.elapsed().as_secs_f64();
    let load = load.finish()?;
    let teardown_s = cluster.stop()?;
    let metrics = json!({"complete": true, "events": events, "build": build, "execution_s": execution_s,
        "teardown_s": teardown_s, "phases": phases, "artifacts": out,
        "scope": "Synthetic lifecycle cases with consumer catch-up after each source phase; separate from concurrent pipeline throughput."});
    save(&out, "metrics.json", &metrics)?;
    save(&out, "load.json", &load.to_json())?;
    let store = Store::open().map_err(|e| e.to_string())?;
    let checkout = env::current_dir().map_err(|e| e.to_string())?;
    let id = bench::record(
        &store,
        &checkout,
        "sinex",
        "synthetic-cases",
        &format!("video-{events}"),
        &out,
        &load,
        &metrics,
    )
    .map_err(|e| e.to_string())?;
    println!(
        "{}",
        json!({"run": id, "load_checks_passed": load.checks_passed(), "metrics": metrics})
    );
    Ok(())
}

fn counts(cluster: &Cluster) -> Result<ConsumerCounts, String> {
    let counts = query(
        cluster,
        "SELECT json_build_object('events',(SELECT count(*) FROM core.events), 'live',(SELECT coalesce(sum(events),0) FROM core.source_counts), 'visible',(SELECT count(*) FROM core.events e WHERE NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)), 'changes',(SELECT count(*) FROM audit.archive_changes), 'failures',(SELECT count(*) FROM raw.failures))",
    )?;
    if counts["live"] != counts["visible"] || counts["failures"].as_u64() != Some(0) {
        return Err("case counts do not match stored rows".into());
    }
    let events = counts["events"].as_u64().ok_or("missing event count")?;
    Ok(ConsumerCounts {
        events,
        live: counts["live"].as_u64().ok_or("missing live count")?,
        messages: events + counts["changes"].as_u64().ok_or("missing change count")?,
    })
}

fn retain_consumer_logs(out: &Path, phase: &Path) -> Result<(), String> {
    for name in [
        "daemon.log",
        "nats.log",
        "sources.json",
        "consumer-positions.json",
        "nats.json",
    ] {
        fs::rename(out.join(name), phase.join(name)).map_err(|e| e.to_string())?;
    }
    Ok(())
}
