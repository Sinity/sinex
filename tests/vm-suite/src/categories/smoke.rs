//! Smoke tests — fast assertions that every sinex VM deployment must satisfy.
//!
//! Tests assert behavioral invariants visible to users:
//! "core.events exists", "pipeline captures filesystem events", etc.
//! Not implementation details.

use std::process::Command;
use std::time::{Duration, Instant};

use color_eyre::eyre::{Context, Result};
use sqlx::PgPool;

use crate::runner::{EvidenceKind, MissingEvidencePolicy, TestRunner};

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Smoke tests ────────────────────────────────");

    let pool = PgPool::connect(database_url).await?;

    // 1. Core schema tables exist
    test_schema_tables(runner, &pool).await;

    // 2. TimescaleDB extension installed
    test_timescaledb_extension(runner, &pool).await;

    // 3. core.events is a hypertable (not a plain table)
    test_events_hypertable(runner, &pool).await;

    // 4. sinexd systemd unit is active
    test_service_active(runner, "sinexd");

    // 5. Filesystem event pipeline: create files → events appear in DB
    test_filesystem_pipeline(runner, &pool).await;

    // 6. Batch event capture: 20 files → count increases
    test_batch_capture(runner, &pool).await;

    // 7. Service restart: pipeline works after daemon restart
    test_service_restart(runner, &pool).await;

    // 8. Database can be queried after restart (no lock/crash)
    test_db_queryable(runner, &pool).await;

    Ok(())
}

// ─── Individual test functions ────────────────────────────────────────────────

async fn test_schema_tables(runner: &mut TestRunner, pool: &PgPool) {
    let name = "schema: core.events and raw.source_material_registry exist";

    let result = sqlx::query_scalar::<_, Option<String>>(
        "SELECT string_agg(schemaname || '.' || tablename, ',' ORDER BY 1) \
         FROM pg_tables \
         WHERE (schemaname = 'core'  AND tablename = 'events') \
            OR (schemaname = 'raw'   AND tablename = 'source_material_registry')",
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(Some(ref tables))
            if tables.contains("core.events")
                && tables.contains("raw.source_material_registry") =>
        {
            runner.pass(name);
        }
        Ok(tables) => runner.fail(name, &format!("expected 2 tables, found: {tables:?}")),
        Err(e) => runner.fail(name, &format!("query error: {e}")),
    }
}

async fn test_timescaledb_extension(runner: &mut TestRunner, pool: &PgPool) {
    let name = "timescaledb extension installed";

    let result = sqlx::query_scalar::<_, String>(
        "SELECT extname FROM pg_extension WHERE extname = 'timescaledb'",
    )
    .fetch_optional(pool)
    .await;

    match result {
        Ok(Some(_)) => runner.pass(name),
        Ok(None) => runner.fail(name, "timescaledb not in pg_extension"),
        Err(e) => runner.fail(name, &format!("query error: {e}")),
    }
}

async fn test_events_hypertable(runner: &mut TestRunner, pool: &PgPool) {
    let name = "core.events is a TimescaleDB hypertable";

    let result = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(\
           SELECT 1 FROM timescaledb_information.hypertables \
           WHERE hypertable_schema = 'core' AND hypertable_name = 'events'\
         )",
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(true) => runner.pass(name),
        Ok(false) => {
            runner.fail(name, "core.events is not a TimescaleDB hypertable");
        }
        Err(e) => runner.fail(name, &format!("query error: {e}")),
    }
}

fn test_service_active(runner: &mut TestRunner, service: &str) {
    let name = format!("{service} systemd unit is active");

    match Command::new("systemctl")
        .args(["is-active", "--quiet", service])
        .status()
    {
        Ok(s) if s.success() => runner.pass(&name),
        Ok(_) => runner.fail(&name, "systemctl is-active returned non-zero"),
        Err(e) => runner.fail(&name, &format!("systemctl error: {e}")),
    }
}

async fn test_filesystem_pipeline(runner: &mut TestRunner, pool: &PgPool) {
    let name = "filesystem events captured to DB within 30s";

    let Some(before) = observed_event_count(runner, name, pool).await else {
        return;
    };
    let watched = "/var/lib/sinex/watched";
    if let Err(error) = std::fs::create_dir_all(watched)
        .with_context(|| format!("failed to create watched directory {watched}"))
    {
        runner.require_evidence(
            name,
            EvidenceKind::SourceMaterial,
            false,
            &format!("filesystem fixture setup failed: {error:#}"),
            MissingEvidencePolicy::Block,
        );
        return;
    }

    for i in 0..5_u32 {
        if let Err(error) = std::fs::write(
            format!("{watched}/smoke-test-{i}.txt"),
            format!("smoke test {i}"),
        )
        .with_context(|| format!("failed to write smoke-test-{i}.txt"))
        {
            runner.require_evidence(
                name,
                EvidenceKind::SourceMaterial,
                false,
                &format!("filesystem fixture write failed: {error:#}"),
                MissingEvidencePolicy::Block,
            );
            return;
        }
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let Some(after) = observed_event_count(runner, name, pool).await else {
            return;
        };
        if after > before {
            runner.pass(name);
            return;
        }
        if Instant::now() >= deadline {
            runner.fail(
                name,
                &format!("no new events after 30s (before={before}, after={after})"),
            );
            return;
        }
    }
}

async fn test_batch_capture(runner: &mut TestRunner, pool: &PgPool) {
    let name = "batch capture: 20 files → event count increases";

    let Some(before) = observed_event_count(runner, name, pool).await else {
        return;
    };
    let watched = "/var/lib/sinex/watched";

    for i in 0..20_u32 {
        if let Err(error) = std::fs::write(format!("{watched}/batch-{i}.txt"), format!("batch {i}"))
            .with_context(|| format!("failed to write batch-{i}.txt"))
        {
            runner.require_evidence(
                name,
                EvidenceKind::SourceMaterial,
                false,
                &format!("batch fixture write failed: {error:#}"),
                MissingEvidencePolicy::Block,
            );
            return;
        }
    }

    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let Some(after) = observed_event_count(runner, name, pool).await else {
            return;
        };
        if after > before {
            runner.pass(name);
            return;
        }
        if Instant::now() >= deadline {
            runner.fail(
                name,
                &format!("batch of 20 files produced no events after 40s (before={before}, after={after})"),
            );
            return;
        }
    }
}

async fn test_service_restart(runner: &mut TestRunner, pool: &PgPool) {
    let name = "service restart resilience: pipeline flows after daemon restart";

    // Restart the unit
    let restart_ok = Command::new("systemctl")
        .args(["restart", "sinexd"])
        .status()
        .is_ok_and(|s| s.success());

    if !restart_ok {
        runner.fail(name, "systemctl restart sinexd failed");
        return;
    }

    // Wait for unit to come back active
    let up_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let active = Command::new("systemctl")
            .args(["is-active", "--quiet", "sinexd"])
            .status()
            .is_ok_and(|s| s.success());
        if active {
            break;
        }
        if Instant::now() >= up_deadline {
            runner.fail(
                name,
                "sinexd did not become active within 30s after restart",
            );
            return;
        }
    }

    // Create files after restart and verify pipeline still flows
    let Some(before) = observed_event_count(runner, name, pool).await else {
        return;
    };
    let watched = "/var/lib/sinex/watched";
    for i in 0..5_u32 {
        if let Err(error) = std::fs::write(
            format!("{watched}/post-restart-{i}.txt"),
            format!("post-restart {i}"),
        )
        .with_context(|| format!("failed to write post-restart-{i}.txt"))
        {
            runner.require_evidence(
                name,
                EvidenceKind::SourceMaterial,
                false,
                &format!("post-restart fixture write failed: {error:#}"),
                MissingEvidencePolicy::Block,
            );
            return;
        }
    }

    let drain_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let Some(after) = observed_event_count(runner, name, pool).await else {
            return;
        };
        if after > before {
            runner.pass(name);
            return;
        }
        if Instant::now() >= drain_deadline {
            runner.fail(
                name,
                &format!("pipeline stalled after restart (before={before}, after={after})"),
            );
            return;
        }
    }
}

async fn test_db_queryable(runner: &mut TestRunner, pool: &PgPool) {
    let name = "database queryable: no NULL id/payload rows";

    let result = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM core.events WHERE id IS NULL OR payload IS NULL",
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(0) => runner.pass(name),
        Ok(n) => runner.fail(name, &format!("{n} rows have NULL id or payload")),
        Err(e) => runner.fail(name, &format!("query error: {e}")),
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

async fn event_count(pool: &PgPool) -> Result<i64> {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(pool)
        .await
        .context("failed to count core.events rows")
}

async fn observed_event_count(runner: &mut TestRunner, name: &str, pool: &PgPool) -> Option<i64> {
    match event_count(pool).await {
        Ok(count) => Some(count),
        Err(error) => {
            runner.require_evidence(
                name,
                EvidenceKind::Database,
                false,
                &format!("event count query failed: {error:#}"),
                MissingEvidencePolicy::Block,
            );
            None
        }
    }
}
