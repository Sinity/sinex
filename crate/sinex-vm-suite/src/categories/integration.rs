//! Integration tests — deeper behavioral invariants beyond smoke.
//!
//! Tests assert that core services are active, provenance invariants hold,
//! and multi-source ingestion produces non-filesystem event types.

use std::process::Command;

use color_eyre::eyre::Result;
use sqlx::PgPool;

use crate::runner::TestRunner;

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Integration tests ──────────────────────────");

    let pool = PgPool::connect(database_url).await?;

    test_core_services_active(runner);
    test_event_provenance(runner, &pool).await;
    test_non_fs_events(runner, &pool).await;

    Ok(())
}

// ─── Individual test functions ────────────────────────────────────────────

fn test_core_services_active(runner: &mut TestRunner) {
    let name = "core services: sinex-gateway and sinex-ingestd are active";

    let gateway_ok = Command::new("systemctl")
        .args(["is-active", "--quiet", "sinex-gateway"])
        .status()
        .is_ok_and(|s| s.success());

    let ingestd_ok = Command::new("systemctl")
        .args(["is-active", "--quiet", "sinex-ingestd"])
        .status()
        .is_ok_and(|s| s.success());

    if gateway_ok && ingestd_ok {
        runner.pass(name);
    } else {
        let mut failures = Vec::new();
        if !gateway_ok {
            failures.push("sinex-gateway");
        }
        if !ingestd_ok {
            failures.push("sinex-ingestd");
        }
        runner.fail(name, &format!("inactive services: {}", failures.join(", ")));
    }
}

async fn test_event_provenance(runner: &mut TestRunner, pool: &PgPool) {
    let name = "provenance: no events with NULL source_material_id AND NULL source_event_ids";

    let result = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM core.events \
         WHERE source_material_id IS NULL \
           AND source_event_ids IS NULL"
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(0) => runner.pass(name),
        Ok(n) => runner.fail(
            name,
            &format!("{n} event(s) violate XOR provenance (both sides NULL)"),
        ),
        Err(e) => runner.fail(name, &format!("query error: {e}")),
    }
}

async fn test_non_fs_events(runner: &mut TestRunner, pool: &PgPool) {
    let name = "multi-source: non-filesystem event types exist";

    let result = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT event_type) \
         FROM core.events \
         WHERE event_type IS NOT NULL \
           AND event_type NOT LIKE 'file.%'"
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(n) if n > 0 => runner.pass(name),
        Ok(n) => runner.fail(
            name,
            &format!("no non-filesystem event types found (count={n})"),
        ),
        Err(e) => runner.fail(name, &format!("query error: {e}")),
    }
}
