use std::process::Command;
use std::time::{Duration, Instant};

use color_eyre::eyre::{Context, Result};
use sqlx::PgPool;

use crate::runner::{TestOutcome, TestRunner};

pub const WATCHED_DIR: &str = "/var/lib/sinex/watched";
pub const SINEXD_SERVICE: &str = "sinexd";

pub async fn event_count(pool: &PgPool) -> Result<i64> {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(pool)
        .await
        .context("failed to count core.events rows")
}

pub async fn observed_event_count(
    runner: &mut TestRunner,
    name: &str,
    pool: &PgPool,
) -> Option<i64> {
    match event_count(pool).await {
        Ok(count) => Some(count),
        Err(error) => {
            runner.record(
                name,
                TestOutcome::EvidenceMissing,
                &format!("event count query failed: {error:#}"),
            );
            None
        }
    }
}

pub fn write_watched_files(prefix: &str, count: u32, body: &str) -> Result<()> {
    std::fs::create_dir_all(WATCHED_DIR)
        .with_context(|| format!("failed to create watched directory {WATCHED_DIR}"))?;
    for i in 0..count {
        std::fs::write(
            format!("{WATCHED_DIR}/{prefix}-{i}.txt"),
            format!("{body} {i}"),
        )
        .with_context(|| format!("failed to write watched file {prefix}-{i}.txt"))?;
    }
    Ok(())
}

pub fn report_watched_files_written(
    runner: &mut TestRunner,
    name: &str,
    prefix: &str,
    count: u32,
    body: &str,
) -> bool {
    match write_watched_files(prefix, count, body) {
        Ok(()) => true,
        Err(error) => {
            runner.record(
                name,
                TestOutcome::EvidenceMissing,
                &format!("watched-file fixture write failed: {error:#}"),
            );
            false
        }
    }
}

pub fn command_status(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .status()
        .is_ok_and(|status| status.success())
}

pub fn service_is_active(service: &str) -> bool {
    command_status("systemctl", &["is-active", "--quiet", service])
}

pub async fn wait_for_service_active(
    service: &str,
    deadline_after: Duration,
    poll_every: Duration,
) -> bool {
    let deadline = Instant::now() + deadline_after;
    loop {
        tokio::time::sleep(poll_every).await;
        if service_is_active(service) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
    }
}

pub async fn wait_for_event_count_increase(
    pool: &PgPool,
    before: i64,
    deadline_after: Duration,
    poll_every: Duration,
) -> Result<Option<i64>> {
    let deadline = Instant::now() + deadline_after;
    loop {
        tokio::time::sleep(poll_every).await;
        let after = event_count(pool).await?;
        if after > before {
            return Ok(Some(after));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
    }
}

pub fn report_service_active(runner: &mut TestRunner, name: &str, inactive_reason: &str) -> bool {
    if service_is_active(SINEXD_SERVICE) {
        runner.pass(name);
        true
    } else {
        runner.fail(name, inactive_reason);
        false
    }
}

pub async fn report_event_count_increase<F>(
    runner: &mut TestRunner,
    name: &str,
    pool: &PgPool,
    before: i64,
    deadline_after: Duration,
    poll_every: Duration,
    failure_reason: F,
) -> Option<i64>
where
    F: FnOnce(i64) -> String,
{
    let after = wait_for_event_count_increase(pool, before, deadline_after, poll_every).await;
    match after {
        Ok(Some(count)) => {
            runner.pass(name);
            Some(count)
        }
        Ok(None) => {
            runner.fail(name, &failure_reason(before));
            None
        }
        Err(error) => {
            runner.record(
                name,
                TestOutcome::EvidenceMissing,
                &format!("event count query failed while waiting for new events: {error:#}"),
            );
            None
        }
    }
}
