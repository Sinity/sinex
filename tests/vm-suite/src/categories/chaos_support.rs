use std::process::Command;
use std::time::{Duration, Instant};

use sqlx::PgPool;

pub const WATCHED_DIR: &str = "/var/lib/sinex/watched";
pub const SINEXD_SERVICE: &str = "sinexd";

pub async fn event_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(pool)
        .await
        .ok()
        .unwrap_or(0)
}

pub fn write_watched_files(prefix: &str, count: u32, body: &str) {
    let _ = std::fs::create_dir_all(WATCHED_DIR);
    for i in 0..count {
        let _ = std::fs::write(
            format!("{WATCHED_DIR}/{prefix}-{i}.txt"),
            format!("{body} {i}"),
        );
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
) -> Option<i64> {
    let deadline = Instant::now() + deadline_after;
    loop {
        tokio::time::sleep(poll_every).await;
        let after = event_count(pool).await;
        if after > before {
            return Some(after);
        }
        if Instant::now() >= deadline {
            return None;
        }
    }
}
