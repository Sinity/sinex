//! Scheduling belongs to the active copy, not to its durable hold.
use crate::{AdmissionContext, Error, Result};
use sinex_model::OperationId;
use sqlx::Row;

// Thirteen ASCII bytes plus a simple UUID: PostgreSQL never truncates this tag.
const PREFIX: &str = "sinex.backup:";
const PATTERN: &str = "^sinex[.]backup:[0-9a-f]{32}$";

pub fn application(hold: OperationId) -> String {
    format!("{PREFIX}{}", hold.0.simple())
}

async fn schedule(owner: &mut AdmissionContext, enabled: bool) -> Result<()> {
    sqlx::query("SELECT alter_job(job_id, scheduled => $1) FROM timescaledb_information.jobs WHERE proc_name='policy_compression' AND hypertable_schema='core'")
        .bind(enabled).execute(&mut **owner).await?;
    Ok(())
}

pub async fn pause(owner: &mut AdmissionContext) -> Result<()> {
    schedule(owner, false).await
}

/// Disabling scheduling does not interrupt a worker that has already started.
/// Wait outside the admission writer so capture continues while it finishes.
pub async fn wait_for_visible_workers(connection: &mut sqlx::PgConnection) -> Result<()> {
    loop {
        let running:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM timescaledb_information.jobs jobs JOIN timescaledb_information.job_stats stats USING(job_id) WHERE jobs.proc_name='policy_compression' AND jobs.hypertable_schema='core' AND stats.job_status='Running')").fetch_one(&mut *connection).await?;
        if !running {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn stop_sessions(owner: &mut AdmissionContext, application: Option<&str>) -> Result<()> {
    loop {
        let sessions = sqlx::query("SELECT pid,backend_start FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND pid<>pg_backend_pid() AND application_name ~ $1 AND ($2::text IS NULL OR application_name=$2)")
            .bind(PATTERN).bind(application).fetch_all(&mut **owner).await?;
        if sessions.is_empty() {
            return Ok(());
        }
        for session in sessions {
            let pid: i32 = session.try_get("pid")?;
            let started: time::OffsetDateTime = session.try_get("backend_start")?;
            let terminated: Option<bool> = sqlx::query_scalar("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND backend_start=$2 AND datname=current_database() AND usename=current_user AND application_name ~ $3 AND ($4::text IS NULL OR application_name=$4)")
                .bind(pid).bind(started).bind(PATTERN).bind(application).fetch_optional(&mut **owner).await?;
            if terminated == Some(false) {
                return Err(Error::Invalid(format!(
                    "could not stop owned backup database session {pid}"
                )));
            }
        }
        // Sending termination is not evidence that its locks have gone.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

pub async fn finish(owner: &mut AdmissionContext, application: Option<&str>) -> Result<()> {
    if let Some(application) = application {
        stop_sessions(owner, Some(application)).await?;
    }
    schedule(owner, true).await
}

/// The typed context owns the exclusive writer claim. No other daemon can
/// legitimately own a copy in this database when startup reaches this point.
pub async fn recover(owner: &mut AdmissionContext) -> Result<()> {
    stop_sessions(owner, None).await?;
    schedule(owner, true).await
}

#[cfg(test)]
mod tests;
