//! Scheduling belongs to the active copy, not to its durable hold.
use crate::{AdmissionContext, Error, Result};
use sinex_model::{DbCheck, OperationId, lifecycle::OperationResult};
use sqlx::{Connection, Row};

// Thirteen ASCII bytes plus a simple UUID: PostgreSQL never truncates this tag.
const PREFIX: &str = "sinex.backup:";
const PATTERN: &str = "^sinex[.]backup:[0-9a-f]{32}$";

pub fn application(hold: OperationId) -> String {
    format!("{PREFIX}{}", hold.0.simple())
}

/// The operation remembers only jobs this copy disabled. An existing deliberate
/// pause is not ours to undo, including when the daemon reopens the database.
pub async fn pause(
    owner: &mut AdmissionContext,
    operation: OperationId,
    actor: &str,
) -> Result<()> {
    let mut tx = owner.begin().await?;
    sqlx::query("INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'backup_hold',$2,'{\"scope\":\"core\"}') ON CONFLICT(id) DO NOTHING")
        .bind(operation.0).bind(actor).execute(&mut *tx).await?;
    let existing =
        sqlx::query("SELECT kind,actor,progress FROM core.operations WHERE id=$1 FOR UPDATE")
            .bind(operation.0)
            .fetch_one(&mut *tx)
            .await?;
    if existing.try_get::<String, _>("kind")? != "backup_hold"
        || existing.try_get::<String, _>("actor")? != actor
    {
        return Err(Error::Invalid(
            "backup operation belongs to a different operation or actor".into(),
        ));
    }
    let mut progress: serde_json::Value = existing.try_get("progress")?;
    if !progress.is_object() {
        return Err(Error::Invalid("backup progress is not an object".into()));
    }
    let mut jobs: Vec<i32> = progress
        .get("compression_jobs")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| Error::Invalid(format!("backup compression jobs: {error}")))?
        .unwrap_or_default();
    let enabled: Vec<i32> = sqlx::query_scalar("SELECT job_id FROM timescaledb_information.jobs WHERE proc_name='policy_compression' AND hypertable_schema='core' AND scheduled")
        .fetch_all(&mut *tx).await?;
    jobs.extend(enabled);
    jobs.sort_unstable();
    jobs.dedup();
    progress["compression_jobs"] = serde_json::json!(&jobs);
    sqlx::query("UPDATE core.operations SET progress=$2 WHERE id=$1")
        .bind(operation.0)
        .bind(progress)
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT alter_job(job_id, scheduled => false) FROM timescaledb_information.jobs WHERE job_id=ANY($1) AND proc_name='policy_compression' AND hypertable_schema='core' AND scheduled")
        .bind(&jobs).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
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

async fn restore(owner: &mut AdmissionContext, operation: Option<OperationId>) -> Result<()> {
    let mut tx = owner.begin().await?;
    let copies = sqlx::query("SELECT id,progress->'compression_jobs' AS jobs FROM core.operations WHERE kind='backup_hold' AND progress ? 'compression_jobs' AND ($1::uuid IS NULL OR id=$1) FOR UPDATE")
        .bind(operation.map(|id| id.0)).fetch_all(&mut *tx).await?;
    for copy in copies {
        let id: uuid::Uuid = copy.try_get("id")?;
        let jobs: Vec<i32> = serde_json::from_value(copy.try_get("jobs")?)
            .map_err(|error| Error::Invalid(format!("backup compression jobs: {error}")))?;
        sqlx::query("SELECT alter_job(job_id, scheduled => true) FROM timescaledb_information.jobs WHERE job_id=ANY($1) AND proc_name='policy_compression' AND hypertable_schema='core'")
            .bind(&jobs).execute(&mut *tx).await?;
        sqlx::query("UPDATE core.operations SET progress=progress-'compression_jobs',result=coalesce(result,$2) WHERE id=$1")
            .bind(id).bind(OperationResult::StoppedEarly.as_db()).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn finish(owner: &mut AdmissionContext, operation: OperationId) -> Result<()> {
    stop_sessions(owner, Some(&application(operation))).await?;
    restore(owner, Some(operation)).await
}

/// The typed context owns the exclusive writer claim. No other daemon can
/// legitimately own a copy in this database when startup reaches this point.
pub async fn recover(owner: &mut AdmissionContext) -> Result<()> {
    stop_sessions(owner, None).await?;
    restore(owner, None).await
}

#[cfg(test)]
mod tests;
