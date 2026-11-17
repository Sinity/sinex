use super::*;
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};

async fn scheduled(owner: &mut AdmissionContext) -> Vec<bool> {
    let jobs = sqlx::query_scalar("SELECT scheduled FROM timescaledb_information.jobs WHERE proc_name='policy_compression' AND hypertable_schema='core'")
        .fetch_all(&mut **owner).await.unwrap();
    assert!(!jobs.is_empty(), "the real compression policy must exist");
    jobs
}

#[devkit::test]
async fn startup_stops_only_owned_sessions_and_invalidates_late_snapshot_import(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let mut owner = AdmissionContext::connect(&url).await.unwrap();
    let id = OperationId::new();
    let tag = application(id);
    let options: PgConnectOptions = url.parse().unwrap();
    let mut unrelated =
        PgConnection::connect_with(&options.clone().application_name("sinex.backup:unrelated"))
            .await
            .unwrap();
    let unrelated_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut unrelated)
        .await
        .unwrap();
    let mut misleading =
        PgConnection::connect_with(&options.clone().application_name(&format!("{tag}extra")))
            .await
            .unwrap();
    let mut other_database =
        PgConnection::connect_with(&options.clone().database("postgres").application_name(&tag))
            .await
            .unwrap();
    let role = format!("synthetic_backup_{}", id.0.simple());
    sqlx::query(&format!("CREATE ROLE {role} LOGIN"))
        .execute(&mut *owner)
        .await
        .unwrap();
    let mut other_user =
        PgConnection::connect_with(&options.clone().username(&role).application_name(&tag))
            .await
            .unwrap();
    let (hold, _) = crate::backup::hold(&mut owner, id, "synthetic")
        .await
        .unwrap();
    let exporter = crate::backup::Snapshot::export(
        PgConnection::connect_with(&options.clone().application_name(&tag))
            .await
            .unwrap(),
        hold.clone(),
        url.clone(),
    )
    .await
    .unwrap();
    let imported = crate::backup::Snapshot::import(
        PgConnection::connect_with(&options.clone().application_name(&tag))
            .await
            .unwrap(),
        exporter.exported.clone(),
        hold.clone(),
        url.clone(),
    )
    .await
    .unwrap();
    pause(&mut owner).await.unwrap();
    assert!(scheduled(&mut owner).await.iter().all(|enabled| !enabled));
    recover(&mut owner).await.unwrap();
    assert!(scheduled(&mut owner).await.iter().all(|enabled| *enabled));
    let left:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND application_name=$1").bind(&tag).fetch_one(&mut *owner).await.unwrap();
    assert_eq!(left, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
            .fetch_one(&mut unrelated)
            .await
            .unwrap(),
        unrelated_pid
    );
    sqlx::query("SELECT 1")
        .execute(&mut other_database)
        .await
        .unwrap();
    sqlx::query("SELECT 1")
        .execute(&mut misleading)
        .await
        .unwrap();
    assert!(
        crate::backup::Snapshot::import(
            PgConnection::connect(&url).await.unwrap(),
            exporter.exported.clone(),
            hold,
            url.clone()
        )
        .await
        .is_err()
    );
    // pg_dump imports before taking relation locks, including when a late table
    // is inaccessible. A dead exporter cannot admit a new orphan dump.
    let dump = std::process::Command::new("pg_dump")
        .arg("--schema-only")
        .arg(format!("--snapshot={}", exporter.exported))
        .arg(&url)
        .output()
        .unwrap();
    assert!(!dump.status.success());
    assert!(
        String::from_utf8_lossy(&dump.stderr)
            .contains(&format!("SET TRANSACTION SNAPSHOT '{}'", exporter.exported)),
        "late pg_dump refused: {}",
        String::from_utf8_lossy(&dump.stderr)
    );
    sqlx::query("SELECT 1")
        .execute(&mut other_user)
        .await
        .unwrap();
    other_user.close().await.unwrap();
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&mut *owner)
        .await
        .unwrap();
    drop((exporter, imported));
    owner.close().await.unwrap();
}

#[devkit::test]
async fn copy_waits_for_an_already_running_policy_before_exporting(db: devkit::pg::TestDatabase) {
    use futures::FutureExt;
    let url = db.url(None);
    let mut owner = AdmissionContext::connect(&url).await.unwrap();
    for statement in [
        "CREATE TABLE core.synthetic_backup_policy(time timestamptz NOT NULL,v integer)",
        "SELECT create_hypertable('core.synthetic_backup_policy','time')",
        "ALTER TABLE core.synthetic_backup_policy SET(timescaledb.compress,timescaledb.compress_segmentby='v')",
        "INSERT INTO core.synthetic_backup_policy VALUES(now()-interval '14 days',1)",
    ] {
        sqlx::query(statement).execute(&mut *owner).await.unwrap();
    }
    let job: i32 = sqlx::query_scalar(
        "SELECT add_compression_policy('core.synthetic_backup_policy',interval '7 days')",
    )
    .fetch_one(&mut *owner)
    .await
    .unwrap();
    let chunk: String =
        sqlx::query_scalar("SELECT show_chunks('core.synthetic_backup_policy')::text LIMIT 1")
            .fetch_one(&mut *owner)
            .await
            .unwrap();
    let mut blocker = PgConnection::connect(&url).await.unwrap();
    sqlx::query("BEGIN").execute(&mut blocker).await.unwrap();
    sqlx::query(&format!("LOCK TABLE {chunk} IN ACCESS SHARE MODE"))
        .execute(&mut blocker)
        .await
        .unwrap();
    let worker_name: String = sqlx::query_scalar(
        "SELECT application_name::text FROM timescaledb_information.jobs WHERE job_id=$1",
    )
    .bind(job)
    .fetch_one(&mut *owner)
    .await
    .unwrap();
    // psql retains the policy's warning detail when Timescale wraps a chunk
    // failure in its aggregate error.
    let run = tokio::process::Command::new("psql")
        .arg("--no-psqlrc")
        .arg("--set=ON_ERROR_STOP=1")
        .arg("--command")
        .arg(format!("CALL run_job({job})"))
        .arg(&url)
        .env("PGAPPNAME", &worker_name)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name=$1 AND wait_event_type='Lock')").bind(&worker_name).fetch_one(&mut *owner).await.unwrap();
            if blocked {break;}
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let running: bool = sqlx::query_scalar(
        "SELECT job_status='Running' FROM timescaledb_information.job_stats WHERE job_id=$1",
    )
    .bind(job)
    .fetch_one(&mut *owner)
    .await
    .unwrap();
    assert!(
        running,
        "the real running job must be visible to the production wait"
    );
    pause(&mut owner).await.unwrap();
    let mut reader = PgConnection::connect(&url).await.unwrap();
    let idle = wait_for_visible_workers(&mut reader);
    tokio::pin!(idle);
    assert!(idle.as_mut().now_or_never().is_none());
    // The separate wait does not retain the admission writer connection.
    sqlx::query("SELECT 1").execute(&mut *owner).await.unwrap();
    sqlx::query("ROLLBACK").execute(&mut blocker).await.unwrap();
    let run = run.wait_with_output().await.unwrap();
    assert!(
        run.status.success(),
        "policy execution failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), idle)
        .await
        .unwrap()
        .unwrap();
    finish(&mut owner, None).await.unwrap();
    owner.close().await.unwrap();
}
