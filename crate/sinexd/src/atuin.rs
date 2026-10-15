//! Read-only Atuin acquisition. A poll owns one SQLite read transaction and yields bounded
//! steps to the canonical writer. A restart retries a full poll; it never resumes a lost snapshot.
use crate::config::AtuinConfig;
use serde_json::{Value, json};
use sinex_store::{Admission, AtuinStep, MAX_STEP_BYTES, MAX_STEP_ROWS, PrivacyRule};
use sqlx::{
    Connection, Row, TypeInfo, ValueRef,
    sqlite::{SqliteConnectOptions, SqliteConnection},
};
use std::{os::unix::fs::MetadataExt, path::Path};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub async fn run(
    config: AtuinConfig,
    rules: Vec<PrivacyRule>,
    admission: Admission,
    mut stop: tokio::sync::watch::Receiver<bool>,
    mut observation: AtuinStep,
) -> Result<(), String> {
    let interpretation = observation.interpretation;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        poll(&config.path, observation, &admission, &stop).await?;
        tokio::select! {
            changed = stop.changed() => { let _ = changed; return Ok(()); }
            _ = tokio::time::sleep(std::time::Duration::from_millis(config.poll_interval_ms)) => {}
        }
        observation = begin_poll(&config.path, &rules, &admission, interpretation).await?;
    }
}

/// Commit the configured input and its reading observation before SQLite I/O.
/// Startup awaits only this writer admission, never the provider read.
pub async fn begin_poll(
    path: &Path,
    rules: &[PrivacyRule],
    admission: &Admission,
    interpretation: sinex_store::AtuinInterpretation,
) -> Result<AtuinStep, String> {
    let observation = Uuid::now_v7();
    let observed_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|_| "cannot format observation clock")?;
    let locator = path.to_string_lossy().into_owned();
    let metadata = match std::fs::metadata(path) {
        Ok(m) => {
            json!({"device":m.dev(),"inode":m.ino(),"size":m.len(),"mtime_ns":i128::from(m.mtime())*1_000_000_000+i128::from(m.mtime_nsec()),"ctime_ns":i128::from(m.ctime())*1_000_000_000+i128::from(m.ctime_nsec()),"mode":m.mode(),"uid":m.uid(),"gid":m.gid(),"path":path,"resolved_path":std::fs::canonicalize(path).ok(),"host":std::fs::read_to_string("/proc/sys/kernel/hostname").ok().map(|s|s.trim().to_string()),"machine_id":std::fs::read_to_string("/etc/machine-id").ok().map(|s|s.trim().to_string())})
        }
        Err(_) => json!({"path":path,"stat":"unavailable"}),
    };
    let step = AtuinStep {
        interpretation,
        observation,
        observed_at,
        locator,
        metadata,
        rows: vec![],
        complete: false,
        unavailable: None,
        privacy_rules: rules.to_vec(),
    };
    admission
        .submit_atuin(step.clone())
        .await
        .map_err(|_| "Atuin reading observation could not commit")?;
    Ok(step)
}

async fn poll(
    path: &Path,
    observation: AtuinStep,
    admission: &Admission,
    stop: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let make_step = |rows, complete, unavailable| AtuinStep {
        rows,
        complete,
        unavailable,
        ..observation.clone()
    };
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .create_if_missing(false)
        .busy_timeout(std::time::Duration::from_secs(2));
    let result = read(options, &make_step, admission, stop).await;
    if let Err(reason) = result {
        admission
            .submit_atuin(make_step(vec![], false, Some(reason)))
            .await
            .map_err(|_| "source unavailable outcome could not commit")?;
    }
    Ok(())
}
async fn read(
    options: SqliteConnectOptions,
    make: &impl Fn(Vec<Value>, bool, Option<String>) -> AtuinStep,
    admission: &Admission,
    stop: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .map_err(|_| "Atuin database unavailable (read-only open failed)")?;
    let mut tx = conn
        .begin()
        .await
        .map_err(|_| "Atuin consistent read unavailable")?;
    let mut through = i64::MIN;
    let mut batch = Vec::new();
    let mut byte_count = 0usize;
    loop {
        if *stop.borrow() {
            return Err(
                "Atuin poll interrupted by daemon shutdown; committed prefix retained".into(),
            );
        }
        let row = sqlx::query("SELECT h.rowid AS _rowid,h.id,h.timestamp,h.duration,h.exit,h.command,h.cwd,h.session,h.hostname,h.deleted_at FROM history h WHERE h.rowid>? ORDER BY h.rowid LIMIT 1")
            .bind(through).fetch_optional(&mut *tx).await.map_err(|_| "Atuin history schema/read unavailable")?;
        let Some(row) = row else {
            break;
        };
        through = row
            .try_get("_rowid")
            .map_err(|_| "Atuin row address unavailable")?;
        let mut object = serde_json::Map::new();
        for name in [
            "_rowid",
            "id",
            "timestamp",
            "duration",
            "exit",
            "command",
            "cwd",
            "session",
            "hostname",
            "deleted_at",
        ] {
            let raw = row
                .try_get_raw(name)
                .map_err(|_| "Atuin field unavailable")?;
            let value = if raw.is_null() {
                Value::Null
            } else {
                match raw.type_info().name() {
                    "INTEGER" => json!(
                        row.try_get::<i64, _>(name)
                            .map_err(|_| "Atuin integer decoding failed")?
                    ),
                    "REAL" => json!(
                        row.try_get::<f64, _>(name)
                            .map_err(|_| "Atuin real decoding failed")?
                    ),
                    "BLOB" => {
                        json!({"sqlite_blob":row.try_get::<Vec<u8>,_>(name).map_err(|_| "Atuin blob decoding failed")?})
                    }
                    _ => json!(
                        row.try_get::<String, _>(name)
                            .map_err(|_| "Atuin text decoding failed")?
                    ),
                }
            };
            object.insert(name.to_string(), value);
        }
        let value = Value::Object(object);
        let size = serde_json::to_vec(&value)
            .map_err(|_| "Atuin canonical row encoding failed")?
            .len()
            + 256;
        if size > MAX_STEP_BYTES {
            return Err("Atuin row exceeds bounded retention budget; poll incomplete".into());
        }
        if batch.len() == MAX_STEP_ROWS || byte_count + size > MAX_STEP_BYTES {
            admission
                .submit_atuin(make(std::mem::take(&mut batch), false, None))
                .await
                .map_err(|_| "Atuin retained admission failed")?;
            byte_count = 0;
        }
        byte_count += size;
        batch.push(value);
    }
    // Finish only after the source snapshot has actually reached EOF. Zero rows is a
    // completed observation; read/open failures above never become completed zero.
    tx.commit()
        .await
        .map_err(|_| "Atuin source read finalization failed")?;
    admission
        .submit_atuin(make(batch, true, None))
        .await
        .map_err(|_| "Atuin retained admission/finalization failed")?;
    Ok(())
}
