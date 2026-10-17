//! Read-only Atuin acquisition. A poll owns one SQLite read transaction and yields bounded
//! steps to the canonical writer. A restart retries a full poll; it never resumes a lost snapshot.
use crate::atuin_guard as guard;
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
    mut qualification: Option<sinex_store::QualificationBoundary>,
) -> Result<(), String> {
    let interpretation = observation.interpretation;
    let mut reader = Reader::default();
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let observation_id = observation.observation;
        poll(&config, &mut reader, observation, &admission, &stop).await?;
        // Sample both clocks at the scheduling decision. Persist that wall-clock target
        // and wait to this monotonic target, including time spent committing metadata.
        let monotonic = tokio::time::Instant::now();
        let schedule = sinex_model::acquisition::PollSchedule::after(
            OffsetDateTime::now_utc(),
            config.poll_interval_ms,
        )
        .map_err(|e| e.to_string())?;
        let target = monotonic
            .checked_add(std::time::Duration::from_millis(config.poll_interval_ms))
            .ok_or("poll monotonic clock overflow")?;
        admission
            .schedule_source(observation_id, schedule.clone())
            .await
            .map_err(|_| "acquisition schedule could not commit")?;
        tokio::select! {
            _ = stop.changed() => return Ok(()),
            result = sinex_store::QualificationBoundary::collector_scheduled(&mut qualification, serde_json::json!({"observation":observation_id,"schedule":schedule})) => result.map_err(|_| "collector qualification refused")?,
        }
        tokio::select! {
            changed = stop.changed() => { let _ = changed; return Ok(()); }
            _ = tokio::time::sleep_until(target) => {}
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
    let mut metadata = match std::fs::metadata(path) {
        Ok(m) => {
            json!({"device":m.dev(),"inode":m.ino(),"size":m.len(),"mtime_ns":i128::from(m.mtime())*1_000_000_000+i128::from(m.mtime_nsec()),"ctime_ns":i128::from(m.ctime())*1_000_000_000+i128::from(m.ctime_nsec()),"mode":m.mode(),"uid":m.uid(),"gid":m.gid(),"path":path,"resolved_path":std::fs::canonicalize(path).ok(),"host":std::fs::read_to_string("/proc/sys/kernel/hostname").ok().map(|s|s.trim().to_string()),"machine_id":std::fs::read_to_string("/etc/machine-id").ok().map(|s|s.trim().to_string())})
        }
        Err(_) => json!({"path":path,"stat":"unavailable"}),
    };
    metadata["_sinex_acquisition_v1"] = json!({"version":1,"receipts":"capture_insert_v1"});
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

#[derive(Default)]
struct Reader {
    connection: Option<SqliteConnection>,
    epoch: Option<Uuid>,
    full_due: Option<tokio::time::Instant>,
}
impl Reader {
    async fn connected(&mut self, path: &Path) -> Result<(&mut SqliteConnection, Uuid), String> {
        if let Some(connection) = self.connection.as_mut() {
            if guard::verify(connection, path).await.is_err() {
                self.connection.take();
                self.epoch = None;
                self.full_due = None;
            }
        }
        if self.connection.is_none() {
            let options = SqliteConnectOptions::new()
                .filename(path)
                .read_only(true)
                .create_if_missing(false)
                .busy_timeout(std::time::Duration::from_secs(2));
            let mut connection = SqliteConnection::connect_with(&options)
                .await
                .map_err(|_| "Atuin database unavailable (read-only open failed)")?;
            guard::verify(&mut connection, path).await?;
            self.connection = Some(connection);
            self.epoch = Some(Uuid::now_v7());
        }
        Ok((
            self.connection.as_mut().expect("opened reader"),
            self.epoch.expect("opened epoch"),
        ))
    }
}
async fn poll(
    config: &AtuinConfig,
    reader: &mut Reader,
    observation: AtuinStep,
    admission: &Admission,
    stop: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let result = read(config, reader, &observation, admission, stop).await;
    if let Err(reason) = result {
        // A lost/partial snapshot can never resume incremental progress. Its
        // already acknowledged prefix stays retained under its original observation.
        reader.connection.take();
        reader.epoch = None;
        reader.full_due = None;
        admission
            .submit_atuin(AtuinStep {
                rows: vec![],
                complete: false,
                unavailable: Some(reason),
                ..observation
            })
            .await
            .map_err(|_| "source unavailable outcome could not commit")?;
    }
    Ok(())
}
const ROW_COLUMNS: &str = "h.rowid AS _rowid,h.id,h.timestamp,h.duration,h.exit,h.command,h.cwd,h.session,h.hostname,h.deleted_at";
fn decode(row: sqlx::sqlite::SqliteRow) -> Result<Value, String> {
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

    Ok(Value::Object(object))
}
struct Batch<'a> {
    observation: &'a AtuinStep,
    admission: &'a Admission,
    rows: Vec<Value>,
    bytes: usize,
}
impl Batch<'_> {
    async fn push(&mut self, row: sqlx::sqlite::SqliteRow) -> Result<(), String> {
        let value = decode(row)?;
        let size = serde_json::to_vec(&value)
            .map_err(|_| "Atuin canonical row encoding failed")?
            .len()
            .checked_add(256)
            .ok_or("Atuin row size overflow")?;
        if size > MAX_STEP_BYTES {
            return Err("Atuin row exceeds bounded retention budget; poll incomplete".into());
        }
        if self.rows.len() == MAX_STEP_ROWS || self.bytes + size > MAX_STEP_BYTES {
            self.admission
                .submit_atuin(AtuinStep {
                    rows: std::mem::take(&mut self.rows),
                    ..self.observation.clone()
                })
                .await
                .map_err(|_| "Atuin retained admission failed")?;
            self.bytes = 0;
        }
        self.bytes += size;
        self.rows.push(value);
        Ok(())
    }
}
async fn read(
    config: &AtuinConfig,
    reader: &mut Reader,
    observation: &AtuinStep,
    admission: &Admission,
    stop: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    use sinex_model::acquisition::{AtuinCheckpoint, AtuinReadKind, AtuinReadPlan};
    let basis = admission
        .atuin_read_basis(observation.locator.clone())
        .await
        .map_err(|_| "Atuin canonical read basis unavailable")?;
    let monotonic_due = reader.full_due;
    let (conn, epoch) = reader.connected(&config.path).await?;
    let mut tx = conn
        .begin()
        .await
        .map_err(|_| "Atuin consistent read unavailable")?;
    // First read pins one snapshot for coordinates, cookies and every selected value.
    let schema: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| "Atuin schema basis unavailable")?;
    let user: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| "Atuin version basis unavailable")?;
    let full = full_required(
        monotonic_due,
        tokio::time::Instant::now(),
        basis
            .checkpoint
            .as_ref()
            .is_some_and(|c| c.reusable(epoch, schema, user, OffsetDateTime::now_utc())),
    );
    let ceiling: Option<i64> = sqlx::query_scalar("SELECT max(rowid) FROM history")
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| "Atuin history coordinate unavailable")?;
    let mut batch = Batch {
        observation,
        admission,
        rows: Vec::new(),
        bytes: 0,
    };
    let old_through = basis
        .checkpoint
        .as_ref()
        .map(|c| c.through_rowid)
        .unwrap_or(i64::MIN);
    let mut through = if full { None } else { Some(old_through) };
    loop {
        if *stop.borrow() {
            return Err(
                "Atuin poll interrupted by daemon shutdown; committed prefix retained".into(),
            );
        }
        // Separate first/full and incremental SQL keeps the rowid range indexed.
        let row = if let Some(through) = through {
            sqlx::query(&format!("SELECT {ROW_COLUMNS} FROM history h WHERE h.rowid>? AND h.rowid<=? ORDER BY h.rowid LIMIT 1"))
                .bind(through).bind(ceiling).fetch_optional(&mut *tx).await
        } else {
            sqlx::query(&format!("SELECT {ROW_COLUMNS} FROM history h ORDER BY h.rowid LIMIT 1"))
                .fetch_optional(&mut *tx).await
        }.map_err(|_| "Atuin history schema/read unavailable")?;
        let Some(row) = row else {
            break;
        };
        through = Some(
            row.try_get("_rowid")
                .map_err(|_| "Atuin row address unavailable")?,
        );
        batch.push(row).await?;
    }
    if !full {
        let mut after = None;
        loop {
            if *stop.borrow() {
                return Err("Atuin unfinished read interrupted; committed prefix retained".into());
            }
            let keys = admission
                .atuin_unfinished_page(basis.input, basis.basis, after)
                .await
                .map_err(|_| "Atuin prior unfinished eligibility unavailable")?;
            if keys.is_empty() {
                break;
            }
            for (event, key) in keys {
                after = Some(event);
                let row = sqlx::query(&format!("SELECT {ROW_COLUMNS} FROM history h WHERE h.id=? AND h.rowid<=? ORDER BY h.rowid LIMIT 1"))
                    .bind(key).bind(old_through).fetch_optional(&mut *tx).await.map_err(|_| "Atuin unfinished value unavailable")?;
                if let Some(row) = row {
                    batch.push(row).await?;
                }
                // Missing prior keys do not establish removal; no census is admitted.
            }
        }
    }
    guard::verify(&mut tx, &config.path).await?;
    tx.commit()
        .await
        .map_err(|_| "Atuin source read finalization failed")?;
    let decision = tokio::time::Instant::now();
    let now = OffsetDateTime::now_utc();
    let next_full = if full {
        Some(
            decision
                .checked_add(std::time::Duration::from_millis(config.full_interval_ms))
                .ok_or("Atuin monotonic full deadline overflow")?,
        )
    } else {
        monotonic_due
    };
    let mut checkpoint = if full {
        let delta = time::Duration::milliseconds(
            i64::try_from(config.full_interval_ms).map_err(|_| "Atuin full interval overflow")?,
        );
        let due = now
            .checked_add(delta)
            .ok_or("Atuin full due clock overflow")?;
        AtuinCheckpoint {
            version: 1,
            epoch,
            schema_version: schema,
            user_version: user,
            through_rowid: ceiling.unwrap_or(i64::MIN),
            full_completed_at: now
                .format(&Rfc3339)
                .map_err(|_| "Atuin full clock encoding")?,
            full_due_at: due
                .format(&Rfc3339)
                .map_err(|_| "Atuin full due encoding")?,
            needs_full: false,
        }
    } else {
        basis.checkpoint.expect("reusable checkpoint")
    };
    checkpoint.through_rowid = ceiling.map_or(checkpoint.through_rowid, |v| {
        checkpoint.through_rowid.max(v)
    });
    let plan = AtuinReadPlan {
        version: 1,
        kind: if full {
            AtuinReadKind::FullValueComparison
        } else {
            AtuinReadKind::NewCoordinatesAndPriorUnfinished
        },
        checkpoint,
    };
    let mut final_step = AtuinStep {
        rows: batch.rows,
        complete: true,
        ..observation.clone()
    };
    final_step.metadata["_sinex_read_plan_v1"] =
        serde_json::to_value(plan).map_err(|_| "Atuin read plan encoding")?;
    // Checkpoint and successful completion share the final admission transaction.
    admission
        .submit_atuin(final_step)
        .await
        .map_err(|_| "Atuin retained admission/finalization failed")?;
    reader.full_due = next_full;
    Ok(())
}

// The persisted wall target describes this decision; the live epoch also has a
// monotonic deadline so realtime rollback cannot postpone a full indefinitely.
fn full_required(
    deadline: Option<tokio::time::Instant>,
    now: tokio::time::Instant,
    basis_reusable: bool,
) -> bool {
    !basis_reusable || deadline.is_none_or(|due| now >= due)
}
#[cfg(test)]
#[path = "atuin/read_plan_tests.rs"]
mod tests;
