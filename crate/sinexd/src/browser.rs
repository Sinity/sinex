//! Configured Chromium History reading. Browser evidence never enters shell derivation.
use crate::{config::ChromiumConfig, sqlite_custody::Custodian};
use serde_json::{Value, json};
use sinex_model::browser::{Checkpoint, ReadBasis};
use sinex_store::{
    Admission, BrowserBegin, BrowserStep, MAX_STEP_BYTES, MAX_STEP_ROWS, PrivacyRule,
};
use sqlx::{
    Column, Connection, Row, TypeInfo, ValueRef,
    sqlite::{SqliteConnectOptions, SqliteConnection, SqliteRow},
};
use std::path::{Path, PathBuf};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub struct Attempt {
    observation: Uuid,
    basis: ReadBasis,
}
pub async fn begin(
    config: &ChromiumConfig,
    rules: &[PrivacyRule],
    admission: &Admission,
) -> Result<Attempt, String> {
    let observation = Uuid::now_v7();
    let observed_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|_| "browser observation clock encoding")?;
    let basis = admission
        .browser_begin(BrowserBegin {
            profile: config.profile.clone(),
            generation: config.database_generation,
            path: config.path.to_string_lossy().into_owned(),
            observation,
            observed_at,
            privacy_rules: rules.to_vec(),
        })
        .await
        .map_err(|_| "browser reading registration unavailable")?;
    Ok(Attempt { observation, basis })
}
pub async fn run(
    config: ChromiumConfig,
    root: PathBuf,
    rules: Vec<PrivacyRule>,
    admission: Admission,
    mut stop: tokio::sync::watch::Receiver<bool>,
    mut attempt: Attempt,
    mut qualification: Option<sinex_store::QualificationBoundary>,
) -> Result<(), String> {
    let mut custodian: Option<Custodian> = None;
    let mut full_due = None;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let observation = attempt.observation;
        let result = poll(
            &config,
            &root,
            &mut custodian,
            &mut full_due,
            &attempt,
            &admission,
            &stop,
            &mut qualification,
        )
        .await;
        if let Err(reason) = result {
            let fence = reason.contains("reconciliation required");
            custodian = None;
            full_due = None;
            admission.browser_step(BrowserStep{input:attempt.basis.input.ok_or("browser input absent")?,observation,rows:vec![],complete:false,unavailable:Some(reason),metadata:json!({"coverage":"unknown; previous retained values remain available"}),checkpoint:None,fence}).await.map_err(|_|"browser unavailable outcome could not commit")?;
        }
        let decision = tokio::time::Instant::now();
        let schedule = sinex_model::acquisition::PollSchedule::after(
            OffsetDateTime::now_utc(),
            config.poll_interval_ms,
        )
        .map_err(|_| "browser next poll clock")?;
        let target = decision
            .checked_add(std::time::Duration::from_millis(config.poll_interval_ms))
            .ok_or("browser next poll deadline overflow")?;
        admission
            .schedule_source(observation, schedule)
            .await
            .map_err(|_| "browser schedule could not commit")?;
        tokio::select! {_ = stop.changed()=>return Ok(()),_ = tokio::time::sleep_until(target)=>{}}
        attempt = begin(&config, &rules, &admission).await?;
    }
}
fn decode(row: SqliteRow) -> Result<Value, String> {
    let mut value = serde_json::Map::new();
    for c in row.columns() {
        let name = c.name();
        let raw = row
            .try_get_raw(name)
            .map_err(|_| "browser column unavailable")?;
        let field = if raw.is_null() {
            Value::Null
        } else {
            match raw.type_info().name() {
                "INTEGER" => json!(
                    row.try_get::<i64, _>(name)
                        .map_err(|_| "browser integer decoding")?
                ),
                "REAL" => json!(
                    row.try_get::<f64, _>(name)
                        .map_err(|_| "browser real decoding")?
                ),
                "BLOB" => {
                    json!({"sqlite_blob":row.try_get::<Vec<u8>,_>(name).map_err(|_|"browser blob decoding")?})
                }
                _ => json!(
                    row.try_get::<String, _>(name)
                        .map_err(|_| "browser text decoding")?
                ),
            }
        };
        value.insert(name.to_owned(), field);
    }
    Ok(Value::Object(value))
}
async fn visit(connection: &mut SqliteConnection, id: i64) -> Result<Value, String> {
    let visit = decode(
        sqlx::query("SELECT * FROM visits WHERE id=?")
            .bind(id)
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| "Chromium visit read unavailable")?,
    )?;
    let url = sqlx::query("SELECT * FROM urls WHERE id=?")
        .bind(visit["url"].as_i64())
        .fetch_optional(&mut *connection)
        .await
        .map_err(|_| "Chromium URL read unavailable")?
        .map(decode)
        .transpose()?
        .unwrap_or(Value::Null);
    let referrer: Option<String> =
        sqlx::query_scalar("SELECT u.url FROM visits v JOIN urls u ON u.id=v.url WHERE v.id=?")
            .bind(visit["from_visit"].as_i64())
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| "Chromium from-visit read unavailable")?;
    Ok(json!({"table":"visits","coordinate":id,"visit":visit,"url":url,"referrer_url":referrer}))
}
async fn download(connection: &mut SqliteConnection, id: i64) -> Result<Value, String> {
    let download = decode(
        sqlx::query("SELECT * FROM downloads WHERE id=?")
            .bind(id)
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| "Chromium download read unavailable")?,
    )?;
    let mut chain = Vec::new();
    let mut after = None;
    let mut size = serde_json::to_vec(&download)
        .map_err(|_| "download encoding")?
        .len();
    loop {
        let row=if let Some(after)=after{sqlx::query("SELECT * FROM downloads_url_chains WHERE id=? AND chain_index>? ORDER BY chain_index LIMIT 1").bind(id).bind(after).fetch_optional(&mut *connection).await}else{sqlx::query("SELECT * FROM downloads_url_chains WHERE id=? ORDER BY chain_index LIMIT 1").bind(id).fetch_optional(&mut *connection).await}.map_err(|_|"Chromium URL chain read unavailable")?;
        let Some(row) = row else {
            break;
        };
        after = Some(
            row.try_get::<i64, _>("chain_index")
                .map_err(|_| "download URL-chain key unavailable")?,
        );
        let row = decode(row)?;
        size = size
            .checked_add(
                serde_json::to_vec(&row)
                    .map_err(|_| "URL-chain encoding")?
                    .len(),
            )
            .ok_or("URL-chain size overflow")?;
        if size > MAX_STEP_BYTES - 1024 {
            return Err(
                "Chromium download record exceeds retention budget; no chain truncation admitted"
                    .into(),
            );
        }
        chain.push(row);
    }
    Ok(json!({"table":"downloads","coordinate":id,"download":download,"url_chain":chain}))
}
struct Batch<'a> {
    input: Uuid,
    observation: Uuid,
    admission: &'a Admission,
    metadata: Value,
    rows: Vec<Value>,
    bytes: usize,
    selected: u64,
    canonical_bytes: u64,
}
impl Batch<'_> {
    async fn flush(&mut self) -> Result<(), String> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.admission
            .browser_step(BrowserStep {
                input: self.input,
                observation: self.observation,
                rows: std::mem::take(&mut self.rows),
                complete: false,
                unavailable: None,
                metadata: self.metadata.clone(),
                checkpoint: None,
                fence: false,
            })
            .await
            .map_err(|_| "browser retained admission unavailable")?;
        self.bytes = 0;
        Ok(())
    }
    async fn push(&mut self, row: Value) -> Result<(), String> {
        let size = serde_json::to_vec(&row)
            .map_err(|_| "browser canonical encoding")?
            .len()
            .checked_add(512)
            .ok_or("browser row size overflow")?;
        if size > MAX_STEP_BYTES {
            return Err("Chromium row exceeds retention budget; poll incomplete".into());
        }
        if self.rows.len() == MAX_STEP_ROWS || self.bytes + size > MAX_STEP_BYTES {
            self.flush().await?;
        }
        self.bytes += size;
        self.selected += 1;
        self.canonical_bytes += size as u64;
        self.rows.push(row);
        Ok(())
    }
}
fn clock(text: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(text, &Rfc3339).map_err(|_| "browser retained full clock invalid".into())
}
fn due(
    previous: Option<&Checkpoint>,
    epoch: Uuid,
    schema: i64,
    deadline: Option<tokio::time::Instant>,
) -> bool {
    previous.is_none_or(|c| {
        c.version != 1
            || c.epoch != epoch
            || c.schema_version != schema
            || c.needs_full
            || clock(&c.full_due_at).is_err()
            || clock(&c.full_due_at).is_ok_and(|t| OffsetDateTime::now_utc() >= t)
    }) || deadline.is_none_or(|t| tokio::time::Instant::now() >= t)
}
async fn poll(
    config: &ChromiumConfig,
    root: &Path,
    custodian: &mut Option<Custodian>,
    full_due: &mut Option<tokio::time::Instant>,
    attempt: &Attempt,
    admission: &Admission,
    stop: &tokio::sync::watch::Receiver<bool>,
    qualification: &mut Option<sinex_store::QualificationBoundary>,
) -> Result<(), String> {
    if attempt.basis.reconciliation_required {
        return Err(
            "configured browser generation is fenced; explicit reconciliation required".into(),
        );
    }
    if custodian.is_none() {
        *custodian = Some(
            Custodian::new(
                &config.path,
                root.join(config.database_generation.to_string())
                    .join(blake3::hash(config.profile.as_bytes()).to_hex().as_str()),
            )
            .await?,
        );
    }
    let owner = custodian.as_mut().expect("opened custodian");
    owner.verify()?;
    let previous = attempt.basis.checkpoint.as_ref();
    if previous
        .is_some_and(|p| p.generation != config.database_generation || p.identity != owner.identity)
    {
        return Err(
            "History identity differs from declared generation; explicit reconciliation required"
                .into(),
        );
    }
    let identity = owner.identity.clone();
    let epoch = owner.epoch;
    let snapshot_path = owner.next_path("snapshot");
    let _snapshot_cleanup = SnapshotScratch(snapshot_path.clone());
    let mut owned = None;
    let custody = if owner.direct.is_none() {
        let (path, evidence) = owner.copied_view(stop, qualification).await?;
        owned = Some(
            SqliteConnection::connect_with(
                &SqliteConnectOptions::new()
                    .filename(path)
                    .read_only(true)
                    .create_if_missing(false)
                    .busy_timeout(std::time::Duration::from_millis(500)),
            )
            .await
            .map_err(|_| "verified owned History view unavailable")?,
        );
        evidence
    } else {
        json!({"kind":"direct_read_only_sqlite_transaction","identity":identity,"copy_bytes":0,"basis":"one verified open provider reader; committed WAL included by SQLite"})
    };
    let connection = if let Some(owned) = owned.as_mut() {
        owned
    } else {
        owner.direct.as_mut().expect("direct reader")
    };
    let mut tx = connection
        .begin()
        .await
        .map_err(|_| "Chromium consistent read unavailable")?;
    let schema: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| "Chromium schema basis unavailable")?;
    let visit_schema: String =
        sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE name='visits' AND type='table'")
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| "Chromium schema unavailable")?
            .ok_or("Chromium visits table missing")?;
    if !visit_schema.to_ascii_uppercase().contains("AUTOINCREMENT") {
        return Err("unsupported Chromium visit key schema; AUTOINCREMENT premise absent".into());
    }
    // Validate required selected columns even on an accessible empty database.
    for query in [
        "SELECT id,url,visit_time,visit_duration,transition,from_visit FROM visits LIMIT 0",
        "SELECT id,url,title FROM urls LIMIT 0",
        "SELECT id,guid,start_time,end_time,current_path,target_path,state,received_bytes,total_bytes FROM downloads LIMIT 0",
        "SELECT id,chain_index,url FROM downloads_url_chains LIMIT 0",
    ] {
        sqlx::query(query)
            .execute(&mut *tx)
            .await
            .map_err(|_| "unsupported Chromium History schema; required columns unavailable")?;
    }
    // Required columns alone do not establish the cursor/chain key basis.
    for (table, expected) in [
        ("visits", vec![("id", 1i64)]),
        ("urls", vec![("id", 1)]),
        ("downloads", vec![("id", 1)]),
        ("downloads_url_chains", vec![("id", 1), ("chain_index", 2)]),
    ] {
        let columns: Vec<(String, String, i64)> = sqlx::query_as(&format!(
            "SELECT name,type,pk FROM pragma_table_info('{table}') WHERE pk>0 ORDER BY pk"
        ))
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| "Chromium key declaration unavailable")?;
        if columns.len() != expected.len()
            || columns
                .iter()
                .zip(expected)
                .any(|((name, ty, pk), (want, index))| {
                    name != want || !ty.eq_ignore_ascii_case("INTEGER") || *pk != index
                })
        {
            return Err("unsupported Chromium primary-key/URL-chain declaration".into());
        }
    }
    let duplicate: Option<String> = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sqlx::query_scalar(
            "SELECT guid FROM downloads GROUP BY guid COLLATE NOCASE HAVING count(*)>1 LIMIT 1",
        )
        .fetch_optional(&mut *tx),
    )
    .await
    .map_err(|_| "Chromium GUID validation budget exhausted")?
    .map_err(|_| "Chromium GUID validation unavailable")?;
    if duplicate.is_some() {
        return Err(
            "ambiguous duplicate Chromium download GUID; no selected value admitted".into(),
        );
    }
    let sequence: Option<i64> =
        sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name='visits'")
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| "Chromium visit sequence unavailable")?;
    let sequence = sequence.unwrap_or(0);
    if previous.is_some_and(|p| sequence < p.visits_sequence) {
        return Err("Chromium visit sequence reset under declared generation; explicit reconciliation required".into());
    }
    let full = due(previous, epoch, schema, *full_due);
    let now = OffsetDateTime::now_utc();
    let snapshot_due = previous
        .and_then(|c| c.snapshot_at.as_deref())
        .and_then(|v| clock(v).ok())
        .and_then(|t| {
            t.checked_add(time::Duration::milliseconds(
                config.snapshot_interval_ms as i64,
            ))
        })
        .is_none_or(|due| now >= due);
    let mut metadata = json!({"browser_read_plan_v1":{"kind":if full{"full_value_comparison"}else{"new_keys_and_prior_unfinished"},"custody":custody,"profile":config.profile,"generation":config.database_generation,"ordering_basis":"AUTOINCREMENT visits; GUID download key census, not an ID monotonicity claim","privacy_class":"plain","coverage":"selected retained values; no attention, population completeness or deletion inference"}});
    let physical_snapshot = if snapshot_due {
        let bytes = crate::sqlite_custody::backup(&mut tx, &snapshot_path, stop).await?;
        let snapshot = admission
            .browser_snapshot(
                attempt.basis.input.ok_or("browser input absent")?,
                attempt.observation,
                snapshot_path.clone(),
            )
            .await
            .map_err(|_| "physical browser snapshot custody unavailable")?;
        metadata["physical_snapshot"] = snapshot.clone();
        metadata["browser_read_plan_v1"]["snapshot_copied_bytes"] = json!(bytes);
        let _ = std::fs::remove_file(&snapshot_path);
        Some(snapshot)
    } else {
        None
    };
    let mut batch = Batch {
        input: attempt.basis.input.ok_or("browser input absent")?,
        observation: attempt.observation,
        admission,
        metadata,
        rows: Vec::new(),
        bytes: 0,
        selected: 0,
        canonical_bytes: 0,
    };
    let old_through = previous.map_or(0, |c| c.visits_through);
    let mut through = if full { 0 } else { old_through };
    let mut download_max = 0;
    loop {
        if *stop.borrow() {
            return Err(
                "Chromium value read cancelled; acknowledged prefix remains incomplete".into(),
            );
        }
        let ids: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM visits WHERE id>? ORDER BY id LIMIT 64")
                .bind(through)
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| "Chromium visit selection unavailable")?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            through = id;
            batch.push(visit(&mut tx, id).await?).await?;
        }
    }
    let mut examined = 0u64;
    let mut cursor = 0;
    loop {
        if *stop.borrow() {
            return Err("Chromium download selection cancelled".into());
        }
        let keys: Vec<(i64, String)> =
            sqlx::query_as("SELECT id,guid FROM downloads WHERE id>? ORDER BY id LIMIT 64")
                .bind(cursor)
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| "Chromium download keys unavailable")?;
        if keys.is_empty() {
            break;
        }
        let normalized: Vec<String> = keys
            .iter()
            .map(|(_, guid)| {
                Uuid::parse_str(guid)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|_| guid.clone())
            })
            .collect();
        let known = if full {
            vec![false; keys.len()]
        } else {
            admission
                .browser_known_downloads(
                    config.profile.clone(),
                    config.database_generation,
                    normalized,
                )
                .await
                .map_err(|_| "Chromium admitted GUID lookup unavailable")?
        };
        if known.len() != keys.len() {
            return Err("Chromium key selection result count differs".into());
        }
        for ((id, _), known) in keys.into_iter().zip(known) {
            cursor = id;
            download_max = download_max.max(id);
            examined += 1;
            if !known {
                batch.push(download(&mut tx, id).await?).await?;
            }
        }
    }
    if !full {
        let mut after = None;
        loop {
            if *stop.borrow() {
                return Err("Chromium unfinished selection cancelled".into());
            }
            let pending = admission
                .browser_pending(batch.input, attempt.basis.basis, after)
                .await
                .map_err(|_| "Chromium canonical unfinished basis unavailable")?;
            if pending.is_empty() {
                break;
            }
            for key in pending {
                after = Some(key.event);
                if key.table == "visits" && key.coordinate <= old_through {
                    let exists: bool =
                        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM visits WHERE id=?)")
                            .bind(key.coordinate)
                            .fetch_one(&mut *tx)
                            .await
                            .map_err(|_| "Chromium unfinished visit key unavailable")?;
                    if exists {
                        batch.push(visit(&mut tx, key.coordinate).await?).await?;
                    }
                } else if key.table == "downloads" {
                    let current: Option<String> =
                        sqlx::query_scalar("SELECT guid FROM downloads WHERE id=?")
                            .bind(key.coordinate)
                            .fetch_optional(&mut *tx)
                            .await
                            .map_err(|_| "Chromium unfinished download key unavailable")?;
                    if current
                        .as_deref()
                        .and_then(|v| Uuid::parse_str(v).ok())
                        .map(|v| v.to_string())
                        .as_deref()
                        == Some(key.key.as_str())
                    {
                        batch.push(download(&mut tx, key.coordinate).await?).await?;
                    }
                }
            }
        }
    }
    // Finish the actual selected read before committing reusable progress. Missing
    // keys are not a completed census and never synthesize removed events.
    tx.commit()
        .await
        .map_err(|_| "Chromium selected read finalization failed")?;
    if owned.is_none() {
        crate::atuin_guard::verify(owner.direct.as_mut().expect("direct reader"), &config.path)
            .await?;
    }
    owner.verify()?;
    let decision = tokio::time::Instant::now();
    let settled = OffsetDateTime::now_utc();
    let deadline = if full {
        Some(
            decision
                .checked_add(std::time::Duration::from_millis(config.full_interval_ms))
                .ok_or("browser monotonic full clock overflow")?,
        )
    } else {
        *full_due
    };
    let mut checkpoint = if full {
        Checkpoint {
            version: 1,
            epoch,
            generation: config.database_generation,
            identity,
            schema_version: schema,
            visits_sequence: sequence,
            visits_through: through.max(old_through),
            download_max_coordinate: download_max,
            full_completed_at: settled
                .format(&Rfc3339)
                .map_err(|_| "browser full clock encoding")?,
            full_due_at: settled
                .checked_add(time::Duration::milliseconds(config.full_interval_ms as i64))
                .ok_or("browser full due overflow")?
                .format(&Rfc3339)
                .map_err(|_| "browser full due encoding")?,
            snapshot_at: previous.and_then(|c| c.snapshot_at.clone()),
            snapshot: previous.and_then(|c| c.snapshot),
            needs_full: false,
        }
    } else {
        previous.expect("reusable browser checkpoint").clone()
    };
    checkpoint.visits_through = checkpoint.visits_through.max(through);
    checkpoint.visits_sequence = sequence;
    checkpoint.download_max_coordinate = download_max;
    if let Some(snapshot) = physical_snapshot {
        checkpoint.snapshot = serde_json::from_value(snapshot["material"].clone()).ok();
        checkpoint.snapshot_at = Some(
            settled
                .format(&Rfc3339)
                .map_err(|_| "browser snapshot clock encoding")?,
        );
    }
    batch.metadata["browser_read_plan_v1"]["selected_records"] = json!(batch.selected);
    batch.metadata["browser_read_plan_v1"]["download_keys_examined"] = json!(examined);
    batch.metadata["browser_read_plan_v1"]["canonical_bytes_upper_bound"] =
        json!(batch.canonical_bytes);
    admission
        .browser_step(BrowserStep {
            input: batch.input,
            observation: attempt.observation,
            rows: batch.rows,
            complete: true,
            unavailable: None,
            metadata: batch.metadata,
            checkpoint: Some(checkpoint),
            fence: false,
        })
        .await
        .map_err(|_| "Chromium final admission unavailable")?;
    *full_due = deadline;
    Ok(())
}

// An interrupted backup or downstream writer error must not leave a private DB copy.
struct SnapshotScratch(PathBuf);
impl Drop for SnapshotScratch {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut path = self.0.as_os_str().to_owned();
            path.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(path));
        }
    }
}

#[cfg(test)]
#[path = "browser/read_plan_tests.rs"]
mod read_plan_tests;
