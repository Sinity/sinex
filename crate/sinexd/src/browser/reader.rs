//! The Chromium poll loop. Each poll reads one SQLite transaction and admits its
//! rows in bounded steps; the checkpoint is saved only with the final step.
use super::{
    rows::ProviderRows,
    snapshot::{self, Failure, HistorySource},
};
use crate::config::ChromiumConfig;
use serde_json::{Value, json};
use sinex_model::browser::{BrowserReadStart, Checkpoint};
use sinex_store::{
    Admission, BrowserBegin, BrowserStep, MAX_STEP_BYTES, MAX_STEP_ROWS, PrivacyRule, TestPause,
};
use sqlx::{
    Connection,
    sqlite::{SqliteConnectOptions, SqliteConnection},
};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{sync::watch, time::Instant};
use uuid::Uuid;

pub struct Attempt {
    observation: Uuid,
    start: BrowserReadStart,
}

/// Register the next reading observation with the writer before touching History.
pub async fn begin(
    config: &ChromiumConfig,
    rules: &[PrivacyRule],
    admission: &Admission,
) -> Result<Attempt, String> {
    let observation = Uuid::now_v7();
    let observed_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|_| "browser observation clock encoding")?;
    let start = admission
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
    Ok(Attempt { observation, start })
}

pub async fn run(
    config: ChromiumConfig,
    root: PathBuf,
    rules: Vec<PrivacyRule>,
    admission: Admission,
    mut stop: watch::Receiver<bool>,
    mut attempt: Attempt,
    pause: Option<TestPause>,
) -> Result<(), String> {
    let mut reader = Reader {
        config,
        root,
        source: None,
        full_due: None,
        snapshot_due: None,
        pause,
    };
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let observation = attempt.observation;
        if let Err(failure) = reader.poll(&attempt, &admission, &stop).await {
            reader.reset();
            let (reason, fence) = failure.into_outcome();
            admission
                .browser_step(BrowserStep {
                    input: attempt.start.input.ok_or("browser input absent")?,
                    observation,
                    rows: vec![],
                    complete: false,
                    unavailable: Some(reason),
                    metadata: json!({"coverage":"unknown; previous retained values remain available"}),
                    checkpoint: None,
                    fence,
                })
                .await
                .map_err(|_| "browser unavailable outcome could not commit")?;
        }
        let interval = reader.config.poll_interval_ms;
        let decision = Instant::now();
        let schedule =
            sinex_model::acquisition::PollSchedule::after(OffsetDateTime::now_utc(), interval)
                .map_err(|_| "browser next poll clock")?;
        let target = decision
            .checked_add(Duration::from_millis(interval))
            .ok_or("browser next poll deadline overflow")?;
        admission
            .schedule_source(observation, schedule)
            .await
            .map_err(|_| "browser schedule could not commit")?;
        tokio::select! {
            _ = stop.changed() => return Ok(()),
            _ = tokio::time::sleep_until(target) => {}
        }
        attempt = begin(&reader.config, &rules, &admission).await?;
    }
}

/// State kept between polls of one profile. A failed poll drops all of it, so
/// the next poll reopens History and does a full read.
struct Reader {
    config: ChromiumConfig,
    /// Directory for this daemon's copies and snapshots of History files.
    root: PathBuf,
    source: Option<HistorySource>,
    full_due: Option<Instant>,
    snapshot_due: Option<Instant>,
    pause: Option<TestPause>,
}

impl Reader {
    fn reset(&mut self) {
        self.source = None;
        self.full_due = None;
        self.snapshot_due = None;
    }

    async fn poll(
        &mut self,
        attempt: &Attempt,
        admission: &Admission,
        stop: &watch::Receiver<bool>,
    ) -> Result<(), Failure> {
        if attempt.start.reconciliation_required {
            return Err(Failure::ReconciliationRequired(
                "configured browser generation is fenced; reconciliation required",
            ));
        }
        let input = attempt.start.input.ok_or("browser input absent")?;
        let config = &self.config;
        if self.source.is_none() {
            let dir = self
                .root
                .join(config.database_generation.to_string())
                .join(blake3::hash(config.profile.as_bytes()).to_hex().as_str());
            self.source = Some(HistorySource::new(&config.path, dir).await?);
        }
        let source = self.source.as_mut().expect("opened above");
        source.verify()?;
        let previous = attempt.start.checkpoint.as_ref();
        if previous.is_some_and(|p| {
            p.generation != config.database_generation || p.identity != source.identity
        }) {
            return Err(Failure::ReconciliationRequired(
                "History identity differs from declared generation; reconciliation required",
            ));
        }
        let identity = source.identity.clone();
        let epoch = source.epoch;
        let snapshot_path = source.next_path("snapshot");
        let _snapshot_cleanup = SnapshotScratch(snapshot_path.clone());

        // Chromium holding an exclusive lock leaves no direct reader; read a
        // verified copy instead.
        let mut copy = None;
        let read_method = if source.direct.is_none() {
            let (path, evidence) = source.copied_view(stop, &mut self.pause).await?;
            copy = Some(open_copy(&path).await?);
            evidence
        } else {
            json!({"kind":"direct_read_only_sqlite_transaction","identity":identity,"copy_bytes":0,"basis":"one verified open provider reader; committed WAL included by SQLite"})
        };
        let connection = match copy.as_mut() {
            Some(copy) => copy,
            None => source.direct.as_mut().expect("direct reader"),
        };
        let mut tx = connection
            .begin()
            .await
            .map_err(|_| "Chromium consistent read unavailable")?;
        let schema = check_schema(&mut tx).await?;
        let rows = ProviderRows::new(&mut tx).await?;
        let sequence: Option<i64> =
            sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name='visits'")
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| "Chromium visit sequence unavailable")?;
        let sequence = sequence.unwrap_or(0);
        if previous.is_some_and(|p| sequence < p.visits_sequence) {
            return Err(Failure::ReconciliationRequired(
                "Chromium visit sequence reset under declared generation; reconciliation required",
            ));
        }
        let full = due(previous, epoch, schema, self.full_due);
        // After a reopen the stored wall-clock time can shorten the first
        // snapshot delay but never extend it past one interval; later deadlines
        // are monotonic.
        let snapshot_deadline = match self.snapshot_due {
            Some(deadline) => deadline,
            None => first_snapshot_deadline(
                previous,
                OffsetDateTime::now_utc(),
                Instant::now(),
                config.snapshot_interval_ms,
            )?,
        };
        let mut metadata = json!({"browser_read_plan_v1":{"kind":if full{"full_value_comparison"}else{"new_keys_and_prior_unfinished"},"custody":read_method,"profile":config.profile,"generation":config.database_generation,"ordering_basis":"AUTOINCREMENT visits; GUID download key census, not an ID monotonicity claim","privacy_class":"plain","coverage":"selected retained values; no attention, population completeness or deletion inference"}});
        let snapshot = if Instant::now() >= snapshot_deadline {
            let (stored, bytes) = store_snapshot(
                &mut tx,
                &snapshot_path,
                input,
                attempt.observation,
                admission,
                stop,
            )
            .await?;
            metadata["physical_snapshot"] = stored.clone();
            metadata["browser_read_plan_v1"]["snapshot_copied_bytes"] = json!(bytes);
            let next = Instant::now()
                .checked_add(Duration::from_millis(config.snapshot_interval_ms))
                .ok_or("browser snapshot monotonic deadline overflow")?;
            Some((stored, next))
        } else {
            None
        };

        let mut batch = Batch {
            input,
            observation: attempt.observation,
            admission,
            metadata,
            rows: Vec::new(),
            bytes: 0,
            selected: 0,
            canonical_bytes: 0,
        };
        let old_through = previous.map_or(0, |c| c.visits_through);
        let through = read_visits(
            &mut tx,
            &rows,
            &mut batch,
            if full { 0 } else { old_through },
            stop,
        )
        .await?;
        let (examined, download_max) =
            read_downloads(&mut tx, &rows, &mut batch, config, full, stop).await?;
        if !full {
            reread_unfinished(
                &mut tx,
                &rows,
                &mut batch,
                attempt.start.snapshot_seq,
                old_through,
                stop,
            )
            .await?;
        }
        // A key missing from History is not evidence of deletion; no removal
        // event is ever written.
        tx.commit()
            .await
            .map_err(|_| "Chromium selected read finalization failed")?;
        if copy.is_none() {
            crate::sqlite_file::verify(
                source.direct.as_mut().expect("direct reader"),
                &config.path,
            )
            .await?;
        }
        source.verify()?;

        let decision = Instant::now();
        let finished = OffsetDateTime::now_utc();
        let full_due = if full {
            Some(
                decision
                    .checked_add(Duration::from_millis(config.full_interval_ms))
                    .ok_or("browser monotonic full clock overflow")?,
            )
        } else {
            self.full_due
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
                full_completed_at: finished
                    .format(&Rfc3339)
                    .map_err(|_| "browser full clock encoding")?,
                full_due_at: finished
                    .checked_add(time::Duration::milliseconds(config.full_interval_ms as i64))
                    .ok_or("browser full due overflow")?
                    .format(&Rfc3339)
                    .map_err(|_| "browser full due encoding")?,
                snapshot_at: previous.and_then(|c| c.snapshot_at.clone()),
                snapshot: previous.and_then(|c| c.snapshot),
                needs_full: false,
            }
        } else {
            previous.expect("a reduced poll has a checkpoint").clone()
        };
        checkpoint.visits_through = checkpoint.visits_through.max(through);
        checkpoint.visits_sequence = sequence;
        checkpoint.download_max_coordinate = download_max;
        if let Some((stored, _)) = &snapshot {
            checkpoint.snapshot = serde_json::from_value(stored["material"].clone()).ok();
            checkpoint.snapshot_at = Some(
                stored["writer_ack_observed_at"]
                    .as_str()
                    .ok_or("browser snapshot acknowledgment clock missing")?
                    .to_owned(),
            );
        }
        let plan = &mut batch.metadata["browser_read_plan_v1"];
        plan["selected_records"] = json!(batch.selected);
        plan["download_keys_examined"] = json!(examined);
        plan["canonical_bytes_upper_bound"] = json!(batch.canonical_bytes);
        admission
            .browser_step(BrowserStep {
                input,
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
        self.full_due = full_due;
        self.snapshot_due = Some(snapshot.map_or(snapshot_deadline, |(_, next)| next));
        Ok(())
    }
}

async fn open_copy(path: &Path) -> Result<SqliteConnection, Failure> {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .read_only(true)
            .create_if_missing(false)
            .busy_timeout(Duration::from_millis(500)),
    )
    .await
    .map_err(|_| "verified History copy unavailable".into())
}

/// Check the tables and keys the reader relies on and return SQLite's schema version.
async fn check_schema(tx: &mut SqliteConnection) -> Result<i64, Failure> {
    let schema: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| "Chromium schema version unavailable")?;
    let visit_schema: String =
        sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE name='visits' AND type='table'")
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| "Chromium schema unavailable")?
            .ok_or("Chromium visits table missing")?;
    if !visit_schema.to_ascii_uppercase().contains("AUTOINCREMENT") {
        return Err("unsupported Chromium visits table: id is not AUTOINCREMENT".into());
    }
    // Run even on an empty database, so a missing column fails every poll.
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
    // Cursors and URL-chain order rely on these integer primary keys.
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
    let oversized_guid: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM downloads WHERE length(CAST(guid AS BLOB))>512)",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(|_| "Chromium GUID length check unavailable")?;
    if oversized_guid {
        return Err("Chromium GUID exceeds supported key size before provider transfer".into());
    }
    let duplicate: Option<String> = tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query_scalar(
            "SELECT guid FROM downloads GROUP BY guid COLLATE NOCASE HAVING count(*)>1 LIMIT 1",
        )
        .fetch_optional(&mut *tx),
    )
    .await
    .map_err(|_| "Chromium GUID check timed out")?
    .map_err(|_| "Chromium GUID check unavailable")?;
    if duplicate.is_some() {
        return Err("duplicate Chromium download GUID; nothing admitted".into());
    }
    Ok(schema)
}

/// Back up the read transaction's view, hand the file to the writer to store,
/// and return the writer's record and the copied byte count.
async fn store_snapshot(
    tx: &mut SqliteConnection,
    path: &Path,
    input: Uuid,
    observation: Uuid,
    admission: &Admission,
    stop: &watch::Receiver<bool>,
) -> Result<(Value, u64), Failure> {
    let bytes = snapshot::backup(tx, path, stop).await?;
    let mut stored = admission
        .browser_snapshot(input, observation, path.to_owned())
        .await
        .map_err(|_| "browser snapshot could not be stored")?;
    stored["writer_ack_observed_at"] = json!(
        OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|_| "browser snapshot acknowledgment clock encoding")?
    );
    let _ = std::fs::remove_file(path);
    Ok((stored, bytes))
}

/// Admit every visit with id above `from`; return the last id read.
async fn read_visits(
    tx: &mut SqliteConnection,
    rows: &ProviderRows,
    batch: &mut Batch<'_>,
    from: i64,
    stop: &watch::Receiver<bool>,
) -> Result<i64, Failure> {
    let mut through = from;
    loop {
        if *stop.borrow() {
            return Err("Chromium visit read cancelled; admitted rows stay incomplete".into());
        }
        let ids: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM visits WHERE id>? ORDER BY id LIMIT 64")
                .bind(through)
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| "Chromium visit selection unavailable")?;
        if ids.is_empty() {
            return Ok(through);
        }
        for id in ids {
            through = id;
            batch.push(visit(rows, tx, id).await?).await?;
        }
    }
}

/// Walk every download key and admit the downloads the store does not know
/// yet (all of them on a full read). Return the count examined and the highest id.
async fn read_downloads(
    tx: &mut SqliteConnection,
    rows: &ProviderRows,
    batch: &mut Batch<'_>,
    config: &ChromiumConfig,
    full: bool,
    stop: &watch::Receiver<bool>,
) -> Result<(u64, i64), Failure> {
    let mut examined = 0u64;
    let mut max_id = 0;
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
            return Ok((examined, max_id));
        }
        let known = if full {
            vec![false; keys.len()]
        } else {
            let guids = keys
                .iter()
                .map(|(_, guid)| {
                    Uuid::parse_str(guid)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|_| guid.clone())
                })
                .collect();
            batch
                .admission
                .browser_known_downloads(config.profile.clone(), config.database_generation, guids)
                .await
                .map_err(|_| "Chromium admitted GUID lookup unavailable")?
        };
        if known.len() != keys.len() {
            return Err("Chromium key selection result count differs".into());
        }
        for ((id, _), known) in keys.into_iter().zip(known) {
            cursor = id;
            max_id = max_id.max(id);
            examined += 1;
            if !known {
                batch.push(download(rows, tx, id).await?).await?;
            }
        }
    }
}

/// Re-read rows the store still marks unfinished (zero-duration visits at or
/// below `old_through`, in-progress downloads) so their final values are admitted.
async fn reread_unfinished(
    tx: &mut SqliteConnection,
    rows: &ProviderRows,
    batch: &mut Batch<'_>,
    snapshot_seq: i64,
    old_through: i64,
    stop: &watch::Receiver<bool>,
) -> Result<(), Failure> {
    let mut after = None;
    loop {
        if *stop.borrow() {
            return Err("Chromium unfinished selection cancelled".into());
        }
        let pending = batch
            .admission
            .browser_pending(batch.input, snapshot_seq, after)
            .await
            .map_err(|_| "Chromium unfinished keys unavailable")?;
        if pending.is_empty() {
            return Ok(());
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
                    batch.push(visit(rows, tx, key.coordinate).await?).await?;
                }
            } else if key.table == "downloads" {
                // Downloads are keyed by GUID; Chromium may renumber the row id
                // while a download is unfinished.
                let current: Option<i64> =
                    sqlx::query_scalar("SELECT id FROM downloads WHERE lower(guid)=?")
                        .bind(&key.key)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|_| "Chromium unfinished download key unavailable")?;
                if let Some(id) = current {
                    batch.push(download(rows, tx, id).await?).await?;
                }
            }
        }
    }
}

async fn visit(
    rows: &ProviderRows,
    connection: &mut SqliteConnection,
    id: i64,
) -> Result<Value, String> {
    let visit = rows
        .read(connection, "visits", id, None)
        .await?
        .ok_or("Chromium visit missing in selected snapshot")?;
    let url = rows
        .read(
            connection,
            "urls",
            visit["url"]
                .as_i64()
                .ok_or("Chromium URL key unavailable")?,
            None,
        )
        .await?
        .unwrap_or(Value::Null);
    let external = visit
        .get("external_referrer_url")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty());
    let referrer = if let Some(external) = external {
        Some(external.to_owned())
    } else {
        let from_visit = visit["from_visit"].as_i64();
        let bytes: Option<i64> = sqlx::query_scalar(
            "SELECT length(CAST(u.url AS BLOB)) FROM visits v JOIN urls u ON u.id=v.url WHERE v.id=?",
        )
        .bind(from_visit)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|_| "Chromium referrer size unavailable")?;
        if bytes.is_some_and(|n| n < 0 || n > MAX_STEP_BYTES as i64) {
            return Err(
                "Chromium referrer exceeds step size limit before provider transfer".into(),
            );
        }
        sqlx::query_scalar("SELECT u.url FROM visits v JOIN urls u ON u.id=v.url WHERE v.id=?")
            .bind(from_visit)
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| "Chromium from-visit read unavailable")?
    };
    let referrer_basis = if external.is_some() {
        "provider external_referrer_url"
    } else if referrer.is_some() {
        "provider from_visit URL in selected snapshot"
    } else {
        "unknown"
    };
    Ok(json!({
        "table": "visits",
        "coordinate": id,
        "visit": visit,
        "url": url,
        "referrer_url": referrer,
        "referrer_basis": referrer_basis,
    }))
}

async fn download(
    rows: &ProviderRows,
    connection: &mut SqliteConnection,
    id: i64,
) -> Result<Value, String> {
    let download = rows
        .read(connection, "downloads", id, None)
        .await?
        .ok_or("Chromium download missing in selected snapshot")?;
    let mut chain = Vec::new();
    let mut after = None;
    let mut size = serde_json::to_vec(&download)
        .map_err(|_| "download encoding")?
        .len();
    loop {
        let index: Option<i64> = if let Some(after) = after {
            sqlx::query_scalar(
                "SELECT chain_index FROM downloads_url_chains WHERE id=? AND chain_index>? ORDER BY chain_index LIMIT 1",
            )
            .bind(id)
            .bind(after)
            .fetch_optional(&mut *connection)
            .await
        } else {
            sqlx::query_scalar(
                "SELECT chain_index FROM downloads_url_chains WHERE id=? ORDER BY chain_index LIMIT 1",
            )
            .bind(id)
            .fetch_optional(&mut *connection)
            .await
        }
        .map_err(|_| "Chromium URL chain key unavailable")?;
        let Some(index) = index else {
            break;
        };
        after = Some(index);
        let row = rows
            .read(connection, "downloads_url_chains", id, Some(index))
            .await?
            .ok_or("Chromium chain row missing in selected snapshot")?;
        size = size
            .checked_add(
                serde_json::to_vec(&row)
                    .map_err(|_| "URL-chain encoding")?
                    .len(),
            )
            .ok_or("URL-chain size overflow")?;
        if size > MAX_STEP_BYTES - 1024 {
            return Err(
                "Chromium download record exceeds step size limit; URL chains are never truncated"
                    .into(),
            );
        }
        chain.push(row);
    }
    Ok(json!({"table":"downloads","coordinate":id,"download":download,"url_chain":chain}))
}

/// Rows waiting for the next writer step, flushed at the row or byte limit.
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
            .map_err(|_| "browser rows could not be admitted")?;
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
            return Err("Chromium row exceeds step size limit; poll incomplete".into());
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
    OffsetDateTime::parse(text, &Rfc3339).map_err(|_| "browser checkpoint clock invalid".into())
}

/// A full read is due when the checkpoint cannot be reused, its stored due time
/// has passed, or this reader's monotonic deadline has passed (or is unset).
fn due(previous: Option<&Checkpoint>, epoch: Uuid, schema: i64, deadline: Option<Instant>) -> bool {
    previous.is_none_or(|c| {
        c.version != 1
            || c.epoch != epoch
            || c.schema_version != schema
            || c.needs_full
            || !clock(&c.full_due_at).is_ok_and(|t| OffsetDateTime::now_utc() < t)
    }) || deadline.is_none_or(|t| Instant::now() >= t)
}

/// The first snapshot deadline after opening History: the stored snapshot time
/// plus one interval, capped at one interval from now.
fn first_snapshot_deadline(
    previous: Option<&Checkpoint>,
    wall: OffsetDateTime,
    monotonic: Instant,
    interval_ms: u64,
) -> Result<Instant, String> {
    let interval = Duration::from_millis(interval_ms);
    let remaining = previous
        .and_then(|c| c.snapshot_at.as_deref())
        .map(clock)
        .transpose()?
        .map(|at| {
            at.checked_add(time::Duration::milliseconds(interval_ms as i64))
                .ok_or("browser snapshot wall deadline overflow")
        })
        .transpose()?
        .and_then(|target| (target - wall).try_into().ok())
        .map_or(Duration::ZERO, |remaining: Duration| {
            remaining.min(interval)
        });
    monotonic
        .checked_add(remaining)
        .ok_or_else(|| "browser snapshot monotonic deadline overflow".into())
}

// Removes the snapshot scratch file and its sidecars on every exit path, so an
// interrupted backup or writer error never leaves a private copy behind.
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
#[path = "reader_tests.rs"]
mod tests;
