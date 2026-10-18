//! Chromium history admission: visits and downloads from a profile's History
//! database are stored as stream parts and admitted as events. Full database
//! snapshots are stored as blobs.
use crate::source_admission::{
    key_hash, observation_status, producer_version, record_failure, row_hash, set_part_length,
};
use crate::{BLOB_PRESENT, Error, Result, Writer, coordinate_hash, next_commit_seq};
use serde_json::{Value, json};
use sinex_model::browser::{BrowserReadStart, Checkpoint, PARSER, SOURCE};
use sqlx::{Connection, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

const PARSER_NAME: &str = "parser.browser.history.chromium";

#[derive(Clone)]
pub struct BrowserBegin {
    pub profile: String,
    pub generation: Uuid,
    pub path: String,
    pub observation: Uuid,
    pub observed_at: String,
    pub privacy_rules: Vec<crate::PrivacyRule>,
}
#[derive(Clone)]
pub struct BrowserStep {
    pub input: Uuid,
    pub observation: Uuid,
    pub rows: Vec<Value>,
    pub complete: bool,
    pub unavailable: Option<String>,
    pub metadata: Value,
    pub checkpoint: Option<Checkpoint>,
    /// The database no longer matches the stored rows; mark the input as
    /// needing reconciliation.
    pub fence: bool,
}
/// A live event that may still change: a visit without a duration or a
/// download that has not finished.
#[derive(Clone, Debug)]
pub struct BrowserPending {
    pub event: Uuid,
    pub table: String,
    pub key: String,
    pub coordinate: i64,
}
fn occurrence(profile: &str, generation: Uuid, kind: &str, key: &str) -> (String, Vec<u8>) {
    let key = json!([profile, generation, kind, key]).to_string();
    let hash = key_hash(SOURCE, kind, &key);
    (key, hash)
}
impl Writer {
    pub(crate) async fn browser_begin(&mut self, begin: BrowserBegin) -> Result<BrowserReadStart> {
        if begin.profile.is_empty() || begin.generation.is_nil() || begin.path.is_empty() {
            return Err(Error::Invalid("invalid browser source declaration"));
        }
        OffsetDateTime::parse(&begin.observed_at, &Rfc3339)
            .map_err(|_| Error::Invalid("browser observation clock"))?;
        for rule in &begin.privacy_rules {
            rule.validate()
                .map_err(|_| Error::Invalid("browser privacy rule"))?;
        }
        let locator = json!([begin.profile, begin.generation]).to_string();
        let input = self.register_input(SOURCE, &locator, "sqlite").await?;
        let outcomes = self.following_outcomes(input).await?;
        let provenance = json!({
            "source": SOURCE,
            "parser": PARSER,
            "representation": "canonical_sqlite_rows",
            "privacy_class": "plain",
            "privacy_rules": begin.privacy_rules,
            "profile": begin.profile,
            "database_generation": begin.generation,
            "identity_basis": "explicit configured profile/generation continuity premise; not proved persistent physical incarnation",
            "physical_snapshot": "periodic full base; page patches unavailable",
        });
        let metadata = json!({
            "path": begin.path,
            "profile": begin.profile,
            "database_generation": begin.generation,
            "_sinex_acquisition_v1": {
                "version": 1,
                "receipts": "capture_insert_v1",
                "owner_run": self.run_id,
                "outcomes": outcomes,
            },
        });
        let mut tx = self.connection.begin().await?;
        let seq: i64 = next_commit_seq(&mut tx).await?;
        sqlx::query(
            "INSERT INTO raw.observations
               (id,input_id,seq,observed_at,source_provenance,metadata,status,commit_seq)
             SELECT $1,$2,coalesce(max(seq),0)+1,$3::text::timestamptz,$4,$5,'reading',$6
             FROM raw.observations WHERE input_id=$2",
        )
        .bind(begin.observation)
        .bind(input)
        .bind(begin.observed_at)
        .bind(provenance)
        .bind(metadata)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
        let cursor: Option<(Value, Value)> = sqlx::query_as(
            "SELECT position,carry FROM raw.source_cursors WHERE binding=$1 AND input=$2",
        )
        .bind(SOURCE)
        .bind(input.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        let reconciliation_required = cursor
            .as_ref()
            .is_some_and(|(p, _)| p["reconciliation_required"] == true);
        let checkpoint = cursor
            .and_then(|(_, c)| serde_json::from_value::<Checkpoint>(c).ok())
            .filter(|c| c.version == 1);
        tx.commit().await?;
        Ok(BrowserReadStart {
            input: Some(input),
            snapshot_seq: seq,
            checkpoint,
            reconciliation_required,
        })
    }
    /// One page of pending events as of `snapshot_seq`.
    pub(crate) async fn browser_pending(
        &mut self,
        input: Uuid,
        snapshot_seq: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<BrowserPending>> {
        let mut tx = self.connection.begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let rows: Vec<(Uuid, String, String, i64)> = sqlx::query_as(
            "SELECT e.id,
               CASE WHEN e.event_type='page.visited' THEN 'visits' ELSE 'downloads' END,
               e.equivalence_key::jsonb->>3,
               CASE WHEN e.event_type='page.visited' THEN (e.payload->>'visit_id')::bigint
                    ELSE (e.payload->>'download_id')::bigint END
             FROM core.events e
             JOIN raw.source_material_registry m ON m.id=e.source_material_id
             WHERE m.input_id=$1 AND e.source=$2 AND e.commit_seq<=$3
               AND e.record_state='present' AND ($4::uuid IS NULL OR e.id>$4)
               AND ((e.event_type='page.visited' AND (e.payload->>'duration_us')::bigint=0)
                    OR (e.event_type='download.observed'
                        AND (e.payload->>'state')::bigint NOT IN (1,2)))
               AND NOT EXISTS(SELECT 1 FROM audit.archived a
                              WHERE a.id=e.id AND a.archived_seq<=$3)
             ORDER BY e.id LIMIT 64",
        )
        .bind(input)
        .bind(SOURCE)
        .bind(snapshot_seq)
        .bind(after)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows
            .into_iter()
            .map(|(event, table, key, coordinate)| BrowserPending {
                event,
                table,
                key,
                coordinate,
            })
            .collect())
    }
    /// Whether each download key already has a live event.
    pub(crate) async fn browser_known_downloads(
        &mut self,
        profile: String,
        generation: Uuid,
        keys: Vec<String>,
    ) -> Result<Vec<bool>> {
        if keys.len() > 64 {
            return Err(Error::Invalid("browser key selection exceeds bound"));
        }
        let hashes: Vec<Vec<u8>> = keys
            .iter()
            .map(|key| occurrence(&profile, generation, "download.observed", key).1)
            .collect();
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.store=0 AND l.key_hash=h)
             FROM unnest($1::bytea[]) WITH ORDINALITY AS t(h,n) ORDER BY n",
        )
        .bind(hashes)
        .fetch_all(&mut self.connection)
        .await?)
    }
    /// Store a copy of the History database taken under `<root>/acquisition`.
    pub(crate) async fn browser_snapshot(
        &mut self,
        input: Uuid,
        observation: Uuid,
        path: std::path::PathBuf,
    ) -> Result<Value> {
        let path = std::fs::canonicalize(path)?;
        if !path.starts_with(std::path::Path::new(&self.root).join("acquisition")) {
            return Err(Error::Invalid(
                "snapshot is outside owned acquisition custody",
            ));
        }
        let hash_path = path.clone();
        let (hash, length) = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut file = std::fs::File::open(hash_path)?;
            let length = i64::try_from(file.metadata()?.len())
                .map_err(|_| Error::Invalid("snapshot length overflow"))?;
            let mut h = blake3::Hasher::new();
            h.update_reader(&mut file)?;
            Ok((hashed_blobs::Hash(*h.finalize().as_bytes()), length))
        })
        .await??;
        let name = hash.to_string();
        let blob = self.register_blob(&name, length).await?;
        let store = self.blobs.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let actual = store.put(std::fs::File::open(&path)?, 3)?;
            if actual != hash || store.verify(&hash)? != hashed_blobs::Verified::Intact {
                return Err(Error::Invalid("physical snapshot failed verification"));
            }
            Ok(())
        })
        .await??;
        let mut tx = self.connection.begin().await?;
        let mut provenance: Value = sqlx::query_scalar(
            "SELECT source_provenance FROM raw.observations WHERE id=$1 AND input_id=$2",
        )
        .bind(observation)
        .bind(input)
        .fetch_one(&mut *tx)
        .await?;
        provenance["representation"] = json!("sqlite_database_snapshot");
        sqlx::query(BLOB_PRESENT)
            .bind(blob)
            .execute(&mut *tx)
            .await?;
        let material = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO raw.source_material_registry
               (id,material_kind,source_identifier,status,ts_quality,blob_id,total_bytes,
                durable_bytes,input_id,observation_id,parser_provenance)
             VALUES($1,'file',$2,'completed','unknown',$3,$4,$4,$5,$6,$7)",
        )
        .bind(material)
        .bind(SOURCE)
        .bind(blob)
        .bind(length)
        .bind(input)
        .bind(observation)
        .bind(provenance)
        .execute(&mut *tx)
        .await?;
        let seq: i64 = next_commit_seq(&mut tx).await?;
        let snapshot = json!({
            "material": material,
            "bytes": length,
            "blake3": name,
            "representation": "sqlite_database_snapshot",
            "storage": "full base; no page patches",
            "captured_by_observation": observation,
        });
        sqlx::query(
            "UPDATE raw.observations
             SET commit_seq=$3,metadata=jsonb_set(metadata,'{physical_snapshot}',$2)
             WHERE id=$1",
        )
        .bind(observation)
        .bind(&snapshot)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(snapshot)
    }
    pub(crate) async fn browser_step(&mut self, step: BrowserStep) -> Result<Value> {
        if step.rows.len() > crate::MAX_STEP_ROWS
            || step.unavailable.is_some() && !step.rows.is_empty()
            || step.complete && step.unavailable.is_some()
        {
            return Err(Error::Invalid("invalid browser bounded step"));
        }
        let row = sqlx::query(
            "SELECT source_provenance,observed_at::text,metadata FROM raw.observations
             WHERE id=$1 AND input_id=$2 AND status='reading'",
        )
        .bind(step.observation)
        .bind(step.input)
        .fetch_one(&mut self.connection)
        .await?;
        let provenance: Value = row.try_get("source_provenance")?;
        if provenance["source"] != SOURCE {
            return Err(Error::Invalid("browser observation source differs"));
        }
        let profile = provenance["profile"]
            .as_str()
            .ok_or(Error::Invalid("browser profile unavailable"))?;
        let generation: Uuid = serde_json::from_value(provenance["database_generation"].clone())
            .map_err(|_| Error::Invalid("browser generation unavailable"))?;
        let rules: Vec<crate::PrivacyRule> =
            serde_json::from_value(provenance["privacy_rules"].clone())
                .map_err(|_| Error::Invalid("browser privacy provenance unavailable"))?;
        let read_order: i64 = sqlx::query_scalar(
            "SELECT (extract(epoch FROM observed_at)*1000000000)::bigint
             FROM raw.observations WHERE id=$1",
        )
        .bind(step.observation)
        .fetch_one(&mut self.connection)
        .await?;
        let mut bytes = Vec::new();
        let mut prepared = Vec::new();
        for mut original in step.rows {
            let result = sinex_model::browser::parse(&original, profile, generation);
            let hash = row_hash(&original)?;
            let anchor = bytes.len() as i64;
            let coordinate = original.get("coordinate").cloned().unwrap_or(Value::Null);
            let table = original["table"].clone();
            original["_from"] =
                json!({"observation": step.observation, "table": table, "rowid": coordinate});
            if let Some(snapshot) = step
                .metadata
                .get("physical_snapshot")
                .and_then(|s| s.get("material"))
            {
                original["_from"]["snapshot"] = snapshot.clone();
            }
            original.sort_all_objects();
            serde_json::to_writer(&mut bytes, &original)
                .map_err(|_| Error::Invalid("browser envelope encoding"))?;
            bytes.push(b'\n');
            prepared.push((anchor, bytes.len() as i64, hash, result));
        }
        if bytes.len() > crate::MAX_STEP_BYTES {
            return Err(Error::Invalid("browser step exceeds the 1 MiB step limit"));
        }
        let (material, start) = self
            .store_stream_step(SOURCE, step.input, step.observation, &provenance, &bytes)
            .await?;
        let stored = json!({
            "input": step.input,
            "observation": step.observation,
            "material": material,
            "start": start,
            "end": start + bytes.len() as i64,
            "canonical_bytes": bytes.len(),
            "step_rows": prepared.len(),
            "source_eof": step.complete,
        });
        if !bytes.is_empty() {
            crate::TestPause::reach(&mut self.pause, "retained-fsynced", stored.clone()).await?;
        }
        let mut tx = self.connection.begin().await?;
        let mut seq: i64 = next_commit_seq(&mut tx).await?;
        let producer =
            producer_version(&mut tx, PARSER_NAME, PARSER, self.run_id, &provenance).await?;
        if let Some(material) = material {
            set_part_length(&mut tx, material, start + bytes.len() as i64).await?;
        }
        let (mut admitted, mut repeated, mut failures) = (0i64, 0i64, 0i64);
        for (anchor, end, hash, result) in prepared {
            let material = material.ok_or(Error::Invalid("browser material absent"))?;
            let anchor = start + anchor;
            let parsed = match result {
                Ok(v) => v,
                Err(reason) => {
                    record_failure(&mut tx, step.observation, material, anchor, reason).await?;
                    failures += 1;
                    continue;
                }
            };
            let (key, keyhash) = occurrence(profile, generation, parsed.kind, &parsed.key);
            let old = sqlx::query(
                "SELECT l.event_id,l.read_order,l.record_hash,p.version
                 FROM core.live_equivalence l
                 JOIN core.events e ON e.id=l.event_id
                 LEFT JOIN core.producer_versions p ON p.id=e.producer_version_id
                 WHERE l.store=0 AND l.key_hash=$1",
            )
            .bind(&keyhash)
            .fetch_optional(&mut *tx)
            .await?;
            // Keep the live event when it came from a newer reading, or when this
            // row and parser are unchanged.
            if let Some(old) = &old
                && (old.try_get::<i64, _>("read_order")? >= read_order
                    || old.try_get::<Vec<u8>, _>("record_hash")? == hash
                        && old.try_get::<Option<String>, _>("version")?.as_deref() == Some(PARSER))
            {
                sqlx::query(
                    "UPDATE core.live_equivalence SET read_order=greatest(read_order,$2)
                     WHERE store=0 AND key_hash=$1",
                )
                .bind(&keyhash)
                .bind(read_order)
                .execute(&mut *tx)
                .await?;
                repeated += 1;
                continue;
            }
            let id = Uuid::now_v7();
            let (event_type, schema): (i32, i32) = if parsed.kind == "page.visited" {
                (6, 7)
            } else {
                (7, 8)
            };
            if let Some(old) = old {
                let old: Uuid = old.try_get("event_id")?;
                sqlx::query(
                    "INSERT INTO audit.archived(id,archived_seq,reason,replaced_by)
                     VALUES($1,$2,'supersede',$3)",
                )
                .bind(old)
                .bind(seq)
                .bind(id)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id)
                     VALUES($1,$2,1,0,$3)",
                )
                .bind(seq)
                .bind(old)
                .bind(event_type)
                .execute(&mut *tx)
                .await?;
                sqlx::query("DELETE FROM core.live_coordinates WHERE store=0 AND event_id=$1")
                    .bind(old)
                    .execute(&mut *tx)
                    .await?;
                seq = next_commit_seq(&mut tx).await?;
            }
            let at = parsed
                .unix_us
                .map(|v| {
                    OffsetDateTime::from_unix_timestamp_nanos(i128::from(v) * 1000)
                        .map_err(|_| Error::Invalid("browser event clock"))
                })
                .transpose()?;
            let time = at
                .map(|at| {
                    at.format(&Rfc3339)
                        .map_err(|_| Error::Invalid("browser clock encoding"))
                })
                .transpose()?;
            let evidence = match at {
                Some(at) => sinex_model::events::TsOrig::from_record_field(
                    at,
                    if parsed.kind == "page.visited" {
                        "visit_time"
                    } else {
                        "start_time"
                    },
                    sinex_model::events::TimePrecision::Instant,
                )
                .map_err(|_| Error::Invalid("browser time evidence"))?,
                None => sinex_model::events::TsOrig::unknown(),
            };
            let projected = evidence
                .project(
                    sinex_model::events::EventId::try_from(id)
                        .map_err(|_| Error::Invalid("browser event ID"))?,
                    sinex_model::events::CaptureClock::Producer,
                )
                .map_err(|_| Error::Invalid("browser time projection"))?;
            let mut payload = parsed.payload;
            crate::atuin::mask(&mut payload, &rules)?;
            sqlx::query(
                "INSERT INTO core.events(id,source,event_type,payload,ts_orig,ts_orig_tiebreak,
                   ts_quality,ts_precision,ts_clock,commit_seq,source_material_id,anchor_byte,
                   offset_start,offset_end,offset_kind,ordinal,record_hash,record_state,run_id,
                   equivalence_key,producer_version_id,payload_schema_id,time_suspect)
                 VALUES($1,$2,$3,$4,$5::text::timestamptz,0,
                   $6,'instant','producer',$7,$8,$9,
                   $9,$10,'bytes',0,$11,'present',$12,
                   $13,$14,$15,$16)",
            )
            .bind(id)
            .bind(SOURCE)
            .bind(parsed.kind)
            .bind(payload)
            .bind(time.as_deref())
            .bind(if time.is_some() {
                "intrinsic"
            } else {
                "unknown"
            })
            .bind(seq)
            .bind(material)
            .bind(anchor)
            .bind(start + end)
            .bind(&hash)
            .bind(self.run_id)
            .bind(&key)
            .bind(producer)
            .bind(schema)
            .bind(projected.suspect)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO core.live_coordinates(store,occ_hash,event_id) VALUES(0,$1,$2)",
            )
            .bind(coordinate_hash(material, anchor, parsed.kind, 0))
            .bind(id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO core.live_equivalence(store,key_hash,event_id,read_order,record_hash)
                 VALUES(0,$1,$2,$3,$4)
                 ON CONFLICT(store,key_hash) DO UPDATE SET event_id=EXCLUDED.event_id,
                   read_order=EXCLUDED.read_order,record_hash=EXCLUDED.record_hash",
            )
            .bind(&keyhash)
            .bind(id)
            .bind(read_order)
            .bind(hash)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id)
                 VALUES($1,$2,0,0,$3)",
            )
            .bind(seq)
            .bind(id)
            .bind(event_type)
            .execute(&mut *tx)
            .await?;
            seq = next_commit_seq(&mut tx).await?;
            admitted += 1;
        }
        let status = observation_status(step.unavailable.is_some(), step.complete);
        let summary: Value = sqlx::query_scalar(
            "UPDATE raw.observations SET rows_read=rows_read+$2+$3+$4,admitted=admitted+$2,
               repeated=repeated+$3,record_errors=record_errors+$4,status=$5,reason=$6,
               metadata=metadata||$7,commit_seq=$8
             WHERE id=$1
             RETURNING jsonb_build_object('observation',id,'status',status,
               'rows_read',rows_read,'admitted',admitted,'repeated',repeated,
               'record_errors',record_errors,'commit_seq',commit_seq)",
        )
        .bind(step.observation)
        .bind(admitted)
        .bind(repeated)
        .bind(failures)
        .bind(status)
        .bind(step.unavailable)
        .bind(step.metadata)
        .bind(seq)
        .fetch_one(&mut *tx)
        .await?;
        let prior: Option<Value> = sqlx::query_scalar(
            "SELECT position FROM raw.source_cursors WHERE binding=$1 AND input=$2",
        )
        .bind(SOURCE)
        .bind(step.input.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        let mut position = summary.clone();
        if step.fence || prior.is_some_and(|v| v["reconciliation_required"] == true) {
            position["reconciliation_required"] = json!(true);
        }
        sqlx::query(
            "INSERT INTO raw.source_cursors(binding,input,position,carry) VALUES($1,$2,$3,'{}')
             ON CONFLICT(binding,input) DO UPDATE SET position=EXCLUDED.position,updated_at=now()",
        )
        .bind(SOURCE)
        .bind(step.input.to_string())
        .bind(&position)
        .execute(&mut *tx)
        .await?;
        if step.complete && summary["record_errors"].as_i64() == Some(0) {
            if position["reconciliation_required"] == true {
                return Err(Error::Invalid("browser generation is fenced"));
            }
            let checkpoint = step
                .checkpoint
                .ok_or(Error::Invalid("completed browser checkpoint absent"))?;
            if checkpoint.version != 1 || checkpoint.generation != generation {
                return Err(Error::Invalid("browser checkpoint generation differs"));
            }
            let checkpoint = serde_json::to_value(checkpoint)
                .map_err(|_| Error::Invalid("browser checkpoint encoding"))?;
            sqlx::query("UPDATE raw.source_cursors SET carry=$3 WHERE binding=$1 AND input=$2")
                .bind(SOURCE)
                .bind(step.input.to_string())
                .bind(&checkpoint)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "UPDATE raw.observations
                 SET metadata=jsonb_set(metadata,'{browser_checkpoint_v1}',$2) WHERE id=$1",
            )
            .bind(step.observation)
            .bind(checkpoint)
            .execute(&mut *tx)
            .await?;
        }
        let committed = json!({"custody": stored, "progress": summary});
        if !bytes.is_empty() {
            crate::TestPause::reach(&mut self.pause, "transaction-staged", committed.clone())
                .await?;
        }
        tx.commit().await?;
        if !bytes.is_empty() {
            crate::TestPause::reach(&mut self.pause, "transaction-committed", committed).await?;
        }
        if step.complete || status == "unavailable" {
            self.close_open_part(step.input).await?;
        }
        Ok(summary)
    }
}
