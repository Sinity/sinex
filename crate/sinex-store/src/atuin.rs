//! The registered Atuin admission variant. The writer owns custody, classification and progress.
use crate::{
    BLOB_LOOKUP, BLOB_PRESENT, BLOB_REGISTER, COUNTER, Error, ReadPool, Result, WriterPool,
    coordinate_hash,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Connection, Row};
use std::io::{Seek, SeekFrom, Write};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub const SOURCE: &str = "terminal.atuin-history";
pub(crate) const PARSER: &str = "atuin-v2";
const PARSER_NAME: &str = "parser.terminal.atuin-history";
pub const MAX_STEP_ROWS: usize = 64;
pub const MAX_STEP_BYTES: usize = 1024 * 1024;
const PART_BYTES: u64 = 100 * 1024 * 1024;

pub(crate) fn payload_schema() -> Value {
    json!({"type":"object","additionalProperties":false,
        "required":["native_id","command","cwd","session","hostname","exit_code","duration_ns","timestamp_ns","completion"],
        "properties":{"native_id":{"type":"string"},"command":{"type":"string"},"cwd":{"type":"string"},"session":{"type":"string"},"hostname":{"type":"string"},
        "exit_code":{"type":["integer","null"],"minimum":0},"duration_ns":{"type":["integer","null"],"minimum":0},"timestamp_ns":{"type":"integer"},"completion":{"enum":["running","end_not_recorded","ended"]}}})
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivacyRule {
    pub pattern: String,
    pub replacement: String,
    pub created_by: String,
}
impl PrivacyRule {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.created_by != "operator"
            || self.pattern.len() > 4096
            || self.replacement.len() > 4096
        {
            return Err("privacy rules require operator attribution and bounded text".into());
        }
        regex::Regex::new(&self.pattern).map_err(|_| "invalid privacy regex".to_string())?;
        Ok(())
    }
}
pub fn mask(value: &mut Value, rules: &[PrivacyRule]) -> Result<()> {
    for rule in rules {
        let regex =
            regex::Regex::new(&rule.pattern).map_err(|_| Error::Invalid("invalid privacy rule"))?;
        fn apply(v: &mut Value, regex: &regex::Regex, replacement: &str) -> Result<()> {
            match v {
                Value::String(s) => {
                    let mut out = String::new();
                    let mut through = 0;
                    for found in regex.find_iter(s) {
                        if out.len() + found.start() - through + replacement.len() > MAX_STEP_BYTES
                        {
                            return Err(Error::Invalid(
                                "masked text exceeds bounded presentation budget",
                            ));
                        }
                        out.push_str(&s[through..found.start()]);
                        out.push_str(replacement);
                        through = found.end();
                    }
                    if out.len() + s.len() - through > MAX_STEP_BYTES {
                        return Err(Error::Invalid(
                            "masked text exceeds bounded presentation budget",
                        ));
                    }
                    out.push_str(&s[through..]);
                    *s = out;
                }
                Value::Array(a) => {
                    for v in a {
                        apply(v, regex, replacement)?;
                    }
                }
                Value::Object(o) => {
                    for v in o.values_mut() {
                        apply(v, regex, replacement)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        apply(value, &regex, &rule.replacement)?;
    }
    Ok(())
}

/// One bounded read from the source's consistent SQLite transaction. Fields name acquisition
/// evidence, never synthetic event IDs. Admission derives the event type and equivalence key.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AtuinInterpretation {
    Current,
    HistoricalFixture,
    #[cfg(test)]
    PriorCompletionFixture,
}
#[derive(Clone)]
pub struct AtuinStep {
    pub interpretation: AtuinInterpretation,
    pub observation: Uuid,
    pub observed_at: String,
    pub locator: String,
    pub metadata: Value,
    pub rows: Vec<Value>,
    pub complete: bool,
    pub unavailable: Option<String>,
    pub privacy_rules: Vec<PrivacyRule>,
}
#[derive(Deserialize)]
struct AtuinRow {
    id: String,
    timestamp: i64,
    duration: i64,
    exit: i64,
    command: String,
    cwd: String,
    session: String,
    hostname: String,
    deleted_at: Option<i64>,
}
pub(crate) struct Parsed {
    pub(crate) payload: Value,
    pub(crate) native_id: String,
    pub(crate) time: String,
    pub(crate) removed: bool,
    pub(crate) nanos: i32,
    pub(crate) time_evidence: sinex_model::events::TsOrig,
}
pub(crate) fn occurrence(input: Uuid, native_id: &str) -> (String, Vec<u8>) {
    let key = json!([input, native_id]).to_string();
    let hash = blake3::hash(format!("{SOURCE}\x1fcommand.executed\x1f{key}").as_bytes()).as_bytes()
        [..16]
        .to_vec();
    (key, hash)
}
pub(crate) fn parse(row: &Value) -> std::result::Result<Parsed, &'static str> {
    let r: AtuinRow = serde_json::from_value(row.clone())
        .map_err(|_| "invalid Atuin record field type or missing field")?;
    if r.id.is_empty() || r.id.len() > 512 || r.exit < -1 || r.duration < -1 {
        return Err("invalid Atuin native key, exit or duration");
    }
    let at = OffsetDateTime::from_unix_timestamp_nanos(i128::from(r.timestamp))
        .map_err(|_| "Atuin timestamp out of range")?;
    // The existing time model owns quality and precision; admission supplies actual evidence.
    let time_evidence = sinex_model::events::TsOrig::from_record_field(
        at,
        "timestamp",
        sinex_model::events::TimePrecision::Instant,
    )
    .map_err(|_| "invalid Atuin time evidence")?;
    let ended = r.duration >= 0 || r.exit >= 0;
    let state = if ended {
        "ended"
    } else {
        // This reading has no end. It proves neither running nor immutable terminal state.
        "end_not_recorded"
    };
    Ok(Parsed {
        payload: json!({"native_id":r.id,"command":r.command,"cwd":r.cwd,"session":r.session,
            "hostname":r.hostname.split(':').next().unwrap_or(&r.hostname),
            "exit_code":if r.exit >= 0 { Some(r.exit) } else { None },
            "timestamp_ns":r.timestamp,"duration_ns":if ended && r.duration >= 0 { Some(r.duration) } else { None }, "completion":state}),
        native_id: r.id,
        time: at
            .replace_nanosecond(at.nanosecond() / 1000 * 1000)
            .map_err(|_| "invalid timestamp precision")?
            .format(&Rfc3339)
            .map_err(|_| "Atuin time cannot format")?,
        removed: r.deleted_at.is_some(),
        nanos: (at.nanosecond() % 1000) as i32,
        time_evidence,
    })
}

pub(crate) fn shell_field_basis(rules: &[PrivacyRule]) -> Value {
    json!({"contract":"sinex.shell-field-basis.v1","comparable":rules.is_empty(),"privacy_rules_blake3":format!("blake3:{}",blake3::hash(&serde_json::to_vec(rules).expect("typed privacy rules")))})
}

pub(crate) async fn parser_producer(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run: Uuid,
    parser: &str,
    rules: &[PrivacyRule],
) -> Result<Uuid> {
    let configuration = json!({"representation":"canonical_sqlite_rows","parser":parser,"qualification_fixture":parser!=PARSER,"shell_field_basis":shell_field_basis(rules)});
    if let Some(id) = sqlx::query_scalar(
        "SELECT id FROM core.producer_versions WHERE name=$1 AND version=$2 AND run_id=$3 AND configuration=$4 LIMIT 1",
    )
    .bind(PARSER_NAME)
    .bind(parser)
    .bind(run)
    .bind(&configuration)
    .fetch_optional(&mut **tx)
    .await?
    {
        return Ok(id);
    }
    let id = Uuid::now_v7();
    sqlx::query("INSERT INTO core.producer_versions(id,name,version,run_id,configuration) VALUES($1,$2,$3,$4,$5)")
        .bind(id).bind(PARSER_NAME).bind(parser).bind(run)
        .bind(configuration)
        .execute(&mut **tx).await?;
    Ok(id)
}

impl WriterPool {
    pub(crate) async fn recover_parts(&mut self) -> Result<()> {
        let rows = sqlx::query(
            "SELECT id,durable_bytes FROM raw.source_material_registry WHERE status='capturing'",
        )
        .fetch_all(&mut self.connection)
        .await?;
        for row in rows {
            let material: Uuid = row.try_get("id")?;
            let length: i64 = row.try_get("durable_bytes")?;
            let path = std::path::Path::new(&self.root)
                .join("open")
                .join(material.to_string());
            let file = std::fs::OpenOptions::new().write(true).open(path)?;
            if file.metadata()?.len() < length as u64 {
                return Err(Error::Invalid(
                    "open material shorter than committed custody",
                ));
            }
            file.set_len(length as u64)?;
            file.sync_all()?;
            self.close_part(material, "recovered_partial").await?;
        }
        sqlx::query("UPDATE raw.observations SET status='interrupted',reason='writer restarted before completed read' WHERE status='reading'").execute(&mut self.connection).await?;
        Ok(())
    }

    pub(crate) async fn close_part(&mut self, material: Uuid, status: &str) -> Result<()> {
        let path = std::path::Path::new(&self.root)
            .join("open")
            .join(material.to_string());
        let hash_path = path.clone();
        let (hash, length) = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut file = std::fs::File::open(hash_path)?;
            let length = file.metadata()?.len() as i64;
            let mut hash = blake3::Hasher::new();
            hash.update_reader(&mut file)?;
            Ok((hashed_blobs::Hash(*hash.finalize().as_bytes()), length))
        })
        .await??;
        let name = hash.to_string();
        sqlx::query(BLOB_REGISTER)
            .bind(Uuid::now_v7())
            .bind(&name)
            .bind(length)
            .bind(&self.root)
            .execute(&mut self.connection)
            .await?;
        let row = sqlx::query(BLOB_LOOKUP)
            .bind(&name)
            .fetch_one(&mut self.connection)
            .await?;
        let blob: Uuid = row.try_get("id")?;
        if row.try_get::<i64, _>("size_bytes")? != length
            || row.try_get::<String, _>("root")? != self.root
        {
            return Err(Error::Invalid("blob custody identity differs"));
        }
        let blobs = self.blobs.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            blobs.put(std::fs::File::open(&path)?, 3)?;
            if blobs.verify(&hash)? != hashed_blobs::Verified::Intact {
                return Err(Error::Invalid("closed material failed verification"));
            }
            Ok(())
        })
        .await??;
        let mut tx = self.connection.begin().await?;
        sqlx::query(BLOB_PRESENT)
            .bind(blob)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE raw.source_material_registry SET blob_id=$2,status=$3,total_bytes=$4,durable_bytes=$4 WHERE id=$1 AND status='capturing'").bind(material).bind(blob).bind(status).bind(length).execute(&mut *tx).await?;
        tx.commit().await?;
        // Retaining a closed open-file copy is harmless; cleanup follows only durable closure.
        let path = std::path::Path::new(&self.root)
            .join("open")
            .join(material.to_string());
        std::fs::remove_file(path)?;
        Ok(())
    }

    /// Shared bounded exact-byte custody for source adapters. The caller commits
    /// durable_bytes with admission/progress after this fsync, through this writer.
    pub(crate) async fn retain_stream_step(
        &mut self,
        source: &str,
        input: Uuid,
        observation: Uuid,
        provenance: &Value,
        bytes: &[u8],
    ) -> Result<(Option<Uuid>, i64)> {
        let mut material = None;
        let mut start = 0i64;
        if !bytes.is_empty() {
            let open = sqlx::query("SELECT id,durable_bytes FROM raw.source_material_registry WHERE input_id=$1 AND status='capturing' ORDER BY staged_at DESC LIMIT 1").bind(input).fetch_optional(&mut self.connection).await?;
            if let Some(row) = open {
                material = Some(row.try_get::<Uuid, _>("id")?);
                start = row.try_get("durable_bytes")?;
            }
            if start as u64 + bytes.len() as u64 > PART_BYTES {
                self.close_part(
                    material.ok_or(Error::Invalid("open material missing"))?,
                    "completed",
                )
                .await?;
                material = None;
                start = 0;
            }
            if material.is_none() {
                let id = Uuid::now_v7();
                std::fs::create_dir_all(std::path::Path::new(&self.root).join("open"))?;
                let path = std::path::Path::new(&self.root)
                    .join("open")
                    .join(id.to_string());
                let file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(path)?;
                file.sync_all()?;
                std::fs::File::open(std::path::Path::new(&self.root).join("open"))?.sync_all()?;
                sqlx::query("INSERT INTO raw.source_material_registry (id,material_kind,source_identifier,status,ts_quality,blob_id,total_bytes,durable_bytes,input_id,observation_id,parser_provenance) VALUES ($1,'stream_chunk',$2,'capturing','unknown',NULL,0,0,$3,$4,$5)")
                    .bind(id).bind(source).bind(input).bind(observation).bind(&provenance).execute(&mut self.connection).await?;
                material = Some(id);
            }
            let path = std::path::Path::new(&self.root)
                .join("open")
                .join(material.unwrap().to_string());
            let write_bytes = bytes.to_vec();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
                file.set_len(start as u64)?;
                file.seek(SeekFrom::Start(start as u64))?;
                file.write_all(&write_bytes)?;
                file.sync_all()?;
                Ok(())
            })
            .await??;
        }
        Ok((material, start))
    }

    pub(crate) async fn admit_atuin(&mut self, step: AtuinStep) -> Result<Value> {
        if step.rows.len() > MAX_STEP_ROWS
            || step.locator.is_empty()
            || step.unavailable.is_some() && !step.rows.is_empty()
        {
            return Err(Error::Invalid("invalid bounded Atuin step"));
        }
        let observed = OffsetDateTime::parse(&step.observed_at, &Rfc3339)
            .map_err(|_| Error::Invalid("invalid observation clock"))?;
        let read_order = i64::try_from(observed.unix_timestamp_nanos())
            .map_err(|_| Error::Invalid("observation clock out of range"))?;
        for rule in &step.privacy_rules {
            rule.validate()
                .map_err(|_| Error::Invalid("invalid attributed privacy rule"))?;
        }
        let new_input: Option<Uuid> = sqlx::query_scalar("INSERT INTO raw.inputs (id,binding,locator,kind) VALUES ($1,$2,$3,'sqlite') ON CONFLICT (binding,locator) DO NOTHING RETURNING id")
            .bind(Uuid::now_v7()).bind(SOURCE).bind(&step.locator).fetch_optional(&mut self.connection).await?;
        let input = match new_input {
            Some(id) => id,
            None => {
                sqlx::query_scalar("SELECT id FROM raw.inputs WHERE binding=$1 AND locator=$2")
                    .bind(SOURCE)
                    .bind(&step.locator)
                    .fetch_one(&mut self.connection)
                    .await?
            }
        };
        let parser = PARSER;
        #[cfg(test)]
        let parser = if step.interpretation == AtuinInterpretation::PriorCompletionFixture {
            "atuin-v1"
        } else {
            parser
        };
        let provenance = json!({"source":SOURCE,"event_type":"command.executed","equivalence":"registered_database + native_id","parser":parser,"representation":"canonical_sqlite_rows","privacy_class":"plain","privacy_rules":step.privacy_rules,"sqlite_snapshot":"unavailable_until_1c"});
        sqlx::query("INSERT INTO raw.observations (id,input_id,seq,observed_at,source_provenance,metadata,status) SELECT $1,$2,COALESCE(MAX(seq),0)+1,$3::text::timestamptz,$4,$5,'reading' FROM raw.observations WHERE input_id=$2 ON CONFLICT (id) DO NOTHING")
            .bind(step.observation).bind(input).bind(&step.observed_at).bind(&provenance).bind(&step.metadata).execute(&mut self.connection).await?;
        let mut bytes = Vec::new();
        let mut prepared = Vec::new();
        for row in step.rows {
            let anchor = bytes.len() as i64;
            let mut original = row
                .as_object()
                .cloned()
                .ok_or(Error::Invalid("row acquisition must be an object"))?;
            let rowid = original.remove("_rowid").unwrap_or(Value::Null);
            let mut fingerprint_row = Value::Object(original.clone());
            fingerprint_row.sort_all_objects();
            let record_hash = blake3::hash(
                &serde_json::to_vec(&fingerprint_row)
                    .map_err(|_| Error::Invalid("row encoding failed"))?,
            )
            .as_bytes()[..16]
                .to_vec();
            let result = parse(&Value::Object(original.clone()));
            #[cfg(test)]
            let result = if step.interpretation == AtuinInterpretation::PriorCompletionFixture {
                result.map(|mut parsed| {
                    // Controlled prior-recipe reading, not an actual predecessor binary.
                    parsed.payload["completion"] = json!("running");
                    parsed.payload["duration_ns"] = Value::Null;
                    parsed
                })
            } else {
                result
            };
            original.insert(
                "_from".into(),
                json!({"observation":step.observation,"table":"history","rowid":rowid}),
            );
            let mut envelope = Value::Object(original);
            envelope.sort_all_objects();
            serde_json::to_writer(&mut bytes, &envelope)
                .map_err(|_| Error::Invalid("row encoding failed"))?;
            bytes.push(b'\n');
            prepared.push((anchor, bytes.len() as i64, record_hash, result));
        }
        if bytes.len() > MAX_STEP_BYTES {
            return Err(Error::Invalid("Atuin step exceeds retained byte budget"));
        }
        let (material, start) = self
            .retain_stream_step(SOURCE, input, step.observation, &provenance, &bytes)
            .await?;
        let boundary_evidence = json!({"input":input,"observation":step.observation,"material":material,"start":start,"end":start+bytes.len() as i64,"canonical_bytes":bytes.len(),"canonical_blake3":blake3::hash(&bytes).to_hex().to_string(),"step_rows":prepared.len(),"source_eof":step.complete,"acknowledged":false});
        if !bytes.is_empty() {
            crate::QualificationBoundary::reach(
                &mut self.qualification,
                "retained-fsynced",
                boundary_evidence.clone(),
            )
            .await?;
        }
        let mut tx = self.connection.begin().await?;
        let mut last_seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
        let producer = parser_producer(&mut tx, self.run_id, parser, &step.privacy_rules).await?;
        if let Some(id) = material {
            sqlx::query("UPDATE raw.source_material_registry SET total_bytes=$2,durable_bytes=$2 WHERE id=$1").bind(id).bind(start+bytes.len() as i64).execute(&mut *tx).await?;
        }
        let (mut admitted, mut repeated, mut removed, mut failures, mut excluded) =
            (0i64, 0i64, 0i64, 0i64, 0i64);
        for (anchor, end, record_hash, result) in prepared {
            let material = material.ok_or(Error::Invalid("row material missing"))?;
            let anchor = start + anchor;
            let parsed = match result {
                Ok(row) => row,
                Err(reason) => {
                    sqlx::query("INSERT INTO raw.record_failures (observation_id,material_id,anchor_byte,reason) VALUES ($1,$2,$3,$4)").bind(step.observation).bind(material).bind(anchor).bind(reason).execute(&mut *tx).await?;
                    failures += 1;
                    continue;
                }
            };
            if step.interpretation == AtuinInterpretation::HistoricalFixture
                && parsed.native_id == "fixture-129"
            {
                sqlx::query("INSERT INTO raw.record_failures (observation_id,material_id,anchor_byte,reason) VALUES ($1,$2,$3,'controlled historical parser omission; current parser accepts this retained row')").bind(step.observation).bind(material).bind(anchor).execute(&mut *tx).await?;
                failures += 1;
                continue;
            }
            let (key, key_hash) = occurrence(input, &parsed.native_id);
            if sqlx::query_scalar::<_, bool>(crate::exclusion::EXCLUDED)
                .bind(&key)
                .fetch_one(&mut *tx)
                .await?
            {
                excluded += 1;
                continue;
            }
            let old = sqlx::query("SELECT l.event_id,l.read_order,l.record_hash,p.version AS parser FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id LEFT JOIN core.producer_versions p ON p.id=e.producer_version_id AND p.name='parser.terminal.atuin-history' WHERE l.store=0 AND l.key_hash=$1").bind(&key_hash).fetch_optional(&mut *tx).await?;
            if let Some(ref old) = old {
                if (old.try_get::<Vec<u8>, _>("record_hash")? == record_hash
                    && old.try_get::<Option<String>, _>("parser")?.as_deref() == Some(parser))
                    || old.try_get::<i64, _>("read_order")? >= read_order
                {
                    sqlx::query("UPDATE core.live_equivalence SET read_order=GREATEST(read_order,$2) WHERE store=0 AND key_hash=$1").bind(&key_hash).bind(read_order).execute(&mut *tx).await?;
                    repeated += 1;
                    continue;
                }
            }
            let id = Uuid::now_v7();
            if let Some(old) = old {
                let old_id: Uuid = old.try_get("event_id")?;
                crate::shell::dirty(&mut tx, old_id).await?;
                sqlx::query("INSERT INTO audit.archived (id,archived_seq,reason,replaced_by) VALUES ($1,$2,'supersede',$3)").bind(old_id).bind(last_seq).bind(id).execute(&mut *tx).await?;
                sqlx::query("INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id) VALUES ($1,$2,1,0,2)").bind(last_seq).bind(old_id).execute(&mut *tx).await?;
                sqlx::query("DELETE FROM core.live_coordinates WHERE store=0 AND event_id=$1")
                    .bind(old_id)
                    .execute(&mut *tx)
                    .await?;
                last_seq = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
            }
            let projected = parsed
                .time_evidence
                .project(
                    sinex_model::events::EventId::try_from(id)
                        .map_err(|_| Error::Invalid("invalid minted event ID"))?,
                    sinex_model::events::CaptureClock::Producer,
                )
                .map_err(|_| Error::Invalid("invalid projected source time"))?;
            let mut payload = parsed.payload;
            let fixture = step.interpretation == AtuinInterpretation::HistoricalFixture
                && parsed.native_id == "fixture-000";
            if fixture {
                payload["hostname"] = json!("controlled-historical-bad-interpretation");
            }
            let fixture_write = fixture;
            #[cfg(test)]
            let fixture_write =
                fixture_write || step.interpretation == AtuinInterpretation::PriorCompletionFixture;
            mask(&mut payload, &step.privacy_rules)?;
            sqlx::query("INSERT INTO core.events (id,source,event_type,payload,ts_orig,ts_orig_tiebreak,ts_quality,ts_clock,time_suspect,payload_schema_id,commit_seq,source_material_id,anchor_byte,offset_start,offset_end,offset_kind,ordinal,record_hash,record_state,run_id,equivalence_key,written_by,producer_version_id) VALUES ($1,$2,'command.executed',$3,$4::text::timestamptz,$13,'intrinsic','producer',$14,2,$5,$6,$7,$7,$8,'bytes',0,$9,$10,$11,$12,$15,$16)")
                .bind(id).bind(SOURCE).bind(payload).bind(parsed.time).bind(last_seq).bind(material).bind(anchor).bind(start+end).bind(&record_hash).bind(if parsed.removed {"removed"} else {"present"}).bind(self.run_id).bind(key).bind(parsed.nanos).bind(projected.suspect).bind(if fixture_write {json!({"kind":"qualification_fixture","description":"controlled historical bad interpretation; not a predecessor binary"})}else{Value::Null}).bind(producer).execute(&mut *tx).await?;
            sqlx::query(
                "INSERT INTO core.live_coordinates (store,occ_hash,event_id) VALUES (0,$1,$2)",
            )
            .bind(coordinate_hash(material, anchor, "command.executed", 0))
            .bind(id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("INSERT INTO core.live_equivalence (store,key_hash,event_id,read_order,record_hash) VALUES (0,$1,$2,$3,$4) ON CONFLICT (store,key_hash) DO UPDATE SET event_id=EXCLUDED.event_id,read_order=EXCLUDED.read_order,record_hash=EXCLUDED.record_hash")
                .bind(key_hash).bind(id).bind(read_order).bind(record_hash).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id) VALUES ($1,$2,0,0,2)").bind(last_seq).bind(id).execute(&mut *tx).await?;
            crate::shell::dirty(&mut tx, id).await?;
            last_seq = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
            admitted += 1;
            removed += i64::from(parsed.removed);
        }
        let status = if step.unavailable.is_some() {
            "unavailable"
        } else if step.complete {
            "completed"
        } else {
            "reading"
        };
        let summary: Value = sqlx::query_scalar("UPDATE raw.observations SET rows_read=rows_read+$2+$3+$4+$9,excluded=excluded+$9,admitted=admitted+$2,repeated=repeated+$3,record_errors=record_errors+$4,removed=removed+$5,status=$6,reason=$7,commit_seq=$8 WHERE id=$1 RETURNING jsonb_build_object('observation',id,'status',status,'rows_read',rows_read,'admitted',admitted,'repeated',repeated,'excluded',excluded,'removed',removed,'record_errors',record_errors,'commit_seq',commit_seq)")
            .bind(step.observation).bind(admitted).bind(repeated).bind(failures).bind(removed).bind(status).bind(step.unavailable).bind(last_seq).bind(excluded).fetch_one(&mut *tx).await?;
        sqlx::query("INSERT INTO raw.source_cursors (binding,input,position,carry) VALUES ($1,$2,$3,'{}'::jsonb) ON CONFLICT (binding,input) DO UPDATE SET position=EXCLUDED.position,updated_at=now()")
            .bind(SOURCE).bind(input.to_string()).bind(&summary).execute(&mut *tx).await?;
        let committed_evidence = json!({"custody":boundary_evidence,"progress":summary});
        // Empty reading registration and EOF/unavailable accounting are not
        // retained data boundaries. Keep qualification on data-bearing steps.
        if !bytes.is_empty() {
            crate::QualificationBoundary::reach(
                &mut self.qualification,
                "transaction-staged",
                committed_evidence.clone(),
            )
            .await?;
        }
        tx.commit().await?;
        if !bytes.is_empty() {
            crate::QualificationBoundary::reach(
                &mut self.qualification,
                "transaction-committed",
                committed_evidence,
            )
            .await?;
        }
        if step.complete {
            let open: Option<Uuid> = sqlx::query_scalar("SELECT id FROM raw.source_material_registry WHERE input_id=$1 AND status='capturing' LIMIT 1").bind(input).fetch_optional(&mut self.connection).await?;
            if let Some(id) = open {
                self.close_part(id, "completed").await?;
            }
        }
        Ok(summary)
    }
}

impl ReadPool {
    pub async fn sources(&self) -> Result<Value> {
        let mut lease = self.read_lease().await?;
        let mut rows: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('source',i.binding,'input',i.id,'locator',i.locator,'representation',CASE WHEN i.binding='terminal.atuin-history' THEN 'canonical_sqlite_rows' ELSE 'original_zsh_history' END,'sqlite_snapshots',CASE WHEN i.binding='terminal.atuin-history' THEN 'unavailable_until_1c' ELSE NULL END,'last_run',to_jsonb(o),'cursor',(SELECT position FROM raw.source_cursors WHERE binding=i.binding AND input=i.id::text),'first_record_error',(SELECT to_jsonb(f) FROM raw.record_failures f JOIN raw.source_material_registry fm ON fm.id=f.material_id WHERE f.observation_id=o.id ORDER BY fm.staged_at,fm.id,f.anchor_byte LIMIT 1)) FROM raw.inputs i LEFT JOIN LATERAL (SELECT * FROM raw.observations WHERE input_id=i.id ORDER BY seq DESC LIMIT 1) o ON true ORDER BY i.binding,i.locator").fetch_all(lease.connection()).await?;
        lease.finish();
        for source in &mut rows {
            if !source["first_record_error"].is_null() {
                let failure = &mut source["first_record_error"];
                let material: Uuid = serde_json::from_value(failure["material_id"].clone())
                    .map_err(|_| Error::Invalid("record failure material unavailable"))?;
                let anchor = failure["anchor_byte"]
                    .as_i64()
                    .ok_or(Error::Invalid("record failure anchor unavailable"))?;
                match self.retained_record(material, anchor, None).await {
                    Ok((record, end)) => {
                        failure["record"] = record;
                        failure["end"] = json!(end);
                        failure["evidence_status"] = json!("available");
                    }
                    Err(_) => {
                        failure["record"] = Value::Null;
                        failure["end"] = Value::Null;
                        failure["evidence_status"] = json!("unavailable");
                    }
                }
                failure["representation"] = json!("canonical_sqlite_row");
            }
        }
        Ok(json!({"notice":sinex_model::DATA_NOTICE,"sources":rows}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires the harness-owned isolated PostgreSQL recipe database"]
    async fn unchanged_source_new_recipe_readmits_and_preserves_old_evidence()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let admin = std::env::var("SINEX_TEST_ADMIN_URL")?;
        let writer = std::env::var("SINEX_TEST_WRITER_URL")?;
        let reader = std::env::var("SINEX_TEST_READER_URL")?;
        let state = std::path::PathBuf::from(std::env::var("SINEX_TEST_STORE_DIR")?);
        crate::bootstrap(&admin).await?;
        let executable = std::fs::read(std::env::current_exe()?)?;
        let store = crate::Store::connect(
            &writer,
            &reader,
            state,
            2,
            crate::RuntimeIdentity {
                service_name: "synthetic-atuin-recipe-proof".into(),
                host: "synthetic-host".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                executable_blake3: format!("blake3:{}", blake3::hash(&executable).to_hex()),
                config_blake3: format!(
                    "blake3:{}",
                    blake3::hash(b"controlled prior recipe fixture").to_hex()
                ),
            },
            None,
        )
        .await?;
        store.admission.enable_shell().await?;
        let source = json!({"_rowid":1,"id":"fixture-completion-v1","timestamp":1_700_000_000_000_000_000i64,"duration":0,"exit":-1,"command":"synthetic recipe proof","cwd":"/synthetic","session":"same","hostname":"host","deleted_at":null});
        let clock = OffsetDateTime::now_utc();
        let step = |interpretation, seconds| AtuinStep {
            interpretation,
            observation: Uuid::now_v7(),
            observed_at: (clock + time::Duration::seconds(seconds))
                .format(&Rfc3339)
                .unwrap(),
            locator: "synthetic-recipe-fixture".into(),
            metadata: json!({"qualification_fixture":"controlled prior parser interpretation; not a predecessor binary"}),
            rows: vec![source.clone()],
            complete: true,
            unavailable: None,
            privacy_rules: vec![],
        };
        let first = store
            .admission
            .submit_atuin(step(AtuinInterpretation::PriorCompletionFixture, 0))
            .await?;
        assert_eq!(first["admitted"], 1);
        let mut conn = sqlx::PgConnection::connect(&reader).await?;
        let snapshot = "SELECT e.id,e.payload,e.record_hash,e.equivalence_key,p.version AS parser FROM core.live_events e JOIN core.producer_versions p ON p.id=e.producer_version_id WHERE e.event_type='command.executed'";
        let old = sqlx::query(snapshot).fetch_one(&mut conn).await?;
        let old_id: Uuid = old.try_get("id")?;
        assert_eq!(old.try_get::<String, _>("parser")?, "atuin-v1");
        assert_eq!(old.try_get::<Value, _>("payload")?["completion"], "running");
        let before = store
            .reader
            .trace(&old_id.to_string(), true)
            .await?
            .unwrap();
        async fn canonical(conn: &mut sqlx::PgConnection, parent: Uuid) -> Result<Uuid> {
            for _ in 0..100 {
                if let Some(id)=sqlx::query_scalar("SELECT id FROM core.live_events WHERE event_type='command.canonical' AND source_event_ids=ARRAY[$1]::uuid[]").bind(parent).fetch_optional(&mut *conn).await? {return Ok(id)}
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(Error::Invalid("canonical fixture did not settle"))
        }
        let old_canonical = canonical(&mut conn, old_id).await?;
        let corrected = store
            .admission
            .submit_atuin(step(AtuinInterpretation::Current, 1))
            .await?;
        assert_eq!(
            corrected["admitted"], 1,
            "equal source hash must not suppress a new parser recipe"
        );
        let new = sqlx::query(snapshot).fetch_one(&mut conn).await?;
        let new_id: Uuid = new.try_get("id")?;
        assert_ne!(new_id, old_id);
        assert_eq!(
            old.try_get::<Vec<u8>, _>("record_hash")?,
            new.try_get::<Vec<u8>, _>("record_hash")?
        );
        assert_eq!(
            old.try_get::<String, _>("equivalence_key")?,
            new.try_get::<String, _>("equivalence_key")?
        );
        assert_eq!(new.try_get::<String, _>("parser")?, PARSER);
        let payload: Value = new.try_get("payload")?;
        assert_eq!(payload["completion"], "ended");
        assert_eq!(payload["duration_ns"], 0);
        assert_eq!(payload["exit_code"], Value::Null);
        let new_canonical = canonical(&mut conn, new_id).await?;
        assert_ne!(old_canonical, new_canonical);
        let canonical_payload: Value =
            sqlx::query_scalar("SELECT payload FROM core.live_events WHERE id=$1")
                .bind(new_canonical)
                .fetch_one(&mut conn)
                .await?;
        assert_eq!(canonical_payload["completion"], "ended");
        let archive_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM audit.archived WHERE id=ANY($1)")
                .bind(vec![old_id, old_canonical])
                .fetch_one(&mut conn)
                .await?;
        assert_eq!(archive_count, 2);
        let repeated = store
            .admission
            .submit_atuin(step(AtuinInterpretation::Current, 2))
            .await?;
        assert_eq!(repeated["admitted"], 0);
        assert_eq!(repeated["repeated"], 1);
        let stale = store
            .admission
            .submit_atuin(step(AtuinInterpretation::PriorCompletionFixture, -1))
            .await?;
        assert_eq!(
            stale["admitted"], 0,
            "an older reading cannot overwrite the current recipe's newer observation"
        );
        let after = store
            .reader
            .trace(&old_id.to_string(), true)
            .await?
            .unwrap();
        for field in ["material", "anchor", "end", "record"] {
            assert_eq!(before[field], after[field]);
        }
        println!(
            "{}",
            json!({"qualification":"controlled typed prior recipe, not a predecessor binary","old":old_id,"new":new_id,"source_record_hash":old.try_get::<Vec<u8>,_>("record_hash")?,"source_hash_equal":true,"equivalence_unchanged":true,"old_parser":"atuin-v1","new_parser":PARSER,"old_trace":before,"old_trace_after":after,"new_payload":payload,"canonical_old":old_canonical,"canonical_new":new_canonical,"current_repeat":repeated,"stale_reading":stale})
        );
        store.shutdown().await?;
        Ok(())
    }
    #[test]
    fn unfinished_is_no_recorded_end_not_proof_of_running() {
        let row = json!({"id":"unfinished","timestamp":1_700_000_000_000_000_000i64,"duration":-1,"exit":-1,"command":"synthetic","cwd":"/synthetic","session":"same","hostname":"host","deleted_at":null});
        assert_eq!(
            parse(&row).unwrap().payload["completion"],
            "end_not_recorded"
        );
    }
    #[test]
    fn zero_duration_records_an_end_with_unknown_exit() {
        let row = json!({"id":"zero","timestamp":1_700_000_000_000_000_000i64,"duration":0,"exit":-1,"command":"synthetic","cwd":"/synthetic","session":"same","hostname":"host","deleted_at":null});
        let parsed = parse(&row).unwrap();
        assert_eq!(parsed.payload["completion"], "ended");
        assert_eq!(parsed.payload["duration_ns"], 0);
        assert_eq!(parsed.payload["exit_code"], Value::Null);
    }
    #[test]
    fn unfinished_exit_is_unknown_and_malformed_time_is_a_record_failure() {
        let row = json!({"id":"synthetic-1","timestamp":1_700_000_000_123_456_789i64,"duration":-1,"exit":-1,"command":"example","cwd":"/synthetic","session":"s","hostname":"h:u","deleted_at":null});
        let parsed = parse(&row).unwrap();
        assert_eq!(parsed.payload["exit_code"], Value::Null);
        assert_eq!(parsed.payload["completion"], "end_not_recorded");
        assert_eq!(parsed.time, "2023-11-14T22:13:20.123456Z");
        assert_eq!(parsed.nanos, 789);
        let mut bad = row.clone();
        bad["timestamp"] = json!("1700000000000000000");
        assert!(parse(&bad).is_err());
        let mut later = row;
        later["_end_not_recorded"] = json!(true);
        assert_eq!(
            parse(&later).unwrap().payload["completion"],
            "end_not_recorded"
        );
    }
}
