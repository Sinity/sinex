//! The registered Atuin admission variant. The writer owns custody, classification and progress.
use crate::{
    BLOB_LOOKUP, BLOB_PRESENT, BLOB_REGISTER, COUNTER, Error, Result, WriterPool, coordinate_hash,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Connection, Row};
use std::io::{Seek, SeekFrom, Write};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub const SOURCE: &str = "terminal.atuin-history";
pub(crate) const PARSER: &str = "atuin-v3";
const PARSER_NAME: &str = "parser.terminal.atuin-history";
pub const MAX_STEP_ROWS: usize = 64;
pub const MAX_STEP_BYTES: usize = 1024 * 1024;
const PART_BYTES: u64 = 100 * 1024 * 1024;

pub(crate) fn payload_schema() -> Value {
    json!({"type":"object","additionalProperties":false,
        "required":["native_id","command","cwd","session","hostname","exit_code","duration_ns","timestamp_ns","completion"],
        "properties":{"native_id":{"type":"string"},"command":{"type":"string"},"cwd":{"type":"string"},"session":{"type":"string"},"hostname":{"type":"string"},
        "exit_code":{"type":["integer","null"]},"duration_ns":{"type":["integer","null"],"minimum":0},"timestamp_ns":{"type":"integer"},"completion":{"enum":["running","end_not_recorded","ended"]}}})
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    TestBadParse,
    #[cfg(test)]
    TestOldParser,
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
    if r.id.is_empty() || r.id.len() > 512 || r.duration < -1 {
        return Err("invalid Atuin native key or duration");
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
    // Atuin records signed status values at history end; -1 is its unknown
    // sentinel. Preserve other values without decoding their signal meaning.
    let ended = r.duration >= 0 || r.exit != -1;
    let state = if ended {
        "ended"
    } else {
        // This reading has no end. It proves neither running nor immutable terminal state.
        "end_not_recorded"
    };
    Ok(Parsed {
        payload: json!({"native_id":r.id,"command":r.command,"cwd":r.cwd,"session":r.session,
            "hostname":r.hostname.split(':').next().unwrap_or(&r.hostname),
            "exit_code":if r.exit != -1 { Some(r.exit) } else { None },
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
    let configuration = json!({"representation":"canonical_sqlite_rows","parser":parser,"test_fixture":parser!=PARSER,"shell_field_basis":shell_field_basis(rules)});
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
            let open = sqlx::query("SELECT id,durable_bytes,observation_id FROM raw.source_material_registry WHERE input_id=$1 AND status='capturing' ORDER BY staged_at DESC LIMIT 1").bind(input).fetch_optional(&mut self.connection).await?;
            if let Some(row) = open {
                let id = row.try_get::<Uuid, _>("id")?;
                if row.try_get::<Option<Uuid>, _>("observation_id")? == Some(observation) {
                    material = Some(id);
                    start = row.try_get("durable_bytes")?;
                } else {
                    // A partial failed predecessor pass may have left a capturing part.
                    // Never attribute a new acquisition's spans to that older pass.
                    self.close_part(id, "completed").await?;
                }
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
                    .bind(id).bind(source).bind(input).bind(observation).bind(provenance).execute(&mut self.connection).await?;
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

    pub(crate) async fn atuin_read_start(
        &mut self,
        locator: &str,
    ) -> Result<sinex_model::acquisition::AtuinReadStart> {
        let (input, carry): (Uuid, Option<Value>) = sqlx::query_as("SELECT i.id,c.carry FROM raw.inputs i LEFT JOIN raw.source_cursors c ON c.binding=i.binding AND c.input=i.id::text WHERE i.binding=$1 AND i.locator=$2")
            .bind(SOURCE).bind(locator).fetch_one(&mut self.connection).await?;
        let snapshot_seq = sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
            .fetch_one(&mut self.connection)
            .await?;
        let checkpoint = carry
            .and_then(|v| {
                serde_json::from_value::<sinex_model::acquisition::AtuinCheckpoint>(v).ok()
            })
            .filter(|v| v.valid());
        Ok(sinex_model::acquisition::AtuinReadStart {
            input,
            snapshot_seq,
            checkpoint,
        })
    }
    pub(crate) async fn atuin_unfinished_page(
        &mut self,
        input: Uuid,
        snapshot_seq: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<(Uuid, String)>> {
        // Historical eligibility is fixed before admission. Updating an old unfinished
        // row to completed cannot remove its later peers from this keyset traversal.
        // LIMIT bounds materialization, not underlying history scan work.
        let mut tx = self.connection.begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let rows = sqlx::query_as("SELECT e.id,e.equivalence_key::jsonb->>1 FROM core.events e WHERE e.source=$1 AND e.event_type='command.executed' AND e.record_state='present' AND e.payload->>'completion'='end_not_recorded' AND (e.equivalence_key::jsonb->>0)=$2 AND e.commit_seq<=$3 AND ($4::uuid IS NULL OR e.id>$4) AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<=$3) ORDER BY e.id LIMIT 64")
            .bind(SOURCE).bind(input.to_string()).bind(snapshot_seq).bind(after).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(rows)
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
        let parser = if step.interpretation == AtuinInterpretation::TestOldParser {
            "atuin-v1"
        } else {
            parser
        };
        let provenance = json!({"source":SOURCE,"event_type":"command.executed","equivalence":"registered_database + native_id","parser":parser,"representation":"canonical_sqlite_rows","privacy_class":"plain","privacy_rules":step.privacy_rules,"sqlite_snapshot":"unavailable_until_1c"});
        let mut metadata = step.metadata.clone();
        if metadata["_sinex_acquisition_v1"]["version"] == 1 {
            metadata["_sinex_acquisition_v1"]["owner_run"] = json!(self.run_id);
            let prior: Option<(Uuid, String, Option<Value>)> = sqlx::query_as("SELECT id,status,metadata->'_sinex_acquisition_v1'->'outcomes' FROM raw.observations WHERE input_id=$1 ORDER BY seq DESC LIMIT 1")
                .bind(input).fetch_optional(&mut self.connection).await?;
            let refs = sinex_model::acquisition::OutcomeRefs::following(prior.as_ref().map(
                |(id, status, value)| {
                    (
                        *id,
                        status.as_str(),
                        value
                            .as_ref()
                            .and_then(|v| serde_json::from_value(v.clone()).ok()),
                    )
                },
            ));
            metadata["_sinex_acquisition_v1"]["outcomes"] = serde_json::to_value(refs)
                .map_err(|_| Error::Invalid("source outcome references unavailable"))?;
        }
        sqlx::query("INSERT INTO raw.observations (id,input_id,seq,observed_at,source_provenance,metadata,status) SELECT $1,$2,COALESCE(MAX(seq),0)+1,$3::text::timestamptz,$4,$5,'reading' FROM raw.observations WHERE input_id=$2 ON CONFLICT (id) DO NOTHING")
            .bind(step.observation).bind(input).bind(&step.observed_at).bind(&provenance).bind(&metadata).execute(&mut self.connection).await?;
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
            let result = if step.interpretation == AtuinInterpretation::TestOldParser {
                result.map(|mut parsed| {
                    // Test: read with the old parser.
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
            crate::TestPause::reach(
                &mut self.pause,
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
            if step.interpretation == AtuinInterpretation::TestBadParse
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
            if let Some(ref old) = old
                && ((old.try_get::<Vec<u8>, _>("record_hash")? == record_hash
                    && old.try_get::<Option<String>, _>("parser")?.as_deref() == Some(parser))
                    || old.try_get::<i64, _>("read_order")? >= read_order)
            {
                sqlx::query("UPDATE core.live_equivalence SET read_order=GREATEST(read_order,$2) WHERE store=0 AND key_hash=$1").bind(&key_hash).bind(read_order).execute(&mut *tx).await?;
                repeated += 1;
                continue;
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
            let fixture = step.interpretation == AtuinInterpretation::TestBadParse
                && parsed.native_id == "fixture-000";
            if fixture {
                payload["hostname"] = json!("controlled-historical-bad-interpretation");
            }
            let fixture_write = fixture;
            #[cfg(test)]
            let fixture_write =
                fixture_write || step.interpretation == AtuinInterpretation::TestOldParser;
            mask(&mut payload, &step.privacy_rules)?;
            sqlx::query("INSERT INTO core.events (id,source,event_type,payload,ts_orig,ts_orig_tiebreak,ts_quality,ts_clock,time_suspect,payload_schema_id,commit_seq,source_material_id,anchor_byte,offset_start,offset_end,offset_kind,ordinal,record_hash,record_state,run_id,equivalence_key,written_by,producer_version_id) VALUES ($1,$2,'command.executed',$3,$4::text::timestamptz,$13,'intrinsic','producer',$14,2,$5,$6,$7,$7,$8,'bytes',0,$9,$10,$11,$12,$15,$16)")
                .bind(id).bind(SOURCE).bind(payload).bind(parsed.time).bind(last_seq).bind(material).bind(anchor).bind(start+end).bind(&record_hash).bind(if parsed.removed {"removed"} else {"present"}).bind(self.run_id).bind(key).bind(parsed.nanos).bind(projected.suspect).bind(if fixture_write {json!({"kind":"test_fixture","description":"deliberately wrong interpretation for replay tests"})}else{Value::Null}).bind(producer).execute(&mut *tx).await?;
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
            .bind(step.observation).bind(admitted).bind(repeated).bind(failures).bind(removed).bind(status).bind(&step.unavailable).bind(last_seq).bind(excluded).fetch_one(&mut *tx).await?;
        sqlx::query("INSERT INTO raw.source_cursors (binding,input,position,carry) VALUES ($1,$2,$3,'{}'::jsonb) ON CONFLICT (binding,input) DO UPDATE SET position=EXCLUDED.position,updated_at=now()")
            .bind(SOURCE).bind(input.to_string()).bind(&summary).execute(&mut *tx).await?;
        if step.complete
            && step.unavailable.is_none()
            && let Some(value) = step.metadata.get("_sinex_read_plan_v1")
        {
            let mut plan: sinex_model::acquisition::AtuinReadPlan =
                serde_json::from_value(value.clone())
                    .map_err(|_| Error::Invalid("invalid Atuin read plan"))?;
            if plan.version != 1 || !plan.checkpoint.valid() {
                return Err(Error::Invalid("invalid Atuin checkpoint"));
            }
            // Parser omissions remain a reason to compare the full provider again.
            // An empty reduced poll must never silently clear an old malformed row.
            plan.checkpoint.needs_full |= summary["record_errors"].as_i64().unwrap_or(1) != 0;
            let checkpoint = serde_json::to_value(&plan.checkpoint)
                .map_err(|_| Error::Invalid("Atuin checkpoint encoding"))?;
            let plan = serde_json::to_value(&plan)
                .map_err(|_| Error::Invalid("Atuin read plan encoding"))?;
            sqlx::query("UPDATE raw.source_cursors SET carry=$3 WHERE binding=$1 AND input=$2")
                .bind(SOURCE)
                .bind(input.to_string())
                .bind(checkpoint)
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE raw.observations SET metadata=jsonb_set(metadata,'{_sinex_read_plan_v1}',$2) WHERE id=$1").bind(step.observation).bind(plan).execute(&mut *tx).await?;
        }
        let committed_evidence = json!({"custody":boundary_evidence,"progress":summary});
        // Empty reading registration and EOF/unavailable accounting are not
        // data-bearing steps, so tests only pause on steps with bytes.
        if !bytes.is_empty() {
            crate::TestPause::reach(
                &mut self.pause,
                "transaction-staged",
                committed_evidence.clone(),
            )
            .await?;
        }
        tx.commit().await?;
        if !bytes.is_empty() {
            crate::TestPause::reach(&mut self.pause, "transaction-committed", committed_evidence)
                .await?;
        }
        if step.complete || step.unavailable.is_some() {
            let open: Option<Uuid> = sqlx::query_scalar("SELECT id FROM raw.source_material_registry WHERE input_id=$1 AND status='capturing' LIMIT 1").bind(input).fetch_optional(&mut self.connection).await?;
            if let Some(id) = open {
                self.close_part(id, "completed").await?;
            }
        }
        Ok(summary)
    }
}

#[cfg(test)]
#[path = "atuin_tests.rs"]
mod tests;
