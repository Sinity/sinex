//! Atuin history admission: rows read from Atuin's SQLite database are stored
//! as stream parts and admitted as `command.executed` events. Also holds the
//! operator privacy rules that every source applies to payloads.
use crate::source_admission::{
    Tx, key_hash, observation_status, producer_version, record_failure, row_hash, set_part_length,
};
use crate::{COUNTER, Error, Result, WriterPool, coordinate_hash};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::{Connection, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub const SOURCE: &str = "terminal.atuin-history";
pub(crate) const PARSER: &str = "atuin-v3";
const PARSER_NAME: &str = "parser.terminal.atuin-history";
const EVENT_TYPE: &str = "command.executed";
pub const MAX_STEP_ROWS: usize = 64;
pub const MAX_STEP_BYTES: usize = 1024 * 1024;

pub(crate) fn payload_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["native_id", "command", "cwd", "session", "hostname", "exit_code",
            "duration_ns", "timestamp_ns", "completion"],
        "properties": {
            "native_id": {"type": "string"},
            "command": {"type": "string"},
            "cwd": {"type": "string"},
            "session": {"type": "string"},
            "hostname": {"type": "string"},
            "exit_code": {"type": ["integer", "null"]},
            "duration_ns": {"type": ["integer", "null"], "minimum": 0},
            "timestamp_ns": {"type": "integer"},
            "completion": {"enum": ["running", "end_not_recorded", "ended"]}
        }
    })
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

/// Apply privacy rules to every string in `value`. A masked string may not grow
/// past `MAX_STEP_BYTES`.
pub fn mask(value: &mut Value, rules: &[PrivacyRule]) -> Result<()> {
    fn apply(v: &mut Value, regex: &regex::Regex, replacement: &str) -> Result<()> {
        match v {
            Value::String(s) => {
                let mut out = String::new();
                let mut through = 0;
                for found in regex.find_iter(s) {
                    if out.len() + found.start() - through + replacement.len() > MAX_STEP_BYTES {
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
    for rule in rules {
        let regex =
            regex::Regex::new(&rule.pattern).map_err(|_| Error::Invalid("invalid privacy rule"))?;
        apply(value, &regex, &rule.replacement)?;
    }
    Ok(())
}

/// Which parser admits a step. The test variants write deliberately wrong events
/// for replay tests: `TestBadParse` is selected by `sinexd --test-bad-atuin-parse`,
/// `TestOldParser` exists only in unit tests.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AtuinInterpretation {
    Current,
    TestBadParse,
    #[cfg(test)]
    TestOldParser,
}

/// Record failure written for the row `TestBadParse` drops.
const TEST_OMISSION: &str =
    "controlled historical parser omission; current parser accepts this retained row";

impl AtuinInterpretation {
    fn parser(self) -> &'static str {
        match self {
            #[cfg(test)]
            Self::TestOldParser => "atuin-v1",
            _ => PARSER,
        }
    }

    fn parse(self, row: &Value) -> std::result::Result<Parsed, &'static str> {
        let parsed = parse(row);
        #[cfg(test)]
        if self == Self::TestOldParser {
            // The old parser recorded every row without an end as still running.
            return parsed.map(|mut parsed| {
                parsed.payload["completion"] = json!("running");
                parsed.payload["duration_ns"] = Value::Null;
                parsed
            });
        }
        parsed
    }

    /// Whether this interpretation drops the row as a record failure.
    fn omits(self, native_id: &str) -> bool {
        self == Self::TestBadParse && native_id == "fixture-129"
    }

    /// Apply this interpretation's deliberate mistake to `parsed`. Returns
    /// whether the event must be marked as a test fixture.
    fn mislabel(self, parsed: &mut Parsed) -> bool {
        match self {
            Self::Current => false,
            Self::TestBadParse => {
                let wrong = parsed.native_id == "fixture-000";
                if wrong {
                    parsed.payload["hostname"] = json!("controlled-historical-bad-interpretation");
                }
                wrong
            }
            #[cfg(test)]
            Self::TestOldParser => true,
        }
    }
}

/// One step of an Atuin read: at most `MAX_STEP_ROWS` rows from one SQLite
/// transaction, or the reading's end or unavailability.
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
    /// Event time truncated to microseconds; `nanos` holds the remainder.
    pub(crate) time: String,
    pub(crate) removed: bool,
    pub(crate) nanos: i32,
    pub(crate) time_evidence: sinex_model::events::TsOrig,
}

/// Equivalence key and its hash for one Atuin history row of an input.
pub(crate) fn occurrence(input: Uuid, native_id: &str) -> (String, Vec<u8>) {
    let key = json!([input, native_id]).to_string();
    let hash = key_hash(SOURCE, EVENT_TYPE, &key);
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
    let time_evidence = sinex_model::events::TsOrig::from_record_field(
        at,
        "timestamp",
        sinex_model::events::TimePrecision::Instant,
    )
    .map_err(|_| "invalid Atuin time evidence")?;
    // Atuin writes -1 for an unknown exit status or duration. Other values,
    // including negative statuses, are kept as recorded.
    let ended = r.duration >= 0 || r.exit != -1;
    // Without an end the command may still be running or may have been killed.
    let completion = if ended { "ended" } else { "end_not_recorded" };
    Ok(Parsed {
        payload: json!({
            "native_id": r.id,
            "command": r.command,
            "cwd": r.cwd,
            "session": r.session,
            "hostname": r.hostname.split(':').next().unwrap_or(&r.hostname),
            "exit_code": if r.exit != -1 { Some(r.exit) } else { None },
            "timestamp_ns": r.timestamp,
            "duration_ns": if ended && r.duration >= 0 { Some(r.duration) } else { None },
            "completion": completion,
        }),
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

/// Shell field settings, stored in producer configuration under `shell_field_basis`.
/// Shell grouping compares Atuin and zsh fields only when no privacy rules apply.
pub(crate) fn shell_field_config(rules: &[PrivacyRule]) -> Value {
    let encoded = serde_json::to_vec(rules).expect("typed privacy rules");
    json!({
        "contract": "sinex.shell-field-basis.v1",
        "comparable": rules.is_empty(),
        "privacy_rules_blake3": format!("blake3:{}", blake3::hash(&encoded)),
    })
}

pub(crate) async fn parser_producer(
    tx: &mut Tx<'_>,
    run: Uuid,
    parser: &str,
    rules: &[PrivacyRule],
) -> Result<Uuid> {
    let configuration = json!({
        "representation": "canonical_sqlite_rows",
        "parser": parser,
        "test_fixture": parser != PARSER,
        "shell_field_basis": shell_field_config(rules),
    });
    producer_version(tx, PARSER_NAME, parser, run, &configuration).await
}

/// A `command.executed` event ready to write. The payload is already masked.
pub(crate) struct NewEvent<'a> {
    pub(crate) id: Uuid,
    pub(crate) parsed: Parsed,
    pub(crate) material: Uuid,
    pub(crate) anchor: i64,
    pub(crate) end: i64,
    pub(crate) record_hash: &'a [u8],
    pub(crate) key: &'a str,
    pub(crate) key_hash: &'a [u8],
    pub(crate) read_order: i64,
    pub(crate) commit_seq: i64,
    pub(crate) run: Uuid,
    pub(crate) producer: Uuid,
    /// The replay operation that wrote the event, if any.
    pub(crate) operation: Option<Uuid>,
    pub(crate) written_by: Value,
}

/// Write an event and make it the live one for its coordinates and equivalence key.
pub(crate) async fn insert_event(tx: &mut Tx<'_>, event: NewEvent<'_>) -> Result<()> {
    let projected = event
        .parsed
        .time_evidence
        .project(
            sinex_model::events::EventId::try_from(event.id)
                .map_err(|_| Error::Invalid("invalid minted event ID"))?,
            sinex_model::events::CaptureClock::Producer,
        )
        .map_err(|_| Error::Invalid("invalid projected source time"))?;
    sqlx::query(
        "INSERT INTO core.events (id,source,event_type,payload,ts_orig,ts_orig_tiebreak,
           ts_quality,ts_clock,time_suspect,payload_schema_id,commit_seq,source_material_id,
           anchor_byte,offset_start,offset_end,offset_kind,ordinal,record_hash,record_state,
           run_id,equivalence_key,created_by_operation,written_by,producer_version_id)
         VALUES ($1,$2,'command.executed',$3,$4::text::timestamptz,$5,
           'intrinsic','producer',$6,2,$7,$8,
           $9,$9,$10,'bytes',0,$11,$12,
           $13,$14,$15,$16,$17)",
    )
    .bind(event.id)
    .bind(SOURCE)
    .bind(event.parsed.payload)
    .bind(event.parsed.time)
    .bind(event.parsed.nanos)
    .bind(projected.suspect)
    .bind(event.commit_seq)
    .bind(event.material)
    .bind(event.anchor)
    .bind(event.end)
    .bind(event.record_hash)
    .bind(if event.parsed.removed {
        "removed"
    } else {
        "present"
    })
    .bind(event.run)
    .bind(event.key)
    .bind(event.operation)
    .bind(event.written_by)
    .bind(event.producer)
    .execute(&mut **tx)
    .await?;
    sqlx::query("INSERT INTO core.live_coordinates (store,occ_hash,event_id) VALUES (0,$1,$2)")
        .bind(coordinate_hash(event.material, event.anchor, EVENT_TYPE, 0))
        .bind(event.id)
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        "INSERT INTO core.live_equivalence (store,key_hash,event_id,read_order,record_hash)
         VALUES (0,$1,$2,$3,$4)
         ON CONFLICT (store,key_hash) DO UPDATE SET event_id=EXCLUDED.event_id,
           read_order=EXCLUDED.read_order,record_hash=EXCLUDED.record_hash",
    )
    .bind(event.key_hash)
    .bind(event.id)
    .bind(event.read_order)
    .bind(event.record_hash)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id)
         VALUES ($1,$2,0,0,2)",
    )
    .bind(event.commit_seq)
    .bind(event.id)
    .execute(&mut **tx)
    .await?;
    crate::shell::dirty(tx, event.id).await
}

/// Archive the live event `old`, replaced by `new`. Returns how many live
/// coordinate rows were removed.
pub(crate) async fn archive_replaced(
    tx: &mut Tx<'_>,
    old: Uuid,
    new: Uuid,
    seq: i64,
    reason: &str,
    operation: Option<Uuid>,
) -> Result<u64> {
    sqlx::query(
        "INSERT INTO audit.archived (id,archived_seq,reason,replaced_by,operation_id)
         VALUES ($1,$2,$3,$4,$5)",
    )
    .bind(old)
    .bind(seq)
    .bind(reason)
    .bind(new)
    .bind(operation)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id)
         VALUES ($1,$2,1,0,2)",
    )
    .bind(seq)
    .bind(old)
    .execute(&mut **tx)
    .await?;
    Ok(
        sqlx::query("DELETE FROM core.live_coordinates WHERE store=0 AND event_id=$1")
            .bind(old)
            .execute(&mut **tx)
            .await?
            .rows_affected(),
    )
}

/// A row encoded into the step's bytes, with its parse result.
struct EncodedRow {
    /// Offsets of the row's line within the step's bytes.
    anchor: i64,
    end: i64,
    record_hash: Vec<u8>,
    parsed: std::result::Result<Parsed, &'static str>,
}

/// Encode rows as sorted JSON lines tagged with where they came from. The record
/// hash covers the source row without Atuin's `_rowid`.
fn encode_rows(step: &AtuinStep) -> Result<(Vec<u8>, Vec<EncodedRow>)> {
    let mut bytes = Vec::new();
    let mut rows = Vec::new();
    for row in &step.rows {
        let anchor = bytes.len() as i64;
        let mut fields: Map<String, Value> = row
            .as_object()
            .cloned()
            .ok_or(Error::Invalid("row acquisition must be an object"))?;
        let rowid = fields.remove("_rowid").unwrap_or(Value::Null);
        let source_row = Value::Object(fields.clone());
        let record_hash = row_hash(&source_row)?;
        let parsed = step.interpretation.parse(&source_row);
        fields.insert(
            "_from".into(),
            json!({"observation": step.observation, "table": "history", "rowid": rowid}),
        );
        let mut line = Value::Object(fields);
        line.sort_all_objects();
        serde_json::to_writer(&mut bytes, &line)
            .map_err(|_| Error::Invalid("row encoding failed"))?;
        bytes.push(b'\n');
        rows.push(EncodedRow {
            anchor,
            end: bytes.len() as i64,
            record_hash,
            parsed,
        });
    }
    if bytes.len() > MAX_STEP_BYTES {
        return Err(Error::Invalid("Atuin step exceeds the 1 MiB step limit"));
    }
    Ok((bytes, rows))
}

#[derive(Default)]
struct Counts {
    admitted: i64,
    repeated: i64,
    removed: i64,
    failures: i64,
    excluded: i64,
}

/// What admission needs to know about the step while it writes rows.
struct StepWrite<'a> {
    step: &'a AtuinStep,
    input: Uuid,
    material: Option<Uuid>,
    /// Offset of the step's bytes within the part.
    start: i64,
    read_order: i64,
    parser: &'static str,
    producer: Uuid,
    run: Uuid,
}

/// Admit one row: record a failure, skip an excluded or already-current row, or
/// write a new event that replaces the live one for the same Atuin row.
async fn admit_row(
    tx: &mut Tx<'_>,
    write: &StepWrite<'_>,
    row: EncodedRow,
    seq: &mut i64,
    counts: &mut Counts,
) -> Result<()> {
    let step = write.step;
    let material = write
        .material
        .ok_or(Error::Invalid("row material missing"))?;
    let anchor = write.start + row.anchor;
    let mut parsed = match row.parsed {
        Ok(parsed) => parsed,
        Err(reason) => {
            record_failure(tx, step.observation, material, anchor, reason).await?;
            counts.failures += 1;
            return Ok(());
        }
    };
    if step.interpretation.omits(&parsed.native_id) {
        record_failure(tx, step.observation, material, anchor, TEST_OMISSION).await?;
        counts.failures += 1;
        return Ok(());
    }
    let (key, key_hash) = occurrence(write.input, &parsed.native_id);
    if sqlx::query_scalar::<_, bool>(crate::exclusion::EXCLUDED)
        .bind(&key)
        .fetch_one(&mut **tx)
        .await?
    {
        counts.excluded += 1;
        return Ok(());
    }
    let old = sqlx::query(
        "SELECT l.event_id,l.read_order,l.record_hash,p.version AS parser
         FROM core.live_equivalence l
         JOIN core.events e ON e.id=l.event_id
         LEFT JOIN core.producer_versions p
           ON p.id=e.producer_version_id AND p.name='parser.terminal.atuin-history'
         WHERE l.store=0 AND l.key_hash=$1",
    )
    .bind(&key_hash)
    .fetch_optional(&mut **tx)
    .await?;
    // Keep the live event when this row and parser are unchanged, or when the
    // live event came from a newer reading.
    if let Some(ref old) = old
        && ((old.try_get::<Vec<u8>, _>("record_hash")? == row.record_hash
            && old.try_get::<Option<String>, _>("parser")?.as_deref() == Some(write.parser))
            || old.try_get::<i64, _>("read_order")? >= write.read_order)
    {
        sqlx::query(
            "UPDATE core.live_equivalence SET read_order=GREATEST(read_order,$2)
             WHERE store=0 AND key_hash=$1",
        )
        .bind(&key_hash)
        .bind(write.read_order)
        .execute(&mut **tx)
        .await?;
        counts.repeated += 1;
        return Ok(());
    }
    let id = Uuid::now_v7();
    if let Some(old) = old {
        let old: Uuid = old.try_get("event_id")?;
        crate::shell::dirty(tx, old).await?;
        archive_replaced(tx, old, id, *seq, "supersede", None).await?;
        *seq = sqlx::query_scalar(COUNTER).fetch_one(&mut **tx).await?;
    }
    let test_fixture = step.interpretation.mislabel(&mut parsed);
    mask(&mut parsed.payload, &step.privacy_rules)?;
    let removed = parsed.removed;
    insert_event(
        tx,
        NewEvent {
            id,
            parsed,
            material,
            anchor,
            end: write.start + row.end,
            record_hash: &row.record_hash,
            key: &key,
            key_hash: &key_hash,
            read_order: write.read_order,
            commit_seq: *seq,
            run: write.run,
            producer: write.producer,
            operation: None,
            written_by: if test_fixture {
                json!({
                    "kind": "test_fixture",
                    "description": "deliberately wrong interpretation for replay tests",
                })
            } else {
                Value::Null
            },
        },
    )
    .await?;
    *seq = sqlx::query_scalar(COUNTER).fetch_one(&mut **tx).await?;
    counts.admitted += 1;
    counts.removed += i64::from(removed);
    Ok(())
}

/// Add the step's counts to its observation and store the result as the cursor
/// position. A completed read with a read plan also stores its checkpoint.
async fn record_progress(
    tx: &mut Tx<'_>,
    step: &AtuinStep,
    input: Uuid,
    counts: &Counts,
    seq: i64,
) -> Result<Value> {
    let status = observation_status(step.unavailable.is_some(), step.complete);
    let summary: Value = sqlx::query_scalar(
        "UPDATE raw.observations SET rows_read=rows_read+$2+$3+$4+$9,excluded=excluded+$9,
           admitted=admitted+$2,repeated=repeated+$3,record_errors=record_errors+$4,
           removed=removed+$5,status=$6,reason=$7,commit_seq=$8
         WHERE id=$1
         RETURNING jsonb_build_object('observation',id,'status',status,'rows_read',rows_read,
           'admitted',admitted,'repeated',repeated,'excluded',excluded,'removed',removed,
           'record_errors',record_errors,'commit_seq',commit_seq)",
    )
    .bind(step.observation)
    .bind(counts.admitted)
    .bind(counts.repeated)
    .bind(counts.failures)
    .bind(counts.removed)
    .bind(status)
    .bind(&step.unavailable)
    .bind(seq)
    .bind(counts.excluded)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO raw.source_cursors (binding,input,position,carry)
         VALUES ($1,$2,$3,'{}'::jsonb)
         ON CONFLICT (binding,input) DO UPDATE SET position=EXCLUDED.position,updated_at=now()",
    )
    .bind(SOURCE)
    .bind(input.to_string())
    .bind(&summary)
    .execute(&mut **tx)
    .await?;
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
        // Record errors make the next read compare every row, so a shorter
        // read cannot hide an old malformed row.
        plan.checkpoint.needs_full |= summary["record_errors"].as_i64().unwrap_or(1) != 0;
        let checkpoint = serde_json::to_value(&plan.checkpoint)
            .map_err(|_| Error::Invalid("Atuin checkpoint encoding"))?;
        let plan =
            serde_json::to_value(&plan).map_err(|_| Error::Invalid("Atuin read plan encoding"))?;
        sqlx::query("UPDATE raw.source_cursors SET carry=$3 WHERE binding=$1 AND input=$2")
            .bind(SOURCE)
            .bind(input.to_string())
            .bind(checkpoint)
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "UPDATE raw.observations
             SET metadata=jsonb_set(metadata,'{_sinex_read_plan_v1}',$2) WHERE id=$1",
        )
        .bind(step.observation)
        .bind(plan)
        .execute(&mut **tx)
        .await?;
    }
    Ok(summary)
}

impl WriterPool {
    pub(crate) async fn atuin_read_start(
        &mut self,
        locator: &str,
    ) -> Result<sinex_model::acquisition::AtuinReadStart> {
        let (input, carry): (Uuid, Option<Value>) = sqlx::query_as(
            "SELECT i.id,c.carry FROM raw.inputs i
             LEFT JOIN raw.source_cursors c ON c.binding=i.binding AND c.input=i.id::text
             WHERE i.binding=$1 AND i.locator=$2",
        )
        .bind(SOURCE)
        .bind(locator)
        .fetch_one(&mut self.connection)
        .await?;
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

    /// One page of live events without a recorded end, as of `snapshot_seq`, so
    /// completing earlier rows during the read does not skip later ones.
    pub(crate) async fn atuin_unfinished_page(
        &mut self,
        input: Uuid,
        snapshot_seq: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<(Uuid, String)>> {
        let mut tx = self.connection.begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *tx)
            .await?;
        let rows = sqlx::query_as(
            "SELECT e.id,e.equivalence_key::jsonb->>1 FROM core.events e
             WHERE e.source=$1 AND e.event_type='command.executed' AND e.record_state='present'
               AND e.payload->>'completion'='end_not_recorded'
               AND (e.equivalence_key::jsonb->>0)=$2 AND e.commit_seq<=$3
               AND ($4::uuid IS NULL OR e.id>$4)
               AND NOT EXISTS (SELECT 1 FROM audit.archived a
                               WHERE a.id=e.id AND a.archived_seq<=$3)
             ORDER BY e.id LIMIT 64",
        )
        .bind(SOURCE)
        .bind(input.to_string())
        .bind(snapshot_seq)
        .bind(after)
        .fetch_all(&mut *tx)
        .await?;
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
        let input = self.register_input(SOURCE, &step.locator, "sqlite").await?;
        let parser = step.interpretation.parser();
        let provenance = json!({
            "source": SOURCE,
            "event_type": EVENT_TYPE,
            "equivalence": "registered_database + native_id",
            "parser": parser,
            "representation": "canonical_sqlite_rows",
            "privacy_class": "plain",
            "privacy_rules": step.privacy_rules,
            "sqlite_snapshot": "unavailable_until_1c",
        });
        self.register_atuin_reading(&step, input, &provenance)
            .await?;
        let (bytes, rows) = encode_rows(&step)?;
        let (material, start) = self
            .store_stream_step(SOURCE, input, step.observation, &provenance, &bytes)
            .await?;
        let stored = json!({
            "input": input,
            "observation": step.observation,
            "material": material,
            "start": start,
            "end": start + bytes.len() as i64,
            "canonical_bytes": bytes.len(),
            "canonical_blake3": blake3::hash(&bytes).to_hex().to_string(),
            "step_rows": rows.len(),
            "source_eof": step.complete,
            "acknowledged": false,
        });
        // Test pauses apply only to steps that carry rows.
        if !bytes.is_empty() {
            crate::TestPause::reach(&mut self.pause, "retained-fsynced", stored.clone()).await?;
        }
        let mut tx = self.connection.begin().await?;
        let mut seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
        let producer = parser_producer(&mut tx, self.run_id, parser, &step.privacy_rules).await?;
        if let Some(id) = material {
            set_part_length(&mut tx, id, start + bytes.len() as i64).await?;
        }
        let write = StepWrite {
            step: &step,
            input,
            material,
            start,
            read_order,
            parser,
            producer,
            run: self.run_id,
        };
        let mut counts = Counts::default();
        for row in rows {
            admit_row(&mut tx, &write, row, &mut seq, &mut counts).await?;
        }
        let summary = record_progress(&mut tx, &step, input, &counts, seq).await?;
        let committed = json!({"custody": stored, "progress": summary});
        if !bytes.is_empty() {
            crate::TestPause::reach(&mut self.pause, "transaction-staged", committed.clone())
                .await?;
        }
        tx.commit().await?;
        if !bytes.is_empty() {
            crate::TestPause::reach(&mut self.pause, "transaction-committed", committed).await?;
        }
        if step.complete || step.unavailable.is_some() {
            self.close_open_part(input).await?;
        }
        Ok(summary)
    }

    /// Insert the reading's observation row on its first step. Acquisition
    /// metadata gets this run and the outcome references of the previous reading.
    async fn register_atuin_reading(
        &mut self,
        step: &AtuinStep,
        input: Uuid,
        provenance: &Value,
    ) -> Result<()> {
        let mut metadata = step.metadata.clone();
        if metadata["_sinex_acquisition_v1"]["version"] == 1 {
            metadata["_sinex_acquisition_v1"]["owner_run"] = json!(self.run_id);
            metadata["_sinex_acquisition_v1"]["outcomes"] = self.following_outcomes(input).await?;
        }
        sqlx::query(
            "INSERT INTO raw.observations
               (id,input_id,seq,observed_at,source_provenance,metadata,status)
             SELECT $1,$2,COALESCE(MAX(seq),0)+1,$3::text::timestamptz,$4,$5,'reading'
             FROM raw.observations WHERE input_id=$2
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(step.observation)
        .bind(input)
        .bind(&step.observed_at)
        .bind(provenance)
        .bind(&metadata)
        .execute(&mut self.connection)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "atuin_tests.rs"]
mod tests;
