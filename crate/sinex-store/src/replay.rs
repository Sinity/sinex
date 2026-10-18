//! Replay: re-parse a stored Atuin stream part with the current parser and
//! replace its events, one chunk per step, as an operation that resumes after a
//! restart. Also stores the JSON records (requests, outcomes, effects) that
//! replay and exclusion operations refer to.
use crate::atuin::{NewEvent, archive_replaced, insert_event};
use crate::source_admission::row_hash;
use crate::{
    BLOB_LOOKUP, BLOB_PRESENT, BLOB_REGISTER, Error, MATERIAL, ReadPool, ReplayCommand, Result,
    WriterPool, next_commit_seq,
};
use serde_json::{Value, json};
use sinex_model::replay::{Outcome, State};
use sqlx::{Connection, Row};
use uuid::Uuid;

pub(crate) const PRINCIPAL: &str = "foundation.operator";
const MAX_SELECTION: usize = 4096;
const CHUNK: usize = 64;
const REQUEST: &str = "sinex.replay.request.v1";
const OUTCOME: &str = "sinex.replay.outcome.v1";

fn selection(query: &str) -> Result<Uuid> {
    let words: Vec<_> = query.split_whitespace().collect();
    match words.as_slice() {
        ["materials", "where", "id", "=", id] => {
            let id = if id.starts_with('"') || id.ends_with('"') {
                id.strip_prefix('"')
                    .and_then(|s| s.strip_suffix('"'))
                    .ok_or(Error::Invalid("replay UUID quotes must be balanced"))?
            } else {
                id
            };
            Uuid::parse_str(id).map_err(|_| Error::Invalid("replay requires a material UUID"))
        }
        _ => Err(Error::Invalid(
            "replay supports only materials where id = UUID; parser overrides and other selections are unavailable",
        )),
    }
}

/// One stored row of the selected part, as recorded in the operation's scope.
/// Field names are stored keys.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    anchor: i64,
    end: i64,
    /// The live event at these coordinates when the operation was recorded.
    interpretation: Option<Uuid>,
    /// The live event for this row's equivalence key, which may come from
    /// another part, and its record hash.
    #[serde(default)]
    head: Option<Uuid>,
    #[serde(default)]
    head_hash: Option<Vec<u8>>,
    #[serde(default)]
    excluded: bool,
}
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ParserSettings {
    format: String,
    parser: String,
    artifact: String,
    privacy_rules_hash: String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestRecord {
    format: String,
    method: String,
    material_id: Uuid,
    expected_basis: Option<String>,
    interpretation_basis: ParserSettings,
}
impl RequestRecord {
    fn decode(value: Value) -> Result<Self> {
        let record: Self = serde_json::from_value(value)
            .map_err(|_| Error::ReplayRefused("unsupported stored replay request"))?;
        if record.format != REQUEST
            || record.method != "replay.start"
            || record.interpretation_basis.format != "sinex.atuin.interpretation.v1"
            || !matches!(
                record.interpretation_basis.parser.as_str(),
                "atuin-v1" | "atuin-v2" | crate::atuin::PARSER
            )
        {
            return Err(Error::ReplayRefused("unsupported stored replay request"));
        }
        Ok(record)
    }
}
/// A row checked and parsed for one replay step.
struct Prepared {
    /// The live event this row replaces.
    old: Option<Uuid>,
    head_hash: Option<Vec<u8>>,
    id: Uuid,
    anchor: i64,
    end: i64,
    record_hash: Vec<u8>,
    key: String,
    key_hash: Vec<u8>,
    read_order: i64,
    parsed: crate::atuin::Parsed,
}
/// How a start request proceeds: run the next chunk, or answer without one.
enum Begin {
    Run {
        id: Uuid,
        records: Vec<Record>,
        current: Outcome,
    },
    Done(Value),
}
/// End of the chunk starting at `from`: at most `CHUNK` rows and `MAX_STEP_BYTES`.
fn chunk_end(records: &[Record], from: usize) -> usize {
    let mut through = from;
    let mut bytes = 0;
    while through < records.len() && through - from < CHUNK {
        let length = records[through].end - records[through].anchor;
        if bytes + length > crate::MAX_STEP_BYTES as i64 {
            break;
        }
        bytes += length;
        through += 1;
    }
    through
}
fn recorded_outcome(id: Uuid, action: Uuid, selected: usize) -> Outcome {
    Outcome {
        operation_id: id,
        action_id: action,
        state: State::Recorded,
        complete: false,
        selected,
        processed: 0,
        next_anchor: 0,
        steps: 0,
        archived: 0,
        admitted: 0,
        reason: None,
    }
}
fn record_hash_text(hash: &[u8]) -> String {
    format!(
        "\\x{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
impl WriterPool {
    /// Read every row of a closed Atuin part with the live events it relates to.
    async fn replay_scope(&mut self, material: Uuid) -> Result<Vec<Record>> {
        let length: Option<i64> = sqlx::query_scalar(
            "SELECT durable_bytes FROM raw.source_material_registry
             WHERE id=$1 AND source_identifier=$2 AND material_kind='stream_chunk'
               AND status<>'capturing'
               AND parser_provenance->>'parser' IN ('atuin-v1','atuin-v2','atuin-v3')
               AND parser_provenance->>'representation'='canonical_sqlite_rows'",
        )
        .bind(material)
        .bind(crate::ATUIN_SOURCE)
        .fetch_optional(&mut self.connection)
        .await?;
        let length = length.ok_or(Error::ReplayRefused(
            "replay requires a closed stored Atuin part",
        ))?;
        self.verify_material(material).await?;
        let input: Uuid =
            sqlx::query_scalar("SELECT input_id FROM raw.source_material_registry WHERE id=$1")
                .bind(material)
                .fetch_one(&mut self.connection)
                .await?;
        let mut records = Vec::new();
        let mut anchor = 0;
        while anchor < length {
            if records.len() == MAX_SELECTION {
                return Err(Error::ReplayRefused(
                    "part has more than the supported 4096 rows",
                ));
            }
            let (record, end) = self
                .reader
                .stored_record(material, anchor, None, None)
                .await
                .map_err(|_| Error::ReplayRefused("stored row unavailable or malformed"))?;
            let interpretation = self.event_at(material, anchor).await?.map(|(id, _)| id);
            let parsed = crate::atuin::parse(&record).map_err(Error::ReplayRefused)?;
            let (key, key_hash) = crate::atuin::occurrence(input, &parsed.native_id);
            let head: Option<(Uuid, Vec<u8>)> = sqlx::query_as(
                "SELECT l.event_id,l.record_hash FROM core.live_equivalence l
                 JOIN core.events e ON e.id=l.event_id
                 WHERE l.store=0 AND l.key_hash=$1 AND e.source=$2
                   AND e.event_type='command.executed' AND e.equivalence_key=$3",
            )
            .bind(key_hash)
            .bind(crate::ATUIN_SOURCE)
            .bind(&key)
            .fetch_optional(&mut self.connection)
            .await?;
            let excluded = sqlx::query_scalar::<_, bool>(crate::exclusion::EXCLUDED)
                .bind(key)
                .fetch_one(&mut self.connection)
                .await?;
            records.push(Record {
                anchor,
                end,
                interpretation,
                head: head.as_ref().map(|(id, _)| *id),
                head_hash: head.map(|(_, hash)| hash),
                excluded,
            });
            anchor = end;
        }
        Ok(records)
    }
    /// The unarchived event at these coordinates, as its id and row JSON.
    async fn event_at(&mut self, material: Uuid, anchor: i64) -> Result<Option<(Uuid, Value)>> {
        let row: Option<(Uuid, Value)> = sqlx::query_as(
            "SELECT e.id,to_jsonb(e) FROM core.events e
             WHERE source_material_id=$1 AND anchor_byte=$2 AND ordinal=0 AND source=$3
               AND event_type='command.executed'
               AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id)",
        )
        .bind(material)
        .bind(anchor)
        .bind(crate::ATUIN_SOURCE)
        .fetch_optional(&mut self.connection)
        .await?;
        Ok(row)
    }
    async fn verify_material(&mut self, material: Uuid) -> Result<()> {
        let name: String = sqlx::query_scalar(
            "SELECT b.blake3 FROM raw.source_material_registry m
             JOIN core.blobs b ON b.id=m.blob_id WHERE m.id=$1 AND b.status='present'",
        )
        .bind(material)
        .fetch_optional(&mut self.connection)
        .await?
        .ok_or(Error::ReplayRefused("stored part unavailable"))?;
        let hash = crate::parse_hash(&name)?;
        let blobs = self.blobs.clone();
        if tokio::task::spawn_blocking(move || blobs.verify(&hash)).await??
            != hashed_blobs::Verified::Intact
        {
            return Err(Error::ReplayRefused("stored part missing or corrupt"));
        }
        Ok(())
    }
    /// Store a JSON record (an operation's request, outcome or effects) as a file
    /// material. Nothing refers to it until the operation row naming it commits.
    pub(crate) async fn store_record(&mut self, value: &Value) -> Result<Uuid> {
        let bytes = serde_json::to_vec(value)
            .map_err(|_| Error::Invalid("cannot encode operation record"))?;
        if bytes.len() > crate::MAX_STEP_BYTES {
            return Err(Error::ReplayRefused("operation record exceeds 1 MiB"));
        }
        let hash = hashed_blobs::Hash::of(&bytes);
        let name = hash.to_string();
        sqlx::query(BLOB_REGISTER)
            .bind(Uuid::now_v7())
            .bind(&name)
            .bind(bytes.len() as i64)
            .bind(&self.root)
            .execute(&mut self.connection)
            .await?;
        let row = sqlx::query(BLOB_LOOKUP)
            .bind(name)
            .fetch_one(&mut self.connection)
            .await?;
        let blob: Uuid = row.try_get("id")?;
        if row.try_get::<String, _>("root")? != self.root
            || row.try_get::<i64, _>("size_bytes")? != bytes.len() as i64
        {
            return Err(Error::Invalid(
                "blob registry entry differs from the operation record",
            ));
        }
        let length = bytes.len() as i64;
        let store = self.blobs.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            store.put(bytes.as_slice(), 3)?;
            if store.verify(&hash)? != hashed_blobs::Verified::Intact {
                return Err(Error::Invalid("operation record failed verification"));
            }
            Ok(())
        })
        .await??;
        let id = Uuid::now_v7();
        let mut tx = self.connection.begin().await?;
        sqlx::query(BLOB_PRESENT)
            .bind(blob)
            .execute(&mut *tx)
            .await?;
        sqlx::query(MATERIAL)
            .bind(id)
            .bind("sinex.action")
            .bind(blob)
            .bind(length)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(id)
    }
    /// Check that a chunk's rows and live events still match what was recorded,
    /// and parse them. Excluded rows are skipped.
    async fn prepare_chunk(
        &mut self,
        material: Uuid,
        records: &[Record],
        rules: &[crate::PrivacyRule],
    ) -> Result<Vec<Prepared>> {
        self.verify_material(material).await?;
        let (input, observed_order): (Uuid, i64) = sqlx::query_as(
            "SELECT m.input_id,(extract(epoch FROM o.observed_at)*1000000000)::bigint
             FROM raw.source_material_registry m
             JOIN raw.observations o ON o.id=m.observation_id WHERE m.id=$1",
        )
        .bind(material)
        .fetch_one(&mut self.connection)
        .await?;
        let mut prepared = Vec::new();
        let mut bytes = 0;
        for expected in records {
            bytes += expected.end - expected.anchor;
            if bytes > crate::MAX_STEP_BYTES as i64 {
                return Err(Error::ReplayRefused("replay chunk exceeds 1 MiB"));
            }
            let old = self.event_at(material, expected.anchor).await?;
            let old_id = old.as_ref().map(|(id, _)| *id);
            if old_id != expected.interpretation {
                return Err(Error::ReplayRefused(
                    "selected interpretation changed; request a new action",
                ));
            }
            let (mut record, _) = self
                .reader
                .stored_record(material, expected.anchor, Some(expected.end), None)
                .await
                .map_err(|_| Error::ReplayRefused("stored row unavailable or malformed"))?;
            record
                .as_object_mut()
                .ok_or(Error::ReplayRefused("stored row is not an object"))?
                .remove("_from");
            let record_hash = row_hash(&record)?;
            let mut parsed = crate::atuin::parse(&record).map_err(Error::ReplayRefused)?;
            let (key, key_hash) = crate::atuin::occurrence(input, &parsed.native_id);
            if let Some((_, row)) = &old
                && (row["record_hash"] != record_hash_text(&record_hash)
                    || row["equivalence_key"] != key
                    || row["offset_end"] != expected.end)
            {
                return Err(Error::ReplayRefused(
                    "stored row differs from its admitted event",
                ));
            }
            let excluded = sqlx::query_scalar::<_, bool>(crate::exclusion::EXCLUDED)
                .bind(&key)
                .fetch_one(&mut self.connection)
                .await?;
            if excluded != expected.excluded {
                return Err(Error::ReplayRefused(
                    "selected occurrence exclusion changed; request a new replay action",
                ));
            }
            if excluded {
                continue;
            }
            let live: Option<(Uuid, i64, Vec<u8>, String)> = sqlx::query_as(
                "SELECT l.event_id,l.read_order,l.record_hash,e.equivalence_key
                 FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id
                 WHERE l.store=0 AND l.key_hash=$1 AND e.source=$2
                   AND e.event_type='command.executed'",
            )
            .bind(&key_hash)
            .bind(crate::ATUIN_SOURCE)
            .fetch_optional(&mut self.connection)
            .await?;
            // Scopes recorded before `head` existed use the selected event as the
            // head. Never look up a new head on resume.
            let expected_head = expected.head.or(expected.interpretation);
            let head_hash = expected
                .head_hash
                .clone()
                .or_else(|| old_id.map(|_| record_hash.clone()));
            if live.as_ref().map(|(id, _, _, _)| *id) != expected_head
                || live
                    .as_ref()
                    .is_some_and(|(_, _, hash, k)| Some(hash) != head_hash.as_ref() || k != &key)
            {
                return Err(Error::ReplayRefused(
                    "selected occurrence head changed; request a new action",
                ));
            }
            if live
                .as_ref()
                .is_some_and(|(_, _, hash, _)| hash != &record_hash)
            {
                return Err(Error::ReplayRefused(
                    "selected stored row differs from the current reading of this occurrence; request current material",
                ));
            }
            crate::atuin::mask(&mut parsed.payload, rules)?;
            prepared.push(Prepared {
                old: expected_head,
                head_hash,
                id: Uuid::now_v7(),
                anchor: expected.anchor,
                end: expected.end,
                record_hash,
                key,
                key_hash,
                read_order: live.map(|(_, order, _, _)| order).unwrap_or(observed_order),
                parsed,
            });
        }
        Ok(prepared)
    }
    /// End an operation with `state` and `reason` without running a chunk.
    async fn finish_without_effect(
        &mut self,
        mut result: Outcome,
        reason: String,
        state: State,
    ) -> Result<Value> {
        result.state = state;
        result.reason = Some(reason);
        let value = json!(result);
        let stored = self
            .store_record(&json!({"format": OUTCOME, "outcome": value}))
            .await?;
        sqlx::query(
            "UPDATE core.operations_log
             SET admission_state=CASE WHEN (progress->>'steps')::bigint>0
                   THEN 'admitted' ELSE 'refused' END,
                 outcome_format='sinex.replay.outcome.v1',outcome_ref=$2,progress=$3
             WHERE id=$1",
        )
        .bind(result.operation_id)
        .bind(stored)
        .bind(&value)
        .execute(&mut self.connection)
        .await?;
        Ok(value)
    }
    pub(crate) async fn replay(&mut self, command: ReplayCommand) -> Result<Value> {
        match command {
            ReplayCommand::Cancel(p) => {
                let value = self.replay_status(p.operation_id).await?;
                let current: Outcome = serde_json::from_value(value.clone())
                    .map_err(|_| Error::Invalid("operation outcome invalid"))?;
                if !matches!(current.state, State::Recorded | State::Running) {
                    return Ok(value);
                }
                self.finish_without_effect(
                    current,
                    "cancelled between committed chunks".into(),
                    State::Cancelled,
                )
                .await
            }
            ReplayCommand::Plan(p, rules) => self.plan_replay(&p.query, &rules).await,
            ReplayCommand::Start(p) => {
                let rules = self.replay_rules.clone().ok_or(Error::ReplayRefused(
                    "current replay operator authorization is unavailable",
                ))?;
                let material = selection(&p.query)?;
                let begin = match self.resume_replay(&p, material, &rules).await? {
                    Some(begin) => begin,
                    None => self.record_replay(&p, material, &rules).await?,
                };
                match begin {
                    Begin::Run {
                        id,
                        records,
                        current,
                    } => {
                        self.run_replay_chunk(id, material, &records, current, &rules)
                            .await
                    }
                    Begin::Done(value) => Ok(value),
                }
            }
        }
    }
    /// Dry run: check every chunk and count what a replay would change.
    async fn plan_replay(&mut self, query: &str, rules: &[crate::PrivacyRule]) -> Result<Value> {
        let material = selection(query)?;
        let records = self.replay_scope(material).await?;
        let mut from = 0;
        let mut chunks = 0;
        let (mut would_admit, mut would_archive) = (0usize, 0usize);
        while from < records.len() {
            let through = chunk_end(&records, from);
            let prepared = self
                .prepare_chunk(material, &records[from..through], rules)
                .await?;
            would_admit += prepared.len();
            would_archive += prepared.iter().filter(|row| row.old.is_some()).count();
            from = through;
            chunks += 1;
        }
        Ok(json!({
            "material_id": material,
            "basis": self.replay_fingerprint(material, &records, rules).await?,
            "selected": records.len(),
            "would_admit": would_admit,
            "would_archive": would_archive,
            "excluded": records.iter().filter(|row| row.excluded).count(),
            "chunks": chunks.max(1),
            "chunk_max_records": CHUNK,
            "chunk_max_bytes": crate::MAX_STEP_BYTES,
            "representation": "canonical_sqlite_rows",
            "dry_run": true,
            "search_projection": {
                "enabled": self.shell_producer.is_some(),
                "status": self.reader.projection_status().await?,
            },
        }))
    }
    /// A start with an action ID already recorded returns or resumes that
    /// operation. This is checked before any current state.
    async fn resume_replay(
        &mut self,
        p: &sinex_model::replay::Start,
        material: Uuid,
        rules: &[crate::PrivacyRule],
    ) -> Result<Option<Begin>> {
        let Some(old) = sqlx::query(
            "SELECT id,scope,request_format,request_ref FROM core.operations_log
             WHERE action_principal=$1 AND action_id=$2",
        )
        .bind(PRINCIPAL)
        .bind(p.action_id)
        .fetch_optional(&mut self.connection)
        .await?
        else {
            return Ok(None);
        };
        let scope: Value = old.try_get("scope")?;
        if old.try_get::<String, _>("request_format")? != REQUEST {
            return Err(Error::ReplayRefused(
                "unsupported stored replay request format",
            ));
        }
        let request = RequestRecord::decode(
            self.reader
                .action_record(old.try_get("request_ref")?, REQUEST)
                .await?,
        )?;
        if request.material_id != material || request.expected_basis != p.expected_basis {
            return Err(Error::ReplayRefused(
                "action ID is already used by a different replay request",
            ));
        }
        let id: Uuid = old.try_get("id")?;
        let current: Outcome = serde_json::from_value(
            self.reader
                .replay_status(id)
                .await?
                .ok_or(Error::Invalid("operation disappeared"))?,
        )
        .map_err(|_| Error::Invalid("operation outcome invalid"))?;
        if !matches!(current.state, State::Recorded | State::Running) {
            return Ok(Some(Begin::Done(json!(current))));
        }
        if request.interpretation_basis != self.parser_settings(rules).await? {
            let reason = "parser settings changed; a new replay action is required".into();
            return Ok(Some(Begin::Done(
                self.finish_without_effect(current, reason, State::Refused)
                    .await?,
            )));
        }
        if scope["preflight_validated"] != true {
            let reason = scope["preflight_refusal"]
                .as_str()
                .unwrap_or("replay check result unavailable")
                .to_string();
            return Ok(Some(Begin::Done(
                self.finish_without_effect(current, reason, State::Refused)
                    .await?,
            )));
        }
        let records: Vec<Record> = serde_json::from_value(scope["records"].clone())
            .map_err(|_| Error::Invalid("operation scope invalid"))?;
        Ok(Some(Begin::Run {
            id,
            records,
            current,
        }))
    }
    /// Record a new operation. Every chunk is checked first, so a resumed
    /// operation never skips the check; a failed check records a refusal.
    async fn record_replay(
        &mut self,
        p: &sinex_model::replay::Start,
        material: Uuid,
        rules: &[crate::PrivacyRule],
    ) -> Result<Begin> {
        let id = Uuid::now_v7();
        let scope = self.replay_scope(material).await;
        let records = scope.as_ref().cloned().unwrap_or_default();
        let initial = recorded_outcome(id, p.action_id, records.len());
        let request = RequestRecord {
            format: REQUEST.into(),
            method: "replay.start".into(),
            material_id: material,
            expected_basis: p.expected_basis.clone(),
            interpretation_basis: self.parser_settings(rules).await?,
        };
        let mut refusal = scope.err().map(|error| error.to_string());
        if refusal.is_none() {
            let fingerprint = self.replay_fingerprint(material, &records, rules).await?;
            if p.expected_basis
                .as_ref()
                .is_some_and(|expected| expected != &fingerprint)
            {
                refusal =
                    Some("preview basis changed; obtain a new plan and use a new action".into());
            }
        }
        let mut from = 0;
        while refusal.is_none() && from < records.len() {
            let through = chunk_end(&records, from);
            if let Err(error) = self
                .prepare_chunk(material, &records[from..through], rules)
                .await
            {
                refusal = Some(error.to_string());
            }
            from = through;
        }
        let request_ref = self.store_record(&json!(request)).await?;
        sqlx::query(
            "INSERT INTO core.operations_log (id,operation_type,actor,action_principal,action_id,
               request_format,request_ref,scope,admission_state,progress)
             VALUES ($1,'replay',$2,$3,$4,'sinex.replay.request.v1',$5,$6,'recorded',$7)",
        )
        .bind(id)
        .bind(json!({"kind": "operator", "principal": PRINCIPAL, "profile": "explicit_operator_token"}))
        .bind(PRINCIPAL)
        .bind(p.action_id)
        .bind(request_ref)
        .bind(json!({
            "records": records,
            "preflight_validated": refusal.is_none(),
            "preflight_refusal": refusal,
        }))
        .bind(json!(initial))
        .execute(&mut self.connection)
        .await?;
        if let Some(reason) = refusal {
            return Ok(Begin::Done(
                self.finish_without_effect(initial, reason, State::Refused)
                    .await?,
            ));
        }
        Ok(Begin::Run {
            id,
            records,
            current: initial,
        })
    }
    /// Replace the next chunk's events in one transaction and advance the operation.
    async fn run_replay_chunk(
        &mut self,
        id: Uuid,
        material: Uuid,
        records: &[Record],
        current: Outcome,
        rules: &[crate::PrivacyRule],
    ) -> Result<Value> {
        let through = chunk_end(records, current.processed);
        let prepared = match self
            .prepare_chunk(material, &records[current.processed..through], rules)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                return self
                    .finish_without_effect(current, error.to_string(), State::Refused)
                    .await;
            }
        };
        let mut done = current.clone();
        done.processed = through;
        done.next_anchor = records
            .get(through.wrapping_sub(1))
            .map(|r| r.end)
            .unwrap_or(0);
        done.steps += 1;
        done.archived += prepared.iter().filter(|p| p.old.is_some()).count();
        done.admitted += prepared.len();
        done.state = if through == records.len() {
            State::Completed
        } else {
            State::Running
        };
        done.complete = done.state == State::Completed;
        let effects = json!({
            "format": "sinex.replay.effects.v1",
            "operation_id": id,
            "step_no": current.steps,
            "range": {"from": current.next_anchor, "through": done.next_anchor},
            "replacements": prepared
                .iter()
                .map(|p| json!({
                    "old": p.old,
                    "new": p.id,
                    "material": material,
                    "anchor": p.anchor,
                    "ordinal": 0,
                }))
                .collect::<Vec<_>>(),
        });
        let effects_ref = self.store_record(&effects).await?;
        let outcome_ref = self
            .store_record(&json!({"format": OUTCOME, "outcome": done}))
            .await?;
        let mut tx = self.connection.begin().await?;
        let producer =
            crate::atuin::parser_producer(&mut tx, self.run_id, crate::atuin::PARSER, rules)
                .await?;
        for p in prepared {
            let live: Option<(Uuid, Vec<u8>, String)> = sqlx::query_as(
                "SELECT l.event_id,l.record_hash,e.equivalence_key
                 FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id
                 WHERE l.store=0 AND l.key_hash=$1 AND e.source=$2
                   AND e.event_type='command.executed'
                 FOR UPDATE OF l",
            )
            .bind(&p.key_hash)
            .bind(crate::ATUIN_SOURCE)
            .fetch_optional(&mut *tx)
            .await?;
            let excluded = sqlx::query_scalar::<_, bool>(crate::exclusion::EXCLUDED)
                .bind(&p.key)
                .fetch_one(&mut *tx)
                .await?;
            if excluded
                || live.as_ref().map(|(id, _, _)| *id) != p.old
                || live.as_ref().is_some_and(|(_, hash, key)| {
                    Some(hash) != p.head_hash.as_ref() || key != &p.key
                })
            {
                return Err(Error::ReplayRefused(
                    "occurrence admission precondition changed",
                ));
            }
            if let Some(old) = p.old {
                crate::shell::dirty(&mut tx, old).await?;
                let seq: i64 = next_commit_seq(&mut tx).await?;
                if archive_replaced(&mut tx, old, p.id, seq, "replay", Some(id)).await? != 1 {
                    return Err(Error::ReplayRefused(
                        "occurrence admission precondition changed",
                    ));
                }
            }
            let seq: i64 = next_commit_seq(&mut tx).await?;
            insert_event(
                &mut tx,
                NewEvent {
                    id: p.id,
                    parsed: p.parsed,
                    material,
                    anchor: p.anchor,
                    end: p.end,
                    record_hash: &p.record_hash,
                    key: &p.key,
                    key_hash: &p.key_hash,
                    read_order: p.read_order,
                    commit_seq: seq,
                    run: self.run_id,
                    producer,
                    operation: Some(id),
                    written_by: json!({"kind": "operator", "principal": PRINCIPAL}),
                },
            )
            .await?;
        }
        sqlx::query(
            "INSERT INTO core.operation_effects (operation_id,step_no,effect_format,effects_ref)
             VALUES ($1,$2,'sinex.replay.effects.v1',$3)",
        )
        .bind(id)
        .bind(current.steps as i64)
        .bind(effects_ref)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE core.operations_log SET admission_state='admitted',
               outcome_format='sinex.replay.outcome.v1',outcome_ref=$2,progress=$3
             WHERE id=$1",
        )
        .bind(id)
        .bind(outcome_ref)
        .bind(json!(done))
        .execute(&mut *tx)
        .await?;
        let pause = json!({"operation": done, "effects": effects});
        crate::TestPause::reach(
            &mut self.pause,
            &format!("replay-staged-{}", current.steps),
            pause.clone(),
        )
        .await?;
        tx.commit().await?;
        // The step is committed; a failed test pause must not turn it into an error.
        let _ = crate::TestPause::reach(
            &mut self.pause,
            &format!("replay-committed-{}", current.steps),
            pause,
        )
        .await;
        Ok(json!(done))
    }
    /// The oldest unfinished replay operation, as a start request to resume.
    /// Operations whose stored records cannot be read are refused and skipped.
    pub(crate) async fn next_replay(&mut self) -> Result<Option<sinex_model::replay::Start>> {
        loop {
            let row = sqlx::query(
                "SELECT id,action_id,request_format,request_ref,progress
                 FROM core.operations_log
                 WHERE operation_type='replay' AND progress->>'state' IN ('recorded','running')
                 ORDER BY id LIMIT 1",
            )
            .fetch_optional(&mut self.connection)
            .await?;
            let Some(row) = row else { return Ok(None) };
            let request = match self.reader.replay_status(row.try_get("id")?).await {
                Err(error) => Err(error),
                Ok(_) if row.try_get::<String, _>("request_format")? != REQUEST => Err(
                    Error::ReplayRefused("unsupported stored replay request format"),
                ),
                Ok(_) => self
                    .reader
                    .action_record(row.try_get("request_ref")?, REQUEST)
                    .await
                    .and_then(RequestRecord::decode),
            };
            match request {
                Ok(request) => {
                    return Ok(Some(sinex_model::replay::Start {
                        query: format!("materials where id = {}", request.material_id),
                        action_id: row.try_get("action_id")?,
                        expected_basis: request.expected_basis,
                    }));
                }
                Err(error) => {
                    let progress: Outcome = serde_json::from_value(row.try_get("progress")?)
                        .map_err(|_| Error::Invalid("operation progress invalid"))?;
                    self.finish_without_effect(progress, error.to_string(), State::Refused)
                        .await?;
                }
            }
        }
    }
    async fn parser_settings(&mut self, rules: &[crate::PrivacyRule]) -> Result<ParserSettings> {
        let artifact: String = sqlx::query_scalar(
            "SELECT m.artifact_digest FROM core.runs r
             JOIN core.manifests m ON m.id=r.manifest_id WHERE r.id=$1",
        )
        .bind(self.run_id)
        .fetch_one(&mut self.connection)
        .await?;
        let policy = serde_json::to_vec(rules)
            .map_err(|_| Error::Invalid("cannot encode current privacy rules"))?;
        Ok(ParserSettings {
            format: "sinex.atuin.interpretation.v1".into(),
            parser: crate::atuin::PARSER.into(),
            artifact,
            privacy_rules_hash: format!("blake3:{}", blake3::hash(&policy).to_hex()),
        })
    }
    /// Hash of the part, its rows' recorded state and the parser settings. A
    /// start that names an expected fingerprint is refused if it changed.
    async fn replay_fingerprint(
        &mut self,
        material: Uuid,
        records: &[Record],
        rules: &[crate::PrivacyRule],
    ) -> Result<String> {
        let settings = self.parser_settings(rules).await?;
        let bytes = serde_json::to_vec(&json!({
            "format": "sinex.replay.basis.v3",
            "material": material,
            "records": records,
            "interpretation_basis": settings,
        }))
        .map_err(|_| Error::Invalid("cannot encode replay fingerprint"))?;
        Ok(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
    }
    async fn replay_status(&mut self, id: Uuid) -> Result<Value> {
        self.reader
            .replay_status(id)
            .await?
            .ok_or(Error::ReplayRefused("replay operation not found"))
    }
}
impl ReadPool {
    /// Read a stored operation record and check its format.
    pub(crate) async fn action_record(&self, id: Uuid, format: &str) -> Result<Value> {
        let mut lease = self.read_lease().await?;
        let row = sqlx::query(
            "SELECT b.blake3,m.durable_bytes FROM raw.source_material_registry m
             JOIN core.blobs b ON b.id=m.blob_id
             WHERE m.id=$1 AND m.material_kind='file' AND m.status='completed'
               AND m.source_identifier IN ('sinex.replay.action','sinex.action')
               AND b.status='present'",
        )
        .bind(id)
        .fetch_optional(lease.connection())
        .await?
        .ok_or(Error::ReplayRefused("stored operation record unavailable"))?;
        lease.finish();
        let length: i64 = row.try_get("durable_bytes")?;
        if !(1..=crate::MAX_STEP_BYTES as i64).contains(&length) {
            return Err(Error::ReplayRefused(
                "stored operation record has an invalid length",
            ));
        }
        let hash = crate::parse_hash(&row.try_get::<String, _>("blake3")?)?;
        let reader = self.1.clone();
        let bytes = tokio::task::spawn_blocking(move || {
            reader.read_range_checked::<hashed_blobs::Error>(
                &hash,
                Some(length as u64),
                0,
                length as u64,
                || Ok(()),
            )
        })
        .await?
        .map_err(|_| Error::ReplayRefused("stored operation record missing or corrupt"))?;
        if hashed_blobs::Hash::of(&bytes) != hash {
            return Err(Error::ReplayRefused(
                "stored operation record does not match its hash",
            ));
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| Error::ReplayRefused("stored operation record malformed"))?;
        if value["format"] != format {
            return Err(Error::ReplayRefused(
                "unsupported stored operation record format",
            ));
        }
        Ok(value)
    }
    /// An operation's progress, checked against its stored request and outcome.
    pub async fn replay_status(&self, id: Uuid) -> Result<Option<Value>> {
        let mut lease = self.read_lease().await?;
        let row = sqlx::query(
            "SELECT request_format,request_ref,outcome_format,outcome_ref,progress
             FROM core.operations_log
             WHERE id=$1 AND action_principal=$2 AND operation_type='replay'",
        )
        .bind(id)
        .bind(PRINCIPAL)
        .fetch_optional(lease.connection())
        .await?;
        lease.finish();
        let Some(row) = row else { return Ok(None) };
        if row.try_get::<String, _>("request_format")? != REQUEST {
            return Err(Error::ReplayRefused(
                "unsupported stored replay request format",
            ));
        }
        RequestRecord::decode(
            self.action_record(row.try_get("request_ref")?, REQUEST)
                .await?,
        )?;
        let progress: Value = row.try_get("progress")?;
        let reference: Option<Uuid> = row.try_get("outcome_ref")?;
        if let Some(reference) = reference {
            if row
                .try_get::<Option<String>, _>("outcome_format")?
                .as_deref()
                != Some(OUTCOME)
            {
                return Err(Error::ReplayRefused(
                    "unsupported stored replay outcome format",
                ));
            }
            let stored = self.action_record(reference, OUTCOME).await?;
            let outcome: Outcome = serde_json::from_value(stored["outcome"].clone())
                .map_err(|_| Error::ReplayRefused("stored replay outcome invalid"))?;
            if json!(outcome) != progress {
                return Err(Error::ReplayRefused(
                    "stored outcome differs from committed progress",
                ));
            }
        } else if progress["state"] != "recorded" || progress["steps"] != 0 {
            return Err(Error::ReplayRefused("committed replay outcome unavailable"));
        }
        Ok(Some(progress))
    }
    pub async fn replay_operations(&self) -> Result<Vec<Outcome>> {
        let mut lease = self.read_lease().await?;
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM core.operations_log WHERE operation_type='replay'
             ORDER BY id DESC LIMIT 32",
        )
        .fetch_all(lease.connection())
        .await?;
        lease.finish();
        let mut outcomes = Vec::new();
        for id in ids {
            outcomes.push(
                serde_json::from_value(
                    self.replay_status(id)
                        .await?
                        .ok_or(Error::Invalid("operation disappeared"))?,
                )
                .map_err(|_| Error::Invalid("stored replay outcome invalid"))?,
            );
        }
        Ok(outcomes)
    }
}
