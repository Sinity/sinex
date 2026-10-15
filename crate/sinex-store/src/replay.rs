//! Bounded retained Atuin replacement through the private writer. No provider reads.
use crate::{
    BLOB_LOOKUP, BLOB_PRESENT, BLOB_REGISTER, COUNTER, Error, MATERIAL, ReadPool, ReplayCommand,
    Result, WriterPool,
};
use serde_json::{Value, json};
use sinex_model::replay::{Outcome, State};
use sqlx::{Connection, Row};
use uuid::Uuid;

const PRINCIPAL: &str = "foundation.operator";
const MAX_SELECTION: usize = 4096;
const CHUNK: usize = 64;

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
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    anchor: i64,
    end: i64,
    interpretation: Option<Uuid>,
}
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InterpretationBasis {
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
    interpretation_basis: InterpretationBasis,
}
impl RequestRecord {
    fn decode(value: Value) -> Result<Self> {
        let record: Self = serde_json::from_value(value)
            .map_err(|_| Error::ReplayRefused("unsupported retained replay request"))?;
        if record.format != "sinex.replay.request.v1"
            || record.method != "replay.start"
            || record.interpretation_basis.format != "sinex.atuin.interpretation.v1"
            || !matches!(
                record.interpretation_basis.parser.as_str(),
                "atuin-v1" | crate::atuin::PARSER
            )
        {
            return Err(Error::ReplayRefused(
                "unsupported retained replay request contract",
            ));
        }
        Ok(record)
    }
}
struct Prepared {
    old: Option<Uuid>,
    id: Uuid,
    anchor: i64,
    end: i64,
    record_hash: Vec<u8>,
    key: String,
    key_hash: Vec<u8>,
    read_order: i64,
    parsed: crate::atuin::Parsed,
}
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
fn outcome(id: Uuid, action: Uuid, selected: usize) -> Outcome {
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
impl WriterPool {
    async fn replay_scope(&mut self, material: Uuid) -> Result<Vec<Record>> {
        let durable:Option<i64>=sqlx::query_scalar("SELECT durable_bytes FROM raw.source_material_registry WHERE id=$1 AND source_identifier=$2 AND material_kind='stream_chunk' AND status<>'capturing' AND parser_provenance->>'parser' IN ('atuin-v1','atuin-v2') AND parser_provenance->>'representation'='canonical_sqlite_rows'").bind(material).bind(crate::ATUIN_SOURCE).fetch_optional(&mut self.connection).await?;
        let durable = durable.ok_or(Error::ReplayRefused(
            "replay requires an existing closed retained Atuin logical-row material",
        ))?;
        self.verify_material(material).await?;
        let mut records = Vec::new();
        let mut anchor = 0;
        while anchor < durable {
            if records.len() == MAX_SELECTION {
                return Err(Error::ReplayRefused(
                    "material exceeds the supported 4096 retained record selection budget",
                ));
            }
            let (_, end) = self
                .reader
                .retained_record_raw(material, anchor, None, false)
                .await
                .map_err(|_| Error::ReplayRefused("retained row unavailable or malformed"))?;
            let interpretation:Option<Uuid>=sqlx::query_scalar("SELECT id FROM core.events e WHERE source_material_id=$1 AND anchor_byte=$2 AND ordinal=0 AND source=$3 AND event_type='command.executed' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id)").bind(material).bind(anchor).bind(crate::ATUIN_SOURCE).fetch_optional(&mut self.connection).await?;
            records.push(Record {
                anchor,
                end,
                interpretation,
            });
            anchor = end;
        }
        Ok(records)
    }
    async fn verify_material(&mut self, material: Uuid) -> Result<()> {
        let name:String=sqlx::query_scalar("SELECT b.blake3 FROM raw.source_material_registry m JOIN core.blobs b ON b.id=m.blob_id WHERE m.id=$1 AND b.status='present'").bind(material).fetch_optional(&mut self.connection).await?.ok_or(Error::ReplayRefused("retained backing unavailable"))?;
        let hash = crate::parse_hash(&name)?;
        let blobs = self.blobs.clone();
        if tokio::task::spawn_blocking(move || blobs.verify(&hash)).await??
            != hashed_blobs::Verified::Intact
        {
            return Err(Error::ReplayRefused("retained backing missing or corrupt"));
        }
        Ok(())
    }
    // Row-first blob custody is shared with normal admission. The material has no
    // selection authority until an operation/effect binding commits its reference.
    async fn retain_record(&mut self, value: &Value) -> Result<Uuid> {
        let bytes = serde_json::to_vec(value)
            .map_err(|_| Error::Invalid("cannot encode retained replay record"))?;
        if bytes.len() > crate::MAX_STEP_BYTES {
            return Err(Error::ReplayRefused(
                "retained replay record exceeds byte budget",
            ));
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
            return Err(Error::Invalid("retained action custody differs"));
        }
        let length = bytes.len() as i64;
        let store = self.blobs.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            store.put(bytes.as_slice(), 3)?;
            if store.verify(&hash)? != hashed_blobs::Verified::Intact {
                return Err(Error::Invalid("retained action verification failed"));
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
            .bind("sinex.replay.action")
            .bind(blob)
            .bind(length)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(id)
    }
    async fn prepare_replay(
        &mut self,
        material: Uuid,
        records: &[Record],
        rules: &[crate::PrivacyRule],
    ) -> Result<Vec<Prepared>> {
        self.verify_material(material).await?;
        let (input,order):(Uuid,i64)=sqlx::query_as("SELECT m.input_id,(extract(epoch FROM o.observed_at)*1000000000)::bigint FROM raw.source_material_registry m JOIN raw.observations o ON o.id=m.observation_id WHERE m.id=$1").bind(material).fetch_one(&mut self.connection).await?;
        let mut prepared = Vec::new();
        let mut bytes = 0;
        for expected in records {
            bytes += expected.end - expected.anchor;
            if bytes > crate::MAX_STEP_BYTES as i64 {
                return Err(Error::ReplayRefused(
                    "replay chunk exceeds 1 MiB retained evidence budget",
                ));
            }
            let old:Option<Value>=sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e WHERE source_material_id=$1 AND anchor_byte=$2 AND ordinal=0 AND source=$3 AND event_type='command.executed' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id)").bind(material).bind(expected.anchor).bind(crate::ATUIN_SOURCE).fetch_optional(&mut self.connection).await?;
            let old_id: Option<Uuid> = old
                .as_ref()
                .map(|row| {
                    serde_json::from_value(row["id"].clone())
                        .map_err(|_| Error::Invalid("selected interpretation identity invalid"))
                })
                .transpose()?;
            if old_id != expected.interpretation {
                return Err(Error::ReplayRefused(
                    "selected interpretation changed; request a new action",
                ));
            }
            let (mut record, _) = self
                .reader
                .retained_record_raw(material, expected.anchor, Some(expected.end), false)
                .await
                .map_err(|_| Error::ReplayRefused("retained row unavailable or malformed"))?;
            record
                .as_object_mut()
                .ok_or(Error::ReplayRefused("retained row is not an object"))?
                .remove("_from");
            record.sort_all_objects();
            let fingerprint = blake3::hash(
                &serde_json::to_vec(&record)
                    .map_err(|_| Error::Invalid("retained encoding failed"))?,
            );
            let record_hash = fingerprint.as_bytes()[..16].to_vec();
            let mut parsed = crate::atuin::parse(&record).map_err(Error::ReplayRefused)?;
            let key = json!([input, parsed.native_id]).to_string();
            let key_hash = blake3::hash(
                format!("{}\x1fcommand.executed\x1f{key}", crate::ATUIN_SOURCE).as_bytes(),
            )
            .as_bytes()[..16]
                .to_vec();
            if let Some(row) = &old {
                let hash = format!(
                    "\\x{}",
                    record_hash
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                );
                if row["record_hash"] != hash
                    || row["equivalence_key"] != key
                    || row["offset_end"] != expected.end
                {
                    return Err(Error::ReplayRefused(
                        "retained row differs from admitted occurrence evidence",
                    ));
                }
            }
            let equivalent:Option<(Uuid,i64)>=sqlx::query_as("SELECT event_id,read_order FROM core.live_equivalence WHERE store=0 AND key_hash=$1").bind(&key_hash).fetch_optional(&mut self.connection).await?;
            if equivalent.as_ref().map(|(id, _)| *id) != old_id {
                return Err(Error::ReplayRefused(
                    "selected source key has another current interpretation; request current material",
                ));
            }
            crate::atuin::mask(&mut parsed.payload, rules)?;
            prepared.push(Prepared {
                old: old_id,
                id: Uuid::now_v7(),
                anchor: expected.anchor,
                end: expected.end,
                record_hash,
                key,
                key_hash,
                read_order: equivalent.map(|(_, order)| order).unwrap_or(order),
                parsed,
            });
        }
        Ok(prepared)
    }
    async fn finish_without_effect(
        &mut self,
        mut result: Outcome,
        reason: String,
        state: State,
    ) -> Result<Value> {
        result.state = state;
        result.reason = Some(reason);
        let value = json!(result);
        let retained = self
            .retain_record(&json!({"format":"sinex.replay.outcome.v1","outcome":value}))
            .await?;
        sqlx::query("UPDATE core.operations_log SET admission_state=CASE WHEN (progress->>'steps')::bigint>0 THEN 'admitted' ELSE 'refused' END,outcome_format='sinex.replay.outcome.v1',outcome_ref=$2,progress=$3 WHERE id=$1").bind(result.operation_id).bind(retained).bind(&value).execute(&mut self.connection).await?;
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
            ReplayCommand::Plan(p, rules) => {
                let material = selection(&p.query)?;
                let ids = self.replay_scope(material).await?;
                let mut from = 0;
                let mut chunks = 0;
                while from < ids.len() {
                    let through = chunk_end(&ids, from);
                    self.prepare_replay(material, &ids[from..through], &rules)
                        .await?;
                    from = through;
                    chunks += 1;
                }
                Ok(
                    json!({"material_id":material,"basis":self.replay_basis(material,&ids,&rules).await?,"selected":ids.len(),"chunks":chunks.max(1),"chunk_max_records":CHUNK,"chunk_max_bytes":crate::MAX_STEP_BYTES,"representation":"canonical_sqlite_rows","dry_run":true,"search_projection":{"enabled":self.shell_producer.is_some(),"status":self.reader.projection_status().await?}}),
                )
            }
            ReplayCommand::Start(p) => {
                let rules = self.replay_rules.clone().ok_or(Error::ReplayRefused(
                    "current replay operator authorization is unavailable",
                ))?;
                let material = selection(&p.query)?;

                // Action lookup precedes current-state preconditions. It remains bound to
                // the versioned semantic request, independent of the credential bytes.
                let old=sqlx::query("SELECT id,scope,progress,request_format,request_ref FROM core.operations_log WHERE action_principal=$1 AND action_id=$2").bind(PRINCIPAL).bind(p.action_id).fetch_optional(&mut self.connection).await?;
                let (id, ids, current) = if let Some(old) = old {
                    let scope: Value = old.try_get("scope")?;
                    if old.try_get::<String, _>("request_format")? != "sinex.replay.request.v1" {
                        return Err(Error::ReplayRefused(
                            "unsupported retained replay request format",
                        ));
                    }
                    let request = RequestRecord::decode(
                        self.reader
                            .action_record(old.try_get("request_ref")?, "sinex.replay.request.v1")
                            .await?,
                    )?;
                    if request.material_id != material || request.expected_basis != p.expected_basis
                    {
                        return Err(Error::ReplayRefused(
                            "action identity is already bound to a different semantic request",
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
                        return Ok(json!(current));
                    }
                    if request.interpretation_basis != self.interpretation_basis(&rules).await? {
                        return self
                            .finish_without_effect(
                                current,
                                "interpretation basis changed; a new replay action is required"
                                    .into(),
                                State::Refused,
                            )
                            .await;
                    }
                    if scope["preflight_validated"] != true {
                        let reason = scope["preflight_refusal"]
                            .as_str()
                            .unwrap_or("retained replay preflight unavailable")
                            .to_string();
                        return self
                            .finish_without_effect(current, reason, State::Refused)
                            .await;
                    }
                    let ids: Vec<Record> = serde_json::from_value(scope["records"].clone())
                        .map_err(|_| Error::Invalid("operation basis invalid"))?;
                    (old.try_get::<Uuid, _>("id")?, ids, current)
                } else {
                    let id = Uuid::now_v7();
                    let scope = self.replay_scope(material).await;
                    let ids = scope.as_ref().cloned().unwrap_or_default();
                    let initial = outcome(id, p.action_id, ids.len());
                    let request = RequestRecord {
                        format: "sinex.replay.request.v1".into(),
                        method: "replay.start".into(),
                        material_id: material,
                        expected_basis: p.expected_basis.clone(),
                        interpretation_basis: self.interpretation_basis(&rules).await?,
                    };
                    // Resolve and validate the entire bounded input before publishing
                    // a resumable operation. A crash cannot skip unfinished preflight.
                    let mut refusal = scope.err().map(|error| error.to_string());
                    if refusal.is_none() {
                        let basis = self.replay_basis(material, &ids, &rules).await?;
                        if p.expected_basis
                            .as_ref()
                            .is_some_and(|expected| expected != &basis)
                        {
                            refusal = Some(
                                "preview basis changed; obtain a new plan and use a new action"
                                    .into(),
                            );
                        }
                    }
                    let mut from = 0;
                    while refusal.is_none() && from < ids.len() {
                        let through = chunk_end(&ids, from);
                        if let Err(error) = self
                            .prepare_replay(material, &ids[from..through], &rules)
                            .await
                        {
                            refusal = Some(error.to_string());
                        }
                        from = through;
                    }
                    let retained = self.retain_record(&json!(request)).await?;
                    sqlx::query("INSERT INTO core.operations_log (id,operation_type,actor,action_principal,action_id,request_format,request_ref,scope,admission_state,progress) VALUES ($1,'replay',$2,$3,$4,'sinex.replay.request.v1',$5,$6,'recorded',$7)").bind(id).bind(json!({"kind":"operator","principal":PRINCIPAL,"profile":"explicit_replay_token"})).bind(PRINCIPAL).bind(p.action_id).bind(retained).bind(json!({"records":ids,"preflight_validated":refusal.is_none(),"preflight_refusal":refusal})).bind(json!(initial)).execute(&mut self.connection).await?;
                    if let Some(reason) = refusal {
                        return self
                            .finish_without_effect(initial, reason, State::Refused)
                            .await;
                    }
                    (id, ids, initial)
                };
                let through = chunk_end(&ids, current.processed);
                let prepared = match self
                    .prepare_replay(material, &ids[current.processed..through], &rules)
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
                done.next_anchor = ids.get(through.wrapping_sub(1)).map(|r| r.end).unwrap_or(0);
                done.steps += 1;
                done.archived += prepared.iter().filter(|p| p.old.is_some()).count();
                done.admitted += prepared.len();
                done.state = if through == ids.len() {
                    State::Completed
                } else {
                    State::Running
                };
                done.complete = done.state == State::Completed;
                let effects = json!({"format":"sinex.replay.effects.v1","operation_id":id,"step_no":current.steps,"range":{"from":current.next_anchor,"through":done.next_anchor},"replacements":prepared.iter().map(|p|json!({"old":p.old,"new":p.id,"material":material,"anchor":p.anchor,"ordinal":0})).collect::<Vec<_>>()});
                let effect_ref = self.retain_record(&effects).await?;
                let outcome_ref = self
                    .retain_record(&json!({"format":"sinex.replay.outcome.v1","outcome":done}))
                    .await?;
                let mut tx = self.connection.begin().await?;
                let producer =
                    crate::atuin::parser_producer(&mut tx, self.run_id, crate::atuin::PARSER)
                        .await?;
                for p in prepared {
                    if let Some(old) = p.old {
                        crate::shell::dirty(&mut tx, old).await?;
                        let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
                        sqlx::query("INSERT INTO audit.archived (id,archived_seq,reason,replaced_by,operation_id) VALUES ($1,$2,'replay',$3,$4)").bind(old).bind(seq).bind(p.id).bind(id).execute(&mut *tx).await?;
                        sqlx::query("INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id) VALUES ($1,$2,1,0,2)").bind(seq).bind(old).execute(&mut *tx).await?;
                        let coordinates = sqlx::query(
                            "DELETE FROM core.live_coordinates WHERE store=0 AND event_id=$1",
                        )
                        .bind(old)
                        .execute(&mut *tx)
                        .await?;
                        if coordinates.rows_affected() != 1 {
                            return Err(Error::ReplayRefused(
                                "occurrence admission precondition changed",
                            ));
                        }
                    }
                    let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
                    let projected = p
                        .parsed
                        .time_evidence
                        .project(
                            sinex_model::events::EventId::try_from(p.id)
                                .map_err(|_| Error::Invalid("invalid replay interpretation ID"))?,
                            sinex_model::events::CaptureClock::Producer,
                        )
                        .map_err(|_| Error::Invalid("invalid replay time evidence"))?;
                    sqlx::query("INSERT INTO core.events (id,source,event_type,payload,ts_orig,ts_orig_tiebreak,ts_quality,ts_clock,time_suspect,payload_schema_id,commit_seq,source_material_id,anchor_byte,offset_start,offset_end,offset_kind,ordinal,record_hash,record_state,run_id,equivalence_key,created_by_operation,written_by,producer_version_id) VALUES ($1,$2,'command.executed',$3,$4::text::timestamptz,$5,'intrinsic','producer',$6,2,$7,$8,$9,$9,$10,'bytes',0,$11,$12,$13,$14,$15,$16,$17)")
                        .bind(p.id).bind(crate::ATUIN_SOURCE).bind(p.parsed.payload).bind(p.parsed.time).bind(p.parsed.nanos).bind(projected.suspect).bind(seq).bind(material).bind(p.anchor).bind(p.end).bind(&p.record_hash).bind(if p.parsed.removed{"removed"}else{"present"}).bind(self.run_id).bind(&p.key).bind(id).bind(json!({"kind":"operator","principal":PRINCIPAL})).bind(producer).execute(&mut *tx).await?;
                    sqlx::query("INSERT INTO core.live_coordinates (store,occ_hash,event_id) VALUES (0,$1,$2)").bind(crate::coordinate_hash(material,p.anchor,"command.executed",0)).bind(p.id).execute(&mut *tx).await?;
                    sqlx::query("INSERT INTO core.live_equivalence (store,key_hash,event_id,read_order,record_hash) VALUES (0,$1,$2,$3,$4) ON CONFLICT (store,key_hash) DO UPDATE SET event_id=EXCLUDED.event_id,read_order=EXCLUDED.read_order,record_hash=EXCLUDED.record_hash").bind(p.key_hash).bind(p.id).bind(p.read_order).bind(p.record_hash).execute(&mut *tx).await?;
                    crate::shell::dirty(&mut tx, p.id).await?;
                    sqlx::query("INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id) VALUES ($1,$2,0,0,2)").bind(seq).bind(p.id).execute(&mut *tx).await?;
                }
                sqlx::query("INSERT INTO core.operation_effects (operation_id,step_no,effect_format,effects_ref) VALUES ($1,$2,'sinex.replay.effects.v1',$3)").bind(id).bind(current.steps as i64).bind(effect_ref).execute(&mut *tx).await?;
                sqlx::query("UPDATE core.operations_log SET admission_state='admitted',outcome_format='sinex.replay.outcome.v1',outcome_ref=$2,progress=$3 WHERE id=$1").bind(id).bind(outcome_ref).bind(json!(done)).execute(&mut *tx).await?;
                crate::QualificationBoundary::reach(
                    &mut self.qualification,
                    &format!("replay-staged-{}", current.steps),
                    json!({"operation":done,"effects":effects}),
                )
                .await?;
                tx.commit().await?;
                // A lost controller after commit cannot turn durable success into failure.
                let _ = crate::QualificationBoundary::reach(
                    &mut self.qualification,
                    &format!("replay-committed-{}", current.steps),
                    json!({"operation":done,"effects":effects}),
                )
                .await;
                Ok(json!(done))
            }
        }
    }
    pub(crate) async fn next_replay(&mut self) -> Result<Option<sinex_model::replay::Start>> {
        loop {
            let row=sqlx::query("SELECT id,action_id,request_format,request_ref,progress FROM core.operations_log WHERE progress->>'state' IN ('recorded','running') ORDER BY id LIMIT 1").fetch_optional(&mut self.connection).await?;
            let Some(row) = row else { return Ok(None) };
            if let Err(error) = self.reader.replay_status(row.try_get("id")?).await {
                let progress: Outcome = serde_json::from_value(row.try_get("progress")?)
                    .map_err(|_| Error::Invalid("operation progress invalid"))?;
                self.finish_without_effect(progress, error.to_string(), State::Refused)
                    .await?;
                continue;
            }
            let request =
                if row.try_get::<String, _>("request_format")? == "sinex.replay.request.v1" {
                    self.reader
                        .action_record(row.try_get("request_ref")?, "sinex.replay.request.v1")
                        .await
                        .and_then(RequestRecord::decode)
                } else {
                    Err(Error::ReplayRefused(
                        "unsupported retained replay request format",
                    ))
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
    async fn interpretation_basis(
        &mut self,
        rules: &[crate::PrivacyRule],
    ) -> Result<InterpretationBasis> {
        let artifact:String=sqlx::query_scalar("SELECT m.artifact_digest FROM core.runs r JOIN core.manifests m ON m.id=r.manifest_id WHERE r.id=$1").bind(self.run_id).fetch_one(&mut self.connection).await?;
        let policy = serde_json::to_vec(rules)
            .map_err(|_| Error::Invalid("cannot encode current privacy rules"))?;
        Ok(InterpretationBasis {
            format: "sinex.atuin.interpretation.v1".into(),
            parser: crate::atuin::PARSER.into(),
            artifact,
            privacy_rules_hash: format!("blake3:{}", blake3::hash(&policy).to_hex()),
        })
    }
    async fn replay_basis(
        &mut self,
        material: Uuid,
        ids: &[Record],
        rules: &[crate::PrivacyRule],
    ) -> Result<String> {
        let interpretation_basis = self.interpretation_basis(rules).await?;
        let bytes=serde_json::to_vec(&json!({"format":"sinex.replay.basis.v1","material":material,"records":ids,"interpretation_basis":interpretation_basis})).map_err(|_|Error::Invalid("cannot encode replay basis"))?;
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
    async fn action_record(&self, id: Uuid, format: &str) -> Result<Value> {
        let mut lease = self.read_lease().await?;
        let row=sqlx::query("SELECT b.blake3,m.durable_bytes FROM raw.source_material_registry m JOIN core.blobs b ON b.id=m.blob_id WHERE m.id=$1 AND m.material_kind='file' AND m.status='completed' AND m.source_identifier='sinex.replay.action' AND b.status='present'").bind(id).fetch_optional(lease.connection()).await?.ok_or(Error::ReplayRefused("retained action backing unavailable"))?;
        lease.finish();
        let length: i64 = row.try_get("durable_bytes")?;
        if !(1..=crate::MAX_STEP_BYTES as i64).contains(&length) {
            return Err(Error::ReplayRefused(
                "retained action record exceeds its contract",
            ));
        }
        let hash = crate::parse_hash(&row.try_get::<String, _>("blake3")?)?;
        let reader = self.1.clone();
        let bytes = tokio::task::spawn_blocking(move || reader.read_range(&hash, 0, length as u64))
            .await?
            .map_err(|_| Error::ReplayRefused("retained action backing missing or corrupt"))?;
        if hashed_blobs::Hash::of(&bytes) != hash {
            return Err(Error::ReplayRefused(
                "retained action content identity differs",
            ));
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| Error::ReplayRefused("retained action record malformed"))?;
        if value["format"] != format {
            return Err(Error::ReplayRefused("unsupported retained action format"));
        }
        Ok(value)
    }
    pub async fn replay_status(&self, id: Uuid) -> Result<Option<Value>> {
        let mut lease = self.read_lease().await?;
        let row=sqlx::query("SELECT request_format,request_ref,outcome_format,outcome_ref,progress FROM core.operations_log WHERE id=$1 AND action_principal=$2").bind(id).bind(PRINCIPAL).fetch_optional(lease.connection()).await?;
        lease.finish();
        let Some(row) = row else { return Ok(None) };
        if row.try_get::<String, _>("request_format")? != "sinex.replay.request.v1" {
            return Err(Error::ReplayRefused(
                "unsupported retained replay request format",
            ));
        }
        RequestRecord::decode(
            self.action_record(row.try_get("request_ref")?, "sinex.replay.request.v1")
                .await?,
        )?;
        let progress: Value = row.try_get("progress")?;
        let reference: Option<Uuid> = row.try_get("outcome_ref")?;
        if let Some(reference) = reference {
            if row
                .try_get::<Option<String>, _>("outcome_format")?
                .as_deref()
                != Some("sinex.replay.outcome.v1")
            {
                return Err(Error::ReplayRefused(
                    "unsupported retained replay outcome format",
                ));
            }
            let retained = self
                .action_record(reference, "sinex.replay.outcome.v1")
                .await?;
            let actual: Outcome = serde_json::from_value(retained["outcome"].clone())
                .map_err(|_| Error::ReplayRefused("retained replay outcome invalid"))?;
            if json!(actual) != progress {
                return Err(Error::ReplayRefused(
                    "retained outcome differs from committed progress",
                ));
            }
        } else if progress["state"] != "recorded" || progress["steps"] != 0 {
            return Err(Error::ReplayRefused("committed replay outcome unavailable"));
        }
        Ok(Some(progress))
    }
    pub async fn replay_operations(&self) -> Result<Vec<Outcome>> {
        let mut lease = self.read_lease().await?;
        let ids: Vec<Uuid> =
            sqlx::query_scalar("SELECT id FROM core.operations_log ORDER BY id DESC LIMIT 32")
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
