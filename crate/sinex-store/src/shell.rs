//! Selected two-source shell reconciliation. PostgreSQL work tables spill;
//! authoritative dirty seeds and an immutable input frontier survive a restart.
use crate::{COUNTER, Error, Result, WriterPool};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{Connection, Row};
use uuid::Uuid;

const WORK_TABLES: &[&str] = &[
    "DROP TABLE IF EXISTS pg_temp.sinex_shell_nodes,pg_temp.sinex_shell_groups",
    "CREATE TEMP TABLE sinex_shell_nodes(id uuid PRIMARY KEY,source smallint NOT NULL,host text,command text NOT NULL,command_hash text NOT NULL,at_ns numeric,digest text NOT NULL,anchor bigint NOT NULL,ekey text NOT NULL,paired boolean NOT NULL DEFAULT false,count2 bigint NOT NULL DEFAULT 0,count60 bigint NOT NULL DEFAULT 0)",
    "CREATE INDEX ON sinex_shell_nodes(source,command_hash,at_ns)",
    "CREATE INDEX sinex_shell_nodes_pending ON sinex_shell_nodes(id) WHERE NOT paired",
    "CREATE TEMP TABLE sinex_shell_groups(token uuid PRIMARY KEY,parents uuid[] NOT NULL,strategy smallint NOT NULL,done boolean NOT NULL DEFAULT false,retained_id uuid)",
    "CREATE INDEX sinex_shell_groups_retained ON sinex_shell_groups(retained_id)",
    "CREATE INDEX sinex_shell_groups_pending ON sinex_shell_groups(token) WHERE NOT done",
];
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    id: String,
    version: i32,
    implementation: String,
    sources: Vec<String>,
    bucket_seconds: i64,
    window_ns: i64,
    fallback_ns: i64,
    normalization: String,
    partition: Vec<String>,
    time: String,
    unique: String,
    prefer: std::collections::BTreeMap<String, Vec<String>>,
}
fn rule() -> Rule {
    let r: Rule =
        serde_json::from_str(include_str!("shell-rule.json")).expect("registered shell rule");
    assert!(
        r.sources == [crate::ATUIN_SOURCE, crate::ZSH_SOURCE]
            && r.normalization == "trim"
            && r.partition == ["hostname"]
            && r.time == "best_quality"
            && r.unique == "mutual_single_at_strategy_entry"
    );
    r
}
pub(crate) fn payload_schema() -> Value {
    json!({"type":"object","required":["command","canonical","observed_by","association","field_evidence","owner_hour","claim_scope","exact_execution_count"],"properties":{"command":{"type":"string"},"canonical":{"type":["string","null"]},"observed_by":{"type":"array","minItems":1,"maxItems":2,"uniqueItems":true,"items":{"enum":[crate::ATUIN_SOURCE,crate::ZSH_SOURCE]}},"association":{"type":"object","required":["basis","rule","version","ambiguous","candidates","candidate_counts","strategy"]},"field_evidence":{"type":"object"},"owner_hour":{"type":["integer","null"]},"claim_scope":{"const":"selected-policy-grouping"},"exact_execution_count":{"type":"null"}}})
}
pub(crate) async fn dirty(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    parent: Uuid,
) -> Result<()> {
    sqlx::query("INSERT INTO derivation.shell_dirty(parent_id,dirty_seq) SELECT $1,seq FROM core.commit_counter WHERE one ON CONFLICT(parent_id) DO UPDATE SET dirty_seq=greatest(shell_dirty.dirty_seq,EXCLUDED.dirty_seq)").bind(parent).execute(&mut **tx).await?;
    Ok(())
}
#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Scan,
    Count2,
    Pair2,
    Count60,
    Pair60,
    Single,
    Publish,
    Archive,
}
pub(crate) struct Work {
    pub basis: i64,
    phase: Phase,
    cursor: Option<Uuid>,
    queued_turns: u64,
}
fn canonical(command: &str) -> Option<String> {
    let words = shlex::split(command)?;
    let mut words = words.iter().skip_while(|w| {
        w.split_once('=').is_some_and(|(k, _)| {
            !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
    });
    let program = words.next()?.rsplit('/').next()?.to_string();
    // A summary must not strip credential recognition context (for example a
    // URL authority passed as the program). Keep the summary unknown instead.
    if program.is_empty()
        || !program
            .chars()
            .all(|c| c.is_alphanumeric() || "_+.-".contains(c))
    {
        return None;
    }
    const ACTIONS: &[&str] = &[
        "git",
        "cargo",
        "nix",
        "systemctl",
        "xtask",
        "sinexctl",
        "agentctl",
        "bd",
        "docker",
        "npm",
        "kubectl",
    ];
    const MUTATIONS: &[(&str, &str)] = &[
        ("sed", "-i"),
        ("perl", "-i"),
        ("find", "-delete"),
        ("find", "-exec"),
        ("rsync", "--delete"),
    ];
    let args: Vec<_> = words.collect();
    let mut out = program.clone();
    if ACTIONS.contains(&program.as_str()) {
        if let Some(action) = args.first() {
            out.push(' ');
            out.push_str(action);
        }
    }
    for (p, flag) in MUTATIONS {
        if *p == program
            && args
                .iter()
                .any(|a| a.as_str() == *flag || (*flag == "-i" && a.starts_with("-i")))
        {
            out.push(' ');
            out.push_str(flag);
        }
    }
    Some(out)
}
impl WriterPool {
    pub(crate) async fn shell_enable(&mut self) -> Result<()> {
        if self.shell_producer.is_some() {
            return Ok(());
        }
        let r = rule();
        let id = Uuid::now_v7();
        sqlx::query("INSERT INTO core.producer_versions(id,name,version,run_id,configuration) VALUES($1,'fusion.shell-command',$2,$3,$4)").bind(id).bind(&r.implementation).bind(self.run_id).bind(serde_json::from_str::<Value>(include_str!("shell-rule.json")).unwrap()).execute(&mut self.connection).await?;
        self.shell_producer = Some(id);
        Ok(())
    }
    async fn shell_begin(&mut self) -> Result<Option<Work>> {
        let dirty: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM derivation.shell_dirty)")
            .fetch_one(&mut self.connection)
            .await?;
        if !dirty {
            return Ok(None);
        }
        let progress = sqlx::query("SELECT basis,phase FROM derivation.shell_progress WHERE one")
            .fetch_one(&mut self.connection)
            .await?;
        let phase: String = progress.try_get("phase")?;
        let basis: i64 = if phase == "settled" {
            sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
                .fetch_one(&mut self.connection)
                .await?
        } else {
            progress.try_get("basis")?
        };
        let incomparable:bool=sqlx::query_scalar("SELECT count(DISTINCT e.source)>1 AND coalesce(bool_or(NOT coalesce(p.configuration#>>'{shell_field_basis,contract}'='sinex.shell-field-basis.v1' AND p.configuration#>'{shell_field_basis,comparable}'='true'::jsonb,false)),false) FROM core.events e LEFT JOIN core.producer_versions p ON p.id=e.producer_version_id WHERE ((e.source='terminal.atuin-history' AND e.event_type='command.executed') OR (e.source='terminal.zsh-history' AND e.event_type='command.imported')) AND e.record_state='present' AND e.commit_seq<=$1 AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<=$1)").bind(basis).fetch_one(&mut self.connection).await?;
        if incomparable {
            self.shell_failed = true;
            sqlx::query("UPDATE derivation.shell_progress SET failure='cross-source original-value comparison unavailable for retained transformed or unrecorded field basis; use supported retained replay under current untransformed rules or disable the two-source capability' WHERE one").execute(&mut self.connection).await?;
            return Ok(None);
        }
        // A material digest is the stable ordering witness. Do not invent a digest
        // for a part which the source is still extending.
        let open:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id WHERE e.source IN ('terminal.atuin-history','terminal.zsh-history') AND e.event_type IN ('command.executed','command.imported') AND e.commit_seq<=$1 AND m.blob_id IS NULL AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<=$1))").bind(basis).fetch_one(&mut self.connection).await?;
        if open {
            return Ok(None);
        }
        for sql in WORK_TABLES {
            sqlx::query(sql).execute(&mut self.connection).await?;
        }
        sqlx::query("UPDATE derivation.shell_progress SET basis=$1,phase='scan',failure=NULL,scanned=0,replaced=0 WHERE one").bind(basis).execute(&mut self.connection).await?;
        Ok(Some(Work {
            basis,
            phase: Phase::Scan,
            cursor: None,
            queued_turns: 0,
        }))
    }
    pub(crate) async fn shell_step(&mut self, queued_commands: usize) -> Result<bool> {
        if self.shell_producer.is_none() {
            return Ok(false);
        }
        let Some(mut work) = (match self.shell_work.take() {
            Some(w) => Some(w),
            None => self.shell_begin().await?,
        }) else {
            return Ok(false);
        };
        if queued_commands > 0 {
            work.queued_turns += 1;
        }
        let r = rule();
        match work.phase {
            Phase::Scan => {
                let rows=sqlx::query("SELECT e.id,e.source,coalesce(nullif(e.host,''),nullif(e.payload->>'hostname','')) AS host,e.payload->>'command' AS command,(extract(epoch FROM e.ts_orig)*1000000000+coalesce(e.ts_orig_tiebreak,0))::numeric::text AS at_ns,b.blake3,e.anchor_byte,e.equivalence_key FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id JOIN core.blobs b ON b.id=m.blob_id WHERE ((e.source='terminal.atuin-history' AND e.event_type='command.executed') OR (e.source='terminal.zsh-history' AND e.event_type='command.imported')) AND e.record_state='present' AND e.commit_seq<=$1 AND ($2::uuid IS NULL OR e.id>$2) AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<=$1) ORDER BY e.id LIMIT 64").bind(work.basis).bind(work.cursor).fetch_all(&mut self.connection).await?;
                if rows.is_empty() {
                    work.phase = Phase::Count2;
                    work.cursor = None;
                } else {
                    let mut tx = self.connection.begin().await?;
                    for row in rows {
                        let id: Uuid = row.try_get("id")?;
                        let source: String = row.try_get("source")?;
                        let command: String = row.try_get("command")?;
                        let command = command.trim();
                        let index = i16::try_from(
                            r.sources
                                .iter()
                                .position(|s| s == &source)
                                .ok_or(Error::Invalid("unregistered shell source"))?,
                        )
                        .unwrap();
                        sqlx::query("INSERT INTO sinex_shell_nodes(id,source,host,command,command_hash,at_ns,digest,anchor,ekey) VALUES($1,$2,$3,$4,$5,$6::text::numeric,$7,$8,$9)").bind(id).bind(index).bind(row.try_get::<Option<String>,_>("host")?).bind(command).bind(blake3::hash(command.as_bytes()).to_hex().to_string()).bind(row.try_get::<Option<String>,_>("at_ns")?).bind(row.try_get::<String,_>("blake3")?).bind(row.try_get::<i64,_>("anchor_byte")?).bind(row.try_get::<String,_>("equivalence_key")?).execute(&mut *tx).await?;
                        work.cursor = Some(id);
                    }
                    sqlx::query("UPDATE derivation.shell_progress SET scanned=(SELECT count(*) FROM sinex_shell_nodes) WHERE one").execute(&mut *tx).await?;
                    tx.commit().await?;
                }
            }
            Phase::Count2 | Phase::Count60 => {
                let fallback = matches!(work.phase, Phase::Count60);
                let ids:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM sinex_shell_nodes WHERE ($1::uuid IS NULL OR id>$1) ORDER BY id LIMIT 64").bind(work.cursor).fetch_all(&mut self.connection).await?;
                if ids.is_empty() {
                    work.phase = if fallback {
                        Phase::Pair60
                    } else {
                        Phase::Pair2
                    };
                    work.cursor = None;
                } else {
                    let bound = if fallback { r.fallback_ns } else { r.window_ns };
                    let field = if fallback { "count60" } else { "count2" };
                    // The candidate graph is frozen before any match at this strategy. With
                    // two sources every candidate group is a singleton endpoint.
                    // The registered rule and scan admit only source coordinates 0/1.
                    // Equality on the opposite source plus numeric time bounds uses the
                    // existing index without narrowing timestamps or weakening collisions.
                    let sql = include_str!("shell-count.sql").replace("{field}", field);
                    sqlx::query(&sql)
                        .bind(&ids)
                        .bind(bound)
                        .bind(fallback)
                        .execute(&mut self.connection)
                        .await?;
                    work.cursor = ids.last().copied();
                }
            }
            Phase::Pair2 | Phase::Pair60 => {
                let fallback = matches!(work.phase, Phase::Pair60);
                let bound = if fallback { r.fallback_ns } else { r.window_ns };
                let mut tx = self.connection.begin().await?;
                for _ in 0..64 {
                    let edge=sqlx::query("SELECT a.id AS a,z.id AS z FROM sinex_shell_nodes a JOIN sinex_shell_nodes z ON a.source=0 AND z.source=1 AND a.host=z.host AND a.command_hash=z.command_hash AND a.command=z.command AND abs(a.at_ns-z.at_ns)<=$1::bigint WHERE NOT a.paired AND NOT z.paired AND (NOT $2 OR (a.count60=1 AND z.count60=1)) ORDER BY abs(a.at_ns-z.at_ns),a.digest COLLATE \"C\",a.anchor,z.digest COLLATE \"C\",z.anchor LIMIT 1").bind(bound).bind(fallback).fetch_optional(&mut *tx).await?;
                    let Some(edge) = edge else {
                        work.phase = if fallback {
                            Phase::Single
                        } else {
                            Phase::Count60
                        };
                        work.cursor = None;
                        break;
                    };
                    let a: Uuid = edge.try_get("a")?;
                    let z: Uuid = edge.try_get("z")?;
                    sqlx::query(
                        "INSERT INTO sinex_shell_groups(token,parents,strategy) VALUES($1,$2,$3)",
                    )
                    .bind(a)
                    .bind(vec![a, z])
                    .bind(if fallback { 60i16 } else { 2i16 })
                    .execute(&mut *tx)
                    .await?;
                    sqlx::query("UPDATE sinex_shell_nodes SET paired=true WHERE id=ANY($1)")
                        .bind(vec![a, z])
                        .execute(&mut *tx)
                        .await?;
                }
                tx.commit().await?;
            }
            Phase::Single => {
                let ids: Vec<Uuid> = sqlx::query_scalar(
                    "SELECT id FROM sinex_shell_nodes WHERE NOT paired ORDER BY id LIMIT 64",
                )
                .fetch_all(&mut self.connection)
                .await?;
                if ids.is_empty() {
                    work.phase = Phase::Publish;
                } else {
                    let mut tx = self.connection.begin().await?;
                    for id in ids {
                        sqlx::query("INSERT INTO sinex_shell_groups(token,parents,strategy) VALUES($1,$2,0)").bind(id).bind(vec![id]).execute(&mut *tx).await?;
                        sqlx::query("UPDATE sinex_shell_nodes SET paired=true WHERE id=$1")
                            .bind(id)
                            .execute(&mut *tx)
                            .await?;
                    }
                    tx.commit().await?;
                }
            }
            Phase::Publish => {
                let groups=sqlx::query("SELECT token,parents,strategy FROM sinex_shell_groups WHERE NOT done ORDER BY token LIMIT 64").fetch_all(&mut self.connection).await?;
                if groups.is_empty() {
                    work.phase = Phase::Archive;
                    work.cursor = None;
                } else {
                    Self::shell_publish_turn(
                        &mut self.connection,
                        self.shell_producer,
                        self.run_id,
                        work.basis,
                        groups,
                    )
                    .await?;
                    crate::QualificationBoundary::reach(
                        &mut self.qualification,
                        "fusion-replacement-committed",
                        json!({"basis":work.basis,"pending":true,"queued_background_turns":work.queued_turns,"queued_commands_at_turn_entry":queued_commands}),
                    )
                    .await?;
                }
            }
            Phase::Archive => {
                let rows=sqlx::query("SELECT e.id FROM core.live_events e WHERE e.event_type='command.canonical' AND ($1::uuid IS NULL OR e.id>$1) ORDER BY e.id LIMIT 64").bind(work.cursor).fetch_all(&mut self.connection).await?;
                if rows.is_empty() {
                    let mut tx = self.connection.begin().await?;
                    sqlx::query("DELETE FROM derivation.shell_dirty WHERE dirty_seq<=$1")
                        .bind(work.basis)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("UPDATE derivation.shell_progress SET last_evaluated=$1,phase='settled',failure=NULL WHERE one").bind(work.basis).execute(&mut *tx).await?;
                    // An empty replacement still has committed progress. Readers observe the
                    // normal counter/status wakeup instead of inferring progress from events.
                    let _: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
                    tx.commit().await?;
                    return Ok(true);
                }
                let mut tx = self.connection.begin().await?;
                for row in rows {
                    let id: Uuid = row.try_get("id")?;
                    let keep: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM sinex_shell_groups WHERE retained_id=$1)",
                    )
                    .bind(id)
                    .fetch_one(&mut *tx)
                    .await?;
                    if !keep {
                        let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
                        sqlx::query("INSERT INTO audit.archived(id,archived_seq,reason) VALUES($1,$2,'supersede')").bind(id).bind(seq).execute(&mut *tx).await?;
                        crate::session::dirty(&mut tx, id).await?;
                        sqlx::query("INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id) VALUES($1,$2,1,0,3)").bind(seq).bind(id).execute(&mut *tx).await?;
                    }
                    work.cursor = Some(id);
                }
                tx.commit().await?;
            }
        }
        let phase = match work.phase {
            Phase::Scan => "scan",
            Phase::Count2 => "count2",
            Phase::Pair2 => "pair2",
            Phase::Count60 => "count60",
            Phase::Pair60 => "pair60",
            Phase::Single => "single",
            Phase::Publish => "publish",
            Phase::Archive => "archive",
        };
        sqlx::query("UPDATE derivation.shell_progress SET phase=$1 WHERE one")
            .bind(phase)
            .execute(&mut self.connection)
            .await?;
        self.shell_work = Some(work);
        Ok(true)
    }
    async fn shell_publish_turn(
        connection: &mut sqlx::PgConnection,
        producer: Option<Uuid>,
        run_id: Uuid,
        basis: i64,
        groups: Vec<sqlx::postgres::PgRow>,
    ) -> Result<()> {
        // The existing bounded turn is the visibility and rollback unit. Admission
        // and controls still run between turns; no payload plan is buffered here.
        let mut tx = connection.begin().await?;
        for group in groups {
            Self::shell_publish(
                &mut tx,
                producer,
                run_id,
                group.try_get("token")?,
                group.try_get("parents")?,
                group.try_get("strategy")?,
                basis,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn shell_publish(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        producer: Option<Uuid>,
        run_id: Uuid,
        token: Uuid,
        parents: Vec<Uuid>,
        strategy: i16,
        basis: i64,
    ) -> Result<()> {
        let r = rule();
        let rows=sqlx::query("SELECT n.id,n.source,n.ekey,n.digest,n.anchor,trunc(n.at_ns)::text AS at_ns,n.count2,n.count60,e.payload,e.ts_quality FROM sinex_shell_nodes n JOIN core.events e ON e.id=n.id WHERE n.id=ANY($1) ORDER BY n.source,n.digest COLLATE \"C\",n.anchor").bind(&parents).fetch_all(&mut **tx).await?;
        if rows.len() != parents.len() || rows.len() > r.sources.len() {
            return Err(Error::Invalid("shell support unavailable"));
        }
        let mut payload = json!({});
        let mut evidence = json!({});
        let mut alternatives = json!({});
        let mut keys = Vec::<String>::new();
        let mut observed = Vec::new();
        let mut candidate_counts = json!({});
        let mut ambiguous = false;
        let mut max_candidates = 0i64;
        let mut owner: Option<i64> = None;
        let mut source_seen = std::collections::HashSet::new();
        for row in &rows {
            let source: i16 = row.try_get("source")?;
            if !source_seen.insert(source) {
                return Err(Error::Invalid("shell group repeats one source"));
            }
            observed.push(r.sources[source as usize].clone());
            keys.push(row.try_get::<String, _>("ekey")?);
            if let Some(ns) = row.try_get::<Option<String>, _>("at_ns")? {
                let ns: i128 = ns
                    .parse()
                    .map_err(|_| Error::Invalid("shell time unavailable"))?;
                let hour =
                    i64::try_from(ns.div_euclid(i128::from(r.bucket_seconds) * 1_000_000_000))
                        .map_err(|_| Error::Invalid("shell owner hour unavailable"))?;
                owner = Some(owner.map_or(hour, |old| old.min(hour)));
            }
            if strategy != 0 {
                let count: i64 = row.try_get(if strategy == 60 { "count60" } else { "count2" })?;
                candidate_counts[row.try_get::<Uuid, _>("id")?.to_string()] = json!(count);
                ambiguous |= count > 1;
                max_candidates = max_candidates.max(count);
            }
        }
        // The complete temporal evidence is copied from this supplier by INSERT
        // SELECT below. Precision and event UUID are never ranking preferences.
        let supplier = rows
            .iter()
            .min_by_key(|row| {
                let unknown = row.try_get::<Option<String>, _>("at_ns").unwrap().is_none();
                let quality: String = row.try_get("ts_quality").unwrap();
                let rank = quality_rank(&quality);
                temporal_key(
                    !unknown,
                    rank,
                    row.try_get::<i16, _>("source").unwrap(),
                    row.try_get::<String, _>("digest").unwrap(),
                    row.try_get::<i64, _>("anchor").unwrap(),
                )
            })
            .ok_or(Error::Invalid("empty shell support"))?
            .try_get::<Uuid, _>("id")?;
        for field in [
            "command",
            "cwd",
            "exit_code",
            "duration_ns",
            "session",
            "hostname",
            "native_id",
            "completion",
        ] {
            let preferences = r.prefer.get(field).unwrap_or(&r.sources);
            let selected = rows
                .iter()
                .filter(|row| !row.try_get::<Value, _>("payload").unwrap()[field].is_null())
                .min_by_key(|row| {
                    let source: i16 = row.try_get("source").unwrap();
                    (
                        preferences
                            .iter()
                            .position(|s| s == &r.sources[source as usize])
                            .unwrap_or(preferences.len()),
                        source,
                    )
                });
            if let Some(row) = selected {
                let value = row.try_get::<Value, _>("payload")?[field].clone();
                payload[field] = value.clone();
                evidence[field] = json!(row.try_get::<Uuid, _>("id")?);
                let claims: Vec<Value> = rows
                    .iter()
                    .filter_map(|other| {
                        let v = other.try_get::<Value, _>("payload").unwrap()[field].clone();
                        (!v.is_null() && v != value).then(
                            || json!({"parent":other.try_get::<Uuid,_>("id").unwrap(),"value":v}),
                        )
                    })
                    .collect();
                if !claims.is_empty() {
                    alternatives[field] = json!(claims);
                }
            } else {
                payload[field] = Value::Null;
                evidence[field] = Value::Null;
            }
        }
        let temporal: Value = sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
            .bind(supplier)
            .fetch_one(&mut **tx)
            .await?;
        payload["timestamp_ns"] = temporal["timestamp_ns"].clone();
        evidence["time"] = json!(supplier);
        if parents.len() == 1 {
            evidence["all"] = json!(parents[0]);
        }
        payload["canonical"] = json!(canonical(payload["command"].as_str().unwrap_or("")));
        payload["observed_by"] = json!(observed);
        payload["field_evidence"] = evidence;
        payload["conflicting_fields"] = alternatives;
        payload["owner_hour"] = json!(owner);
        payload["association"] = json!({"basis":if strategy==0{"single"}else{"rule"},"rule":r.id,"version":r.version,"strategy":match strategy{2=>Some("window-2s"),60=>Some("window-60s-unique"),_=>None},"ambiguous":ambiguous,"candidates":max_candidates,"candidate_counts":candidate_counts,"claim":"selected association; not independent confirmation or execution identity"});
        payload["claim_scope"] = json!("selected-policy-grouping");
        payload["exact_execution_count"] = Value::Null;
        payload["capture_coverage"] = json!("unknown");
        payload["source_continuity"]=json!(rows.iter().filter_map(|row|{let p:Value=row.try_get("payload").unwrap();(!p["continuity"].is_null()).then(||json!({"parent":row.try_get::<Uuid,_>("id").unwrap(),"continuity":p["continuity"]}))}).collect::<Vec<_>>());
        keys.sort();
        let key = json!([r.id, keys]).to_string();
        let hash = blake3::hash(&serde_json::to_vec(&payload).unwrap())
            .as_bytes()
            .to_vec();
        let active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM core.live_events WHERE id=ANY($1) AND record_state='present'",
        )
        .bind(&parents)
        .fetch_one(&mut **tx)
        .await?;
        if active != parents.len() as i64 {
            // A newer exclusion/removal is a current eligibility fence, even while
            // the older pinned frontier finishes. Its dirty seed is not consumed.
            sqlx::query("UPDATE sinex_shell_groups SET done=true WHERE token=$1")
                .bind(token)
                .execute(&mut **tx)
                .await?;
            return Ok(());
        }
        let prior:Option<Uuid>=sqlx::query_scalar("SELECT e.id FROM core.live_events e JOIN core.producer_versions p ON p.id=e.producer_version_id WHERE e.event_type='command.canonical' AND e.equivalence_key=$1 AND e.source_event_ids=$2 AND e.payload=$3 AND p.version=$4 ORDER BY e.commit_seq LIMIT 1").bind(&key).bind(&parents).bind(&payload).bind(&r.implementation).fetch_optional(&mut **tx).await?;
        let id = if let Some(id) = prior {
            id
        } else {
            let id = Uuid::now_v7();
            let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut **tx).await?;
            let host = payload["hostname"].as_str().map(str::to_string);
            sqlx::query("INSERT INTO core.events(id,source,event_type,host,payload,ts_orig,ts_orig_tiebreak,ts_quality,time_suspect,ts_precision,ts_orig_until,ts_clock,commit_seq,record_hash,source_event_ids,producer_version_id,run_id,equivalence_key,payload_schema_id) SELECT $1,'fusion.shell-command','command.canonical',$2,$3,ts_orig,ts_orig_tiebreak,ts_quality,time_suspect,ts_precision,ts_orig_until,ts_clock,$4,$5,$6,$7,$8,$9,5 FROM core.events WHERE id=$10").bind(id).bind(host).bind(payload).bind(seq).bind(hash).bind(&parents).bind(producer).bind(run_id).bind(key).bind(supplier).execute(&mut **tx).await?;
            crate::session::dirty(tx, id).await?;
            sqlx::query("INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id) VALUES($1,$2,0,0,3)").bind(seq).bind(id).execute(&mut **tx).await?;
            id
        };
        sqlx::query("UPDATE sinex_shell_groups SET done=true,retained_id=$2 WHERE token=$1")
            .bind(token)
            .bind(id)
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "UPDATE derivation.shell_progress SET replaced=replaced+1 WHERE one AND basis=$1",
        )
        .bind(basis)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "shell_publication_tests.rs"]
mod publication_tests;
#[cfg(test)]
#[path = "shell_work_tests.rs"]
mod work_tests;

fn temporal_key(
    known: bool,
    rank: usize,
    source: i16,
    digest: String,
    anchor: i64,
) -> (bool, usize, i16, String, i64) {
    (!known, rank, source, digest, anchor)
}
fn quality_rank(quality: &str) -> usize {
    [
        "captured",
        "intrinsic",
        "file_mtime",
        "file_ctime",
        "operator",
        "staged",
    ]
    .iter()
    .position(|q| *q == quality)
    .unwrap_or(6)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn temporal_supplier_uses_quality_then_source_and_retained_record() {
        let key = |known: bool, quality: &str, source: i16, digest: &str, anchor: i64| {
            temporal_key(
                known,
                quality_rank(quality),
                source,
                digest.to_owned(),
                anchor,
            )
        };
        assert!(key(true, "intrinsic", 0, "ff", 80) < key(true, "intrinsic", 1, "00", 0));
        assert!(key(true, "captured", 1, "ff", 80) < key(true, "intrinsic", 0, "00", 0));
        assert!(key(true, "intrinsic", 1, "01", 80) < key(true, "intrinsic", 1, "02", 0));
        assert!(key(true, "intrinsic", 1, "01", 9) < key(true, "intrinsic", 1, "01", 10));
        assert!(key(true, "staged", 1, "ff", 80) < key(false, "captured", 0, "00", 0));
    }
}
