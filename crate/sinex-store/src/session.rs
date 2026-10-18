//! Selected shell-only logical-day reconciler. The writer owns dirty keys and
//! publication; restart discards temporary work and repeats its durable dirty key.
use crate::{COUNTER, Error, Result, WriterPool};
use serde_json::{Value, json};
use sqlx::{Connection, Row};
use uuid::Uuid;

const GAP: i128 = 300_000_000_000;
const MAX_PARENTS: usize = 8192;
pub(crate) fn payload_schema() -> Value {
    json!({"type":"object","required":["start_ns","end_ns","owner_day","profile","continuity","attention","afk","claim_scope"],"properties":{"profile":{"const":"shell-session-v1-warsaw04-gap300"},"continuity":{"type":"null"},"attention":{"type":"null"},"afk":{"type":"null"},"claim_scope":{"const":"recorded-shell-policy-grouping"}}})
}
// Admission/archive and guarded-upgrade seeding use the same range rule.
// An exact ID includes its archived interpretation; a null ID seeds live inputs.
pub(crate) const DIRTY_SPANS: &str = r#"
INSERT INTO derivation.session_dirty(day,dirty_seq,through_day)
SELECT (((e.ts_orig AT TIME ZONE 'Europe/Warsaw')-interval '4 hours')::date-1)::text,
       c.seq,
       max(((((e.ts_orig+greatest(coalesce((e.payload->>'duration_ns')::numeric,0),0)
           /1000000000*interval '1 second') AT TIME ZONE 'Europe/Warsaw')
           -interval '4 hours')::date+1)::text)
FROM core.events e CROSS JOIN core.commit_counter c
WHERE c.one AND e.event_type='command.canonical' AND e.ts_orig IS NOT NULL
  AND (e.id=$1::uuid OR ($1::uuid IS NULL AND e.record_state='present'
       AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id)))
GROUP BY 1,c.seq
ON CONFLICT(day) DO UPDATE
SET dirty_seq=greatest(session_dirty.dirty_seq,EXCLUDED.dirty_seq),
    through_day=greatest(coalesce(session_dirty.through_day,session_dirty.day),EXCLUDED.through_day)
"#;
pub(crate) async fn dirty(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, id: Uuid) -> Result<()> {
    sqlx::query(DIRTY_SPANS)
        .bind(Some(id))
        .execute(&mut **tx)
        .await?;
    Ok(())
}
// One durable range bound in the existing dirty ledger. Completion advances
// at most one day, including empty days, and never materializes a date range.
async fn mark_range(
    connection: &mut sqlx::PgConnection,
    day: &str,
    end_ns: &str,
    basis: i64,
    next: bool,
) -> Result<()> {
    sqlx::query("INSERT INTO derivation.session_dirty(day,dirty_seq,through_day) SELECT ($1::text::date+CASE WHEN $4 THEN 1 ELSE 0 END)::text,$3,(((to_timestamp(floor($2::text::numeric/1000000000)::double precision) AT TIME ZONE 'Europe/Warsaw')-interval '4 hours')::date+1)::text WHERE NOT $4 OR $2::text::numeric>=extract(epoch FROM (($1::text::date+1+time '04:00') AT TIME ZONE 'Europe/Warsaw'))*1000000000 ON CONFLICT(day) DO UPDATE SET dirty_seq=greatest(session_dirty.dirty_seq,EXCLUDED.dirty_seq),through_day=greatest(coalesce(session_dirty.through_day,session_dirty.day),EXCLUDED.through_day)").bind(day).bind(end_ns).bind(basis).bind(next).execute(connection).await?;
    Ok(())
}

enum Phase {
    Scan,
    Group,
    Publish,
    Archive,
}
struct Group {
    start: i128,
    end: i128,
    parents: Vec<Uuid>,
}
pub(crate) struct Work {
    basis: i64,
    day: String,
    through_day: String,
    phase: Phase,
    cursor: Option<Uuid>,
    span_position: Option<(String, Uuid)>,
    group: Option<Group>,
}
impl WriterPool {
    pub(crate) async fn session_enable(&mut self) -> Result<()> {
        let producer = Uuid::now_v7();
        sqlx::query("INSERT INTO core.producer_versions(id,name,version,run_id,configuration) VALUES($1,'session','shell-session-v1',$2,$3)").bind(producer).bind(self.run_id).bind(json!({"profile":"shell-session-v1-warsaw04-gap300","zone":"Europe/Warsaw","logical_start":"04:00","gap_seconds":300,"inputs":"command.canonical recorded spans; unknown end is a point","focus":"unavailable","afk":"unavailable"})).execute(&mut self.connection).await?;
        self.session_producer = Some(producer);
        Ok(())
    }
    pub(crate) async fn session_step(&mut self) -> Result<bool> {
        if self.session_producer.is_none() || self.session_failed {
            return Ok(false);
        }
        // No pinned session basis may include an unfinished fusion replacement.
        if self.session_work.is_none() {
            let pending: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM derivation.shell_dirty)")
                    .fetch_one(&mut self.connection)
                    .await?;
            if pending {
                return Ok(false);
            }
            let row=sqlx::query("SELECT day,coalesce(through_day,day) AS through_day FROM derivation.session_dirty ORDER BY dirty_seq,day LIMIT 1").fetch_optional(&mut self.connection).await?;
            let Some(row) = row else {
                // A transient read failure, or an uncertain final commit, may leave
                // a failure notice after owed work is gone. A successful empty
                // ledger observation by this writer is the recovery fence.
                sqlx::query("UPDATE derivation.session_progress SET failure=NULL WHERE one AND failure IS NOT NULL").execute(&mut self.connection).await?;
                return Ok(false);
            };
            let day: String = row.try_get("day")?;
            let through_day: String = row.try_get("through_day")?;
            let basis: i64 = sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
                .fetch_one(&mut self.connection)
                .await?;
            for statement in [
                "DROP TABLE IF EXISTS pg_temp.sinex_session_spans,pg_temp.sinex_session_groups",
                "CREATE TEMP TABLE sinex_session_spans(at numeric NOT NULL,id uuid NOT NULL,end_at numeric NOT NULL,PRIMARY KEY(at,id))",
                "CREATE TEMP TABLE sinex_session_groups(token uuid PRIMARY KEY,start_ns numeric NOT NULL,end_ns numeric NOT NULL,owner_day text NOT NULL,parents uuid[] NOT NULL,retained uuid,done boolean NOT NULL DEFAULT false)",
            ] {
                sqlx::query(statement).execute(&mut self.connection).await?;
            }
            sqlx::query("UPDATE derivation.session_progress SET basis=$1,day=$2,through_day=$3,phase='scan',failure=NULL WHERE one").bind(basis).bind(&day).bind(&through_day).execute(&mut self.connection).await?;
            self.session_work = Some(Work {
                basis,
                day,
                through_day,
                phase: Phase::Scan,
                cursor: None,
                span_position: None,
                group: None,
            });
        }
        let mut w = self.session_work.take().unwrap();
        match w.phase {
            Phase::Scan => {
                let rows=sqlx::query("SELECT e.id,trunc(extract(epoch FROM e.ts_orig)*1000000000+coalesce(e.ts_orig_tiebreak,0))::text AS ns,e.payload->>'duration_ns' AS duration FROM core.events e WHERE e.event_type='command.canonical' AND e.ts_orig IS NOT NULL AND e.record_state='present' AND e.commit_seq<=$1 AND ($2::uuid IS NULL OR e.id>$2) AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<=$1) AND NOT EXISTS(SELECT 1 FROM unnest(e.source_event_ids) p(id) WHERE NOT EXISTS(SELECT 1 FROM core.events raw WHERE raw.id=p.id AND raw.record_state='present' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=raw.id AND a.archived_seq<=$1))) ORDER BY e.id LIMIT 16").bind(w.basis).bind(w.cursor).fetch_all(&mut self.connection).await?;
                if rows.is_empty() {
                    w.phase = Phase::Group;
                } else {
                    let mut tx = self.connection.begin().await?;
                    for row in rows {
                        let id: Uuid = row.try_get("id")?;
                        let ns: String = row.try_get("ns")?;
                        let start: i128 = ns
                            .parse()
                            .map_err(|_| Error::Invalid("session time unavailable"))?;
                        let duration: Option<String> = row.try_get("duration")?;
                        let duration = duration
                            .as_deref()
                            .map(str::parse::<i128>)
                            .transpose()
                            .map_err(|_| Error::Invalid("session duration unavailable"))?
                            .unwrap_or(0)
                            .max(0);
                        let end = start
                            .checked_add(duration)
                            .ok_or(Error::Invalid("session endpoint overflow"))?;
                        sqlx::query("INSERT INTO sinex_session_spans(at,id,end_at) VALUES($1::text::numeric,$2,$3::text::numeric)").bind(start.to_string()).bind(id).bind(end.to_string()).execute(&mut *tx).await?;
                        w.cursor = Some(id);
                    }
                    tx.commit().await?;
                }
            }
            Phase::Group => {
                let rows=sqlx::query("SELECT at::text,end_at::text,id FROM sinex_session_spans WHERE ($1::text IS NULL OR (at,id)>($1::text::numeric,$2)) ORDER BY at,id LIMIT 16").bind(w.span_position.as_ref().map(|p|p.0.clone())).bind(w.span_position.as_ref().map(|p|p.1)).fetch_all(&mut self.connection).await?;
                if rows.is_empty() {
                    if let Some(g) = w.group.take() {
                        self.session_group(&w.day, g, w.basis).await?;
                    }
                    // A removed bridge can leave no backwards crossing at all.
                    // Dirty the old owner as well as any newly observed one.
                    sqlx::query("INSERT INTO derivation.session_dirty(day,dirty_seq,through_day) SELECT e.payload->>'owner_day',$2,max((((to_timestamp(floor((e.payload->>'end_ns')::numeric/1000000000)::double precision) AT TIME ZONE 'Europe/Warsaw')-interval '4 hours')::date+1)::text) FROM core.live_events e WHERE e.event_type='activity.session.boundary' AND e.payload->>'owner_day'<$1 AND (e.payload->>'end_ns')::numeric>=extract(epoch FROM (($1::text::date+time '04:00') AT TIME ZONE 'Europe/Warsaw'))*1000000000 AND NOT EXISTS(SELECT 1 FROM sinex_session_groups g WHERE g.start_ns=(e.payload->>'start_ns')::numeric AND g.end_ns=(e.payload->>'end_ns')::numeric AND g.parents=e.source_event_ids) GROUP BY 1 ON CONFLICT(day) DO UPDATE SET dirty_seq=greatest(session_dirty.dirty_seq,EXCLUDED.dirty_seq),through_day=greatest(coalesce(session_dirty.through_day,session_dirty.day),EXCLUDED.through_day)").bind(&w.day).bind(w.basis).execute(&mut self.connection).await?;
                    w.phase = Phase::Publish;
                } else {
                    for row in rows {
                        let ns: String = row.try_get("at")?;
                        let at: i128 = ns
                            .parse()
                            .map_err(|_| Error::Invalid("session span unavailable"))?;
                        let end: i128 = row
                            .try_get::<String, _>("end_at")?
                            .parse()
                            .map_err(|_| Error::Invalid("session end unavailable"))?;
                        let id: Uuid = row.try_get("id")?;
                        if w.group.as_ref().is_some_and(|g| at - g.end > GAP) {
                            let g = w.group.take().unwrap();
                            self.session_group(&w.day, g, w.basis).await?;
                        }
                        let g = w.group.get_or_insert(Group {
                            start: at,
                            end: at,
                            parents: vec![],
                        });
                        g.end = g.end.max(end);
                        if !g.parents.contains(&id) {
                            if g.parents.len() == MAX_PARENTS {
                                return Err(Error::Invalid(
                                    "session exceeds supported exact 8192-parent output representation; dirty key remains incomplete",
                                ));
                            }
                            g.parents.push(id);
                        }
                        w.span_position = Some((ns, id));
                    }
                }
            }
            Phase::Publish => {
                let rows=sqlx::query("SELECT token,start_ns::text,end_ns::text,parents FROM sinex_session_groups WHERE NOT done AND owner_day=$1 ORDER BY token LIMIT 2").bind(&w.day).fetch_all(&mut self.connection).await?;
                if rows.is_empty() {
                    w.phase = Phase::Archive;
                    w.cursor = None;
                } else {
                    for row in rows {
                        let token: Uuid = row.try_get("token")?;
                        let start: String = row.try_get("start_ns")?;
                        let end: String = row.try_get("end_ns")?;
                        let parents: Vec<Uuid> = row.try_get("parents")?;
                        let sources:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(DISTINCT source_name ORDER BY source_name),'[]'::jsonb) FROM core.events e CROSS JOIN LATERAL jsonb_array_elements_text(e.payload->'observed_by') AS observed(source_name) WHERE e.id=ANY($1)").bind(&parents).fetch_one(&mut self.connection).await?;
                        let payload = json!({"start_ns":start,"end_ns":end,"owner_day":w.day,"profile":"shell-session-v1-warsaw04-gap300","sources":sources,"continuity":null,"attention":null,"afk":null,"active_s":null,"switches":null,"claim_scope":"recorded-shell-policy-grouping","coverage":"unknown; no focus or AFK observations; known command spans are recorded claims, not continuous activity"});
                        let key = json!(["session.shell-v1", start, end]).to_string();
                        let mut tx = self.connection.begin().await?;
                        let active:i64=sqlx::query_scalar("SELECT count(*) FROM core.live_events e WHERE e.id=ANY($1) AND e.record_state='present' AND NOT EXISTS(SELECT 1 FROM unnest(e.source_event_ids) p(id) WHERE NOT EXISTS(SELECT 1 FROM core.live_events raw WHERE raw.id=p.id AND raw.record_state='present'))").bind(&parents).fetch_one(&mut *tx).await?;
                        if active != parents.len() as i64 {
                            sqlx::query("UPDATE sinex_session_groups SET done=true WHERE token=$1")
                                .bind(token)
                                .execute(&mut *tx)
                                .await?;
                            tx.commit().await?;
                            continue;
                        }
                        let prior:Option<Uuid>=sqlx::query_scalar("SELECT id FROM core.live_events WHERE event_type='activity.session.boundary' AND equivalence_key=$1 AND source_event_ids=$2 AND payload=$3 ORDER BY id LIMIT 1").bind(&key).bind(&parents).bind(&payload).fetch_optional(&mut *tx).await?;
                        let id = if let Some(id) = prior {
                            id
                        } else {
                            // A changed crossing invalidates the following day's
                            // comparison. Unchanged retained groups do not ping-pong.
                            mark_range(&mut tx, &w.day, &end, w.basis, true).await?;
                            let id = Uuid::now_v7();
                            let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
                            let hash = blake3::hash(&serde_json::to_vec(&payload).unwrap())
                                .as_bytes()
                                .to_vec();
                            sqlx::query("INSERT INTO core.events(id,source,event_type,payload,ts_orig,ts_orig_tiebreak,ts_quality,ts_precision,ts_clock,commit_seq,record_hash,source_event_ids,producer_version_id,run_id,equivalence_key,payload_schema_id) VALUES($1,'session','activity.session.boundary',$2,to_timestamp(floor($3::text::numeric/1000000000)::double precision)+mod(mod(floor($3::text::numeric/1000),1000000)+1000000,1000000)*interval '1 microsecond',mod(mod($3::text::numeric,1000)+1000,1000)::integer,'intrinsic','instant','producer',$4,$5,$6,$7,$8,$9,6)").bind(id).bind(&payload).bind(&start).bind(seq).bind(hash).bind(&parents).bind(self.session_producer).bind(self.run_id).bind(&key).execute(&mut *tx).await?;
                            sqlx::query("INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id) VALUES($1,$2,0,0,5)").bind(seq).bind(id).execute(&mut *tx).await?;
                            id
                        };
                        sqlx::query(
                            "UPDATE sinex_session_groups SET retained=$2,done=true WHERE token=$1",
                        )
                        .bind(token)
                        .bind(id)
                        .execute(&mut *tx)
                        .await?;
                        tx.commit().await?;
                    }
                    crate::TestPause::reach(
                        &mut self.pause,
                        "session-replacement-committed",
                        json!({"basis":w.basis,"day":w.day,"through_day":w.through_day,"pending":true}),
                    )
                    .await?;
                }
            }
            Phase::Archive => {
                let rows=sqlx::query("SELECT e.id,e.payload->>'end_ns' AS end_ns FROM core.live_events e WHERE e.event_type='activity.session.boundary' AND e.payload->>'owner_day'=$1 AND ($2::uuid IS NULL OR e.id>$2) ORDER BY e.id LIMIT 16").bind(&w.day).bind(w.cursor).fetch_all(&mut self.connection).await?;
                let mut tx = self.connection.begin().await?;
                if rows.is_empty() {
                    if w.day < w.through_day {
                        sqlx::query("INSERT INTO derivation.session_dirty(day,dirty_seq,through_day) VALUES(($1::text::date+1)::text,$2,$3) ON CONFLICT(day) DO UPDATE SET dirty_seq=greatest(session_dirty.dirty_seq,EXCLUDED.dirty_seq),through_day=greatest(coalesce(session_dirty.through_day,session_dirty.day),EXCLUDED.through_day)").bind(&w.day).bind(w.basis).bind(&w.through_day).execute(&mut *tx).await?;
                    }
                    sqlx::query("DELETE FROM derivation.session_dirty WHERE day=$1 AND dirty_seq<=$2 AND coalesce(through_day,day)<=$3").bind(&w.day).bind(w.basis).bind(&w.through_day).execute(&mut *tx).await?;
                    sqlx::query("UPDATE derivation.session_progress SET phase='settled',failure=NULL WHERE one").execute(&mut *tx).await?;
                    let _: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
                    tx.commit().await?;
                    return Ok(true);
                }
                for row in rows {
                    let id: Uuid = row.try_get("id")?;
                    let keep: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM sinex_session_groups WHERE retained=$1)",
                    )
                    .bind(id)
                    .fetch_one(&mut *tx)
                    .await?;
                    if !keep {
                        mark_range(
                            &mut tx,
                            &w.day,
                            &row.try_get::<String, _>("end_ns")?,
                            w.basis,
                            true,
                        )
                        .await?;
                        let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
                        sqlx::query("INSERT INTO audit.archived(id,archived_seq,reason) VALUES($1,$2,'supersede')").bind(id).bind(seq).execute(&mut *tx).await?;
                        sqlx::query("INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id) VALUES($1,$2,1,0,5)").bind(seq).bind(id).execute(&mut *tx).await?;
                    }
                    w.cursor = Some(id);
                }
                tx.commit().await?;
            }
        }
        let phase = match w.phase {
            Phase::Scan => "scan",
            Phase::Group => "group",
            Phase::Publish => "publish",
            Phase::Archive => "archive",
        };
        sqlx::query("UPDATE derivation.session_progress SET phase=$1 WHERE one")
            .bind(phase)
            .execute(&mut self.connection)
            .await?;
        self.session_work = Some(w);
        Ok(true)
    }
    async fn session_group(&mut self, day: &str, mut g: Group, basis: i64) -> Result<()> {
        g.parents.sort();
        let owner:String=sqlx::query_scalar("SELECT ((to_timestamp(floor($1::text::numeric/1000000000)::double precision) AT TIME ZONE 'Europe/Warsaw')-interval '4 hours')::date::text").bind(g.start.to_string()).fetch_one(&mut self.connection).await?;
        if owner == day {
            sqlx::query("INSERT INTO sinex_session_groups(token,start_ns,end_ns,owner_day,parents) VALUES($1,$2::text::numeric,$3::text::numeric,$4,$5)").bind(Uuid::now_v7()).bind(g.start.to_string()).bind(g.end.to_string()).bind(&owner).bind(g.parents).execute(&mut self.connection).await?;
        } else {
            // Only a grouping crossing the key's own start is relevant. Equality
            // checks complete membership as well as bounds, so neighbour work stops.
            let crosses:bool=sqlx::query_scalar("SELECT $1::text::numeric < extract(epoch FROM (($3::text::date+time '04:00') AT TIME ZONE 'Europe/Warsaw'))*1000000000 AND $2::text::numeric >= extract(epoch FROM (($3::text::date+time '04:00') AT TIME ZONE 'Europe/Warsaw'))*1000000000").bind(g.start.to_string()).bind(g.end.to_string()).bind(day).fetch_one(&mut self.connection).await?;
            if crosses {
                sqlx::query("INSERT INTO sinex_session_groups(token,start_ns,end_ns,owner_day,parents) VALUES($1,$2::text::numeric,$3::text::numeric,$4,$5)").bind(Uuid::now_v7()).bind(g.start.to_string()).bind(g.end.to_string()).bind(&owner).bind(&g.parents).execute(&mut self.connection).await?;
                let same:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.live_events WHERE event_type='activity.session.boundary' AND payload->>'start_ns'=$1 AND payload->>'end_ns'=$2 AND source_event_ids=$3)").bind(g.start.to_string()).bind(g.end.to_string()).bind(g.parents).fetch_one(&mut self.connection).await?;
                if !same {
                    mark_range(
                        &mut self.connection,
                        &owner,
                        &g.end.to_string(),
                        basis,
                        false,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }
}
