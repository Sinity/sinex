//! Shell sessions: groups `command.canonical` events into
//! `activity.session.boundary` events, one logical day at a time.
//! `derivation.session_dirty` lists the days to redo; temp work is discarded
//! on restart and the day is redone.
use crate::commit::{Change, event_type, log_commit};
use crate::{Error, Result, Writer, current_commit_seq, next_commit_seq};
use serde_json::{Value, json};
use sqlx::{Connection, Row};
use uuid::Uuid;

const PROFILE: &str = "shell-session-v1-warsaw04-gap300";
/// A gap longer than this between commands starts a new session.
const GAP_NS: i128 = 300_000_000_000;
const MAX_PARENTS: usize = 8192;
/// `derivation.session_progress.phase` when no day is being processed.
const IDLE_PHASE: &str = "settled";

/// Upsert tail for `derivation.session_dirty`: keeps the newest sequence and
/// the furthest `through_day`.
macro_rules! merge_dirty_day {
    () => {
        "
ON CONFLICT(day) DO UPDATE
SET dirty_seq=greatest(session_dirty.dirty_seq,EXCLUDED.dirty_seq),
    through_day=greatest(coalesce(session_dirty.through_day,session_dirty.day),
                         EXCLUDED.through_day)"
    };
}

pub(crate) fn payload_schema() -> Value {
    json!({
        "type": "object",
        "required": ["start_ns", "end_ns", "owner_day", "profile", "continuity", "attention",
                     "afk", "claim_scope"],
        "properties": {
            "profile": {"const": PROFILE},
            "continuity": {"type": "null"},
            "attention": {"type": "null"},
            "afk": {"type": "null"},
            "claim_scope": {"const": "recorded-shell-policy-grouping"}
        }
    })
}

/// Marks the logical days a canonical command spans, from the day before its
/// start through the day after its end. `$1` is one event id (archived or
/// not); NULL marks every live canonical command, for schema upgrades.
pub(crate) const DIRTY_SPANS: &str = concat!(
    "
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
GROUP BY 1,c.seq",
    merge_dirty_day!()
);

/// Marks the days `id` spans, in the caller's transaction.
pub(crate) async fn dirty(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, id: Uuid) -> Result<()> {
    sqlx::query(DIRTY_SPANS)
        .bind(Some(id))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Marks `day` (or the day after, with `from_next_day`) dirty through the day
/// after `end_ns`, as one ranged row. With `from_next_day`, only when `end_ns`
/// reaches into that next day.
async fn mark_days_dirty(
    connection: &mut sqlx::PgConnection,
    day: &str,
    end_ns: &str,
    snapshot_seq: i64,
    from_next_day: bool,
) -> Result<()> {
    sqlx::query(concat!(
        "
INSERT INTO derivation.session_dirty(day,dirty_seq,through_day)
SELECT ($1::text::date+CASE WHEN $4 THEN 1 ELSE 0 END)::text,
       $3,
       (((to_timestamp(floor($2::text::numeric/1000000000)::double precision)
          AT TIME ZONE 'Europe/Warsaw')-interval '4 hours')::date+1)::text
WHERE NOT $4
   OR $2::text::numeric>=extract(epoch FROM (($1::text::date+1+time '04:00')
                                            AT TIME ZONE 'Europe/Warsaw'))*1000000000",
        merge_dirty_day!()
    ))
    .bind(day)
    .bind(end_ns)
    .bind(snapshot_seq)
    .bind(from_next_day)
    .execute(connection)
    .await?;
    Ok(())
}

/// Steps of one day's pass, in order.
#[derive(Clone, Copy)]
enum Phase {
    /// Copy the day's candidate command spans into `sinex_session_spans`.
    Scan,
    /// Walk spans in time order and close a group at each long gap.
    Group,
    /// Write or reuse one boundary event per group owned by the day.
    Publish,
    /// Archive the day's boundary events that no group reused.
    Archive,
}
impl Phase {
    /// Value stored in `derivation.session_progress.phase`.
    fn name(self) -> &'static str {
        match self {
            Phase::Scan => "scan",
            Phase::Group => "group",
            Phase::Publish => "publish",
            Phase::Archive => "archive",
        }
    }
}

struct Group {
    start: i128,
    end: i128,
    parents: Vec<Uuid>,
}
pub(crate) struct Work {
    snapshot_seq: i64,
    day: String,
    through_day: String,
    phase: Phase,
    cursor: Option<Uuid>,
    span_position: Option<(String, Uuid)>,
    group: Option<Group>,
}
impl Work {
    fn advance(&mut self, phase: Phase) {
        self.phase = phase;
        self.cursor = None;
    }
}

impl Writer {
    pub(crate) async fn session_enable(&mut self) -> Result<()> {
        let producer = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO core.producer_versions(id,name,version,run_id,configuration)
             VALUES($1,'session','shell-session-v1',$2,$3)",
        )
        .bind(producer)
        .bind(self.run_id)
        .bind(json!({
            "profile": PROFILE,
            "zone": "Europe/Warsaw",
            "logical_start": "04:00",
            "gap_seconds": 300,
            "inputs": "command.canonical recorded spans; unknown end is a point",
            "focus": "unavailable",
            "afk": "unavailable"
        }))
        .execute(&mut self.connection)
        .await?;
        self.session_producer = Some(producer);
        Ok(())
    }

    /// Runs one turn of session work. Returns false when there is none.
    pub(crate) async fn session_step(&mut self) -> Result<bool> {
        if self.session_producer.is_none() || self.session_failed {
            return Ok(false);
        }
        let mut work = match self.session_work.take() {
            Some(work) => work,
            None => match self.session_begin().await? {
                Some(work) => work,
                None => return Ok(false),
            },
        };
        match work.phase {
            Phase::Scan => self.session_scan(&mut work).await?,
            Phase::Group => self.session_group(&mut work).await?,
            Phase::Publish => self.session_publish(&mut work).await?,
            Phase::Archive => {
                if self.session_archive(&mut work).await? {
                    return Ok(true);
                }
            }
        }
        sqlx::query("UPDATE derivation.session_progress SET phase=$1 WHERE one")
            .bind(work.phase.name())
            .execute(&mut self.connection)
            .await?;
        self.session_work = Some(work);
        Ok(true)
    }

    /// Starts on the oldest dirty day, once shell grouping has caught up.
    async fn session_begin(&mut self) -> Result<Option<Work>> {
        let grouping_pending: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM derivation.shell_dirty)")
                .fetch_one(&mut self.connection)
                .await?;
        if grouping_pending {
            return Ok(None);
        }
        let row = sqlx::query(
            "SELECT day,coalesce(through_day,day) AS through_day FROM derivation.session_dirty
             ORDER BY dirty_seq,day LIMIT 1",
        )
        .fetch_optional(&mut self.connection)
        .await?;
        let Some(row) = row else {
            // A failed read or an uncertain final commit can leave a failure
            // after the work is gone; reading an empty table clears it.
            sqlx::query(
                "UPDATE derivation.session_progress SET failure=NULL
                 WHERE one AND failure IS NOT NULL",
            )
            .execute(&mut self.connection)
            .await?;
            return Ok(None);
        };
        let day: String = row.try_get("day")?;
        let through_day: String = row.try_get("through_day")?;
        let snapshot_seq: i64 = current_commit_seq(&mut self.connection).await?;
        for statement in [
            "DROP TABLE IF EXISTS pg_temp.sinex_session_spans,pg_temp.sinex_session_groups",
            "CREATE TEMP TABLE sinex_session_spans(
                at numeric NOT NULL,
                id uuid NOT NULL,
                end_at numeric NOT NULL,
                PRIMARY KEY(at,id))",
            "CREATE TEMP TABLE sinex_session_groups(
                token uuid PRIMARY KEY,
                start_ns numeric NOT NULL,
                end_ns numeric NOT NULL,
                owner_day text NOT NULL,
                parents uuid[] NOT NULL,
                output_id uuid,
                done boolean NOT NULL DEFAULT false)",
        ] {
            sqlx::query(statement).execute(&mut self.connection).await?;
        }
        sqlx::query(
            "UPDATE derivation.session_progress
             SET basis=$1,day=$2,through_day=$3,phase='scan',failure=NULL WHERE one",
        )
        .bind(snapshot_seq)
        .bind(&day)
        .bind(&through_day)
        .execute(&mut self.connection)
        .await?;
        Ok(Some(Work {
            snapshot_seq,
            day,
            through_day,
            phase: Phase::Scan,
            cursor: None,
            span_position: None,
            group: None,
        }))
    }

    /// Loads present canonical commands whose parents are all still present.
    async fn session_scan(&mut self, work: &mut Work) -> Result<()> {
        let rows = sqlx::query(
            "SELECT e.id,
                    trunc(extract(epoch FROM e.ts_orig)*1000000000+coalesce(e.ts_orig_tiebreak,0))
                        ::text AS ns,
                    e.payload->>'duration_ns' AS duration
             FROM core.events e
             WHERE e.event_type='command.canonical' AND e.ts_orig IS NOT NULL
               AND e.record_state='present' AND e.commit_seq<=$1
               AND ($2::uuid IS NULL OR e.id>$2)
               AND NOT EXISTS(SELECT 1 FROM audit.archived a
                              WHERE a.id=e.id AND a.archived_seq<=$1)
               AND NOT EXISTS(
                   SELECT 1 FROM unnest(e.source_event_ids) p(id)
                   WHERE NOT EXISTS(
                       SELECT 1 FROM core.events raw
                       WHERE raw.id=p.id AND raw.record_state='present'
                         AND NOT EXISTS(SELECT 1 FROM audit.archived a
                                        WHERE a.id=raw.id AND a.archived_seq<=$1)))
             ORDER BY e.id LIMIT 16",
        )
        .bind(work.snapshot_seq)
        .bind(work.cursor)
        .fetch_all(&mut self.connection)
        .await?;
        if rows.is_empty() {
            work.advance(Phase::Group);
            return Ok(());
        }
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
            sqlx::query(
                "INSERT INTO sinex_session_spans(at,id,end_at)
                 VALUES($1::text::numeric,$2,$3::text::numeric)",
            )
            .bind(start.to_string())
            .bind(id)
            .bind(end.to_string())
            .execute(&mut *tx)
            .await?;
            work.cursor = Some(id);
        }
        tx.commit().await?;
        Ok(())
    }

    async fn session_group(&mut self, work: &mut Work) -> Result<()> {
        let rows = sqlx::query(
            "SELECT at::text,end_at::text,id FROM sinex_session_spans
             WHERE ($1::text IS NULL OR (at,id)>($1::text::numeric,$2))
             ORDER BY at,id LIMIT 16",
        )
        .bind(work.span_position.as_ref().map(|p| p.0.clone()))
        .bind(work.span_position.as_ref().map(|p| p.1))
        .fetch_all(&mut self.connection)
        .await?;
        if rows.is_empty() {
            if let Some(group) = work.group.take() {
                self.session_close_group(&work.day, group, work.snapshot_seq)
                    .await?;
            }
            // A removed command that bridged two days leaves no group
            // crossing into this day, so also mark every earlier day whose
            // stored session reached into this one and was not regrouped.
            sqlx::query(concat!(
                "
INSERT INTO derivation.session_dirty(day,dirty_seq,through_day)
SELECT e.payload->>'owner_day',
       $2,
       max((((to_timestamp(floor((e.payload->>'end_ns')::numeric/1000000000)::double precision)
             AT TIME ZONE 'Europe/Warsaw')-interval '4 hours')::date+1)::text)
FROM core.live_events e
WHERE e.event_type='activity.session.boundary'
  AND e.payload->>'owner_day'<$1
  AND (e.payload->>'end_ns')::numeric>=extract(epoch FROM (($1::text::date+time '04:00')
                                                         AT TIME ZONE 'Europe/Warsaw'))*1000000000
  AND NOT EXISTS(SELECT 1 FROM sinex_session_groups g
                 WHERE g.start_ns=(e.payload->>'start_ns')::numeric
                   AND g.end_ns=(e.payload->>'end_ns')::numeric
                   AND g.parents=e.source_event_ids)
GROUP BY 1",
                merge_dirty_day!()
            ))
            .bind(&work.day)
            .bind(work.snapshot_seq)
            .execute(&mut self.connection)
            .await?;
            work.advance(Phase::Publish);
            return Ok(());
        }
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
            if work.group.as_ref().is_some_and(|g| at - g.end > GAP_NS) {
                let group = work.group.take().unwrap();
                self.session_close_group(&work.day, group, work.snapshot_seq)
                    .await?;
            }
            let group = work.group.get_or_insert(Group {
                start: at,
                end: at,
                parents: vec![],
            });
            group.end = group.end.max(end);
            if !group.parents.contains(&id) {
                if group.parents.len() == MAX_PARENTS {
                    return Err(Error::Invalid(
                        "session has more than 8192 commands; the day stays dirty",
                    ));
                }
                group.parents.push(id);
            }
            work.span_position = Some((ns, id));
        }
        Ok(())
    }

    /// Publishes up to two of the day's groups, each in its own transaction.
    async fn session_publish(&mut self, work: &mut Work) -> Result<()> {
        let rows = sqlx::query(
            "SELECT token,start_ns::text,end_ns::text,parents FROM sinex_session_groups
             WHERE NOT done AND owner_day=$1 ORDER BY token LIMIT 2",
        )
        .bind(&work.day)
        .fetch_all(&mut self.connection)
        .await?;
        if rows.is_empty() {
            work.advance(Phase::Archive);
            return Ok(());
        }
        for row in rows {
            self.session_publish_group(
                work,
                row.try_get("token")?,
                row.try_get("start_ns")?,
                row.try_get("end_ns")?,
                row.try_get("parents")?,
            )
            .await?;
        }
        crate::TestPause::reach(
            &mut self.pause,
            "session-replacement-committed",
            json!({
                "basis": work.snapshot_seq,
                "day": work.day,
                "through_day": work.through_day,
                "pending": true
            }),
        )
        .await?;
        Ok(())
    }

    async fn session_publish_group(
        &mut self,
        work: &Work,
        token: Uuid,
        start: String,
        end: String,
        parents: Vec<Uuid>,
    ) -> Result<()> {
        let sources: Value = sqlx::query_scalar(
            "SELECT coalesce(jsonb_agg(DISTINCT source_name ORDER BY source_name),'[]'::jsonb)
             FROM core.events e
             CROSS JOIN LATERAL jsonb_array_elements_text(e.payload->'observed_by')
                 AS observed(source_name)
             WHERE e.id=ANY($1)",
        )
        .bind(&parents)
        .fetch_one(&mut self.connection)
        .await?;
        let payload = json!({
            "start_ns": start,
            "end_ns": end,
            "owner_day": work.day,
            "profile": PROFILE,
            "sources": sources,
            "continuity": null,
            "attention": null,
            "afk": null,
            "active_s": null,
            "switches": null,
            "claim_scope": "recorded-shell-policy-grouping",
            "coverage": "unknown; no focus or AFK observations; known command spans are recorded claims, not continuous activity"
        });
        let key = json!(["session.shell-v1", start, end]).to_string();
        let mut tx = self.connection.begin().await?;
        let live: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM core.live_events e
             WHERE e.id=ANY($1) AND e.record_state='present'
               AND NOT EXISTS(
                   SELECT 1 FROM unnest(e.source_event_ids) p(id)
                   WHERE NOT EXISTS(SELECT 1 FROM core.live_events raw
                                    WHERE raw.id=p.id AND raw.record_state='present'))",
        )
        .bind(&parents)
        .fetch_one(&mut *tx)
        .await?;
        if live != parents.len() as i64 {
            sqlx::query("UPDATE sinex_session_groups SET done=true WHERE token=$1")
                .bind(token)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(());
        }
        let existing: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM core.live_events
             WHERE event_type='activity.session.boundary' AND equivalence_key=$1
               AND source_event_ids=$2 AND payload=$3
             ORDER BY id LIMIT 1",
        )
        .bind(&key)
        .bind(&parents)
        .bind(&payload)
        .fetch_optional(&mut *tx)
        .await?;
        let id = match existing {
            Some(id) => id,
            None => {
                // A changed session may cross into the next day; redo that day
                // too. A reused session marks nothing, so neighbouring days stop.
                mark_days_dirty(&mut tx, &work.day, &end, work.snapshot_seq, true).await?;
                let id = Uuid::now_v7();
                let seq: i64 = next_commit_seq(&mut tx).await?;
                let hash = blake3::hash(&serde_json::to_vec(&payload).unwrap())
                    .as_bytes()
                    .to_vec();
                sqlx::query(
                    "INSERT INTO core.events(
                        id,source,event_type,payload,ts_orig,ts_orig_tiebreak,ts_quality,
                        ts_precision,ts_clock,commit_seq,record_hash,source_event_ids,
                        producer_version_id,run_id,equivalence_key,payload_schema_id)
                     VALUES($1,'session','activity.session.boundary',$2,
                        to_timestamp(floor($3::text::numeric/1000000000)::double precision)
                            +mod(mod(floor($3::text::numeric/1000),1000000)+1000000,1000000)
                             *interval '1 microsecond',
                        mod(mod($3::text::numeric,1000)+1000,1000)::integer,
                        'intrinsic','instant','producer',$4,$5,$6,$7,$8,$9,6)",
                )
                .bind(id)
                .bind(&payload)
                .bind(&start)
                .bind(seq)
                .bind(hash)
                .bind(&parents)
                .bind(self.session_producer)
                .bind(self.run_id)
                .bind(&key)
                .execute(&mut *tx)
                .await?;
                log_commit(
                    &mut tx,
                    seq,
                    id,
                    Change::Added,
                    event_type::SESSION_BOUNDARY,
                )
                .await?;
                id
            }
        };
        sqlx::query("UPDATE sinex_session_groups SET output_id=$2,done=true WHERE token=$1")
            .bind(token)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Archives the day's boundary events that no group reused. Returns true
    /// when the day is finished.
    async fn session_archive(&mut self, work: &mut Work) -> Result<bool> {
        let rows = sqlx::query(
            "SELECT e.id,e.payload->>'end_ns' AS end_ns FROM core.live_events e
             WHERE e.event_type='activity.session.boundary' AND e.payload->>'owner_day'=$1
               AND ($2::uuid IS NULL OR e.id>$2)
             ORDER BY e.id LIMIT 16",
        )
        .bind(&work.day)
        .bind(work.cursor)
        .fetch_all(&mut self.connection)
        .await?;
        let mut tx = self.connection.begin().await?;
        if rows.is_empty() {
            if work.day < work.through_day {
                sqlx::query(concat!(
                    "INSERT INTO derivation.session_dirty(day,dirty_seq,through_day)
                     VALUES(($1::text::date+1)::text,$2,$3)",
                    merge_dirty_day!()
                ))
                .bind(&work.day)
                .bind(work.snapshot_seq)
                .bind(&work.through_day)
                .execute(&mut *tx)
                .await?;
            }
            sqlx::query(
                "DELETE FROM derivation.session_dirty
                 WHERE day=$1 AND dirty_seq<=$2 AND coalesce(through_day,day)<=$3",
            )
            .bind(&work.day)
            .bind(work.snapshot_seq)
            .bind(&work.through_day)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE derivation.session_progress SET phase=$1,failure=NULL WHERE one")
                .bind(IDLE_PHASE)
                .execute(&mut *tx)
                .await?;
            let _: i64 = next_commit_seq(&mut tx).await?;
            tx.commit().await?;
            return Ok(true);
        }
        for row in rows {
            let id: Uuid = row.try_get("id")?;
            let reused: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM sinex_session_groups WHERE output_id=$1)",
            )
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
            if !reused {
                mark_days_dirty(
                    &mut tx,
                    &work.day,
                    &row.try_get::<String, _>("end_ns")?,
                    work.snapshot_seq,
                    true,
                )
                .await?;
                let seq: i64 = next_commit_seq(&mut tx).await?;
                sqlx::query(
                    "INSERT INTO audit.archived(id,archived_seq,reason) VALUES($1,$2,'supersede')",
                )
                .bind(id)
                .bind(seq)
                .execute(&mut *tx)
                .await?;
                log_commit(
                    &mut tx,
                    seq,
                    id,
                    Change::Removed,
                    event_type::SESSION_BOUNDARY,
                )
                .await?;
            }
            work.cursor = Some(id);
        }
        tx.commit().await?;
        Ok(false)
    }

    /// Stores a finished group if this day owns it. A group starting on an
    /// earlier day matters only when it crosses into this day; it is stored
    /// so its unchanged event is reused, and its owner day is marked dirty
    /// unless an identical session already exists.
    async fn session_close_group(
        &mut self,
        day: &str,
        mut group: Group,
        snapshot_seq: i64,
    ) -> Result<()> {
        group.parents.sort();
        let start = group.start.to_string();
        let end = group.end.to_string();
        let owner: String = sqlx::query_scalar(
            "SELECT ((to_timestamp(floor($1::text::numeric/1000000000)::double precision)
                     AT TIME ZONE 'Europe/Warsaw')-interval '4 hours')::date::text",
        )
        .bind(&start)
        .fetch_one(&mut self.connection)
        .await?;
        if owner != day {
            let crosses: bool = sqlx::query_scalar(
                "WITH b AS (SELECT extract(epoch FROM (($3::text::date+time '04:00')
                                       AT TIME ZONE 'Europe/Warsaw'))*1000000000 AS day_start)
                 SELECT $1::text::numeric<day_start AND $2::text::numeric>=day_start FROM b",
            )
            .bind(&start)
            .bind(&end)
            .bind(day)
            .fetch_one(&mut self.connection)
            .await?;
            if !crosses {
                return Ok(());
            }
        }
        sqlx::query(
            "INSERT INTO sinex_session_groups(token,start_ns,end_ns,owner_day,parents)
             VALUES($1,$2::text::numeric,$3::text::numeric,$4,$5)",
        )
        .bind(Uuid::now_v7())
        .bind(&start)
        .bind(&end)
        .bind(&owner)
        .bind(&group.parents)
        .execute(&mut self.connection)
        .await?;
        if owner != day {
            let unchanged: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM core.live_events
                   WHERE event_type='activity.session.boundary' AND payload->>'start_ns'=$1
                     AND payload->>'end_ns'=$2 AND source_event_ids=$3)",
            )
            .bind(&start)
            .bind(&end)
            .bind(&group.parents)
            .fetch_one(&mut self.connection)
            .await?;
            if !unchanged {
                mark_days_dirty(&mut self.connection, &owner, &end, snapshot_seq, false).await?;
            }
        }
        Ok(())
    }
}
