//! Shell command grouping: joins the Atuin and zsh records of one command into
//! a `command.canonical` event. A pass works in turns over per-connection
//! temp tables, 64 rows per turn; `derivation.shell_dirty` survives a restart
//! and the pass reruns.
use crate::{Error, Result, WriterPool, current_commit_seq, next_commit_seq};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::postgres::PgRow;
use sqlx::{Connection, Row};
use uuid::Uuid;

const RULE_JSON: &str = include_str!("shell-rule.json");

/// `derivation.shell_progress.phase` when no pass is running.
const IDLE_PHASE: &str = "settled";

const WORK_TABLES: &[&str] = &[
    "DROP TABLE IF EXISTS pg_temp.sinex_shell_nodes,pg_temp.sinex_shell_groups",
    "CREATE TEMP TABLE sinex_shell_nodes(
        id uuid PRIMARY KEY,
        source smallint NOT NULL,
        host text,
        command text NOT NULL,
        command_hash text NOT NULL,
        at_ns numeric,
        digest text NOT NULL,
        anchor bigint NOT NULL,
        ekey text NOT NULL,
        paired boolean NOT NULL DEFAULT false,
        count2 bigint NOT NULL DEFAULT 0,
        count60 bigint NOT NULL DEFAULT 0)",
    "CREATE INDEX ON sinex_shell_nodes(source,command_hash,at_ns)",
    "CREATE INDEX sinex_shell_nodes_pending ON sinex_shell_nodes(id) WHERE NOT paired",
    "CREATE TEMP TABLE sinex_shell_groups(
        token uuid PRIMARY KEY,
        parents uuid[] NOT NULL,
        strategy smallint NOT NULL,
        done boolean NOT NULL DEFAULT false,
        output_id uuid)",
    "CREATE INDEX sinex_shell_groups_output ON sinex_shell_groups(output_id)",
    "CREATE INDEX sinex_shell_groups_pending ON sinex_shell_groups(token) WHERE NOT done",
];

/// Present Atuin and zsh command events visible at snapshot `$1`.
macro_rules! inputs_at_snapshot {
    () => {
        "((e.source='terminal.atuin-history' AND e.event_type='command.executed')
            OR (e.source='terminal.zsh-history' AND e.event_type='command.imported'))
          AND e.record_state='present'
          AND e.commit_seq<=$1
          AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<=$1)"
    };
}

const INSERT_DIRTY: &str = "
    INSERT INTO derivation.shell_dirty(parent_id,dirty_seq)
    SELECT $1,seq FROM core.commit_counter WHERE one
    ON CONFLICT(parent_id) DO UPDATE
    SET dirty_seq=greatest(shell_dirty.dirty_seq,EXCLUDED.dirty_seq)";

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
    let r: Rule = serde_json::from_str(RULE_JSON).expect("registered shell rule");
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
    json!({
        "type": "object",
        "required": ["command", "canonical", "observed_by", "association", "field_evidence",
                     "owner_hour", "claim_scope", "exact_execution_count"],
        "properties": {
            "command": {"type": "string"},
            "canonical": {"type": ["string", "null"]},
            "observed_by": {
                "type": "array", "minItems": 1, "maxItems": 2, "uniqueItems": true,
                "items": {"enum": [crate::ATUIN_SOURCE, crate::ZSH_SOURCE]}
            },
            "association": {
                "type": "object",
                "required": ["basis", "rule", "version", "ambiguous", "candidates",
                             "candidate_counts", "strategy"]
            },
            "field_evidence": {"type": "object"},
            "owner_hour": {"type": ["integer", "null"]},
            "claim_scope": {"const": "selected-policy-grouping"},
            "exact_execution_count": {"type": "null"}
        }
    })
}
/// Marks `parent` for regrouping, in the caller's transaction.
pub(crate) async fn dirty(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    parent: Uuid,
) -> Result<()> {
    sqlx::query(INSERT_DIRTY)
        .bind(parent)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// The two matching passes: a strong 2-second window, then a 60-second
/// window that accepts only records with exactly one match on each side.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Window {
    Strong,
    Fallback,
}
impl Window {
    fn bound_ns(self, rule: &Rule) -> i64 {
        match self {
            Window::Strong => rule.window_ns,
            Window::Fallback => rule.fallback_ns,
        }
    }
    /// Stored in `sinex_shell_groups.strategy`; 0 means a singleton.
    fn code(self) -> i16 {
        match self {
            Window::Strong => 2,
            Window::Fallback => 60,
        }
    }
    fn from_code(code: i16) -> Option<Self> {
        match code {
            2 => Some(Window::Strong),
            60 => Some(Window::Fallback),
            _ => None,
        }
    }
    fn count_column(self) -> &'static str {
        match self {
            Window::Strong => "count2",
            Window::Fallback => "count60",
        }
    }
    fn label(self) -> &'static str {
        match self {
            Window::Strong => "window-2s",
            Window::Fallback => "window-60s-unique",
        }
    }
}

/// Steps of one grouping pass, in order.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Phase {
    /// Copy the inputs visible at the snapshot into `sinex_shell_nodes`.
    Scan,
    /// Count each node's possible matches within the window.
    Count(Window),
    /// Pair nodes, closest first.
    Pair(Window),
    /// Every unpaired node becomes a group of one.
    Single,
    /// Write or reuse one canonical event per group.
    Publish,
    /// Archive canonical events that no group reused.
    Archive,
}
impl Phase {
    /// Value stored in `derivation.shell_progress.phase`.
    fn name(self) -> &'static str {
        match self {
            Phase::Scan => "scan",
            Phase::Count(Window::Strong) => "count2",
            Phase::Pair(Window::Strong) => "pair2",
            Phase::Count(Window::Fallback) => "count60",
            Phase::Pair(Window::Fallback) => "pair60",
            Phase::Single => "single",
            Phase::Publish => "publish",
            Phase::Archive => "archive",
        }
    }
    fn next(self) -> Self {
        match self {
            Phase::Scan => Phase::Count(Window::Strong),
            Phase::Count(window) => Phase::Pair(window),
            Phase::Pair(Window::Strong) => Phase::Count(Window::Fallback),
            Phase::Pair(Window::Fallback) => Phase::Single,
            Phase::Single => Phase::Publish,
            Phase::Publish | Phase::Archive => Phase::Archive,
        }
    }
}

pub(crate) struct Work {
    pub snapshot_seq: i64,
    phase: Phase,
    cursor: Option<Uuid>,
    queued_turns: u64,
}
impl Work {
    fn advance(&mut self) {
        self.phase = self.phase.next();
        self.cursor = None;
    }
}

/// A short summary such as `git commit` or `sed -i`, or `None` when the
/// command does not parse into a plain program name.
fn canonical(command: &str) -> Option<String> {
    let words = shlex::split(command)?;
    let mut words = words.iter().skip_while(|w| {
        w.split_once('=').is_some_and(|(k, _)| {
            !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
    });
    let program = words.next()?.rsplit('/').next()?.to_string();
    // Stripping a URL or other credential-bearing word down to a "program"
    // would hide it from credential checks; leave the summary unknown.
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
    if ACTIONS.contains(&program.as_str())
        && let Some(action) = args.first()
    {
        out.push(' ');
        out.push_str(action);
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
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO core.producer_versions(id,name,version,run_id,configuration)
             VALUES($1,'fusion.shell-command',$2,$3,$4)",
        )
        .bind(id)
        .bind(&rule().implementation)
        .bind(self.run_id)
        .bind(serde_json::from_str::<Value>(RULE_JSON).unwrap())
        .execute(&mut self.connection)
        .await?;
        self.shell_producer = Some(id);
        Ok(())
    }

    /// Starts a pass when work is dirty and every input can be grouped.
    async fn shell_begin(&mut self) -> Result<Option<Work>> {
        let dirty: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM derivation.shell_dirty)")
            .fetch_one(&mut self.connection)
            .await?;
        if !dirty {
            return Ok(None);
        }
        // An interrupted pass resumes at its own snapshot.
        let progress = sqlx::query("SELECT basis,phase FROM derivation.shell_progress WHERE one")
            .fetch_one(&mut self.connection)
            .await?;
        let phase: String = progress.try_get("phase")?;
        let snapshot_seq: i64 = if phase == IDLE_PHASE {
            current_commit_seq(&mut self.connection).await?
        } else {
            progress.try_get("basis")?
        };
        // Comparing two sources needs every parent parsed with untransformed,
        // recorded field settings.
        let incomparable: bool = sqlx::query_scalar(concat!(
            "SELECT count(DISTINCT e.source)>1 AND coalesce(bool_or(NOT coalesce(
                    p.configuration#>>'{shell_field_basis,contract}'='sinex.shell-field-basis.v1'
                    AND p.configuration#>'{shell_field_basis,comparable}'='true'::jsonb,
                false)),false)
             FROM core.events e
             LEFT JOIN core.producer_versions p ON p.id=e.producer_version_id
             WHERE ",
            inputs_at_snapshot!()
        ))
        .bind(snapshot_seq)
        .fetch_one(&mut self.connection)
        .await?;
        if incomparable {
            self.shell_failed = true;
            sqlx::query(
                "UPDATE derivation.shell_progress SET failure='cross-source comparison unavailable: some shell \
                 inputs were parsed with transformed or unrecorded field settings; replay them \
                 under the current rules or disable two-source grouping' WHERE one",
            )
            .execute(&mut self.connection)
            .await?;
            return Ok(None);
        }
        // Grouping orders by material digest, which exists only once the
        // source has finished extending the material.
        let unsealed: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM core.events e
                JOIN raw.source_material_registry m ON m.id=e.source_material_id
                WHERE e.source IN ('terminal.atuin-history','terminal.zsh-history')
                  AND e.event_type IN ('command.executed','command.imported')
                  AND e.commit_seq<=$1
                  AND m.blob_id IS NULL
                  AND NOT EXISTS(SELECT 1 FROM audit.archived a
                                 WHERE a.id=e.id AND a.archived_seq<=$1))",
        )
        .bind(snapshot_seq)
        .fetch_one(&mut self.connection)
        .await?;
        if unsealed {
            return Ok(None);
        }
        for sql in WORK_TABLES {
            sqlx::query(sql).execute(&mut self.connection).await?;
        }
        sqlx::query(
            "UPDATE derivation.shell_progress
             SET basis=$1,phase='scan',failure=NULL,scanned=0,replaced=0 WHERE one",
        )
        .bind(snapshot_seq)
        .execute(&mut self.connection)
        .await?;
        Ok(Some(Work {
            snapshot_seq,
            phase: Phase::Scan,
            cursor: None,
            queued_turns: 0,
        }))
    }

    /// Runs one turn of grouping work. Returns false when there is none.
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
        match work.phase {
            Phase::Scan => self.shell_scan(&mut work).await?,
            Phase::Count(window) => self.shell_count(&mut work, window).await?,
            Phase::Pair(window) => self.shell_pair(&mut work, window).await?,
            Phase::Single => self.shell_singles(&mut work).await?,
            Phase::Publish => self.shell_publish(&mut work, queued_commands).await?,
            Phase::Archive => {
                if self.shell_archive(&mut work).await? {
                    return Ok(true);
                }
            }
        }
        sqlx::query("UPDATE derivation.shell_progress SET phase=$1 WHERE one")
            .bind(work.phase.name())
            .execute(&mut self.connection)
            .await?;
        self.shell_work = Some(work);
        Ok(true)
    }

    async fn shell_scan(&mut self, work: &mut Work) -> Result<()> {
        let rows = sqlx::query(concat!(
            "SELECT e.id,e.source,
                    coalesce(nullif(e.host,''),nullif(e.payload->>'hostname','')) AS host,
                    e.payload->>'command' AS command,
                    (extract(epoch FROM e.ts_orig)*1000000000+coalesce(e.ts_orig_tiebreak,0))
                        ::numeric::text AS at_ns,
                    b.blake3,e.anchor_byte,e.equivalence_key
             FROM core.events e
             JOIN raw.source_material_registry m ON m.id=e.source_material_id
             JOIN core.blobs b ON b.id=m.blob_id
             WHERE ($2::uuid IS NULL OR e.id>$2) AND ",
            inputs_at_snapshot!(),
            " ORDER BY e.id LIMIT 64"
        ))
        .bind(work.snapshot_seq)
        .bind(work.cursor)
        .fetch_all(&mut self.connection)
        .await?;
        if rows.is_empty() {
            work.advance();
            return Ok(());
        }
        let sources = rule().sources;
        let mut tx = self.connection.begin().await?;
        for row in rows {
            let id: Uuid = row.try_get("id")?;
            let source: String = row.try_get("source")?;
            let index = sources
                .iter()
                .position(|s| s == &source)
                .ok_or(Error::Invalid("unregistered shell source"))?;
            let command: String = row.try_get("command")?;
            let command = command.trim();
            sqlx::query(
                "INSERT INTO sinex_shell_nodes(
                    id,source,host,command,command_hash,at_ns,digest,anchor,ekey)
                 VALUES($1,$2,$3,$4,$5,$6::text::numeric,$7,$8,$9)",
            )
            .bind(id)
            .bind(i16::try_from(index).unwrap())
            .bind(row.try_get::<Option<String>, _>("host")?)
            .bind(command)
            .bind(blake3::hash(command.as_bytes()).to_hex().to_string())
            .bind(row.try_get::<Option<String>, _>("at_ns")?)
            .bind(row.try_get::<String, _>("blake3")?)
            .bind(row.try_get::<i64, _>("anchor_byte")?)
            .bind(row.try_get::<String, _>("equivalence_key")?)
            .execute(&mut *tx)
            .await?;
            work.cursor = Some(id);
        }
        sqlx::query(
            "UPDATE derivation.shell_progress
             SET scanned=(SELECT count(*) FROM sinex_shell_nodes) WHERE one",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Counts matches for every node before any pairing in this window, so
    /// the fallback's "exactly one match" test sees the whole set.
    async fn shell_count(&mut self, work: &mut Work, window: Window) -> Result<()> {
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM sinex_shell_nodes WHERE ($1::uuid IS NULL OR id>$1) ORDER BY id LIMIT 64",
        )
        .bind(work.cursor)
        .fetch_all(&mut self.connection)
        .await?;
        if ids.is_empty() {
            work.advance();
            return Ok(());
        }
        let sql = include_str!("shell-count.sql").replace("{field}", window.count_column());
        sqlx::query(&sql)
            .bind(&ids)
            .bind(window.bound_ns(&rule()))
            .bind(window == Window::Fallback)
            .execute(&mut self.connection)
            .await?;
        work.cursor = ids.last().copied();
        Ok(())
    }

    /// Pairs up to one page of nodes, always taking the closest remaining
    /// pair; ties break on source order, material digest and anchor.
    async fn shell_pair(&mut self, work: &mut Work, window: Window) -> Result<()> {
        let mut tx = self.connection.begin().await?;
        for _ in 0..64 {
            let edge = sqlx::query(
                r#"SELECT a.id AS a,z.id AS z
                   FROM sinex_shell_nodes a
                   JOIN sinex_shell_nodes z
                     ON a.source=0 AND z.source=1
                    AND a.host=z.host
                    AND a.command_hash=z.command_hash
                    AND a.command=z.command
                    AND abs(a.at_ns-z.at_ns)<=$1::bigint
                   WHERE NOT a.paired AND NOT z.paired
                     AND (NOT $2 OR (a.count60=1 AND z.count60=1))
                   ORDER BY abs(a.at_ns-z.at_ns),
                            a.digest COLLATE "C",a.anchor,z.digest COLLATE "C",z.anchor
                   LIMIT 1"#,
            )
            .bind(window.bound_ns(&rule()))
            .bind(window == Window::Fallback)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(edge) = edge else {
                work.advance();
                break;
            };
            let a: Uuid = edge.try_get("a")?;
            let z: Uuid = edge.try_get("z")?;
            sqlx::query("INSERT INTO sinex_shell_groups(token,parents,strategy) VALUES($1,$2,$3)")
                .bind(a)
                .bind(vec![a, z])
                .bind(window.code())
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE sinex_shell_nodes SET paired=true WHERE id=ANY($1)")
                .bind(vec![a, z])
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn shell_singles(&mut self, work: &mut Work) -> Result<()> {
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM sinex_shell_nodes WHERE NOT paired ORDER BY id LIMIT 64",
        )
        .fetch_all(&mut self.connection)
        .await?;
        if ids.is_empty() {
            work.advance();
            return Ok(());
        }
        let mut tx = self.connection.begin().await?;
        for id in ids {
            sqlx::query("INSERT INTO sinex_shell_groups(token,parents,strategy) VALUES($1,$2,0)")
                .bind(id)
                .bind(vec![id])
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE sinex_shell_nodes SET paired=true WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn shell_publish(&mut self, work: &mut Work, queued_commands: usize) -> Result<()> {
        let groups = Self::shell_pending_groups(&mut self.connection).await?;
        if groups.is_empty() {
            work.advance();
            return Ok(());
        }
        Self::shell_publish_batch(
            &mut self.connection,
            self.shell_producer,
            self.run_id,
            work.snapshot_seq,
            groups,
        )
        .await?;
        crate::TestPause::reach(
            &mut self.pause,
            "fusion-replacement-committed",
            json!({
                "basis": work.snapshot_seq,
                "pending": true,
                "queued_background_turns": work.queued_turns,
                "queued_commands_at_turn_entry": queued_commands
            }),
        )
        .await?;
        Ok(())
    }

    async fn shell_pending_groups(connection: &mut sqlx::PgConnection) -> Result<Vec<PgRow>> {
        Ok(sqlx::query(
            "SELECT token,parents,strategy FROM sinex_shell_groups
             WHERE NOT done ORDER BY token LIMIT 64",
        )
        .fetch_all(connection)
        .await?)
    }

    /// Archives canonical events that this pass did not reuse. Returns true
    /// when the pass has finished.
    async fn shell_archive(&mut self, work: &mut Work) -> Result<bool> {
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT e.id FROM core.live_events e
             WHERE e.event_type='command.canonical' AND ($1::uuid IS NULL OR e.id>$1)
             ORDER BY e.id LIMIT 64",
        )
        .bind(work.cursor)
        .fetch_all(&mut self.connection)
        .await?;
        let mut tx = self.connection.begin().await?;
        if ids.is_empty() {
            sqlx::query("DELETE FROM derivation.shell_dirty WHERE dirty_seq<=$1")
                .bind(work.snapshot_seq)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "UPDATE derivation.shell_progress
                 SET last_evaluated=$1,phase=$2,failure=NULL WHERE one",
            )
            .bind(work.snapshot_seq)
            .bind(IDLE_PHASE)
            .execute(&mut *tx)
            .await?;
            // Bump the counter even when nothing changed, so readers waiting
            // on the counter see that the pass finished.
            let _: i64 = next_commit_seq(&mut tx).await?;
            tx.commit().await?;
            return Ok(true);
        }
        for id in ids {
            let reused: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM sinex_shell_groups WHERE output_id=$1)",
            )
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
            if !reused {
                let seq: i64 = next_commit_seq(&mut tx).await?;
                sqlx::query(
                    "INSERT INTO audit.archived(id,archived_seq,reason) VALUES($1,$2,'supersede')",
                )
                .bind(id)
                .bind(seq)
                .execute(&mut *tx)
                .await?;
                crate::session::dirty(&mut tx, id).await?;
                sqlx::query(
                    "INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id)
                     VALUES($1,$2,1,0,3)",
                )
                .bind(seq)
                .bind(id)
                .execute(&mut *tx)
                .await?;
            }
            work.cursor = Some(id);
        }
        tx.commit().await?;
        Ok(false)
    }

    /// Publishes a page of groups in one transaction: a failure rolls back
    /// the whole batch.
    async fn shell_publish_batch(
        connection: &mut sqlx::PgConnection,
        producer: Option<Uuid>,
        run_id: Uuid,
        snapshot_seq: i64,
        groups: Vec<PgRow>,
    ) -> Result<()> {
        let mut tx = connection.begin().await?;
        for group in groups {
            Self::shell_publish_group(
                &mut tx,
                producer,
                run_id,
                group.try_get("token")?,
                group.try_get("parents")?,
                group.try_get("strategy")?,
                snapshot_seq,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Writes the group's canonical event, or reuses an identical live one.
    async fn shell_publish_group(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        producer: Option<Uuid>,
        run_id: Uuid,
        token: Uuid,
        parents: Vec<Uuid>,
        strategy: i16,
        snapshot_seq: i64,
    ) -> Result<()> {
        let r = rule();
        let rows = sqlx::query(
            r#"SELECT n.id,n.source,n.ekey,n.digest,n.anchor,trunc(n.at_ns)::text AS at_ns,
                      n.count2,n.count60,e.payload,e.ts_quality
               FROM sinex_shell_nodes n JOIN core.events e ON e.id=n.id
               WHERE n.id=ANY($1)
               ORDER BY n.source,n.digest COLLATE "C",n.anchor"#,
        )
        .bind(&parents)
        .fetch_all(&mut **tx)
        .await?;
        if rows.len() != parents.len() || rows.len() > r.sources.len() {
            return Err(Error::Invalid("shell support unavailable"));
        }
        let members = rows
            .iter()
            .map(Member::from_row)
            .collect::<Result<Vec<_>>>()?;
        let merged = merge_group(&r, &members, strategy)?;
        let live: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM core.live_events WHERE id=ANY($1) AND record_state='present'",
        )
        .bind(&parents)
        .fetch_one(&mut **tx)
        .await?;
        if live != parents.len() as i64 {
            // A parent removed or excluded after the snapshot drops the group
            // now; its dirty row stays, so the next pass sees the change.
            sqlx::query("UPDATE sinex_shell_groups SET done=true WHERE token=$1")
                .bind(token)
                .execute(&mut **tx)
                .await?;
            return Ok(());
        }
        let existing: Option<Uuid> = sqlx::query_scalar(
            "SELECT e.id FROM core.live_events e
             JOIN core.producer_versions p ON p.id=e.producer_version_id
             WHERE e.event_type='command.canonical' AND e.equivalence_key=$1
               AND e.source_event_ids=$2 AND e.payload=$3 AND p.version=$4
             ORDER BY e.commit_seq LIMIT 1",
        )
        .bind(&merged.key)
        .bind(&parents)
        .bind(&merged.payload)
        .bind(&r.implementation)
        .fetch_optional(&mut **tx)
        .await?;
        let id = match existing {
            Some(id) => id,
            None => {
                let id = Uuid::now_v7();
                let seq: i64 = next_commit_seq(tx).await?;
                let host = merged.payload["hostname"].as_str().map(str::to_string);
                let hash = blake3::hash(&serde_json::to_vec(&merged.payload).unwrap())
                    .as_bytes()
                    .to_vec();
                // The time columns are copied from the parent that supplied the time.
                sqlx::query(
                    "INSERT INTO core.events(
                        id,source,event_type,host,payload,ts_orig,ts_orig_tiebreak,ts_quality,
                        time_suspect,ts_precision,ts_orig_until,ts_clock,commit_seq,record_hash,
                        source_event_ids,producer_version_id,run_id,equivalence_key,
                        payload_schema_id)
                     SELECT $1,'fusion.shell-command','command.canonical',$2,$3,ts_orig,
                        ts_orig_tiebreak,ts_quality,time_suspect,ts_precision,ts_orig_until,
                        ts_clock,$4,$5,$6,$7,$8,$9,5
                     FROM core.events WHERE id=$10",
                )
                .bind(id)
                .bind(host)
                .bind(merged.payload)
                .bind(seq)
                .bind(hash)
                .bind(&parents)
                .bind(producer)
                .bind(run_id)
                .bind(merged.key)
                .bind(merged.time_parent)
                .execute(&mut **tx)
                .await?;
                crate::session::dirty(tx, id).await?;
                sqlx::query(
                    "INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id)
                     VALUES($1,$2,0,0,3)",
                )
                .bind(seq)
                .bind(id)
                .execute(&mut **tx)
                .await?;
                id
            }
        };
        sqlx::query("UPDATE sinex_shell_groups SET done=true,output_id=$2 WHERE token=$1")
            .bind(token)
            .bind(id)
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "UPDATE derivation.shell_progress SET replaced=replaced+1 WHERE one AND basis=$1",
        )
        .bind(snapshot_seq)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

/// One parent record of a group, as loaded for publication.
struct Member {
    id: Uuid,
    source: i16,
    ekey: String,
    digest: String,
    anchor: i64,
    at_ns: Option<i128>,
    count2: i64,
    count60: i64,
    payload: Value,
    quality: String,
}
impl Member {
    fn from_row(row: &PgRow) -> Result<Self> {
        let at_ns = row
            .try_get::<Option<String>, _>("at_ns")?
            .map(|ns| ns.parse::<i128>())
            .transpose()
            .map_err(|_| Error::Invalid("shell time unavailable"))?;
        Ok(Self {
            id: row.try_get("id")?,
            source: row.try_get("source")?,
            ekey: row.try_get("ekey")?,
            digest: row.try_get("digest")?,
            anchor: row.try_get("anchor")?,
            at_ns,
            count2: row.try_get("count2")?,
            count60: row.try_get("count60")?,
            payload: row.try_get("payload")?,
            quality: row.try_get("ts_quality")?,
        })
    }
    /// Lowest first: known time, then time quality, source order, material
    /// digest and anchor. Precision and event id never decide.
    fn time_rank(&self) -> (bool, usize, i16, &str, i64) {
        (
            self.at_ns.is_none(),
            quality_rank(&self.quality),
            self.source,
            &self.digest,
            self.anchor,
        )
    }
}

struct Merged {
    payload: Value,
    /// The member whose time columns the canonical event copies.
    time_parent: Uuid,
    key: String,
}

/// Builds the canonical payload from members ordered by source, digest and
/// anchor. Key order matters: the record hash covers the serialized payload.
fn merge_group(r: &Rule, members: &[Member], strategy: i16) -> Result<Merged> {
    let window = Window::from_code(strategy);
    let mut payload = json!({});
    let mut evidence = json!({});
    let mut conflicts = json!({});
    let mut keys = Vec::<String>::new();
    let mut observed = Vec::new();
    let mut candidate_counts = json!({});
    let mut ambiguous = false;
    let mut max_candidates = 0i64;
    let mut owner: Option<i64> = None;
    let mut sources_seen = std::collections::HashSet::new();
    for member in members {
        if !sources_seen.insert(member.source) {
            return Err(Error::Invalid("shell group repeats one source"));
        }
        observed.push(r.sources[member.source as usize].clone());
        keys.push(member.ekey.clone());
        if let Some(ns) = member.at_ns {
            let hour = i64::try_from(ns.div_euclid(i128::from(r.bucket_seconds) * 1_000_000_000))
                .map_err(|_| Error::Invalid("shell owner hour unavailable"))?;
            owner = Some(owner.map_or(hour, |old| old.min(hour)));
        }
        if strategy != 0 {
            let count = if strategy == 60 {
                member.count60
            } else {
                member.count2
            };
            candidate_counts[member.id.to_string()] = json!(count);
            ambiguous |= count > 1;
            max_candidates = max_candidates.max(count);
        }
    }
    let time_parent = members
        .iter()
        .min_by(|a, b| a.time_rank().cmp(&b.time_rank()))
        .ok_or(Error::Invalid("empty shell support"))?;
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
        let selected = members
            .iter()
            .filter(|m| !m.payload[field].is_null())
            .min_by_key(|m| {
                (
                    preferences
                        .iter()
                        .position(|s| s == &r.sources[m.source as usize])
                        .unwrap_or(preferences.len()),
                    m.source,
                )
            });
        let Some(selected) = selected else {
            payload[field] = Value::Null;
            evidence[field] = Value::Null;
            continue;
        };
        let value = selected.payload[field].clone();
        payload[field] = value.clone();
        evidence[field] = json!(selected.id);
        let claims: Vec<Value> = members
            .iter()
            .filter(|other| !other.payload[field].is_null() && other.payload[field] != value)
            .map(|other| json!({"parent": other.id, "value": other.payload[field]}))
            .collect();
        if !claims.is_empty() {
            conflicts[field] = json!(claims);
        }
    }
    payload["timestamp_ns"] = time_parent.payload["timestamp_ns"].clone();
    evidence["time"] = json!(time_parent.id);
    if let [only] = members {
        evidence["all"] = json!(only.id);
    }
    payload["canonical"] = json!(canonical(payload["command"].as_str().unwrap_or("")));
    payload["observed_by"] = json!(observed);
    payload["field_evidence"] = evidence;
    payload["conflicting_fields"] = conflicts;
    payload["owner_hour"] = json!(owner);
    payload["association"] = json!({
        "basis": if strategy == 0 { "single" } else { "rule" },
        "rule": r.id,
        "version": r.version,
        "strategy": window.map(Window::label),
        "ambiguous": ambiguous,
        "candidates": max_candidates,
        "candidate_counts": candidate_counts,
        "claim": "selected association; not independent confirmation or execution identity"
    });
    payload["claim_scope"] = json!("selected-policy-grouping");
    payload["exact_execution_count"] = Value::Null;
    payload["capture_coverage"] = json!("unknown");
    payload["source_continuity"] = json!(
        members
            .iter()
            .filter(|m| !m.payload["continuity"].is_null())
            .map(|m| json!({"parent": m.id, "continuity": m.payload["continuity"]}))
            .collect::<Vec<_>>()
    );
    keys.sort();
    Ok(Merged {
        key: json!([r.id, keys]).to_string(),
        time_parent: time_parent.id,
        payload,
    })
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
#[path = "shell_publication_tests.rs"]
mod publication_tests;
#[cfg(test)]
#[path = "shell_tests.rs"]
mod tests;
#[cfg(test)]
#[path = "shell_work_tests.rs"]
mod work_tests;
