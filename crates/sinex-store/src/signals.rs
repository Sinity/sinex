//! One host checkpoint, committed by the writer with ordinary signal readings.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sinex_model::{EventId, Timestamp};
use sqlx::PgConnection;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForwardedKind {
    Intent,
    Emergency,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Forwarded {
    pub kind: ForwardedKind,
    pub available: bool,
    pub producer: String,
    pub event: EventId,
    pub intent: ::signals::FiredIntent,
    pub notice: ::signals::notifications::Notice,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingDelivery {
    pub handed_at: Timestamp,
    pub delivery: ::signals::Delivery,
    pub rule: Option<::signals::ActiveRule>,
    pub notice: Option<::signals::notifications::Notice>,
    pub producer: Option<String>,
    pub import: Option<::signals::notifications::Attempt>,
    pub settings_identity: String,
    pub parents: Vec<EventId>,
}

/// Provenance frozen when the shared host first receives an intent, including
/// intents held by quiet hours. Presentation reads do not replace this basis.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MatchBasis {
    pub parents: Vec<EventId>,
    pub read: crate::ReadMark,
    pub settings_identity: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub settings_identity: String,
    pub evaluator: ::signals::Evaluator,
    pub host_policy: ::signals::HostPolicy,
    pub notifications: ::signals::notifications::NotificationState,
    pub forwarded: BTreeMap<String, Forwarded>,
    pub outbox: BTreeMap<String, PendingDelivery>,
    pub rule_events: BTreeMap<String, EventId>,
    pub rules: BTreeMap<String, ::signals::ActiveRule>,
    pub match_basis: BTreeMap<String, MatchBasis>,
    #[serde(skip)]
    pub current_inputs: BTreeMap<String, Vec<EventId>>,
    pub rows: BTreeMap<String, BTreeMap<String, ::signals::Row>>,
    pub attempts: BTreeMap<String, ::signals::DeliveryReceipt>,
    pub observed_seq: i64,
    pub passive_count: u64,
}

/// A writer-owned state transition, never deserialized from an authored rule.
pub struct Commit {
    pub host: String,
    pub class: sinex_model::Class,
    pub evaluations: Vec<sinex_model::Event>,
    pub decisions: Vec<sinex_model::Event>,
    pub decision_marks: BTreeMap<String, crate::ReadMark>,
    pub read_mark: Option<crate::ReadMark>,
    pub expected_revision: Option<u64>,
    /// Evaluations must still describe this input snapshot. Receipt-only
    /// updates do not replace an evaluation and omit this check.
    pub observed_seq: Option<i64>,
    pub checkpoint: Checkpoint,
}

pub struct Saved {
    pub revision: u64,
    pub checkpoint: Checkpoint,
}

pub async fn load(conn: &mut PgConnection, host: &str) -> Result<Option<Saved>> {
    let row: Option<(i64, serde_json::Value)> =
        sqlx::query_as("SELECT revision,state FROM core.signal_checkpoints WHERE host=$1")
            .bind(host)
            .fetch_optional(conn)
            .await?;
    row.map(|(revision, state)| {
        Ok(Saved {
            revision: u64::try_from(revision)
                .map_err(|_| Error::Invalid("negative signal checkpoint revision".into()))?,
            checkpoint: serde_json::from_value(state)
                .map_err(|error| Error::Invalid(format!("signal checkpoint: {error}")))?,
        })
    })
    .transpose()
}

/// Called inside the admission transaction. A stale computation cannot replace
/// a newer evaluator, host budget, consumed trigger or notification attempt.
pub async fn save(
    conn: &mut PgConnection,
    host: &str,
    expected: Option<u64>,
    checkpoint: &Checkpoint,
) -> Result<u64> {
    let next = expected
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("signal checkpoint revision exceeds u64".into()))?;
    let stored = i64::try_from(next)
        .map_err(|_| Error::Invalid("signal checkpoint revision exceeds bigint".into()))?;
    let state =
        serde_json::to_value(checkpoint).map_err(|error| Error::Invalid(error.to_string()))?;
    let changed=match expected {
        None=>sqlx::query("INSERT INTO core.signal_checkpoints(host,revision,state) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
            .bind(host).bind(stored).bind(state).execute(conn).await?.rows_affected(),
        Some(expected)=>sqlx::query("UPDATE core.signal_checkpoints SET revision=$2,state=$3 WHERE host=$1 AND revision=$4")
            .bind(host).bind(stored).bind(state).bind(i64::try_from(expected).map_err(|_|Error::Invalid("signal checkpoint revision exceeds bigint".into()))?).execute(conn).await?.rows_affected(),
    };
    if changed != 1 {
        return Err(Error::Invalid(
            "signal checkpoint changed; evaluate the committed state again".into(),
        ));
    }
    Ok(next)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterruptBudgetProjection {
    pub per_hour: u64,
    pub used: u64,
    pub remaining: u64,
    pub window_ms: i64,
}

/// The public context carries limits and actual budget use, never trigger IDs,
/// notification attempts, rule evidence, private intervals or outlet commands.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum HostPolicyProjection {
    Available {
        checkpoint_revision: u64,
        settings_identity: String,
        quiet_hours: Option<::signals::QuietHours>,
        interrupt_budget: InterruptBudgetProjection,
        observed_at: Timestamp,
    },
    Pending {
        reason: String,
    },
    Unavailable {
        reason: String,
    },
}

pub async fn public_host_policy(
    conn: &mut PgConnection,
    host: &str,
    configured: bool,
    current_identity: &str,
    at: Timestamp,
) -> Result<HostPolicyProjection> {
    if !configured {
        return Ok(HostPolicyProjection::Unavailable {
            reason: "signals_not_configured".into(),
        });
    }
    let Some(saved) = load(conn, host).await? else {
        return Ok(HostPolicyProjection::Pending {
            reason: "host_policy_not_initialized".into(),
        });
    };
    if saved.checkpoint.settings_identity != current_identity {
        return Ok(HostPolicyProjection::Pending {
            reason: "host_policy_settings_changed".into(),
        });
    }
    let now = i64::try_from(at.unix_ns() / 1_000_000)
        .map_err(|_| Error::Invalid("host policy observation clock exceeds milliseconds".into()))?;
    if now < saved.checkpoint.evaluator.now() {
        return Ok(HostPolicyProjection::Pending {
            reason: "owner_clock_precedes_checkpoint".into(),
        });
    }
    let budget = &saved.checkpoint.host_policy.budget;
    // Shared HostPolicy records only budget-consuming (non-operator) handoffs.
    let used = u64::try_from(
        budget
            .handoffs
            .iter()
            .filter(|handoff| **handoff <= now && now.saturating_sub(**handoff) < ::signals::HOUR)
            .count(),
    )
    .map_err(|_| Error::Invalid("interrupt budget count exceeds u64".into()))?;
    let per_hour = u64::try_from(budget.per_hour)
        .map_err(|_| Error::Invalid("interrupt budget limit exceeds u64".into()))?;
    Ok(HostPolicyProjection::Available {
        checkpoint_revision: saved.revision,
        settings_identity: saved.checkpoint.settings_identity,
        quiet_hours: saved.checkpoint.host_policy.quiet_hours,
        interrupt_budget: InterruptBudgetProjection {
            per_hour,
            used,
            remaining: per_hour.saturating_sub(used),
            window_ms: ::signals::HOUR,
        },
        observed_at: at,
    })
}

#[cfg(test)]
mod tests;

/// Visible authored rule rows at the evaluation position.
pub async fn rule_rows(
    conn: &mut PgConnection,
    kind: &str,
    seq: i64,
) -> Result<Vec<(EventId, serde_json::Value)>> {
    Ok(sqlx::query!(r#"SELECT id AS "id!",payload AS "payload!" FROM core.visible_events e WHERE event_type=$1 AND NOT audit.is_archived_at(id,$2) AND commit_seq<=$2 ORDER BY payload->>'name',commit_seq"#,kind,seq)
        .fetch_all(conn).await?.into_iter().map(|row|(EventId(row.id),row.payload)).collect())
}

/// Retained transport ownership comes from the admitted frame, including replay.
pub async fn producer_records(
    conn: &mut PgConnection,
    kind: &str,
    acquisition_kind: &str,
) -> Result<Vec<(EventId, serde_json::Value, String, Option<String>)>> {
    Ok(sqlx::query!(r#"SELECT e.id AS "id!",e.payload AS "payload!",f.producer,f.family FROM core.visible_events e JOIN core.event_children acquisition ON acquisition.child=e.id JOIN core.events parent ON parent.id=acquisition.parent JOIN core.ingress_frames f ON f.material_id=parent.material_id WHERE e.event_type=$1 AND parent.event_type=$2 AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) ORDER BY e.commit_seq"#,kind,acquisition_kind)
        .fetch_all(conn).await?.into_iter().map(|row|(EventId(row.id),row.payload,row.producer,row.family)).collect())
}

/// Only delivery ownership and channel are exposed to a receipt producer.
pub async fn owns_handoff(
    conn: &mut PgConnection,
    kind: &str,
    id: &str,
    family: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar!(r#"SELECT EXISTS(SELECT 1 FROM core.events WHERE event_type=$1 AND payload->>'delivery_id'=$2 AND payload->'delivery'->'trigger'->'rule'->>'product'=$3 AND payload->'delivery'->>'channel' IN ('action','value')) AS "owned!""#,kind,id,family)
        .fetch_one(conn).await?)
}

/// The effect admission snapshot can briefly lock current policy before platform I/O.
pub async fn effect_snapshot(conn: &mut PgConnection) -> Result<()> {
    sqlx::query!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(conn)
        .await?;
    Ok(())
}

/// A stable delivery ID cannot be reused for different frozen evidence.
pub struct FrozenEvidence {
    pub position: Option<i64>,
    pub payload: serde_json::Value,
    pub settings_identity: Option<String>,
}
pub async fn frozen_payload(
    conn: &mut PgConnection,
    kind: &str,
    id: &str,
    family: &str,
    acquisition_kind: &str,
) -> Result<Option<FrozenEvidence>> {
    Ok(sqlx::query!(r#"SELECT e.payload AS "payload!",m.manifest->>'signal_settings_identity' AS "settings_identity?",(m.manifest->>'signal_position')::bigint AS "position?" FROM core.events e JOIN core.event_children acquisition ON acquisition.child=e.id JOIN core.events parent ON parent.id=acquisition.parent JOIN raw.materials m ON m.id=parent.material_id JOIN core.ingress_frames f ON f.material_id=parent.material_id WHERE e.event_type=$1 AND e.payload->>'delivery_id'=$2 AND f.family=$3 AND parent.event_type=$4 ORDER BY e.commit_seq DESC LIMIT 1"#,kind,id,family,acquisition_kind).fetch_optional(conn).await?.map(|row|FrozenEvidence{position:row.position,payload:row.payload,settings_identity:row.settings_identity}))
}

/// Resolve an acquisition repeated in another frame to its retained live reading.
pub async fn live_reading(
    conn: &mut PgConnection,
    event: &sinex_model::Event,
) -> Result<Option<EventId>> {
    if let Some(key) = &event.equivalence_key {
        return Ok(sqlx::query_scalar!("SELECT event_id FROM core.live_equivalence WHERE source=$1 AND event_type=$2 AND equivalence_key=core.encode_equivalence_key($3)",event.source,event.event_type,key.0).fetch_optional(conn).await?.map(EventId));
    }
    let Some((location, output)) = event.location() else {
        return Ok(None);
    };
    let anchor = i64::try_from(location.anchor)
        .map_err(|_| crate::Error::Invalid("acquisition anchor exceeds SQL bigint".into()))?;
    let output = i64::try_from(output).map_err(|_| {
        crate::Error::Invalid("acquisition output ordinal exceeds SQL bigint".into())
    })?;
    Ok(sqlx::query_scalar!("SELECT event_id FROM core.live_locations WHERE material_id=$1 AND anchor=$2 AND event_type=$3 AND output_index=$4",location.material.0,anchor,event.event_type,output).fetch_optional(conn).await?.map(EventId))
}

/// A replay uses the original owner-certified input snapshot, never a fresh
/// policy mark that would certify copied evidence under a different transform.
pub async fn material_read_mark(
    conn: &mut PgConnection,
    material: sinex_model::MaterialId,
    delivery: &str,
) -> Result<Option<crate::ReadMark>> {
    let value = sqlx::query_scalar!(
        "SELECT CASE WHEN (manifest->'uncertified_signals') ? $2 THEN NULL ELSE manifest->'signal_read_mark' END FROM raw.materials WHERE id=$1",
        material.0,delivery
    )
    .fetch_optional(conn)
    .await?
    .flatten();
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| {
            crate::Error::Invalid(format!("invalid signal acquisition read mark: {error}"))
        })
}

/// Complete the existing ingress receipt after all phases of one admission.
/// This update is inside the original transaction, before any observer can
/// see a partial recorded result.
pub async fn finish_frame(
    conn: &mut PgConnection,
    frame: &sinex_model::IngressFrame,
    outcome: &crate::Outcome,
) -> Result<()> {
    let ids: Vec<_> = outcome.admitted_ids.iter().map(|id| id.0).collect();
    let counts = outcome.frame_counts(frame);
    sqlx::query!("UPDATE core.ingress_frames SET admitted=$3,outcome=$4,commit_seq=$5 WHERE producer=$1 AND frame=$2",frame.producer,frame.frame,&ids,counts,outcome.commit_seq.map(|(_,last)|last)).execute(conn).await?;
    Ok(())
}

pub async fn recorded_frame(
    conn: &mut PgConnection,
    frame: &sinex_model::IngressFrame,
) -> Result<Option<crate::Outcome>> {
    crate::admission::recorded(conn, frame).await
}

/// Exact native reading membership for a producer's frozen decision basis.
pub async fn input_position(conn: &mut PgConnection, id: EventId) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar!("SELECT e.commit_seq FROM core.events e WHERE e.id=$1 AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)",id.0).fetch_optional(conn).await?)
}

/// Rereading a native fact may change reading IDs, but cannot change the exact
/// native input set or promote legacy unknown originals to a proved lineage.
async fn lineage(
    conn: &mut PgConnection,
    ids: &[uuid::Uuid],
) -> Result<Option<BTreeMap<uuid::Uuid, String>>> {
    let rows=sqlx::query!(r#"SELECT e.id,coalesce(o.recoverable,false) AS "known!",jsonb_build_array(e.source,e.event_type,CASE WHEN o.equivalence_key IS NOT NULL THEN jsonb_build_array('key',o.equivalence_key) WHEN e.material_id IS NOT NULL THEN jsonb_build_array('record',e.material_id,e.anchor,e.output_index) ELSE jsonb_build_array('event',e.id) END) AS "identity!" FROM core.events e LEFT JOIN privacy.original_events o ON o.event_id=e.id WHERE e.id=ANY($1)"#,ids).fetch_all(conn).await?;
    if rows.len() != ids.len() || rows.iter().any(|row| !row.known) {
        return Ok(None);
    }
    rows.into_iter()
        .map(|row| {
            Ok((
                row.id,
                serde_json::to_string(&row.identity)
                    .map_err(|error| crate::Error::Invalid(error.to_string()))?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()
        .map(Some)
}
pub async fn same_input_lineage(
    conn: &mut PgConnection,
    left: &[EventId],
    right: &[EventId],
) -> Result<bool> {
    let left = left
        .iter()
        .map(|id| id.0)
        .collect::<std::collections::BTreeSet<_>>();
    let right = right
        .iter()
        .map(|id| id.0)
        .collect::<std::collections::BTreeSet<_>>();
    let ids = left.union(&right).copied().collect::<Vec<_>>();
    let Some(lineage) = lineage(conn, &ids).await? else {
        return Ok(false);
    };
    let old = left
        .iter()
        .map(|id| &lineage[id])
        .collect::<std::collections::BTreeSet<_>>();
    let new = right
        .iter()
        .map(|id| &lineage[id])
        .collect::<std::collections::BTreeSet<_>>();
    Ok(old == new)
}
/// Rebind only the originally selected native facts from a fresh complete
/// selection. Extra later rows do not rewrite a historical occurrence.
pub async fn rebind_inputs(
    conn: &mut PgConnection,
    original: &[EventId],
    selected: &[EventId],
) -> Result<Option<Vec<EventId>>> {
    let ids = original
        .iter()
        .chain(selected)
        .map(|id| id.0)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let Some(lineage) = lineage(conn, &ids).await? else {
        return Ok(None);
    };
    let wanted = original
        .iter()
        .map(|id| lineage[&id.0].clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut matches = BTreeMap::<String, EventId>::new();
    for id in selected {
        let native = lineage[&id.0].clone();
        if wanted.contains(&lineage[&id.0])
            && matches
                .insert(native, *id)
                .is_some_and(|previous| previous != *id)
        {
            return Ok(None);
        }
    }
    if matches.len() != wanted.len() {
        return Ok(None);
    }
    let mut parents = matches.into_values().collect::<Vec<_>>();
    parents.sort_unstable();
    Ok(Some(parents))
}

/// This existing owner receipt sequence orders repeated evidence independently
/// of producer or host clocks. A native replay retains its original order.
pub async fn material_order(
    conn: &mut PgConnection,
    material: sinex_model::MaterialId,
) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar!(
        "SELECT number FROM core.ingress_frames WHERE material_id=$1 AND refusal IS NULL",
        material.0
    )
    .fetch_optional(conn)
    .await?)
}
