//! The host deliverer's checkpoint, committed by the writer in the same
//! transaction as the signal events each step produced.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sinex_model::{Class, EventId, Timestamp};
use sqlx::PgConnection;
use std::collections::BTreeMap;

/// A rule the evaluator runs, with the authored reading that declared it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Rule {
    pub active: ::signals::ActiveRule,
    pub event: EventId,
    /// The highest class any of its answers read. Its deliveries carry the
    /// rows they matched, so they are stored at least this guarded.
    pub class: Class,
}

/// An outlet a handed-off delivery still needs: a desktop notification, a
/// value written to a configured target, or a configured action command.
/// Removed once its attempt is recorded, so a restart never sends twice.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outlet", rename_all = "snake_case")]
pub enum Pending {
    Notice {
        notice: ::signals::notifications::Notice,
    },
    Clear,
    Value {
        target: String,
        delivery: Box<::signals::Delivery>,
    },
    Action {
        command: String,
        reference: String,
    },
}

/// The readings one matched row was computed from, and the read mark of the
/// snapshot they were read in: a delivery is admitted under that mark, so an
/// input archived since then is detected.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Inputs {
    pub parents: Vec<EventId>,
    pub read: crate::ReadMark,
}

/// A firing another product forwarded that the host policy still holds: the
/// report reading that carried it becomes its delivery's parent.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Forwarded {
    pub report: EventId,
    pub class: Class,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The public host settings this state was built under.
    pub settings_identity: String,
    pub evaluator: ::signals::Evaluator,
    pub host: ::signals::HostPolicy,
    pub notifications: ::signals::notifications::NotificationState,
    pub rules: BTreeMap<String, Rule>,
    /// The latest complete rows of each rule, so a row keeps the start and
    /// observation it was first seen with.
    pub rows: BTreeMap<String, BTreeMap<String, ::signals::Row>>,
    /// For each rule and occurrence, the readings its row was computed from
    /// when first seen: the reading itself, or every reading a group or
    /// aggregate selected. Kept while the row is in the answer or a held
    /// firing cites it, so a delivery cites what it matched; forgotten when a
    /// firing citing them is withdrawn, so the row is selected afresh.
    pub inputs: BTreeMap<String, BTreeMap<String, Inputs>>,
    /// Handed-off current-state deliveries whose condition has not cleared:
    /// a released or cleared outcome refers back to them.
    pub open: BTreeMap<String, ::signals::Delivery>,
    pub outbox: BTreeMap<String, Pending>,
    pub forwarded: BTreeMap<String, Forwarded>,
    /// Forwarded reports up to this commit position are taken in.
    pub reports_seq: i64,
    /// The commit position the rules were last evaluated at.
    pub observed_seq: i64,
}

/// The deliveries one step handed off: derived events, each citing its rule
/// or forwarded report and the readings its rows matched, with the read mark
/// those readings were selected under, written by the deliverer's run.
pub struct Deliveries {
    pub run: sinex_model::RunId,
    pub events: Vec<(sinex_model::Event, crate::ReadMark)>,
}

/// A writer-owned state transition, never deserialized from an authored rule.
pub struct Commit {
    pub host: String,
    /// The class of the material holding this step's outcome records.
    pub class: Class,
    pub deliveries: Option<Deliveries>,
    pub expected_revision: Option<u64>,
    /// Evaluations must still describe this input snapshot. Outcome-only
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
/// a newer evaluator, host policy or notification attempt.
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
        return Err(Error::StaleCheckpoint);
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

/// The public context carries limits and actual budget use, never delivery
/// identities, notification attempts, rule evidence or outlet commands.
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
    let budget = &saved.checkpoint.host.budget;
    // The host budget records only charged (agent and producer) handoffs.
    let used = u64::try_from(budget.used(now))
        .map_err(|_| Error::Invalid("interrupt budget count exceeds u64".into()))?;
    let per_hour = u64::try_from(budget.per_hour)
        .map_err(|_| Error::Invalid("interrupt budget limit exceeds u64".into()))?;
    Ok(HostPolicyProjection::Available {
        checkpoint_revision: saved.revision,
        settings_identity: saved.checkpoint.settings_identity,
        quiet_hours: saved.checkpoint.host.settings.quiet_hours,
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

/// A forwarded report as read: its reading, class and payload.
pub struct Report {
    pub event: EventId,
    pub class: Class,
    pub payload: serde_json::Value,
}

/// Live forwarded reports committed after `after` and up to `through`, in
/// commit order.
pub async fn reports(
    conn: &mut PgConnection,
    kind: &str,
    after: i64,
    through: i64,
) -> Result<Vec<Report>> {
    sqlx::query!(r#"SELECT id AS "id!",class::text AS "class!",payload AS "payload!" FROM core.visible_events WHERE event_type=$1 AND commit_seq>$2 AND commit_seq<=$3 AND NOT audit.is_archived_at(id,$3) ORDER BY commit_seq,id"#,kind,after,through)
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(|row| {
            Ok(Report {
                event: EventId(row.id),
                class: serde_json::from_value(serde_json::Value::String(row.class))
                    .map_err(|error| Error::Invalid(format!("report class: {error}")))?,
                payload: row.payload,
            })
        })
        .collect()
}
