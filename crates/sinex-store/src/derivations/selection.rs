//! A version switch consumes one full comparison that the running build made
//! of the declaration it switches to. The comparison is a trial of that
//! build's code, not a hold on the inputs: a switch computes from the inputs
//! live when it runs, so what was committed since the comparison is computed
//! like any other input and never refuses the switch.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Selection {
    pub comparison: i64,
    pub operation: uuid::Uuid,
    pub actor: String,
    pub history: sinex_model::DerivationHistory,
    pub version: i64,
    /// The build that switches. It made the comparison, and the switch
    /// operation records it; the selection itself is the version.
    pub build: String,
    pub settings: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub kind: String,
}

pub(crate) async fn validate(
    conn: &mut PgConnection,
    name: &str,
    selected: &Selection,
) -> Result<()> {
    let consumed: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM core.operations WHERE kind='producer_switch' AND scope->>'comparison'=$1)")
        .bind(selected.comparison.to_string()).fetch_one(&mut *conn).await?;
    if consumed {
        return Err(Error::Invalid(
            "producer comparison was already selected".into(),
        ));
    }
    let row: Option<(String, i64, bool, serde_json::Value)> = sqlx::query_as(
        "SELECT derivation,version,range_from IS NULL AND range_until IS NULL,context FROM derivation.comparisons WHERE id=$1")
        .bind(selected.comparison).fetch_optional(&mut *conn).await?;
    let Some((derivation, version, full, context)) = row else {
        return Err(Error::NotFound("no such producer comparison".into()));
    };
    if derivation != name
        || version != selected.version
        || !full
        || context.get("build").and_then(serde_json::Value::as_str) != Some(&selected.build)
    {
        return Err(Error::Invalid(
            "switch needs a full comparison of this producer version and build".into(),
        ));
    }
    if selected.actor.trim().is_empty()
        || context.get("input_types") != Some(&serde_json::json!(selected.inputs))
        || context.get("output_types") != Some(&serde_json::json!(selected.outputs))
        || context.get("kind") != Some(&serde_json::json!(selected.kind))
        || context.get("history") != Some(&serde_json::json!(selected.history))
    {
        return Err(Error::Invalid(
            "comparison does not describe this producer declaration".into(),
        ));
    }
    if context.get("settings") != Some(&serde_json::json!(selected.settings)) {
        return Err(Error::Invalid(
            "logical-day settings changed after comparison; compare again".into(),
        ));
    }
    Ok(())
}

/// The kind (`keyed` or `per_event`) of `derivation`, whose switch
/// `operation` recomputes history and has not ended; any other operation is
/// refused.
pub(crate) async fn unfinished(
    conn: &mut PgConnection,
    derivation: &str,
    operation: uuid::Uuid,
) -> Result<String> {
    sqlx::query_scalar!(
        "SELECT d.kind FROM core.operations o JOIN registry.derivations d ON d.name = $2
         WHERE o.id = $1 AND o.kind = 'producer_switch' AND o.scope->>'derivation' = $2
           AND o.result IS NULL AND o.start_seq IS NOT NULL",
        operation,
        derivation,
    )
    .fetch_optional(conn)
    .await?
    .ok_or_else(|| Error::Invalid(format!("{derivation} has no unfinished switch {operation}")))
}

/// A switch that has selected its version and is still archiving the
/// previous outputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnfinishedSwitch {
    pub derivation: String,
    pub operation: uuid::Uuid,
    /// The writer position of the selection: outputs committed up to it
    /// were made before the switch.
    pub selected_at: i64,
}

/// The switches still archiving previous outputs, oldest first: the one a
/// caller just made, or those a stopped daemon left.
pub async fn unfinished_switches(conn: &mut PgConnection) -> Result<Vec<UnfinishedSwitch>> {
    Ok(sqlx::query_as!(
        UnfinishedSwitch,
        r#"SELECT scope->>'derivation' AS "derivation!", id AS operation, start_seq AS "selected_at!"
           FROM core.operations
           WHERE kind = 'producer_switch' AND result IS NULL AND start_seq IS NOT NULL
           ORDER BY start_seq"#,
    )
    .fetch_all(conn)
    .await?)
}

/// How many live outputs one read of their commit positions covers.
const OUTPUTS_PER_READ: usize = 4096;

/// The live outputs of `derivation` committed up to `position`, with
/// nothing read but their identities and positions: the live table lists the
/// derivation's outputs, and their positions are read in ID order, each
/// group bounded by its IDs and types.
pub async fn live_outputs_through(
    conn: &mut PgConnection,
    derivation: &str,
    position: i64,
) -> Result<Vec<uuid::Uuid>> {
    let mut live = sqlx::query!(
        "SELECT event_id, event_type FROM core.live_equivalence WHERE source = $1",
        derivation,
    )
    .fetch_all(&mut *conn)
    .await?;
    live.sort_unstable_by_key(|row| row.event_id);
    let mut outputs = Vec::with_capacity(live.len());
    for group in live.chunks(OUTPUTS_PER_READ) {
        let ids: Vec<uuid::Uuid> = group.iter().map(|row| row.event_id).collect();
        let mut kinds: Vec<String> = group.iter().map(|row| row.event_type.clone()).collect();
        kinds.sort_unstable();
        kinds.dedup();
        outputs.extend(
            sqlx::query_scalar::<_, uuid::Uuid>(
                "SELECT e.id FROM core.events e
                 WHERE e.id = ANY($1) AND e.id >= $2 AND e.id <= $3 AND e.event_type = ANY($4)
                   AND e.commit_seq <= $5
                 ORDER BY e.id",
            )
            .bind(&ids)
            .bind(ids[0])
            .bind(ids[ids.len() - 1])
            .bind(&kinds)
            .bind(position)
            .persistent(false)
            .fetch_all(&mut *conn)
            .await?,
        );
    }
    Ok(outputs)
}

#[cfg(test)]
mod tests;
