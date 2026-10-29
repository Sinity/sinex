//! A producer switch consumes a comparison over unchanged inputs.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputState {
    pub commit: i64,
    pub changes: i64,
    pub excised: i64,
}

pub async fn input_state(conn: &mut PgConnection, inputs: &[String]) -> Result<InputState> {
    let (commit, changes, excised): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT coalesce(max(commit_seq),0) FROM core.events WHERE event_type=ANY($1)),
          (SELECT coalesce(max(a.change_seq),0) FROM audit.archive_changes a JOIN core.events e ON e.id=a.event_id WHERE e.event_type=ANY($1)),
          (SELECT value FROM core.counters WHERE name='excised')")
        .bind(inputs).fetch_one(conn).await?;
    Ok(InputState {
        commit,
        changes,
        excised,
    })
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Selection {
    pub comparison: i64,
    pub operation: uuid::Uuid,
    pub actor: String,
    pub history: sinex_model::DerivationHistory,
    pub version: i64,
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
        return Err(Error::Invalid("no such producer comparison".into()));
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
    let compared: InputState = serde_json::from_value(
        context.get("inputs").cloned().unwrap_or_default(),
    )
    .map_err(|_| Error::Invalid("comparison has no input snapshot; compare again".into()))?;
    if input_state(conn, &selected.inputs).await? != compared {
        return Err(Error::Invalid(
            "producer inputs changed after comparison; compare again".into(),
        ));
    }
    Ok(())
}
