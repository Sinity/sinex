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

#[cfg(test)]
mod tests;
