//! A producer switch consumes a comparison over unchanged inputs.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct InputState {
    pub commit: i64,
    pub changes: i64,
    pub tombstoned: i64,
    /// Native positions through which this statement saw committed rows.
    /// They bound later reads; unrelated newer rows do not invalidate it.
    pub through_commit: i64,
    pub through_changes: i64,
}

pub async fn input_state(conn: &mut PgConnection, inputs: &[String]) -> Result<InputState> {
    Ok(sqlx::query_as(include_str!("selection/input_state.sql"))
        .bind(inputs)
        .persistent(false)
        .fetch_one(conn)
        .await?)
}

impl InputState {
    /// A committed input or visibility change after this snapshot invalidates
    /// it. The one writer assigns new commit/change positions; tombstones are
    /// the only operation that removes stored input rows and keep their
    /// existing global counter check. Old timestamps and UUIDs are irrelevant.
    pub async fn is_current(&self, conn: &mut PgConnection, inputs: &[String]) -> Result<bool> {
        Ok(sqlx::query_scalar(include_str!("selection/is_current.sql"))
            .bind(inputs)
            .bind(self.through_commit)
            .bind(self.through_changes)
            .bind(self.tombstoned)
            .persistent(false)
            .fetch_one(conn)
            .await?)
    }
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
    let compared: InputState = serde_json::from_value(
        context.get("inputs").cloned().unwrap_or_default(),
    )
    .map_err(|_| Error::Invalid("comparison has no input snapshot; compare again".into()))?;
    if !compared.is_current(conn, &selected.inputs).await? {
        return Err(Error::Invalid(
            "producer inputs changed after comparison; compare again".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
