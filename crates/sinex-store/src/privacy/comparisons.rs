//! Stored previews are readable only under the policy and grants that made
//! their examples. Candidate outputs use the same Rust transform as events.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::Event;
use sqlx::PgConnection;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    pub policy_epoch: i64,
    pub outputs_revision: i64,
    pub grants: Vec<String>,
}
pub async fn scope(conn: &mut PgConnection) -> Result<Scope> {
    let (desired,active,outputs_revision,grants):(i64,i64,i64,String)=sqlx::query_as("SELECT desired_epoch,active_epoch,(SELECT value FROM core.counters WHERE name='privacy_outputs'),coalesce(current_setting('sinex.grants',true),'') FROM privacy.policy_state WHERE id").fetch_one(conn).await?;
    if desired != active {
        return Err(Error::Invalid("privacy_projection_stale".into()));
    }
    let mut grants: Vec<_> = grants
        .split(',')
        .filter(|grant| !grant.is_empty())
        .map(str::to_owned)
        .collect();
    grants.sort();
    grants.dedup();
    Ok(Scope {
        policy_epoch: active,
        outputs_revision,
        grants,
    })
}
pub async fn require_scope(conn: &mut PgConnection, context: &Value) -> Result<()> {
    let saved: Scope =
        serde_json::from_value(context.get("read_scope").cloned().unwrap_or(Value::Null))
            .map_err(|_| Error::Invalid("comparison_privacy_scope_unavailable".into()))?;
    if scope(conn).await? != saved {
        return Err(Error::Invalid("comparison_privacy_scope_changed".into()));
    }
    Ok(())
}
pub async fn require_grants(conn: &mut PgConnection, events: &[Event]) -> Result<()> {
    let classes: Vec<_> = events
        .iter()
        .map(|event| super::class_name(event.class).to_owned())
        .collect();
    let allowed: bool = sqlx::query_scalar(
        "SELECT coalesce(bool_and(core.class_visible(class)),true) FROM unnest($1::text[]) class",
    )
    .bind(classes)
    .fetch_one(conn)
    .await?;
    if !allowed {
        return Err(Error::Invalid("comparison_grants_unavailable".into()));
    }
    Ok(())
}
pub async fn project_new(conn: &mut PgConnection, events: &mut [Event]) -> Result<bool> {
    let rules = super::load(conn, events.iter().map(|event| event.source.as_str())).await?;
    let mut changed = false;
    for event in events.iter_mut() {
        changed |= rules.apply(event)?.changed;
    }
    require_grants(conn, events).await?;
    Ok(changed)
}

/// A changed output must not expose its private computation key as another
/// copy of a value the operator just masked. The shared keyed hash preserves
/// joins without treating the key as a new detector input.
pub async fn public_key(
    conn: &mut PgConnection,
    key: &Value,
    old: &[Event],
    new_changed: bool,
) -> Result<Value> {
    let ids: Vec<_> = old.iter().map(|event| event.id.0).collect();
    let old_changed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM privacy.event_outputs o LEFT JOIN privacy.original_events p ON p.event_id=o.event_id WHERE o.event_id=ANY($1) AND (o.changed OR o.equivalence_key IS DISTINCT FROM p.equivalence_key))").bind(ids).fetch_one(&mut *conn).await?;
    if new_changed || old_changed {
        let rules = super::load(conn, std::iter::empty()).await?;
        Ok(Value::String(rules.keyed(&key.to_string())))
    } else {
        Ok(key.clone())
    }
}

#[cfg(test)]
mod tests;
