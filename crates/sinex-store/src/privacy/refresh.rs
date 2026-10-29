//! Policy re-derivation uses ordinary recompute batches and their transaction.
use crate::{Error,Result};
use serde::{Serialize,Deserialize};
use serde_json::Value;
use sqlx::PgConnection;
#[derive(Clone,Debug,Default,PartialEq,Serialize,Deserialize)]
#[serde(rename_all="snake_case")]
pub enum Phase {#[default] Inputs, Outputs, Complete}
#[derive(Clone,Debug,Default,PartialEq,Serialize,Deserialize)]
pub struct Cursor {pub phase:Phase,pub input_after:i64,pub output_after:Option<Value>}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
pub struct Refresh {pub epoch:i64,pub expected:Option<Cursor>,pub next:Cursor,pub reset_position:bool}
pub async fn cursor(conn:&mut PgConnection,derivation:&str,epoch:i64)->Result<Option<Cursor>>{
    let value:Option<Value>=sqlx::query_scalar("SELECT cursor FROM privacy.derivation_epochs WHERE derivation=$1 AND epoch=$2").bind(derivation).bind(epoch).fetch_optional(conn).await?;
    value.map(|value|serde_json::from_value(value).map_err(|error|Error::Invalid(format!("policy refresh cursor: {error}")))).transpose()
}
pub(crate) async fn record(conn:&mut PgConnection,derivation:&str,refresh:&Refresh)->Result<()> {
    let policy=super::projection::epoch(conn).await?;
    if !policy.ready()||policy.active!=refresh.epoch{return Err(Error::Invalid("derivation_policy_epoch_changed".into()))}
    let previous=cursor(conn,derivation,refresh.epoch).await?;
    if previous!=refresh.expected{return Err(Error::Invalid("policy_refresh_changed".into()))}
    if refresh.reset_position{crate::derivations::reset_position(conn,derivation).await?;}
    sqlx::query("INSERT INTO privacy.derivation_epochs(derivation,epoch,cursor) VALUES($1,$2,$3) ON CONFLICT(derivation) DO UPDATE SET epoch=EXCLUDED.epoch,cursor=EXCLUDED.cursor").bind(derivation).bind(refresh.epoch).bind(serde_json::to_value(&refresh.next).map_err(|error|Error::Invalid(error.to_string()))?).execute(conn).await?;
    Ok(())
}
/// JSONB ordering is PostgreSQL's structural ordering, shared by predicate and
/// cursor. Pages carry whole semantic keys, including an oversized first key.
pub async fn output_keys_after(conn:&mut PgConnection,derivation:&str,after:Option<&Value>,limit:i64,bytes:i64)->Result<Vec<Value>> {
    Ok(sqlx::query_scalar("SELECT key FROM(SELECT key,sum(pg_column_size(key)::bigint) OVER(ORDER BY key)-pg_column_size(key)::bigint AS before FROM(SELECT DISTINCT key FROM derivation.outputs WHERE derivation=$1 AND ($2::jsonb IS NULL OR key>$2) ORDER BY key LIMIT $3) keys) bounded WHERE before<$4 ORDER BY key").bind(derivation).bind(after).bind(limit).bind(bytes).fetch_all(conn).await?)
}
