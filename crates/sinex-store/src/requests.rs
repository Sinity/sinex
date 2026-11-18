//! Accepted requests and committed step outcomes in the operations log.
//! The writer saves an outcome in the transaction that applies its effects.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::OperationId;
use sqlx::PgConnection;

/// The authenticated retry namespace and original normalized request.
pub struct Request<'a> {
    pub caller: &'a str,
    pub id: &'a str,
    pub actor: &'a str,
    pub arguments: &'a Value,
}

/// One accepted request. The step map contains only committed outcomes.
#[derive(Clone, Debug)]
pub struct Accepted {
    pub operation: OperationId,
    pub progress: Value,
}

impl Accepted {
    pub fn step(&self, index: usize) -> Option<&Value> {
        self.progress.get("steps")?.get(index.to_string())
    }
}

/// Accept once, or return the same operation after a lost response or restart.
/// Call this through the writer, which serializes request creation with effects.
pub async fn begin(conn: &mut PgConnection, request: &Request<'_>) -> Result<Accepted> {
    if let Some(accepted) = lookup(conn, request).await? {
        return Ok(accepted);
    }
    let id = OperationId::new();
    sqlx::query!(
        r#"INSERT INTO core.operations (id, kind, actor, request_scope, request_id, scope, start_seq)
           VALUES ($1, 'request', $2, $3, $4, $5,
                   (SELECT value FROM core.counters WHERE name='commit_seq'))"#,
        id.0, request.actor, request.caller, request.id, request.arguments,
    ).execute(conn).await?;
    Ok(Accepted {
        operation: id,
        progress: serde_json::json!({}),
    })
}

/// A repeat can return its committed outcome before preparing source material.
pub async fn lookup(conn: &mut PgConnection, request: &Request<'_>) -> Result<Option<Accepted>> {
    let row = sqlx::query!(
        r#"SELECT id, actor, scope, progress FROM core.operations
           WHERE request_scope=$1 AND request_id=$2 AND kind='request'"#,
        request.caller,
        request.id,
    )
    .fetch_optional(conn)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.actor != request.actor || row.scope != *request.arguments {
        return Err(Error::RequestConflict);
    }
    Ok(Some(Accepted {
        operation: OperationId(row.id),
        progress: row.progress,
    }))
}

/// Save an accepted question before any effect can commit. Repeats never replace it.
pub async fn accept_question(
    conn: &mut PgConnection,
    operation: OperationId,
    mut question: crate::query::Question,
    grants: &str,
) -> Result<crate::query::Question> {
    question.accept_population(conn, operation, grants).await?;
    let stored =
        serde_json::to_value(&question).map_err(|error| Error::Invalid(error.to_string()))?;
    let updated = sqlx::query!(
        r#"UPDATE core.operations SET progress=jsonb_set(progress, '{question}', $2)
           WHERE id=$1 AND kind='request' AND NOT (progress ? 'question')"#,
        operation.0,
        stored,
    )
    .execute(conn)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::Invalid(
            "request question is missing or already accepted".into(),
        ));
    }
    Ok(question)
}

/// A continuation may read only the question accepted for its authenticated owner.
pub async fn question(
    conn: &mut PgConnection,
    operation: OperationId,
    caller: &str,
) -> Result<crate::query::Question> {
    let question = sqlx::query_scalar!(
        r#"SELECT progress->'question' AS "question?" FROM core.operations
           WHERE id=$1 AND kind='request' AND request_scope=$2"#,
        operation.0,
        caller,
    )
    .fetch_optional(conn)
    .await?
    .flatten()
    .ok_or_else(|| Error::NotFound("accepted question is unavailable for this caller".into()))?;
    serde_json::from_value(question)
        .map_err(|error| Error::Invalid(format!("accepted question: {error}")))
}

/// Read a committed outcome before preparing a repeated step.
pub async fn step(
    conn: &mut PgConnection,
    operation: OperationId,
    index: usize,
) -> Result<Option<Value>> {
    let index = index.to_string();
    Ok(sqlx::query_scalar!(
        r#"SELECT progress->'steps'->$2 AS "reply?" FROM core.operations WHERE id=$1 AND kind='request'"#,
        operation.0, index,
    ).fetch_one(conn).await?)
}

/// Record the exact reply beside a mutation, never after its commit.
/// An existing outcome is immutable; the caller returns it before applying effects.
pub async fn save_step(
    conn: &mut PgConnection,
    operation: OperationId,
    index: usize,
    reply: &Value,
) -> Result<()> {
    let index = index.to_string();
    let updated = sqlx::query!(
        r#"UPDATE core.operations SET progress = jsonb_set(
             progress || jsonb_build_object('steps', COALESCE(progress->'steps', '{}'::jsonb)),
             ARRAY['steps', $2::text], $3),
             end_seq=(SELECT value FROM core.counters WHERE name='commit_seq')
           WHERE id=$1 AND kind='request' AND NOT (COALESCE(progress->'steps', '{}'::jsonb) ? $2)"#,
        operation.0,
        index,
        reply,
    )
    .execute(conn)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::Invalid(
            "request step is missing or already committed".into(),
        ));
    }
    Ok(())
}

/// The exact outcome persisted with a step's effects.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommittedStep {
    pub reply: Value,
    pub counts: sinex_model::AdmissionCounts,
    pub commit_seq: Option<(i64, i64)>,
    pub admitted_ids: Vec<sinex_model::EventId>,
}

impl CommittedStep {
    pub fn new(reply: Value, outcome: &crate::Outcome) -> Self {
        Self {
            reply,
            counts: outcome.counts(),
            commit_seq: outcome.commit_seq,
            admitted_ids: outcome.admitted_ids.clone(),
        }
    }

    pub fn outcome(&self) -> crate::Outcome {
        crate::Outcome {
            commit_seq: self.commit_seq,
            admitted_ids: self.admitted_ids.clone(),
            ..crate::Outcome::from_counts(self.counts)
        }
    }
}

#[cfg(test)]
mod tests;
