//! Stage operations: what `stage` records in the operations log, and the
//! materials it stores before any parser recognises them. A stage's scope is
//! what was asked; its progress is the inventory of everything it staged,
//! with each product's accounting as it is read; its counts are the totals.
use crate::{Error, Result};
use serde::Serialize;
use serde_json::Value;
use sinex_model::DbCheck;
use sinex_model::lifecycle::OperationResult;
use sinex_model::{InputRef, Manifest, MaterialId, ObservationFacts, OperationId, RevisionChange};
use sqlx::PgConnection;

/// The operations log's name for a stage.
pub const KIND: &str = "stage";

/// Record a stage that is starting, with its inventory.
pub async fn begin(
    conn: &mut PgConnection,
    operation: OperationId,
    actor: &str,
    scope: &Value,
    inventory: &Value,
) -> Result<()> {
    sqlx::query!(
        r#"INSERT INTO core.operations (id, kind, actor, scope, start_seq, progress)
           VALUES ($1, $2, $3, $4, (SELECT value FROM core.counters WHERE name = 'commit_seq'), $5)"#,
        operation.0,
        KIND,
        actor,
        scope,
        inventory,
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Save the inventory as it stands while the stage runs.
pub async fn progress(
    conn: &mut PgConnection,
    operation: OperationId,
    inventory: &Value,
) -> Result<()> {
    let updated = sqlx::query!(
        "UPDATE core.operations SET progress = $2 WHERE id = $1 AND kind = $3 AND result IS NULL",
        operation.0,
        inventory,
        KIND,
    )
    .execute(conn)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::Invalid(format!(
            "operation {} is not a running stage",
            operation.0
        )));
    }
    Ok(())
}

/// Record how the stage ended: its final inventory, its totals and, when it
/// stopped early, why.
pub async fn end(
    conn: &mut PgConnection,
    operation: OperationId,
    result: OperationResult,
    counts: &Value,
    inventory: &Value,
    refusals: &Value,
) -> Result<()> {
    let updated = sqlx::query!(
        r#"UPDATE core.operations SET result = $2, counts = $3, progress = $4, refusals = $5,
             end_seq = (SELECT value FROM core.counters WHERE name = 'commit_seq')
           WHERE id = $1 AND kind = $6"#,
        operation.0,
        result.as_db(),
        counts,
        inventory,
        refusals,
        KIND,
    )
    .execute(conn)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::Invalid(format!(
            "no stage operation {}",
            operation.0
        )));
    }
    Ok(())
}

/// A file stored as received with no parser: `stage` found no parser for it,
/// or several.
#[derive(Clone, Debug)]
pub struct Unparsed {
    pub id: MaterialId,
    /// The binding name the input is filed under.
    pub binding: String,
    pub input: InputRef,
    pub observation: ObservationFacts,
    /// `blake3:<hex>` of the bytes, already appended to the content store.
    pub blob: String,
    pub length: u64,
    /// Where the bytes came from, and the candidate parsers under
    /// `candidates`.
    pub manifest: Manifest,
}

/// Register an unparsed material in the caller's transaction, which also
/// commits the stored bytes' extents.
pub async fn register_unparsed(conn: &mut PgConnection, unparsed: &Unparsed) -> Result<()> {
    let (input, revision) = crate::admission::observed(
        conn,
        &unparsed.binding,
        &unparsed.input,
        &unparsed.observation,
        RevisionChange::Snapshot,
    )
    .await?;
    let length = i64::try_from(unparsed.length)
        .map_err(|_| Error::Invalid("material length beyond i64".into()))?;
    sqlx::query!(
        r#"INSERT INTO core.blobs (hash, size, status) VALUES ($1, $2, 'present')
           ON CONFLICT (hash) DO UPDATE SET status = 'present' WHERE core.blobs.status IN ('deleting', 'removed')"#,
        unparsed.blob,
        length,
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        r#"INSERT INTO raw.materials (id, input_id, revision_id, blob, length, manifest, parser, parser_version)
           VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL)"#,
        unparsed.id.0,
        input,
        revision,
        unparsed.blob,
        length,
        serde_json::to_value(&unparsed.manifest).expect("manifests serialize"),
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// One stage operation as the log holds it.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Stage {
    pub operation: OperationId,
    pub actor: String,
    pub at: sinex_model::Timestamp,
    pub scope: Value,
    /// `None` while it has not ended: running, or interrupted and waiting
    /// for `resume`.
    pub result: Option<OperationResult>,
    pub counts: Value,
    pub inventory: Value,
    pub refusals: Value,
    pub start_seq: Option<i64>,
    pub end_seq: Option<i64>,
}

struct Row {
    id: uuid::Uuid,
    actor: String,
    at: time::OffsetDateTime,
    scope: Value,
    result: Option<String>,
    counts: Value,
    progress: Value,
    refusals: Value,
    start_seq: Option<i64>,
    end_seq: Option<i64>,
}

impl TryFrom<Row> for Stage {
    type Error = Error;

    fn try_from(r: Row) -> Result<Self> {
        let result = match r.result.as_deref() {
            None => None,
            Some(text) => Some(
                [OperationResult::Ok, OperationResult::StoppedEarly]
                    .into_iter()
                    .find(|result| result.as_db() == text)
                    .ok_or_else(|| {
                        Error::Invalid(format!("operation {}: unknown result {text}", r.id))
                    })?,
            ),
        };
        Ok(Stage {
            operation: OperationId(r.id),
            actor: r.actor,
            at: sinex_model::Timestamp(r.at),
            scope: r.scope,
            result,
            counts: r.counts,
            inventory: r.progress,
            refusals: r.refusals,
            start_seq: r.start_seq,
            end_seq: r.end_seq,
        })
    }
}

/// Stage operations, newest first; with `active`, only those that have not
/// ended.
pub async fn list(conn: &mut PgConnection, active: bool) -> Result<Vec<Stage>> {
    let rows = sqlx::query_as!(
        Row,
        r#"SELECT id, actor, at, scope, result, counts, progress, refusals, start_seq, end_seq
           FROM core.operations WHERE kind = $1 AND (NOT $2 OR result IS NULL)
           ORDER BY at DESC, id DESC"#,
        KIND,
        active,
    )
    .fetch_all(conn)
    .await?;
    rows.into_iter().map(Stage::try_from).collect()
}

/// One stage operation; `None` when no stage has the ID.
pub async fn show(conn: &mut PgConnection, operation: OperationId) -> Result<Option<Stage>> {
    let row = sqlx::query_as!(
        Row,
        r#"SELECT id, actor, at, scope, result, counts, progress, refusals, start_seq, end_seq
           FROM core.operations WHERE id = $1 AND kind = $2"#,
        operation.0,
        KIND,
    )
    .fetch_optional(conn)
    .await?;
    row.map(Stage::try_from).transpose()
}

#[cfg(test)]
mod tests;
