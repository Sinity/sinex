//! Native batch metadata bounds commit-position reads of compressed events.
use crate::Error;
use sqlx::PgConnection;

// These are the current event lookup paths. Explicit sparse configuration
// suppresses TimescaleDB's automatic blooms, so retain them for fresh chunks
// as well as preserving additional native objects during an upgrade.
const REQUIRED: &str = r#"[{"type":"bloom","column":["material_id","anchor"]},{"type":"bloom","column":"material_id"},{"type":"bloom","column":"anchor"},{"type":"minmax","column":"commit_seq"}]"#;

pub(crate) fn configuration() -> String {
    format!(
        r#"DO $event_sparse_indexes$
DECLARE configured text;
BEGIN
  WITH candidates AS (
    SELECT value AS object, 0 AS priority FROM jsonb_array_elements('{REQUIRED}'::jsonb)
    UNION ALL
    SELECT value-'source', 1 FROM _timescaledb_catalog.compression_settings s
      CROSS JOIN LATERAL jsonb_array_elements(coalesce(s."index",'[]'::jsonb)) i(value)
      WHERE (s.relid='core.events'::regclass OR s.relid IN (SELECT show_chunks('core.events')))
        AND value->>'source' IS DISTINCT FROM 'orderby'
  ), normalized AS (
    SELECT object, priority, jsonb_build_object('type',object->'type','column',
      CASE WHEN jsonb_typeof(object->'column')='array' THEN
        (SELECT jsonb_agg(value ORDER BY value) FROM jsonb_array_elements(object->'column'))
      ELSE object->'column' END) AS identity FROM candidates
  ), objects AS (
    SELECT DISTINCT ON (identity) object FROM normalized ORDER BY identity,priority
  )
  SELECT string_agg(format('%s(%s)',object->>'type',
    CASE WHEN jsonb_typeof(object->'column')='array' THEN
      (SELECT string_agg(format('%I',value),',' ORDER BY ordinal)
       FROM jsonb_array_elements_text(object->'column') WITH ORDINALITY c(value,ordinal))
    ELSE format('%I',object->>'column') END),',' ORDER BY object::text)
  INTO configured FROM objects;
  EXECUTE format('ALTER TABLE core.events SET (timescaledb.compress_index=%L)',configured);
END $event_sparse_indexes$"#
    )
}

pub(crate) fn previous(after_tables: &mut Vec<String>) {
    after_tables.retain(|statement| statement != &configuration());
}

/// Logical chunk OIDs remain the authority: rebuilding commits both native
/// metadata columns and these settings. Sources and JSON array order do not
/// change the meaning of an index object.
fn pending_query() -> String {
    format!(
        r#"WITH desired AS (
      SELECT jsonb_agg(DISTINCT object) AS indexes FROM (
        SELECT value AS object FROM jsonb_array_elements('{REQUIRED}'::jsonb)
        UNION ALL SELECT value-'source' FROM _timescaledb_catalog.compression_settings h
          CROSS JOIN LATERAL jsonb_array_elements(coalesce(h."index",'[]'::jsonb)) i(value)
          WHERE h.relid='core.events'::regclass
            AND value->>'source' IS DISTINCT FROM 'orderby'
      ) objects
    ) SELECT c.relid, c.relid::regclass::text AS chunk
      FROM show_chunks('core.events') chunks(relid)
      JOIN _timescaledb_catalog.compression_settings c ON c.relid=chunks.relid
      CROSS JOIN desired
      WHERE c.compress_relid IS NOT NULL AND NOT coalesce((
        SELECT jsonb_agg(value-'source') FROM jsonb_array_elements(coalesce(c."index",'[]'::jsonb)) i(value)
        WHERE value->>'source' IS DISTINCT FROM 'orderby'
      ) @> desired.indexes,false)"#
    )
}

pub(crate) async fn pending(conn: &mut PgConnection) -> Result<bool, Error> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM ({}) pending)",
        pending_query()
    ))
    .fetch_one(conn)
    .await?)
}

/// Rebuild one compressed chunk per call. Its native transaction commits
/// before completion is checked, so a crash cannot claim unwritten metadata.
pub(crate) async fn step(conn: &mut PgConnection) -> Result<bool, Error> {
    let chunk: Option<String> = sqlx::query_scalar(&format!(
        "SELECT chunk FROM ({}) pending ORDER BY relid LIMIT 1",
        pending_query()
    ))
    .fetch_optional(&mut *conn)
    .await?;
    let Some(chunk) = chunk else { return Ok(false) };
    sqlx::query("SELECT _timescaledb_functions.rebuild_sparse_index($1::regclass)")
        .bind(&chunk)
        .execute(&mut *conn)
        .await?;
    let incomplete: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM ({}) pending WHERE relid=$1::regclass)",
        pending_query()
    ))
    .bind(&chunk)
    .fetch_one(&mut *conn)
    .await?;
    if incomplete {
        let settings: serde_json::Value = sqlx::query_scalar(
            "SELECT jsonb_build_object('hypertable',to_jsonb(h),'chunk',to_jsonb(c))
             FROM _timescaledb_catalog.compression_settings h
             JOIN _timescaledb_catalog.compression_settings c ON c.relid=$1::regclass
             WHERE h.relid='core.events'::regclass",
        )
        .bind(&chunk)
        .fetch_one(&mut *conn)
        .await?;
        return Err(sqlx::Error::Protocol(format!(
            "native sparse-index rebuild left {chunk} incomplete; settings: {settings}; inspect the native skip notice"
        )).into());
    }
    pending(conn).await
}

#[cfg(test)]
mod tests;
