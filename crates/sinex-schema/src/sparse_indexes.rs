//! Native batch metadata bounds commit-position and record-location reads of
//! compressed events: each chunk compressed after this configuration carries
//! these sparse indexes.

// These are the current event lookup paths. Explicit sparse configuration
// suppresses TimescaleDB's automatic blooms, so the objects TimescaleDB
// configured on its own are kept beside them.
const REQUIRED: &str = r#"[{"type":"bloom","column":["material_id","record_offset"]},{"type":"bloom","column":"material_id"},{"type":"bloom","column":"record_offset"},{"type":"minmax","column":"commit_seq"}]"#;

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
