WITH selected AS (SELECT DISTINCT unnest($1::uuid[]) AS id)
SELECT jsonb_build_object(
  'id', e.id, 'event_type', e.event_type, 'kind_version', e.kind_version, 'validation', e.validation, 'source', e.source,
  'when', e.ts_orig, 'time_quality', e.ts_quality, 'seen', e.ts_coided,
  'written', e.ts_persisted, 'commit_seq', e.commit_seq,
  'state', CASE WHEN a.event_id IS NULL THEN 'live' ELSE 'archived' END,
  'archived', a.archived_at, 'archive_reason', a.reason,
  'record_state', e.record_state, 'class', e.class, 'key', e.equivalence_key,
  'reading_order',e.reading_order,'record_hash',encode(e.record_hash,'hex'),
  'material', e.material_id, 'offset', e.record_offset, 'output', e.output_index,
  'parents', to_jsonb(e.parents), 'run', e.run_id, 'producer', r.name, 'host', r.host,
  'parser', m.parser, 'parser_version', m.parser_version, 'blob', m.blob,
  'citation', CASE WHEN e.material_id IS NOT NULL
    THEN 'sinex:record/' || e.material_id || '/' || e.record_offset || '/' || e.event_type || '/' || e.output_index
    ELSE 'sinex:ev/' || e.id END,
  'payload', e.payload) AS "row!"
FROM selected f CROSS JOIN LATERAL (
  SELECT * FROM core.visible_events WHERE id=f.id
  AND ($2::bigint IS NULL OR commit_seq <= $2) LIMIT 1
) e
LEFT JOIN LATERAL (
  SELECT a.event_id,a.archived_at,a.reason FROM audit.archived a WHERE a.event_id=e.id AND $2::bigint IS NULL
  UNION ALL
  SELECT e.id AS event_id,c.recorded_at,c.reason FROM (
    SELECT archived,recorded_at,reason FROM audit.archive_changes
    WHERE event_id=e.id AND commit_seq <= $2 ORDER BY commit_seq DESC LIMIT 1
  ) c WHERE $2::bigint IS NOT NULL AND c.archived
) a ON true
LEFT JOIN core.runs r ON r.id = e.run_id
LEFT JOIN raw.materials m ON m.id = e.material_id
