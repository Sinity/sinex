-- The rows of one group of intervals the index selected. Inclusive ID and
-- time bounds and the kinds skip compressed batches by their order and
-- segment columns; the ID list keeps exact membership. The window's kinds
-- ($7, $8) and liveness are checked on the row, as for events inside the
-- window.
SELECT e.id, e.event_type, e.kind_version, e.validation, e.source, e.run_id, e.material_id,
       e.record_offset, e.output_index, e.parents, e.ts_orig, e.ts_quality, e.ts_coided,
       e.ts_persisted, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
       e.record_state, e.class, e.commit_seq,
       (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
FROM core.events e
WHERE e.id = ANY($1) AND e.id >= $2::uuid AND e.id <= $3::uuid AND e.event_type = ANY($4)
  AND e.ts_orig >= $5::timestamptz AND e.ts_orig <= $6::timestamptz
  AND EXISTS (SELECT 1 FROM unnest($7::text[], $8::jsonb[]) AS k(event_type, contains)
              WHERE k.event_type = e.event_type AND e.payload @> k.contains)
  AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
