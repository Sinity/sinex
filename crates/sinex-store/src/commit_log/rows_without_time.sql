-- The rows of events whose time is unknown: inclusive ID bounds and the kinds
-- skip compressed batches; the ID list keeps exact membership.
SELECT e.id, e.event_type, e.kind_version, e.validation, e.source, e.run_id, e.material_id,
       e.record_offset, e.output_index, e.parents, e.ts_orig, e.ts_quality, e.ts_coided,
       e.ts_persisted, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
       e.record_state, e.class
FROM core.events e
WHERE e.id = ANY($1) AND e.id >= $2::uuid AND e.id <= $3::uuid AND e.event_type = ANY($4)
  AND e.ts_orig IS NULL
