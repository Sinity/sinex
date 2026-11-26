-- The key's live members come from its own index. Their rows are then read
-- by ID: the inclusive ID bounds exclude the chunks that hold none, and the
-- producer's name, which every output of a derivation carries as its source
-- and compressed batches are segmented by, opens only that producer's
-- batches inside them. ANY keeps exact membership.
WITH live AS MATERIALIZED (
  SELECT o.event_id FROM derivation.outputs o
  WHERE o.derivation = $1 AND o.key = $2
    AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = o.event_id)
)
SELECT e.id AS "id!", e.event_type AS "event_type!", e.kind_version AS "kind_version!", e.validation AS "validation!", e.source AS "source!", e.run_id AS "run_id!", e.material_id, e.record_offset, e.output_index AS "output_index!", e.parents, e.ts_orig, e.ts_quality AS "ts_quality!", e.ts_coided, e.ts_persisted AS "ts_persisted!", e.payload AS "payload!", e.equivalence_key, e.reading_order AS "reading_order!", e.record_hash AS "record_hash!", e.record_state AS "record_state!", e.class AS "class!", e.commit_seq AS "commit_seq!", (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
FROM core.events e
WHERE e.source = $1
  AND e.id >= (SELECT event_id FROM live ORDER BY event_id LIMIT 1)
  AND e.id <= (SELECT event_id FROM live ORDER BY event_id DESC LIMIT 1)
  AND e.id = ANY(ARRAY(SELECT event_id FROM live))
ORDER BY e.ts_orig, e.commit_seq
