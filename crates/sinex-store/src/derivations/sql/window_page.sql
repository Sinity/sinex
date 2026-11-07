-- Materialize payloads for the selected IDs once, before joining page metadata.
WITH page AS MATERIALIZED (
SELECT p.id, p.matched FROM (
               SELECT q.id, count(*) OVER () AS matched,
                      (sum(q.size) OVER (ORDER BY q.ts_orig, q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
                 FROM core.events s
                 WHERE s.event_type = ANY($1) AND s.ts_orig IS NOT NULL
                   AND ($2::timestamptz IS NULL OR s.ts_orig >= $2)
                   AND ($3::timestamptz IS NULL OR s.ts_orig < $3)
                   AND EXISTS (SELECT 1 FROM unnest($1::text[], $4::jsonb[]) AS k(event_type, contains)
                               WHERE k.event_type = s.event_type AND s.payload @> k.contains)
                   AND ($5::timestamptz IS NULL OR (s.ts_orig, s.commit_seq) > ($5, $6::bigint))
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 ORDER BY s.ts_orig, s.commit_seq
                 LIMIT $7
               ) q
             ) p WHERE p.before < $8::bigint
), payload AS MATERIALIZED (
  SELECT payload_event.* FROM core.events payload_event
  WHERE payload_event.id = ANY(ARRAY(SELECT id FROM page))
)
SELECT e.id AS "id!", e.event_type AS "event_type!", e.source AS "source!", e.material_id, e.anchor, e.output_index AS "output_index!", e.parents,
             e.ts_orig, e.ts_quality AS "ts_quality!", e.payload AS "payload!", e.equivalence_key, e.reading_order AS "reading_order!", e.record_hash AS "record_hash!",
             e.record_state AS "record_state!", e.class AS "class!", e.commit_seq AS "commit_seq!", e.kind_version AS "kind_version!", e.validation AS "validation!",
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule, page.matched AS "matched!"
           FROM page JOIN payload e ON e.id = page.id
           ORDER BY e.ts_orig, e.commit_seq
