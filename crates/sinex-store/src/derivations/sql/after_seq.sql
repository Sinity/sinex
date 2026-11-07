-- Materialize payloads for the selected IDs once, before joining page metadata.
WITH page AS MATERIALIZED (
SELECT p.id, p.seq FROM (
               SELECT q.id, q.seq, (sum(q.size) OVER (ORDER BY q.seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.commit_seq AS seq, pg_column_size(s.payload)::bigint AS size
                 FROM core.events s
                 WHERE s.commit_seq > $2 AND s.event_type = ANY($1)
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 UNION ALL
                 SELECT c.event_id AS id, c.commit_seq AS seq, pg_column_size(s.payload)::bigint AS size
                 FROM audit.archive_changes c JOIN core.events s ON s.id = c.event_id
                 WHERE c.commit_seq > $2 AND NOT c.archived AND s.event_type = ANY($1)
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 ORDER BY seq LIMIT $3
               ) q
             ) p WHERE p.before < $4::bigint
), payload AS MATERIALIZED (
  SELECT payload_event.* FROM core.events payload_event
  WHERE payload_event.id = ANY(ARRAY(SELECT id FROM page))
)
SELECT e.id AS "id!", e.event_type AS "event_type!", e.source AS "source!", e.material_id, e.anchor, e.output_index AS "output_index!", e.parents,
             e.ts_orig, e.ts_quality AS "ts_quality!", e.payload AS "payload!", e.equivalence_key, e.reading_order AS "reading_order!", e.record_hash AS "record_hash!",
             e.record_state AS "record_state!", e.class AS "class!", page.seq AS "commit_seq!", e.kind_version AS "kind_version!", e.validation AS "validation!",
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
           FROM page JOIN payload e ON e.id = page.id
           ORDER BY page.seq
