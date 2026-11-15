-- A scalar head bounds every branch in this statement's snapshot. Compressed
-- batches use native commit-position min/max before selected payload hydration.
WITH head AS MATERIALIZED (
  SELECT value FROM core.counters WHERE name='commit_seq'
), event_keys AS MATERIALIZED (
  SELECT e.commit_seq,e.id FROM core.events e
  WHERE e.commit_seq > $1 AND e.commit_seq <= (SELECT value FROM head)
    AND (SELECT value FROM head)>$1
  ORDER BY e.commit_seq,e.id LIMIT $2
), candidates AS MATERIALIZED (
  (SELECT k.commit_seq,k.id AS event_id,e.size FROM event_keys k
   CROSS JOIN LATERAL (SELECT coalesce(pg_column_size(e.payload),0)::bigint AS size
      FROM core.events e WHERE e.id=k.id OFFSET 0)e)
  UNION ALL
  (SELECT l.commit_seq,NULL::uuid,0::bigint FROM core.commit_log l
   WHERE l.commit_seq>$1 AND l.commit_seq <= (SELECT value FROM head)
     AND (SELECT value FROM head)>$1
   ORDER BY l.commit_seq LIMIT $2)
), selected AS MATERIALIZED (
  SELECT * FROM candidates ORDER BY commit_seq LIMIT $2
), bounded AS (
  SELECT s.*,sum(size) OVER(ORDER BY commit_seq)-size AS before FROM selected s
), page AS MATERIALIZED (
  SELECT head.value AS head,c.commit_seq,c.event_id FROM head
  LEFT JOIN (SELECT commit_seq,event_id FROM bounded WHERE before<$3::bigint)c ON true
), payload AS MATERIALIZED (
  SELECT e.* FROM page CROSS JOIN LATERAL (
    SELECT payload_event.* FROM core.events payload_event
    WHERE payload_event.id >= page.event_id AND payload_event.id <= page.event_id
      AND payload_event.id=ANY(ARRAY[page.event_id]) OFFSET 0
  )e WHERE page.event_id IS NOT NULL
)
SELECT page.head AS "head!", page.commit_seq AS "commit_seq?", e.id AS "id?", e.event_type AS "event_type?",
             e.source AS "source?", e.run_id AS "run_id?", e.material_id, e.anchor,
             e.output_index AS "output_index?", e.parents, e.ts_orig, e.ts_quality AS "ts_quality?",
             e.ts_coided, e.ts_persisted AS "ts_persisted?", e.payload AS "payload?",
             e.equivalence_key, e.reading_order AS "reading_order?",
             e.record_hash AS "record_hash?", e.record_state AS "record_state?", e.class AS "class?", e.kind_version AS "kind_version?", e.validation AS "validation?"
           FROM page LEFT JOIN payload e ON e.id = page.event_id
ORDER BY page.commit_seq
