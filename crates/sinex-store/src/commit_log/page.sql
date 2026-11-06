-- Keep the empty-page head row and fetch every selected payload once.
WITH page AS MATERIALIZED (
  SELECT head.value AS head, c.commit_seq, c.event_id
  FROM core.counters head
           LEFT JOIN LATERAL (
             SELECT p.commit_seq, p.event_id FROM (
               SELECT q.commit_seq, q.event_id,
                      (sum(q.size) OVER (ORDER BY q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT u.commit_seq, u.event_id, u.size FROM (
                   (SELECT s.commit_seq, s.id AS event_id,
                      coalesce(pg_column_size(s.payload), 0)::bigint AS size
                    FROM core.events s
                    WHERE s.commit_seq > $1 AND s.commit_seq <= head.value
                    ORDER BY s.commit_seq LIMIT $2)
                   UNION ALL
                   (SELECT l.commit_seq, NULL::uuid, 0::bigint
                    FROM core.commit_log l
                    WHERE l.commit_seq > $1 AND l.commit_seq <= head.value
                    ORDER BY l.commit_seq LIMIT $2)
                 ) u ORDER BY u.commit_seq LIMIT $2
               ) q
             ) p WHERE p.before < $3::bigint
           ) c ON true
           WHERE head.name='commit_seq'
), payload AS MATERIALIZED (
  SELECT payload_event.* FROM core.events payload_event
  WHERE payload_event.id = ANY(ARRAY(SELECT event_id FROM page))
)
SELECT page.head AS "head!", page.commit_seq AS "commit_seq?", e.id AS "id?", e.event_type AS "event_type?",
             e.source AS "source?", e.run_id AS "run_id?", e.material_id, e.anchor,
             e.output_index AS "output_index?", e.parents, e.ts_orig, e.ts_quality AS "ts_quality?",
             e.ts_coided, e.ts_persisted AS "ts_persisted?", e.payload AS "payload?",
             e.equivalence_key, e.reading_order AS "reading_order?",
             e.record_hash AS "record_hash?", e.record_state AS "record_state?", e.class AS "class?", e.kind_version AS "kind_version?", e.validation AS "validation?"
           FROM page LEFT JOIN payload e ON e.id = page.event_id
ORDER BY page.commit_seq
