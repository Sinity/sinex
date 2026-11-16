-- Positions only; the shared event reader fetches payloads in bounded groups.
WITH page AS MATERIALIZED (
SELECT p.id, p.commit_seq, p.event_type, p.ts_orig, p.value, p.matched FROM (
                   SELECT q.id, q.commit_seq, q.event_type, q.ts_orig, q.value, count(*) OVER () AS matched,
                          (sum(q.size) OVER (ORDER BY q.value, q.ts_orig, q.commit_seq) - q.size)::bigint AS before
                   FROM (
                     SELECT s.id, s.event_type, v.value COLLATE "C" AS value, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
                     FROM unnest($1::text[]) v(value)
                     JOIN core.events s ON s.payload->>$2 = v.value AND jsonb_typeof(s.payload->$2) = 'string'
                     WHERE s.event_type = ANY($3) AND s.ts_orig >= $4 AND s.ts_orig < $5
                       AND ($6::text IS NULL OR (v.value COLLATE "C", s.ts_orig, s.commit_seq) > ($6 COLLATE "C", $7::timestamptz, $8::bigint))
                       AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                     ORDER BY v.value COLLATE "C", s.ts_orig, s.commit_seq
                     LIMIT $9
                   ) q
                 ) p WHERE p.before < $10::bigint
)
SELECT id AS "id!", commit_seq AS "commit_seq!", event_type AS "event_type!", ts_orig, matched AS "matched!", value AS "value!"
FROM page
ORDER BY value, ts_orig, commit_seq
