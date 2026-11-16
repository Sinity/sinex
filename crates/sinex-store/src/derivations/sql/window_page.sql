-- Positions only; the shared event reader fetches payloads in bounded groups.
WITH page AS MATERIALIZED (
SELECT p.id, p.commit_seq, p.event_type, p.ts_orig, p.matched FROM (
               SELECT q.id, q.commit_seq, q.event_type, q.ts_orig, count(*) OVER () AS matched,
                      (sum(q.size) OVER (ORDER BY q.ts_orig, q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.event_type, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
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
)
SELECT id AS "id!", commit_seq AS "commit_seq!", event_type AS "event_type!", ts_orig, matched AS "matched!"
FROM page
ORDER BY ts_orig, commit_seq
